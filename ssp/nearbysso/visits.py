"""WP3: DiaSources, visits, candidate visits for an orbit, and matching.
See ``_contract.VISIT_DTYPE`` and docs/design/nearbysso.md.

- ``read_dia`` reads the ``DIA_COLUMNS`` of any DiaSource catalog (DP2
  ``dia_source``, PPDB, the extractor's output), sorted by visit.
- ``build_visits`` derives one ``VISIT_DTYPE`` row per visit from its sources.
- ``DiaIndex`` finds the DiaSources of a visit near predicted positions.
- ``VisitIndex`` finds the visits an orbit's coarse track may fall into.

Everything is plain NumPy arrays, so the indices are cheap to share with
fork-pool workers.
"""

from __future__ import annotations

import os
import warnings
from concurrent.futures import ThreadPoolExecutor

import astropy.units as u
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from astropy.coordinates import Latitude, Longitude
from astropy.time import Time
from cdshealpix import nested

from .. import util
from ..ephem_assist import MJD_J2000
from ._contract import DIA_COLUMNS, OBSCODE, SIGMA_MAX_ARCSEC, VISIT_DTYPE

_ARCSEC = np.pi / (180.0 * 3600.0)   # radians per arcsec

#: HEALPix order (NESTED) of the DiaSource index; a cell is ~6.4".
DIA_DEPTH = 15

# Bits of the (visit, cell) key taken by the cell: 12 * 4**15 < 2**34.
_CELL_BITS = 34
# Cell id given to DiaSources without finite coordinates; above every real
# cell, so it is never looked up.
_NO_CELL = (1 << _CELL_BITS) - 1

#: A lower bound on the width of every HEALPix cell at order 15 (the
#: smallest distance between opposite edges; cells are ~6.4" on average).
#: The width halves with each order, so at order d it is at least
#: ``_MIN_WIDTH_15 * 2**(15 - d)``. The exact value (healpix-rust's
#: ``SMALLER_EDGE2OPEDGE_DIST`` for depth 15) is 4.366"; we use 4.0" to
#: stay conservative. A check with random points, points near the poles
#: and near the corners of the base cells confirms the consequence used
#: below: no point 4" from any point p lies outside p's cell and its
#: neighbours at order 15, while 12,564 of 4M points 5" away do (so a 5"
#: search can't use the order-15 neighbours; it uses the order-14 ones).
_MIN_WIDTH_15_ARCSEC = 4.0

#: Largest gap [day] between a night and its nearest CoarseTrack sample for
#: VisitIndex.candidates to consider that night (farther nights are outside
#: the track).
MAX_SAMPLE_GAP_DAYS = 1.0

#: A margin for ``VisitIndex.candidates`` that covers the differences between
#: the coarse track and the precise prediction (see ``candidates``).
DEFAULT_CANDIDATE_MARGIN_ARCSEC = 600.0

# Rows processed at a time, to bound temporaries.
_CHUNK = 1 << 20

#: Threads used by build_visits and DiaIndex (NumPy releases the GIL in the
#: heavy parts). Everything else is single-threaded, for fork-pool workers.
DEFAULT_THREADS = min(16, os.cpu_count() or 1)


def _unit_vectors(ra_deg, dec_deg):
    """(N, 3) unit vectors of RA, Dec [deg]."""
    a, d = np.radians(ra_deg), np.radians(dec_deg)
    cd = np.cos(d)
    return np.stack([cd * np.cos(a), cd * np.sin(a), np.sin(d)], axis=-1)


def _angle(u1, u2):
    """Angle [rad] between (unit) vectors, accurate at all separations."""
    return np.arctan2(np.linalg.norm(np.cross(u1, u2), axis=-1), np.einsum("...i,...i->...", u1, u2))


def _healpix(ra_deg, dec_deg, depth):
    """NESTED cells at ``depth``; non-finite coordinates get ``_NO_CELL``."""
    ra_deg = np.asarray(ra_deg, dtype=np.float64)
    dec_deg = np.asarray(dec_deg, dtype=np.float64)
    out = np.full(ra_deg.shape, _NO_CELL, dtype=np.int64)
    good = np.isfinite(ra_deg) & np.isfinite(dec_deg) & (np.abs(dec_deg) <= 90.0)
    if good.all():
        ra_g, dec_g = ra_deg, dec_deg
    else:
        ra_g, dec_g = ra_deg[good], dec_deg[good]
    if ra_g.size:
        cells = nested.lonlat_to_healpix(Longitude(ra_g, unit=u.deg, copy=False),
                                         Latitude(dec_g, unit=u.deg, copy=False), depth, num_threads=1)
        if good.all():
            out[...] = cells
        else:
            out[good] = cells
    return out


# --------------------------------------------------------------------------
# read_dia
# --------------------------------------------------------------------------

def read_dia(path, t_lo_mjd=None, t_hi_mjd=None):
    """The ``DIA_COLUMNS`` of the DiaSources with
    ``t_lo_mjd <= midpointMjdTai < t_hi_mjd`` (either bound optional), as
    a dict of NumPy arrays sorted by (visit, diaSourceId).

    Reads only those columns, with the time filter pushed down to Parquet
    (row groups outside the range are skipped by their statistics). ``path``
    is anything ``pyarrow.dataset.dataset`` accepts (a file, a list of
    files, or a dataset directory). Rows with a null in any of the columns
    are dropped, with a warning.

    Memory: the rows are streamed, a batch at a time, into preallocated
    arrays (their count comes from a first, filter-only pass), so the peak
    is about the result (36 bytes per row) plus one batch, not the several
    times that a whole Arrow table and its conversion would take.
    """
    dset = ds.dataset(path, format="parquet")
    expr = None
    t = ds.field("midpointMjdTai")
    if t_lo_mjd is not None:
        expr = t >= float(t_lo_mjd)
    if t_hi_mjd is not None:
        e = t < float(t_hi_mjd)
        expr = e if expr is None else expr & e
    n = dset.count_rows(filter=expr)

    dtypes = {c: np.float64 for c in DIA_COLUMNS} | {"diaSourceId": np.int64, "visit": np.int64}
    dia = {c: np.empty(n, dtype=dtypes[c]) for c in DIA_COLUMNS}
    m = 0
    pool = pa.default_memory_pool()
    for i, batch in enumerate(dset.to_batches(columns=DIA_COLUMNS, filter=expr, batch_size=1 << 20)):
        if i % 8 == 7:
            pool.release_unused()   # else the allocator holds on to the batches read
        if not batch.num_rows:
            continue
        if any(batch.column(c).null_count for c in DIA_COLUMNS):
            valid = pc.is_valid(batch.column(DIA_COLUMNS[0]))
            for c in DIA_COLUMNS[1:]:
                valid = pc.and_(valid, pc.is_valid(batch.column(c)))
            batch = batch.filter(valid)
        k = batch.num_rows
        if m + k > n:
            raise RuntimeError("read_dia: the dataset changed while being read")
        for c in DIA_COLUMNS:
            dia[c][m:m + k] = batch.column(c).to_numpy(zero_copy_only=False)
        m += k
    if m < n:
        warnings.warn(f"read_dia: dropped {n - m} DiaSources with nulls in {DIA_COLUMNS}", stacklevel=2)
        dia = {c: dia[c][:m].copy() for c in DIA_COLUMNS}
    batch = None
    pool.release_unused()

    visit, sid = dia["visit"], dia["diaSourceId"]
    if visit.size > 1:
        dv = np.diff(visit)
        in_order = ((dv > 0) | ((dv == 0) & (np.diff(sid) > 0))).all()
        del dv
        if not in_order:
            order = np.lexsort((sid, visit))
            for c in DIA_COLUMNS:
                dia[c] = dia[c][order]
    return dia


# --------------------------------------------------------------------------
# build_visits
# --------------------------------------------------------------------------

def _visit_blocks(starts, ends, chunk=None):
    """Yield (v0, v1): runs of whole visits of about ``chunk`` rows."""
    chunk = _CHUNK if chunk is None else chunk
    nv = starts.size
    v0 = 0
    while v0 < nv:
        v1 = int(np.searchsorted(ends, starts[v0] + chunk, side="right"))
        v1 = max(v1, v0 + 1)
        yield v0, v1
        v0 = v1


def _run_blocks(fn, starts, ends, threads):
    """fn(v0, v1) over the blocks of whole visits, in ``threads`` threads."""
    blocks = list(_visit_blocks(starts, ends))
    if threads is None:
        threads = DEFAULT_THREADS
    if threads <= 1 or len(blocks) <= 1:
        for b in blocks:
            fn(*b)
        return
    with ThreadPoolExecutor(min(threads, len(blocks))) as ex:
        for f in [ex.submit(fn, *b) for b in blocks]:
            f.result()


def build_visits(dia, threads=None):
    """One ``VISIT_DTYPE`` row per visit of the visit-sorted ``dia`` (as
    returned by ``read_dia``), sorted by visit.

    - ``t_tai_mjd``: the visit's ``midpointMjdTai``, which all its sources
      share (to 1e-6 d); if they don't, the median, with a warning.
    - ``t``: ASSIST time, ``JD_TDB - 2451545.0``, computed as
      ``ssp.ephem_assist`` does (``Time(...).tdb.mjd - MJD_J2000``).
    - ``center``: the normalized mean unit vector of its sources; ``radius``
      the largest angle [rad] from it to one of them.
    - ``obs_pos`` [AU], ``obs_vel`` [km/s]: X05 barycentric ICRF, from
      ``util.observatory_barycentric_posvel`` (as SSSource).
    - ``night``: ``visit // 100000`` (day_obs).
    """
    visit = np.asarray(dia["visit"])
    n = visit.size
    if n == 0:
        return np.zeros(0, dtype=VISIT_DTYPE)
    if (np.diff(visit) < 0).any():
        raise ValueError("build_visits: dia must be sorted by visit (see read_dia)")

    starts = np.flatnonzero(np.r_[True, visit[1:] != visit[:-1]])
    ends = np.r_[starts[1:], n]
    nv = starts.size

    out = np.zeros(nv, dtype=VISIT_DTYPE)
    out["visit"] = visit[starts]
    out["night"] = out["visit"] // 100000
    out["dia_start"] = starts
    out["dia_end"] = ends

    # time
    tsrc = np.asarray(dia["midpointMjdTai"], dtype=np.float64)
    tmin = np.minimum.reduceat(tsrc, starts)
    tmax = np.maximum.reduceat(tsrc, starts)
    t_tai = tmin.copy()
    bad = np.flatnonzero(~(tmax - tmin <= 1e-6))
    if bad.size:
        for v in bad:
            t_tai[v] = np.nanmedian(tsrc[starts[v]:ends[v]])
        warnings.warn(f"build_visits: {bad.size} visit(s) whose sources' midpointMjdTai differ by more "
                      f"than 1e-6 d (e.g. visit {out['visit'][bad[0]]}); using the median", stacklevel=2)
    out["t_tai_mjd"] = t_tai

    # centre and radius, a block of whole visits at a time
    ra = np.asarray(dia["ra"], dtype=np.float64)
    dec = np.asarray(dia["dec"], dtype=np.float64)

    def block(v0, v1):
        r0, r1 = starts[v0], ends[v1 - 1]
        uv = _unit_vectors(ra[r0:r1], dec[r0:r1])
        s = starts[v0:v1] - r0
        c = np.add.reduceat(uv, s, axis=0)
        c /= np.linalg.norm(c, axis=1)[:, None]
        cnt = ends[v0:v1] - starts[v0:v1]
        uv -= np.repeat(c, cnt, axis=0)
        chord = np.maximum.reduceat(np.linalg.norm(uv, axis=1), s)
        out["center"][v0:v1] = c
        # the chord is accurate at small angles, where arccos(dot) isn't;
        # the 1e-9 rad (0.2 mas) covers rounding
        out["radius"][v0:v1] = 2.0 * np.arcsin(np.minimum(chord / 2.0, 1.0)) + 1e-9
    _run_blocks(block, starts, ends, threads)

    # time scales and the observer
    tt = Time(t_tai, format="mjd", scale="tai")
    out["t"] = tt.tdb.mjd - MJD_J2000
    r_obs, v_obs = util.observatory_barycentric_posvel(OBSCODE, tt)
    out["obs_pos"] = np.asarray(r_obs.to_value(u.au)).reshape(3, nv).T
    out["obs_vel"] = np.asarray(v_obs.to_value(u.km / u.s)).reshape(3, nv).T
    return out


# --------------------------------------------------------------------------
# DiaIndex
# --------------------------------------------------------------------------

class DiaIndex:
    """Per-visit HEALPix index of the DiaSources.

    Each DiaSource gets the key ``(visit index << 34) | cell``, with its
    order-15 NESTED cell; the keys are sorted (``self.key``) along with
    the row of each (``self.row``, into the visit-sorted dia arrays). That
    is 12-16 bytes per DiaSource, in two flat arrays, cheap to fork.

    Because the cells are NESTED, the order-15 cells inside an order-d cell
    are one contiguous range of ids, so a lookup at any coarser order is a
    ``searchsorted`` range too (see ``match``).
    """

    def __init__(self, dia, visits, threads=None):
        ra = np.asarray(dia["ra"], dtype=np.float64)
        dec = np.asarray(dia["dec"], dtype=np.float64)
        n = ra.size
        starts, ends = visits["dia_start"], visits["dia_end"]
        if n and (starts[0] != 0 or ends[-1] != n or (starts[1:] != ends[:-1]).any()):
            raise ValueError("DiaIndex: visits don't partition the dia arrays (see build_visits)")
        self.ra, self.dec = ra, dec
        self.nvisits = len(visits)

        # the keys' leading bits are the visit index, so sorting each block
        # of whole visits on its own sorts them all
        self.key = np.empty(n, dtype=np.int64)
        self.row = np.empty(n, dtype=np.int32 if n < 2**31 else np.int64)

        def block(v0, v1):
            r0, r1 = starts[v0], ends[v1 - 1]
            vidx = np.repeat(np.arange(v0, v1, dtype=np.int64), ends[v0:v1] - starts[v0:v1])
            key = _healpix(ra[r0:r1], dec[r0:r1], DIA_DEPTH)
            key |= vidx << _CELL_BITS
            order = np.argsort(key, kind="stable")
            self.key[r0:r1] = key[order]
            order += r0
            self.row[r0:r1] = order
        if len(visits):
            _run_blocks(block, starts, ends, threads)

    @staticmethod
    def query_depth(radius_arcsec):
        """The finest order whose cells are all wider than ``radius_arcsec``,
        so that the cell of a point and its (up to 8) neighbours contain
        every point within the radius (14 for 5")."""
        if not radius_arcsec > 0:
            raise ValueError("radius_arcsec must be positive")
        d = DIA_DEPTH
        while d > 0 and _MIN_WIDTH_15_ARCSEC * 2.0 ** (DIA_DEPTH - d) < radius_arcsec:
            d -= 1
        if _MIN_WIDTH_15_ARCSEC * 2.0 ** (DIA_DEPTH - d) < radius_arcsec:
            raise ValueError(f"radius_arcsec {radius_arcsec} too large")
        return d

    def match(self, visit_idx, ra, dec, radius_arcsec):
        """Every DiaSource of visit ``visit_idx[k]`` within ``radius_arcsec``
        of (``ra[k]``, ``dec[k]``) [deg], as ``(pred, dia_row, sep_arcsec)``,
        sorted by (pred, dia_row). ``sep_arcsec`` is
        ``util.sky_separation_arcsec(ra, dec, dia ra, dia dec)``.

        Completeness: at order d = ``query_depth(radius)`` every cell is wider
        than the radius, so every point within it of a prediction lies in the
        prediction's order-d cell or one of that cell's neighbours
        (``cdshealpix.nested.neighbours``: 8, or 7 at the corners of the base
        cells; this holds at the poles too, and cells have no RA seam). The
        DiaSources of those (up to 9) cells are the order-15 key ranges
        ``[(v << 34) | (c << 2(15-d)), ... + 4**(15-d))``, which are then
        filtered by the exact separation. Predictions with non-finite
        coordinates or a visit index out of range match nothing.
        """
        visit_idx = np.asarray(visit_idx, dtype=np.int64).ravel()
        ra = np.asarray(ra, dtype=np.float64).ravel()
        dec = np.asarray(dec, dtype=np.float64).ravel()
        if not (visit_idx.size == ra.size == dec.size):
            raise ValueError("visit_idx, ra and dec must have the same length")
        dq = self.query_depth(radius_arcsec)
        shift = 2 * (DIA_DEPTH - dq)

        preds, rows, seps = [], [], []
        step = 1 << 20
        for k0 in range(0, ra.size, step):
            k = np.arange(k0, min(k0 + step, ra.size), dtype=np.int64)
            vi, r, d = visit_idx[k], ra[k], dec[k]
            good = (np.isfinite(r) & np.isfinite(d) & (np.abs(d) <= 90.0)
                    & (vi >= 0) & (vi < self.nvisits))
            if not good.all():
                k, vi, r, d = k[good], vi[good], r[good], d[good]
            if not k.size:
                continue
            cell = _healpix(r, d, dq)
            nb = nested.neighbours(cell, dq, num_threads=1)   # (M, 9), -1 if absent
            j = np.nonzero(nb >= 0)
            pk = j[0]                                         # local prediction of each cell
            lo = (vi[pk] << _CELL_BITS) | (nb[j].astype(np.int64) << shift)
            i0 = np.searchsorted(self.key, lo, side="left")
            i1 = np.searchsorted(self.key, lo + (1 << shift), side="left")
            cnt = i1 - i0
            tot = int(cnt.sum())
            if not tot:
                continue
            # flatten the ranges [i0, i1)
            pk = np.repeat(pk, cnt)
            pos = np.arange(tot, dtype=np.int64) - np.repeat(np.cumsum(cnt) - cnt - i0, cnt)
            drow = self.row[pos].astype(np.int64)
            sep = util.sky_separation_arcsec(r[pk], d[pk], self.ra[drow], self.dec[drow])
            keep = sep <= radius_arcsec
            preds.append(k[pk[keep]])
            rows.append(drow[keep])
            seps.append(sep[keep])

        if not preds:
            return np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0, np.float64)
        pred, dia_row, sep = np.concatenate(preds), np.concatenate(rows), np.concatenate(seps)
        order = np.lexsort((dia_row, pred))
        return pred[order], dia_row[order], sep[order]


# --------------------------------------------------------------------------
# VisitIndex
# --------------------------------------------------------------------------

class VisitIndex:
    """Per-night spatial index of the visits, for ``candidates``.

    Each visit is registered in the HEALPix cells (order ``depth``, ~1.8 deg
    at the default 5) that its disk of radius ``radius + pad_deg`` about its
    centre overlaps, under the key ``night index * ncells + cell``. A track
    position then finds, with one ``searchsorted``, every visit of the night
    whose centre is within ``radius + pad`` of it. Nights where the object
    may move farther than the pad allows are tested against all of their
    visits instead.

    Also provides, per night (``nights``, sorted), ``night_tmin``,
    ``night_tmax`` and ``night_t`` (the midpoint): ASSIST times, of which
    ``night_t`` is the natural CoarseTrack sample time (it minimizes the
    extrapolation interval).
    """

    def __init__(self, visits, pad_deg=2.0, depth=5):
        self.visits = visits
        nv = len(visits)
        night = visits["night"]
        if nv and (np.diff(night) < 0).any():
            raise ValueError("VisitIndex: visits must be sorted by visit (see build_visits)")
        self.center = np.ascontiguousarray(visits["center"])
        self.radius = np.ascontiguousarray(visits["radius"])
        self.t = np.ascontiguousarray(visits["t"])

        ns = np.flatnonzero(np.r_[True, night[1:] != night[:-1]]) if nv else np.zeros(0, np.int64)
        self.night_start = ns
        self.night_end = np.r_[ns[1:], nv].astype(np.int64)
        self.nights = night[ns]
        self.night_of_visit = np.repeat(np.arange(ns.size), self.night_end - ns)
        self.night_tmin = np.minimum.reduceat(self.t, ns) if nv else np.zeros(0)
        self.night_tmax = np.maximum.reduceat(self.t, ns) if nv else np.zeros(0)
        self.night_t = 0.5 * (self.night_tmin + self.night_tmax)

        self.depth = depth
        self.ncells = 12 * 4**depth
        # cone_search reports every cell the disk overlaps; the usable pad is
        # a bit smaller than the registered one, against edge effects
        self.pad_rad = np.radians(pad_deg) * 0.98
        keys, vids = [], []
        c = self.center
        lon = np.degrees(np.arctan2(c[:, 1], c[:, 0])) % 360.0
        lat = np.degrees(np.arcsin(np.clip(c[:, 2], -1.0, 1.0)))
        for v in range(nv):
            rad = min(np.degrees(self.radius[v]) + pad_deg, 179.0)
            cells, cdepth, _ = nested.cone_search(Longitude(lon[v], u.deg), Latitude(lat[v], u.deg),
                                                  rad * u.deg, depth, flat=True)
            assert (cdepth == depth).all()
            keys.append(self.night_of_visit[v] * self.ncells + cells.astype(np.int64))
            vids.append(np.full(cells.size, v, dtype=np.int64))
        key = np.concatenate(keys) if keys else np.zeros(0, np.int64)
        vid = np.concatenate(vids) if vids else np.zeros(0, np.int64)
        order = np.lexsort((vid, key))
        self.reg_key, self.reg_visit = key[order], vid[order]

    def candidates(self, track, margin_arcsec):
        """Sorted indices (into ``visits``) of the visits the orbit of the
        CoarseTrack ``track`` may fall into.

        Each night of the index takes the track sample nearest in time to
        its ``night_t`` (nights more than ``MAX_SAMPLE_GAP_DAYS`` from every
        sample are skipped). If that sample k has ``ok`` and
        ``sigma_major <= SIGMA_MAX_ARCSEC``, a visit v of the night is a
        candidate if

            angle(center_v, p_k(t_v)) <= radius_v + w_k |t_v - t_k| + margin,

        where w_k = hypot(rate_ra, rate_dec) and p_k(t) is the sample position
        moved along the great circle of its rate by w_k (t - t_k).

        Choosing the margin. A visit contains a prediction q (the precise
        pass's, which the match compares with DiaSources) only if
        angle(center, q) <= radius + r_match; and angle(center, p_k(t_v)) <=
        angle(center, q) + |q - p_k(t_v)|. So the test never misses a visit if
        margin >= r_match + |q - p_k(t_v)| - w_k |dt|, i.e. if the margin
        covers what the w_k |dt| term doesn't of the difference between the
        linear extrapolation and the precise prediction:

        - the match radius, 5";
        - light time: the coarse track is geometric, the prediction
          light-time corrected, which moves it by (rate x light time) =
          v_perp / c, independently of the distance; <= 55" for any bound
          orbit near 1 AU (v_perp <= ~80 km/s);
        - the curvature of the (geocentric) path within |dt|: the object's
          motion departs from the linear extrapolation by less than its
          extrapolated displacement w |dt| unless its rate more than doubles
          within |dt|, which takes a passage within a few Earth-Moon
          distances in that time; the w_k |dt| term covers the rest;
        - diurnal parallax, A = R_earth / distance: its rate is part of w_k,
          but its curvature over |dt| is not, and it can cancel the geocentric
          rate. With w_d = 2 pi / day, it adds at most
          A (w_d |dt| + (w_d dt)^2 / 2): 3.7 A for |dt| <= 0.3 d (samples at
          ``night_t``, Rubin nights being < 12 h), 8 A for |dt| <= 0.5 d.

        ``DEFAULT_CANDIDATE_MARGIN_ARCSEC`` = 600" thus guarantees no miss for
        objects farther than 0.06 AU when sampled at ``night_t`` (0.13 AU if
        |dt| reaches 12 h). Closer ones are covered in practice by their
        large rates (at < 0.1 AU a relative velocity of just 5 km/s is
        >1.5 deg/day, i.e. a w |dt| slack of ~0.5 deg), but not guaranteed:
        WP5's brute-force check is the test of that. The margin is cheap:
        600" adds ~20% to the area searched around a 1.75-deg-radius field.
        """
        nn = self.nights.size
        empty = np.zeros(0, dtype=np.int64)
        tk = np.asarray(track.t, dtype=np.float64)
        if not nn or not tk.size:
            return empty
        margin = margin_arcsec * _ARCSEC

        # the nearest sample to each night
        srt = np.argsort(tk, kind="stable")
        ts = tk[srt]
        j = np.clip(np.searchsorted(ts, self.night_t), 1, max(ts.size - 1, 1))
        jl = j - 1
        jr = np.minimum(j, ts.size - 1)
        use_r = np.abs(ts[jr] - self.night_t) < np.abs(ts[jl] - self.night_t)
        k_n = srt[np.where(use_r, jr, jl)]
        gap = np.abs(tk[k_n] - self.night_t)

        ra = np.asarray(track.ra, dtype=np.float64)
        dec = np.asarray(track.dec, dtype=np.float64)
        rra = np.asarray(track.rate_ra, dtype=np.float64)
        rdec = np.asarray(track.rate_dec, dtype=np.float64)
        elig = (np.asarray(track.ok, dtype=bool)
                & (np.asarray(track.sigma_major, dtype=np.float64) <= SIGMA_MAX_ARCSEC)
                & np.isfinite(ra) & np.isfinite(dec) & np.isfinite(rra) & np.isfinite(rdec))
        nights = np.flatnonzero((gap <= MAX_SAMPLE_GAP_DAYS) & elig[k_n])
        if not nights.size:
            return empty
        k = k_n[nights]
        w = np.radians(np.hypot(rra[k], rdec[k]))                     # rad/day
        dt_max = np.maximum(np.abs(self.night_tmax[nights] - tk[k]), np.abs(self.night_tmin[nights] - tk[k]))
        # the centre of a candidate is within radius + 2 w dt + margin of the
        # sample itself; the cell lookup finds those within radius + pad
        fall = 2.0 * w * dt_max + margin > self.pad_rad

        pair_n, pair_v = [], []
        # lookup
        lk = ~fall
        if lk.any():
            nl = nights[lk]
            cell = _healpix(ra[k[lk]], dec[k[lk]], self.depth)
            key = nl * self.ncells + cell
            i0 = np.searchsorted(self.reg_key, key, side="left")
            i1 = np.searchsorted(self.reg_key, key, side="right")
            cnt = i1 - i0
            tot = int(cnt.sum())
            if tot:
                pos = np.arange(tot, dtype=np.int64) - np.repeat(np.cumsum(cnt) - cnt - i0, cnt)
                pair_n.append(np.repeat(np.flatnonzero(lk), cnt))
                pair_v.append(self.reg_visit[pos])
        # fallback: every visit of the night
        if fall.any():
            fi = np.flatnonzero(fall)
            s, e = self.night_start[nights[fi]], self.night_end[nights[fi]]
            cnt = e - s
            tot = int(cnt.sum())
            pos = np.arange(tot, dtype=np.int64) - np.repeat(np.cumsum(cnt) - cnt - s, cnt)
            pair_n.append(np.repeat(fi, cnt))
            pair_v.append(pos)
        if not pair_n:
            return empty
        pn, pv = np.concatenate(pair_n), np.concatenate(pair_v)

        # the exact test, per (night's sample, visit)
        kk = k[pn]
        a, d = np.radians(ra[kk]), np.radians(dec[kk])
        sa, ca, sd, cd = np.sin(a), np.cos(a), np.sin(d), np.cos(d)
        p = np.stack([cd * ca, cd * sa, sd], axis=-1)
        e_ra = np.stack([-sa, ca, np.zeros_like(a)], axis=-1)
        e_dec = np.stack([-sd * ca, -sd * sa, cd], axis=-1)
        vel = np.radians(rra[kk])[:, None] * e_ra + np.radians(rdec[kk])[:, None] * e_dec   # rad/day
        wk = w[pn]
        dt = self.t[pv] - tk[kk]
        ang = wk * dt
        with np.errstate(invalid="ignore", divide="ignore"):
            dirn = np.where(wk[:, None] > 0, vel / wk[:, None], 0.0)
        p_ext = p * np.cos(ang)[:, None] + dirn * np.sin(ang)[:, None]
        sep = _angle(self.center[pv], p_ext)
        keep = sep <= self.radius[pv] + wk * np.abs(dt) + margin
        return np.unique(pv[keep])
