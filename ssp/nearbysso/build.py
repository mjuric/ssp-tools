"""WP4: build ``nearbysso.parquet`` (docs/design/nearbysso.md).

The pipeline, per time slice of the DiaSources:

- **Parent:** read the slice (``visits.read_dia``), derive its visits
  (``build_visits``), index the DiaSources (``DiaIndex``) and the visits
  (``VisitIndex``), and compute the observer at the coarse sample times.
- **Workers** (``util.run_chunks``, forked; inputs shared as module
  globals), per orbit: ``propagate.coarse``, ``VisitIndex.candidates``, the
  precise ``compute_ephemerides_one`` at the candidate visits, the error
  ellipse there (``propagate.ellipse_at``), the sigma gate and
  ``DiaIndex.match``. Every match comes back as a row of ``_MATCH_DTYPE``.
- **Reduce:** the nearest match per diaSourceId (ties by designation),
  ``ssObjectId`` from an SSObject table, and the Parquet file plus a JSON
  run report next to it.

Slices never split a night (``night = visit // 100000``, i.e. day_obs):
they are blocks of ``slice_days`` consecutive day_obs dates, read with time
bounds that cover their nights and then cut on the night itself. So every
DiaSource belongs to exactly one slice, and the per-slice reduction is
exact.

The coarse samples are three per night: its ``VisitIndex.night_t`` (the
midpoint of its visits, where ``candidates`` looks) and ``night_t -+ h``
with h = max(half the night's span of visit times, ``MIN_HALF_SPAN_DAYS``),
so that every visit time is bracketed (``ellipse_at`` clamps outside the
sampled span) by samples of its own night. Everything a visit's result
depends on -- the sample ``candidates`` uses for its night, the rate and
distance changes it estimates from that sample's neighbours, and the two
samples ``ellipse_at`` interpolates between -- is then a function of that
night's visits alone, so the candidates, the sigma gate and the ellipses
don't depend on how the nights are sliced. (Sampling only at the nights'
``night_t``, plus a trailing and a leading one, would make the first and
last nights of each slice differ from the same nights inside a longer one.)

The output is byte-identical for any ``workers`` and chunking. Across
``slice_days`` the rows are the same, but ``ephRa``/``ephDec`` can differ
at the ~1e-11 deg level: the precise pass integrates each orbit through the
candidate times of its slice, and the integrator's path depends on the set
of times asked for (``ephem_assist._propagate_one`` visits them in
ascending order, even when integrating backwards from the epoch). SSSource,
which asks for an object's times all at once, is subject to the same noise.
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import resource
import sys
import time
from collections import Counter

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from astropy.time import Time
import astropy.units as u

from .. import util
from ..ephem_assist import MJD_J2000, compute_ephemerides_one, open_ephem
from ..photfit import hg_V_mag
from . import orbits as _orbits
from . import propagate, visits as _visits
from ._contract import MATCH_RADIUS_ARCSEC, NEARBYSSO_DTYPE, OBSCODE, ORBIT_DTYPE, SIGMA_MAX_ARCSEC

#: The smallest spacing [day] of a night's three coarse samples (for a night
#: with a single visit, or visits close in time; see the module docstring).
MIN_HALF_SPAN_DAYS = 1.0 / 24.0

#: Exceptions kept per type, for the run report.
_N_EXAMPLES = 5

#: An eligible prediction: an orbit at a candidate visit.
_PRED_DTYPE = np.dtype([
    ("visit", "i8"),         # into the slice's visits
    ("orbit", "i8"),         # into the (designation-sorted) orbits
    ("ra", "f8"), ("dec", "f8"),
    ("vmag", "f4"), ("rate_ra", "f4"), ("rate_dec", "f4"),
    ("ra_err", "f4"), ("dec_err", "f4"), ("ra_dec_cov", "f4"),
])

#: A worker's output: one row per (prediction, DiaSource) match.
_MATCH_DTYPE = np.dtype([
    ("dia_row", "i8"),       # into the slice's visit-sorted dia arrays
    ("sep", "f8"),           # ephOffset [arcsec]
] + [(f, _PRED_DTYPE[f]) for f in _PRED_DTYPE.names[1:]])

#: Predictions collected (from several orbits) per DiaIndex.match call.
_MATCH_BATCH = 20000

#: Per-orbit counters a worker returns (summed over chunks and slices, so
#: "orbits", "with_candidates" and the "coarse_*" ones count orbit-slices;
#: the run report's "failed_orbits" counts orbits).
_COUNTS = ("orbits", "coarse_partial_fail", "coarse_all_fail", "with_candidates", "candidate_visits",
           "precise_evals", "eligible_evals", "sigma_rejected", "matches", "nights_skipped",
           "step_cap_stops")
_STAGES = ("coarse", "candidates", "precise", "ellipse", "match")


# --------------------------------------------------------------------------
# Slicing
# --------------------------------------------------------------------------

def night_ranges(path):
    """Per night (``visit // 100000``) of the DiaSources in ``path``:
    ``(nights, tmin, tmax, n)`` (sorted by night; TAI MJD). One streaming
    pass over the two columns, so its memory doesn't grow with the input."""
    dset = ds.dataset(path, format="parquet")
    parts = []
    for batch in dset.to_batches(columns=["visit", "midpointMjdTai"], batch_size=1 << 22):
        if batch.num_rows == 0:
            continue
        if batch.column(0).null_count or batch.column(1).null_count:
            batch = batch.filter(pc.and_(pc.is_valid(batch.column(0)), pc.is_valid(batch.column(1))))
        night = batch.column(0).to_numpy(zero_copy_only=False) // 100000
        t = batch.column(1).to_numpy(zero_copy_only=False)
        # runs of equal nights (one per night for visit-sorted input)
        s = np.flatnonzero(np.r_[True, night[1:] != night[:-1]])
        parts.append((night[s], np.fmin.reduceat(t, s), np.fmax.reduceat(t, s),
                      np.diff(np.r_[s, night.size])))
    if not parts:
        return np.zeros(0, np.int64), np.zeros(0), np.zeros(0), np.zeros(0, np.int64)
    n_, lo, hi, cnt = (np.concatenate(p) for p in zip(*parts))
    nights, inv = np.unique(n_, return_inverse=True)
    tmin = np.full(nights.size, np.inf)
    tmax = np.full(nights.size, -np.inf)
    n = np.zeros(nights.size, np.int64)
    np.fmin.at(tmin, inv, lo)
    np.fmax.at(tmax, inv, hi)
    np.add.at(n, inv, cnt)
    return nights, tmin, tmax, n


def _night_day_number(nights):
    """Days since the epoch of each night id, read as a YYYYMMDD day_obs
    (or the ids themselves, if they aren't all valid dates)."""
    try:
        s = [f"{d // 10000:04d}-{d // 100 % 100:02d}-{d % 100:02d}" for d in nights.tolist()]
        return np.array(s, dtype="datetime64[D]").astype(np.int64)
    except ValueError:
        return np.asarray(nights, dtype=np.int64)


def plan_slices(nights, tmin, tmax, slice_days, n=None):
    """Group sorted nights into slices of ``slice_days`` consecutive
    day_obs dates, counted from the first night. Returns a list of dicts
    with the slice's first and last night, its read bounds ``[t_lo,
    t_hi)`` (TAI MJD), which cover all its nights' sources (the slice then
    keeps only the rows of its own nights; see ``_read_slice``), and the
    number of its rows (from ``n``, per night; for the dropped-row count)."""
    if slice_days <= 0:
        raise ValueError("slice_days must be positive")
    if not len(nights):
        return []
    day = _night_day_number(nights)
    sid = (day - day[0]) // int(np.ceil(slice_days))
    out = []
    for s in np.unique(sid):
        k = np.flatnonzero(sid == s)
        out.append(dict(night_lo=int(nights[k[0]]), night_hi=int(nights[k[-1]]),
                        t_lo=float(tmin[k].min()), t_hi=float(np.nextafter(tmax[k].max(), np.inf)),
                        n=None if n is None else int(np.sum(n[k]))))
    return out


def _read_slice(path, sl):
    """The slice's DiaSources: read by its time bounds, then cut to its own
    nights (a no-op unless nights overlap in time)."""
    dia = _visits.read_dia(path, sl["t_lo"], sl["t_hi"])
    night = dia["visit"] // 100000
    keep = (night >= sl["night_lo"]) & (night <= sl["night_hi"])
    if not keep.all():
        dia = {c: v[keep] for c, v in dia.items()}
    return dia


def sample_times(vindex):
    """The coarse sample times of a slice (ASSIST, TDB): per night,
    ``night_t - h``, ``night_t``, ``night_t + h`` (see the module
    docstring)."""
    nt = np.asarray(vindex.night_t, dtype=np.float64)
    h = np.maximum(0.5 * (vindex.night_tmax - vindex.night_tmin), MIN_HALF_SPAN_DAYS)
    return np.stack([nt - h, nt, nt + h], axis=1).ravel()


def observer_at(t_assist):
    """X05 barycentric ICRF position [AU], (K, 3), at ASSIST times (TDB)."""
    tt = Time(np.asarray(t_assist) + MJD_J2000, format="mjd", scale="tdb")
    r, _ = util.observatory_barycentric_posvel(OBSCODE, tt)
    return np.asarray(r.to_value(u.au)).reshape(3, -1).T.copy()


# --------------------------------------------------------------------------
# The per-orbit pass
# --------------------------------------------------------------------------

# Filled by the parent before the workers fork (and for the serial run), so
# that the large arrays are inherited, never pickled.
_W = {}
_EPHEM = None   # one ASSIST ephemeris per worker process, opened lazily


def process_orbit(i, w, ephem, stage_t):
    """Orbit ``i`` of ``w["orbits"]`` in the current slice, up to the
    match: its eligible predictions (``_PRED_DTYPE``, or None) and its
    counters."""
    orbit = w["orbits"][i]
    c = dict.fromkeys(_COUNTS, 0)
    c["orbits"] = 1
    t0 = time.perf_counter()
    track = propagate.coarse(orbit, w["ts"], w["obs_ts"], ephem)
    t1 = time.perf_counter()
    stage_t["coarse"] += t1 - t0
    if not track.ok.all():
        c["coarse_all_fail" if not track.ok.any() else "coarse_partial_fail"] = 1
    vindex = w["vindex"]
    cand = vindex.candidates(track, _visits.DEFAULT_CANDIDATE_MARGIN_ARCSEC)
    c["nights_skipped"] = int(vindex.last_skipped)
    t2 = time.perf_counter()
    stage_t["candidates"] += t2 - t1
    if not cand.size:
        return None, c
    c["with_candidates"] = 1
    c["candidate_visits"] = c["precise_evals"] = int(cand.size)

    v = w["visits"][cand]
    e = compute_ephemerides_one(str(orbit["designation"]), w["times"][cand], None, ephem, row=orbit,
                                obs_pos=v["obs_pos"].T, obs_vel=v["obs_vel"].T)
    t3 = time.perf_counter()
    stage_t["precise"] += t3 - t2

    ra_err, dec_err, ra_dec_cov, smaj = propagate.ellipse_at(track, v["t"], topo_pos=e.topo_pos.T)
    k = np.flatnonzero(np.isfinite(smaj) & (smaj <= SIGMA_MAX_ARCSEC))
    c["eligible_evals"] = int(k.size)
    c["sigma_rejected"] = int(cand.size - k.size)
    if not k.size:
        stage_t["ellipse"] += time.perf_counter() - t3
        return None, c

    # V exactly as SSSource computes it: from its float32 helio/topo
    # columns (and its float64 phase angle)
    helio = e.helio_pos[:, k].astype(np.float32)
    topo = e.topo_pos[:, k].astype(np.float32)
    helio_r = np.sqrt(helio[0] ** 2 + helio[1] ** 2 + helio[2] ** 2)
    topo_r = np.sqrt(topo[0] ** 2 + topo[1] ** 2 + topo[2] ** 2)

    p = np.empty(k.size, dtype=_PRED_DTYPE)
    p["visit"] = cand[k]
    p["orbit"] = i
    # (as SSSource: RA wrapped to [0, 360), and the separation measured
    # from the prediction)
    p["ra"] = util.wrap_ra_deg(e.ra_deg[k])
    p["dec"] = e.dec_deg[k]
    p["vmag"] = hg_V_mag(e.H, e.G, helio_r, topo_r, e.phase_angle[k])
    p["rate_ra"] = e.mu_lon[k]
    p["rate_dec"] = e.mu_lat[k]
    p["ra_err"] = ra_err[k]
    p["dec_err"] = dec_err[k]
    p["ra_dec_cov"] = ra_dec_cov[k]
    stage_t["ellipse"] += time.perf_counter() - t3
    return p, c


def _match(preds, w, stage_t):
    """The DiaSources within the radius of the predictions of several
    orbits, in one ``DiaIndex.match`` call (it has a fixed cost per call):
    ``_MATCH_DTYPE`` rows, in the order of ``preds``."""
    t0 = time.perf_counter()
    p = np.concatenate(preds)
    pred, dia_row, sep = w["dindex"].match(p["visit"], p["ra"], p["dec"], MATCH_RADIUS_ARCSEC)
    m = np.empty(pred.size, dtype=_MATCH_DTYPE)
    m["dia_row"] = dia_row
    m["sep"] = sep
    q = p[pred]
    for f in _PRED_DTYPE.names[1:]:
        m[f] = q[f]
    stage_t["match"] += time.perf_counter() - t0
    return m


def _chunk(o0, o1):
    """Orbits [o0, o1) of the current slice: (matches, counters, exceptions
    by type, examples, stage times, {"coarse_all", "coarse_partial",
    "exception"}: the orbits whose coarse pass failed entirely or in part,
    or that raised). One bad orbit doesn't stop the rest."""
    global _EPHEM
    w = _W
    ephem = w.get("ephem")
    if ephem is None:
        if _EPHEM is None:
            _EPHEM = open_ephem()
        ephem = _EPHEM
    propagate.STEP_CAP_STOPS = 0
    counts = dict.fromkeys(_COUNTS, 0)
    stage_t = dict.fromkeys(_STAGES, 0.0)
    errors, examples = Counter(), {}
    bad = {"coarse_all": [], "coarse_partial": [], "exception": []}
    out, preds, n_pred = [], [], 0
    for i in range(o0, o1):
        try:
            p, c = process_orbit(i, w, ephem, stage_t)
        except Exception as ex:     # one bad orbit must not stop a run
            name = type(ex).__name__
            errors[name] += 1
            ex_list = examples.setdefault(name, [])
            if len(ex_list) < _N_EXAMPLES:
                ex_list.append(f"{w['orbits'][i]['designation']}: {ex}"[:300])
            bad["exception"].append(i)
            continue
        if c["coarse_all_fail"]:
            bad["coarse_all"].append(i)
        elif c["coarse_partial_fail"]:
            bad["coarse_partial"].append(i)
        for k, v in c.items():
            counts[k] += v
        if p is not None:
            preds.append(p)
            n_pred += p.size
            if n_pred >= _MATCH_BATCH:
                out.append(_match(preds, w, stage_t))
                preds, n_pred = [], 0
    if preds:
        out.append(_match(preds, w, stage_t))
    matches = np.concatenate(out) if out else np.zeros(0, dtype=_MATCH_DTYPE)
    counts["matches"] = int(matches.size)
    counts["step_cap_stops"] = int(propagate.STEP_CAP_STOPS)
    bad = {k: np.array(v, dtype=np.int64) for k, v in bad.items()}
    return matches, counts, dict(errors), examples, stage_t, bad


def chunk_weights(orbits, t_mid):
    """A cheap cost proxy per orbit: the integration span in years (the
    coarse pass integrates from the epoch), with NEOs (q < 1.3 AU), whose
    close approaches force small steps, counted 3x, plus a fixed cost."""
    years = np.abs(orbits["epoch"] - t_mid) / 365.25
    neo = np.where(orbits["q"] < 1.3, 3.0, 1.0)
    w = 0.5 + years * neo
    return np.where(np.isfinite(w), w, 1.0)


# --------------------------------------------------------------------------
# Reduce and write
# --------------------------------------------------------------------------

def nearest(dia_id, sep, orbit):
    """Indices of the nearest match per diaSourceId (ties broken by
    ``orbit``, i.e. by designation, since orbits are designation-sorted),
    sorted by diaSourceId."""
    if not len(dia_id):
        return np.zeros(0, np.int64)
    order = np.lexsort((orbit, sep, dia_id))
    d = dia_id[order]
    first = np.r_[True, d[1:] != d[:-1]]
    return order[first]


def ssobject_ids(path, designations):
    """``ssObjectId`` of each designation from an SSObject Parquet table
    (its ``designation`` and ``ssObjectId`` columns), and whether it has a
    row there."""
    designations = np.asarray(designations, dtype="U16")
    ids = np.zeros(designations.size, np.int64)
    found = np.zeros(designations.size, bool)
    if path is None:
        return ids, found
    t = pq.read_table(path, columns=["designation", "ssObjectId"]).filter(
        pc.is_valid(pc.field("ssObjectId")))
    sd = np.asarray(t["designation"].to_pylist(), dtype="U16")
    sid = t["ssObjectId"].to_numpy()
    if not sd.size:
        return ids, found
    order = np.argsort(sd, kind="stable")
    sd, sid = sd[order], sid[order]
    j = np.minimum(np.searchsorted(sd, designations), sd.size - 1)
    found = sd[j] == designations
    ids[found] = sid[j[found]]
    return ids, found


def write_parquet(rows, has_ssobject, path):
    """Write ``rows`` (NEARBYSSO_DTYPE, sorted by diaSourceId) with
    ``ssObjectId`` null where ``has_ssobject`` is False."""
    arrays, fields = [], []
    for name in NEARBYSSO_DTYPE.names:
        col = rows[name]
        if name == "ssObjectId":
            a = pa.array(col, type=pa.int64(), mask=~has_ssobject)
        elif col.dtype.kind == "U":
            a = pa.array(col.tolist(), type=pa.string())
        else:
            a = pa.array(col)
        arrays.append(a)
        fields.append(pa.field(name, a.type, nullable=(name == "ssObjectId")))
    table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    pq.write_table(table, path, row_group_size=1 << 20)


def _rss_gb():
    """Peak RSS [GB] of this process and of its (reaped) children."""
    s = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    c = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    return s / 2**20, c / 2**20


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------

def build(dia_path, orbits_path, out_path, ssobject_path=None, workers=1, slice_days=30,
          chunk_factor=8, report_path=None, verbose=True):
    """Build ``out_path`` (``nearbysso.parquet``) from the DiaSources at
    ``dia_path`` and the ``mpc_orbits`` Parquet at ``orbits_path`` (or an
    ``ORBIT_DTYPE`` array, e.g. for tests). ``ssObjectId`` comes from the
    SSObject table at ``ssobject_path`` (null for objects without a row,
    and everywhere without one).

    ``workers`` > 1 runs the per-orbit pass in that many forked processes;
    the output does not depend on it, nor (but for integrator noise; see the
    module docstring) on ``slice_days``. Writes the run
    report as JSON to ``report_path`` (default: next to the output,
    ``<stem>.report.json``) and returns it.
    """
    T0 = time.perf_counter()
    rep = dict(inputs=dict(dia=str(dia_path), ssobject=None if ssobject_path is None else str(ssobject_path),
                           orbits="<array>" if isinstance(orbits_path, np.ndarray) else str(orbits_path)),
               workers=workers, slice_days=slice_days, timings={}, slices=[])
    tim = rep["timings"]

    def log(*a):
        if verbose:
            print(*a, flush=True)

    # Orbits and the ephemeris, once ---------------------------------------
    t = time.perf_counter()
    ephem = open_ephem()
    if isinstance(orbits_path, np.ndarray):
        orbits = orbits_path
        if orbits.dtype != ORBIT_DTYPE:
            raise TypeError("orbits: expected an ORBIT_DTYPE array")
        if orbits.size > 1 and not (orbits["designation"][1:] > orbits["designation"][:-1]).all():
            raise ValueError("orbits: must be sorted by designation, without duplicates")
        summary = f"{orbits.size:,} orbits given"
    else:
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            orbits = _orbits.load_orbits(orbits_path, ephem=ephem)
        summary = buf.getvalue().strip()
        log(summary)
    rep["orbits"] = dict(kept=int(orbits.size), has_cov_false=int((~orbits["has_cov"]).sum()),
                         load_orbits=summary)
    tim["load_orbits"] = time.perf_counter() - t

    # Slices ---------------------------------------------------------------
    t = time.perf_counter()
    nights, tmin, tmax, nsrc = night_ranges(dia_path)
    slices = plan_slices(nights, tmin, tmax, slice_days, nsrc)
    tim["plan_slices"] = time.perf_counter() - t
    log(f"{nsrc.sum():,} DiaSources in {nights.size} nights, {len(slices)} slice(s) of {slice_days} d "
        f"({tim['plan_slices']:.1f} s)")

    counts = dict.fromkeys(_COUNTS, 0)
    stage_cpu = dict.fromkeys(_STAGES, 0.0)
    errors, examples = Counter(), {}
    bad = {"coarse_all": [], "coarse_partial": [], "exception": []}
    kept = []                     # per slice: nearest matches, with diaSourceId
    n_dia = 0
    use_pool = workers > 1 and util.fork_context() is not None
    for si, sl in enumerate(slices):
        st = dict(nights=[sl["night_lo"], sl["night_hi"]])
        t = time.perf_counter()
        dia = _read_slice(dia_path, sl)
        st["read"] = time.perf_counter() - t
        st["dia"] = int(dia["diaSourceId"].size)
        # (read_dia drops rows with nulls or non-finite values, and warns)
        st["dia_dropped"] = sl["n"] - st["dia"]
        n_dia += st["dia"]

        t = time.perf_counter()
        vis = _visits.build_visits(dia)
        st["build_visits"] = time.perf_counter() - t
        t = time.perf_counter()
        dindex = _visits.DiaIndex(dia, vis)
        st["dia_index"] = time.perf_counter() - t
        t = time.perf_counter()
        vindex = _visits.VisitIndex(vis)
        ts = sample_times(vindex)
        obs_ts = observer_at(ts)
        times = Time(vis["t_tai_mjd"], format="mjd", scale="tai").tdb
        st["visit_index"] = time.perf_counter() - t
        st["visits"] = int(vis.size)
        # (only what the workers need: dia's ra/dec live in dindex)
        dia_id = dia["diaSourceId"]
        del dia

        t = time.perf_counter()
        weights = chunk_weights(orbits, 0.5 * (ts[0] + ts[-1]))
        n_chunks = chunk_factor * workers if use_pool else 1
        chunks = util.balanced_chunks(weights, n_chunks)
        _W.update(orbits=orbits, ts=ts, obs_ts=obs_ts, visits=vis, times=times,
                  vindex=vindex, dindex=dindex, ephem=None if use_pool else ephem)
        try:
            if use_pool:
                log(f"slice {si + 1}/{len(slices)}: {st['dia']:,} DiaSources, {vis.size:,} visits; "
                    f"{orbits.size:,} orbits in {len(chunks)} chunks on {workers} workers")
                res = util.run_chunks(_chunk, chunks, workers, f"slice {si + 1}/{len(slices)}",
                                      weights=[float(weights[a:b].sum()) for a, b in chunks])
            else:
                res = [_chunk(a, b) for a, b in chunks]
        finally:
            _W.clear()
        st["orbit_pass"] = time.perf_counter() - t

        t = time.perf_counter()
        m = np.concatenate([r[0] for r in res]) if res else np.zeros(0, _MATCH_DTYPE)
        sc = dict.fromkeys(_COUNTS, 0)
        for _, c, err, exs, stt, bd in res:
            for k, v in bd.items():
                bad[k].append(v)
            for k, v in c.items():
                sc[k] += v
            for k, v in stt.items():
                stage_cpu[k] += v
            errors.update(err)
            for k, v in exs.items():
                lst = examples.setdefault(k, [])
                lst.extend(v[:_N_EXAMPLES - len(lst)])
        for k, v in sc.items():
            counts[k] += v
        ids = dia_id[m["dia_row"]]
        near = nearest(ids, m["sep"], m["orbit"])
        kept.append((ids[near], m[near]))
        st["reduce"] = time.perf_counter() - t
        st["matches"] = int(m.size)
        st["nearest"] = int(near.size)
        st["errors"] = int(sum(sum(r[2].values()) for r in res))
        rep["slices"].append(st)
        log(f"slice {si + 1}/{len(slices)} nights {sl['night_lo']}..{sl['night_hi']}: "
            f"{st['dia']:,} DiaSources, {st['visits']:,} visits, {sc['candidate_visits']:,} candidate "
            f"visits, {sc['eligible_evals']:,} eligible, {st['matches']:,} matches -> {st['nearest']:,}; "
            f"read {st['read']:.1f} s, visits {st['build_visits']:.1f} s, index {st['dia_index']:.1f} s, "
            f"orbit pass {st['orbit_pass']:.1f} s")
        del m, dindex, vindex, vis, dia_id, times

    # Reduce over slices (a diaSourceId is in one slice, unless the input
    # repeats it) --------------------------------------------------------
    t = time.perf_counter()
    ids = np.concatenate([k[0] for k in kept]) if kept else np.zeros(0, np.int64)
    m = np.concatenate([k[1] for k in kept]) if kept else np.zeros(0, _MATCH_DTYPE)
    sel = nearest(ids, m["sep"], m["orbit"])
    ids, m = ids[sel], m[sel]
    rows = np.zeros(m.size, dtype=NEARBYSSO_DTYPE)
    rows["diaSourceId"] = ids
    rows["designation"] = orbits["designation"][m["orbit"]]
    rows["ephRa"] = m["ra"]
    rows["ephDec"] = m["dec"]
    rows["ephOffset"] = m["sep"]
    rows["ephVmag"] = m["vmag"]
    rows["ephRateRa"] = m["rate_ra"]
    rows["ephRateDec"] = m["rate_dec"]
    rows["ephRaErr"] = m["ra_err"]
    rows["ephDecErr"] = m["dec_err"]
    rows["ephRa_ephDec_Cov"] = m["ra_dec_cov"]
    sso_id, has_sso = ssobject_ids(ssobject_path, rows["designation"])
    rows["ssObjectId"] = sso_id
    tim["reduce"] = time.perf_counter() - t

    t = time.perf_counter()
    write_parquet(rows, has_sso, out_path)
    tim["write"] = time.perf_counter() - t

    for s in ("read", "build_visits", "dia_index", "visit_index", "orbit_pass", "reduce"):
        tim[f"slices_{s}"] = float(sum(st[s] for st in rep["slices"]))
    tim["worker_cpu"] = stage_cpu
    tim["total"] = time.perf_counter() - T0
    rss_self, rss_child = _rss_gb()
    # (orbits, not orbit-slices: one failing in several slices counts once)
    failed = {}
    for k, v in bad.items():
        idx = np.unique(np.concatenate(v)) if v else np.zeros(0, np.int64)
        failed[k] = dict(n=int(idx.size), examples=orbits["designation"][idx[:_N_EXAMPLES]].tolist())
    rep.update(
        dia_sources=int(n_dia), nights=int(nights.size),
        dia_dropped=int(sum(st["dia_dropped"] for st in rep["slices"])),
        counts=counts, failed_orbits=failed,
        exceptions=dict(errors), exception_examples=examples,
        matches_before_nearest=int(sum(st["matches"] for st in rep["slices"])),
        matches_after_nearest=int(m.size),
        output_rows=int(rows.size),
        with_ssobject=int(has_sso.sum()),
        peak_rss_gb=dict(parent=rss_self, largest_worker=rss_child),
    )
    if report_path is None:
        stem = str(out_path)[:-len(".parquet")] if str(out_path).endswith(".parquet") else str(out_path)
        report_path = stem + ".report.json"
    with open(report_path, "w") as fh:
        json.dump(rep, fh, indent=1)
    if verbose:
        _print_report(rep)
    return rep


def _print_report(rep):
    c, tim, f = rep["counts"], rep["timings"], rep["failed_orbits"]
    print(f"orbits: {rep['orbits']['kept']:,} (has_cov false {rep['orbits']['has_cov_false']:,}); "
          f"coarse failed: {f['coarse_all']['n']:,} entirely, {f['coarse_partial']['n']:,} partly "
          f"(e.g. {f['coarse_all']['examples'] + f['coarse_partial']['examples']}); "
          f"exceptions: {rep['exceptions'] or 'none'}")
    for k, v in rep["exception_examples"].items():
        print(f"  {k}: {v}")
    print(f"DiaSources: {rep['dia_sources']:,} in {rep['nights']:,} nights ({rep['dia_dropped']:,} dropped "
          f"by read_dia); step-cap stops: {c['step_cap_stops']:,}; nights skipped by candidates: "
          f"{c['nights_skipped']:,}")
    print(f"{c['with_candidates']:,} orbits with candidates; {c['candidate_visits']:,} candidate visits "
          f"= precise evaluations; {c['eligible_evals']:,} eligible, {c['sigma_rejected']:,} rejected by "
          f"sigma; {rep['matches_before_nearest']:,} matches, {rep['matches_after_nearest']:,} nearest; "
          f"{rep['output_rows']:,} rows ({rep['with_ssobject']:,} with an ssObjectId)")
    print("timings [s]: " + ", ".join(f"{k} {v:.1f}" for k, v in tim.items() if not isinstance(v, dict))
          + "; worker CPU: " + ", ".join(f"{k} {v:.1f}" for k, v in tim["worker_cpu"].items()))
    print(f"peak RSS: parent {rep['peak_rss_gb']['parent']:.1f} GB, largest worker "
          f"{rep['peak_rss_gb']['largest_worker']:.1f} GB", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Build the NearbySSO table: the nearest known Solar System object predicted "
                    "within 5\" of each DiaSource",
        epilog=(
            "The ASSIST ephemeris files are taken from the SSP_ASSIST_PLANETS and "
            "SSP_ASSIST_ASTEROIDS environment variables. The run report is written as JSON next "
            "to the output (<stem>.report.json)."
        ),
    )
    parser.add_argument("--dia", required=True,
                        help="DiaSource Parquet (a file or dataset directory) with diaSourceId, visit, "
                             "midpointMjdTai, ra, dec")
    parser.add_argument("--orbits", required=True, help="mpc_orbits Parquet")
    parser.add_argument("--output", "-o", default="nearbysso.parquet", help="Output (default: %(default)s)")
    parser.add_argument("--ssobject", default=None,
                        help="SSObject Parquet, to fill ssObjectId (designation -> ssObjectId); "
                             "without it ssObjectId is null")
    parser.add_argument("--slice-days", type=int, default=30,
                        help="Process the DiaSources in slices of this many nights' dates, to bound "
                             "memory (default: %(default)s). The output does not depend on it.")
    parser.add_argument(
        "--workers", type=int, default=min(64, os.cpu_count() or 1),
        help="Number of worker processes for the per-orbit pass (default: min(64, number of CPUs)). "
             "1 runs serially, with no process pool. The output does not depend on it.",
    )
    parser.add_argument(
        "--chunk-factor", type=int, default=8,
        help="With --workers > 1, split the orbits into about this many chunks per worker, to "
             "balance the load (default: %(default)s).",
    )
    parser.add_argument("--reraise", action="store_true",
                        help="Re-raise exceptions instead of exiting gracefully (for debugging)")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    if args.chunk_factor < 1:
        parser.error("--chunk-factor must be at least 1")
    if args.slice_days < 1:
        parser.error("--slice-days must be at least 1")

    try:
        build(args.dia, args.orbits, args.output, ssobject_path=args.ssobject, workers=args.workers,
              slice_days=args.slice_days, chunk_factor=args.chunk_factor)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
