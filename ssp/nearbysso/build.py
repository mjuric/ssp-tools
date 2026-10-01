"""WP4: build ``nearbysso.parquet`` (docs/design/nearbysso.md).

Three passes; each orbit is integrated once, and only the DiaSources are
sliced:

1. **Visits** (``read_workers`` forked processes, one slice each): read a
   slice (``visits.read_dia``) and derive its visits (``build_visits``).
   The parent concatenates them into one ``visits`` array over all nights,
   builds one ``VisitIndex`` and computes the observer at the coarse
   sample times.
2. **Orbits** (``workers`` forked processes, chunks of orbits, the
   costliest first; see ``orbit_schedule``), per orbit over all nights:
   ``propagate.coarse``, ``VisitIndex.candidates``, the precise
   ``compute_ephemerides_one`` at the candidate visits (as SSSource), the
   error ellipse there (``propagate.ellipse_at``) and the sigma gate. The
   eligible predictions (``PRED_DTYPE``, 48 bytes each) come back to the
   parent, which sorts them by (visit, orbit).
3. **Matching** (``read_workers`` processes, one slice each; the
   predictions shared through fork): read the slice again, index it
   (``DiaIndex``), match the slice's predictions (a contiguous range of the
   visit-sorted ones), rank each prediction's matches by separation
   (``diaDistanceRank``, ties by diaSourceId) and keep the nearest per
   DiaSource (ties by designation). A slice owns its DiaSources, and so
   every DiaSource of a visit, so both are exact.

The parent then attaches ``ssObjectId`` from an SSObject table, writes the
Parquet file (sorted by diaSourceId) and a JSON run report next to it.

Slices never split a night (``night = visit // 100000``, i.e. day_obs):
they are blocks of ``slice_days`` consecutive day_obs dates, read with time
bounds that cover their nights and then cut on the night itself. So every
DiaSource belongs to exactly one slice.

The coarse samples are three per night: its ``VisitIndex.night_t`` (the
midpoint of its visits, where ``candidates`` looks) and ``night_t -+ h``
with h = max(half the night's span of visit times, ``MIN_HALF_SPAN_DAYS``),
so that every visit time is bracketed (``ellipse_at`` clamps outside the
sampled span) by samples of its own night, and the rate and distance
changes ``candidates`` estimates for a night come from that night alone.

The output is byte-identical for any ``workers``, ``read_workers``,
``chunk_factor`` and ``slice_days``: the orbit pass sees all nights at
once whatever the slicing, and matching and the reduction are per visit
and per DiaSource.
"""

from __future__ import annotations

import argparse
import json
import os
import resource
import sys
import time
import warnings
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
from ._contract import (MATCH_RADIUS_ARCSEC, NEARBYSSO_DTYPE, OBSCODE, ORBIT_DTYPE, SIGMA_MAX_ARCSEC,
                        VISIT_DTYPE)

#: The smallest spacing [day] of a night's three coarse samples (for a night
#: with a single visit, or visits close in time; see the module docstring).
MIN_HALF_SPAN_DAYS = 1.0 / 24.0

#: Exceptions kept per type, for the run report.
_N_EXAMPLES = 5

#: An eligible prediction: an orbit at a candidate visit (48 bytes).
PRED_DTYPE = np.dtype([
    ("visit", "i4"),         # into the visits (of all nights)
    ("orbit", "i4"),         # into the (designation-sorted) orbits
    ("ra", "f8"), ("dec", "f8"),
    ("vmag", "f4"), ("rate_ra", "f4"), ("rate_dec", "f4"),
    ("ra_err", "f4"), ("dec_err", "f4"), ("ra_dec_cov", "f4"),
])

#: Predictions per DiaIndex.match call (it has a fixed cost per call).
_MATCH_BATCH = 1 << 18

#: Per-orbit counters of the orbit pass.
_COUNTS = ("orbits", "coarse_partial_fail", "coarse_all_fail", "exceptions", "with_candidates",
           "candidate_visits", "eligible", "sigma_rejected", "sigma_gated_nights", "nights_skipped",
           "step_cap_stops")
_STAGES = ("coarse", "candidates", "precise", "ellipse")


# --------------------------------------------------------------------------
# Slicing
# --------------------------------------------------------------------------

def night_ranges(path, stats=None):
    """Per night (``visit // 100000``) of the DiaSources in ``path``:
    ``(nights, tmin, tmax, n)`` (sorted by night; TAI MJD). One streaming
    pass over the two columns, so its memory doesn't grow with the input.
    A ``stats`` dict gets the rows read (``rows``) and those dropped for a
    null visit or time (``null_dropped``)."""
    dset = ds.dataset(path, format="parquet")
    parts = []
    n_rows = n_null = 0
    for batch in dset.to_batches(columns=["visit", "midpointMjdTai"], batch_size=1 << 22):
        n_rows += batch.num_rows
        if batch.num_rows == 0:
            continue
        if batch.column(0).null_count or batch.column(1).null_count:
            k = batch.num_rows
            batch = batch.filter(pc.and_(pc.is_valid(batch.column(0)), pc.is_valid(batch.column(1))))
            n_null += k - batch.num_rows
        night = batch.column(0).to_numpy(zero_copy_only=False) // 100000
        t = batch.column(1).to_numpy(zero_copy_only=False)
        # runs of equal nights (one per night for visit-sorted input)
        s = np.flatnonzero(np.r_[True, night[1:] != night[:-1]])
        parts.append((night[s], np.fmin.reduceat(t, s), np.fmax.reduceat(t, s),
                      np.diff(np.r_[s, night.size])))
    if stats is not None:
        stats.update(rows=int(n_rows), null_dropped=int(n_null))
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
        warnings.warn("night ids aren't all YYYYMMDD day_obs dates; slicing on the ids themselves",
                      stacklevel=2)
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
    """The coarse sample times (ASSIST, TDB) of the nights of ``vindex``:
    per night, ``night_t - h``, ``night_t``, ``night_t + h`` (see the
    module docstring)."""
    nt = np.asarray(vindex.night_t, dtype=np.float64)
    # (not half the span: nt -+ that can miss tmin or tmax by an ulp)
    h = np.maximum(np.maximum(vindex.night_tmax - nt, nt - vindex.night_tmin), MIN_HALF_SPAN_DAYS)
    return np.stack([nt - h, nt, nt + h], axis=1).ravel()


def observer_at(t_assist):
    """X05 barycentric ICRF position [AU], (K, 3), at ASSIST times (TDB)."""
    tt = Time(np.asarray(t_assist) + MJD_J2000, format="mjd", scale="tdb")
    r, _ = util.observatory_barycentric_posvel(OBSCODE, tt)
    return np.asarray(r.to_value(u.au)).reshape(3, -1).T.copy()


# --------------------------------------------------------------------------
# Fork pools
# --------------------------------------------------------------------------

# Filled by the parent before a pool forks (and for the serial runs), so
# that the large arrays are inherited, never pickled.
_W = {}
_EPHEM = None   # one ASSIST ephemeris per worker process, opened lazily


def _pooled(workers, n_chunks):
    """Whether ``_map`` runs ``n_chunks`` chunks in a fork pool."""
    return workers > 1 and n_chunks > 1 and util.fork_context() is not None


def _map(func, chunks, workers, label, weights=None, unit="objects"):
    """``func(a, b)`` over ``chunks``, in a fork pool of ``workers`` if
    ``_pooled``, else in this process; results in order."""
    if _pooled(workers, len(chunks)):
        return util.run_chunks(func, chunks, workers, label, weights=weights, unit=unit)
    return [func(a, b) for a, b in chunks]


def _peak_rss_gb():
    """This process's peak RSS [GB] (for a forked worker, it counts the
    pages it shares with the parent, too)."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**20


# --------------------------------------------------------------------------
# Pass 1: visits, per slice
# --------------------------------------------------------------------------

def _visits_slice(s0, s1):
    """Slices [s0, s1): per slice (its visits, rows read, seconds reading,
    seconds in build_visits), and the peak RSS."""
    w = _W
    out = []
    for s in range(s0, s1):
        t0 = time.perf_counter()
        dia = _read_slice(w["dia_path"], w["slices"][s])
        t1 = time.perf_counter()
        vis = _visits.build_visits(dia, threads=w["threads"])
        out.append((vis, int(dia["diaSourceId"].size), t1 - t0, time.perf_counter() - t1))
        del dia
    return out, _peak_rss_gb()


# --------------------------------------------------------------------------
# Pass 2: the orbits, once
# --------------------------------------------------------------------------

def process_orbit(i, w, ephem, stage_t):
    """Orbit ``i`` of ``w["orbits"]`` over every night: its eligible
    predictions (``PRED_DTYPE``, or None) and its counters."""
    orbit = w["orbits"][i]
    c = dict.fromkeys(_COUNTS, 0)
    t0 = time.perf_counter()
    track = propagate.coarse(orbit, w["ts"], w["obs_ts"], ephem)
    t1 = time.perf_counter()
    stage_t["coarse"] += t1 - t0
    if not track.ok.all():
        c["coarse_all_fail" if not track.ok.any() else "coarse_partial_fail"] = 1
    vindex = w["vindex"]
    cand = vindex.candidates(track, _visits.DEFAULT_CANDIDATE_MARGIN_ARCSEC)
    c["nights_skipped"] = int(vindex.last_skipped)
    c["sigma_gated_nights"] = int(vindex.last_sigma_gated)
    t2 = time.perf_counter()
    stage_t["candidates"] += t2 - t1
    if not cand.size:
        return None, c
    c["with_candidates"] = 1
    c["candidate_visits"] = int(cand.size)

    v = w["visits"][cand]
    e = compute_ephemerides_one(str(orbit["designation"]), w["times"][cand], None, ephem, row=orbit,
                                obs_pos=v["obs_pos"].T, obs_vel=v["obs_vel"].T)
    t3 = time.perf_counter()
    stage_t["precise"] += t3 - t2

    ra_err, dec_err, ra_dec_cov, smaj = propagate.ellipse_at(track, v["t"], topo_pos=e.topo_pos.T)
    k = np.flatnonzero(np.isfinite(smaj) & (smaj <= SIGMA_MAX_ARCSEC))
    c["eligible"] = int(k.size)
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

    p = np.empty(k.size, dtype=PRED_DTYPE)
    p["visit"] = cand[k]
    p["orbit"] = i
    # (as SSSource: RA wrapped to [0, 360); the separation is measured from
    # the prediction, in pass 3)
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


def _orbit_chunk(o0, o1):
    """Orbits ``w["order"][o0:o1]``: (predictions, counters, exceptions by
    type, examples, stage times, {"coarse_all", "coarse_partial",
    "exception"}: the orbits whose coarse pass failed entirely or in part,
    or that raised, peak RSS). One bad orbit doesn't stop the rest."""
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
    out = []
    for i in w["order"][o0:o1].tolist():
        try:
            p, c = process_orbit(i, w, ephem, stage_t)
        except Exception as ex:     # one bad orbit must not stop a run
            name = type(ex).__name__
            errors[name] += 1
            ex_list = examples.setdefault(name, [])
            if len(ex_list) < _N_EXAMPLES:
                ex_list.append(f"{w['orbits'][i]['designation']}: {ex}"[:300])
            bad["exception"].append(i)
            counts["exceptions"] += 1
            continue
        if c["coarse_all_fail"]:
            bad["coarse_all"].append(i)
        elif c["coarse_partial_fail"]:
            bad["coarse_partial"].append(i)
        for k, v in c.items():
            counts[k] += v
        if p is not None:
            out.append(p)
    preds = np.concatenate(out) if out else np.zeros(0, dtype=PRED_DTYPE)
    counts["orbits"] = o1 - o0
    counts["step_cap_stops"] = int(propagate.STEP_CAP_STOPS)
    bad = {k: np.array(v, dtype=np.int64) for k, v in bad.items()}
    return preds, counts, dict(errors), examples, stage_t, bad, _peak_rss_gb()


#: NEOs' (q < NEO_Q_AU) relative cost in the orbit pass: 12x the others,
#: measured over a synthetic year (138 against 11.5 ms per orbit), with a
#: heavy tail: the precise pass of an NEO passing within ~0.001 AU, over
#: every visit of its nights (NEAR_DELTA_AU), can take minutes.
NEO_Q_AU, NEO_COST = 1.3, 12.0


def chunk_weights(orbits, t_lo, t_hi):
    """A cheap cost proxy per orbit: a fixed cost plus the integration span
    in years (the coarse pass integrates from the epoch through [t_lo,
    t_hi]), times ``NEO_COST`` for NEOs."""
    years = (np.maximum(orbits["epoch"], t_hi) - np.minimum(orbits["epoch"], t_lo)) / 365.25
    w = (0.5 + years) * np.where(orbits["q"] < NEO_Q_AU, NEO_COST, 1.0)
    return np.where(np.isfinite(w), w, NEO_COST)


def orbit_schedule(weights, n_chunks):
    """The order to process orbits in, and chunks of it: the costliest
    first (NEOs, then the rest; stably, so designation order within a
    weight), in ranges of about equal weight, so that the pool's queue
    hands out the NEOs' heavy tail early, in small chunks. Returns
    (order, chunks)."""
    order = np.argsort(-np.asarray(weights, dtype=np.float64), kind="stable")
    return order, util.balanced_chunks(np.asarray(weights)[order], n_chunks)


def sort_predictions(chunks, nvisits):
    """Concatenate the chunks' predictions (in any order), sorted by (visit,
    orbit), emptying ``chunks``. Returns them and the offsets (nvisits + 1,)
    of each visit's predictions. The peak is about twice the predictions,
    plus an index."""
    p = np.concatenate(chunks) if chunks else np.zeros(0, dtype=PRED_DTYPE)
    chunks.clear()
    # (one prediction per orbit and visit, so the key is unique)
    key = (p["visit"].astype(np.int64) << 32) | p["orbit"].astype(np.int64)
    o = np.argsort(key)
    del key
    p = p[o]
    del o
    return p, np.searchsorted(p["visit"], np.arange(nvisits + 1)).astype(np.int64)


# --------------------------------------------------------------------------
# Pass 3: matching, per slice
# --------------------------------------------------------------------------

def _match_slice(s0, s1):
    """Slices [s0, s1): per slice, the nearest match of each of its
    DiaSources, as (diaSourceId, prediction index, separation,
    diaDistanceRank), plus the number of matches before that reduction;
    then (seconds reading, indexing, matching) and the peak RSS."""
    w = _W
    preds, vstart, poff = w["preds"], w["vstart"], w["poff"]
    out, tim = [], np.zeros(3)
    for s in range(s0, s1):
        t0 = time.perf_counter()
        dia = _read_slice(w["dia_path"], w["slices"][s])
        v0, v1 = vstart[s], vstart[s + 1]
        vis = w["visits"][v0:v1]
        t1 = time.perf_counter()
        dindex = _visits.DiaIndex(dia, vis, threads=w["threads"])
        ids = dia["diaSourceId"]
        del dia
        t2 = time.perf_counter()
        p0, p1 = int(poff[v0]), int(poff[v1])
        k_all, row_all, sep_all = [], [], []
        for b0 in range(p0, p1, _MATCH_BATCH):
            b1 = min(b0 + _MATCH_BATCH, p1)
            p = preds[b0:b1]
            k, row, sep = dindex.match(p["visit"].astype(np.int64) - v0, p["ra"], p["dec"],
                                       MATCH_RADIUS_ARCSEC)
            k_all.append(k + b0)
            row_all.append(row)
            sep_all.append(sep)
        k = np.concatenate(k_all) if k_all else np.zeros(0, np.int64)
        row = np.concatenate(row_all) if row_all else np.zeros(0, np.int64)
        sep = np.concatenate(sep_all) if sep_all else np.zeros(0)
        n_match = int(k.size)
        # (every match of a prediction is here: a slice holds whole visits)
        rank = distance_rank(k, ids[row], sep)
        # (prediction order is (visit, orbit), so k breaks ties by designation)
        sel = nearest(row, sep, k)
        out.append((ids[row[sel]], k[sel], sep[sel], rank[sel], n_match))
        tim += (t1 - t0, t2 - t1, time.perf_counter() - t2)
    return out, tim, _peak_rss_gb()


# --------------------------------------------------------------------------
# Reduce and write
# --------------------------------------------------------------------------

def distance_rank(pred, dia_id, sep):
    """``diaDistanceRank`` of each match (int16): the 1-based rank of its
    DiaSource by ``sep`` among all the matches of its prediction ``pred``,
    ties going to the lower ``dia_id``."""
    n = len(pred)
    if not n:
        return np.zeros(0, np.int16)
    order = np.lexsort((dia_id, sep, pred))
    ps = pred[order]
    idx = np.arange(n)
    first = np.maximum.accumulate(np.where(np.r_[True, ps[1:] != ps[:-1]], idx, 0))
    r = idx - first + 1
    if r.max() > np.iinfo(np.int16).max:
        raise OverflowError("diaDistanceRank: more matches of one prediction than int16 holds")
    rank = np.empty(n, np.int16)
    rank[order] = r
    return rank


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
    sd = np.asarray(t["designation"].to_pylist(), dtype=object)
    sid = t["ssObjectId"].to_numpy()
    long = np.array([len(d) > 16 for d in sd], dtype=bool)
    if long.any():   # (would be truncated to U16, and could match wrongly)
        warnings.warn(f"{path}: ignoring {int(long.sum())} designations longer than 16 characters, "
                      f"e.g. {sd[long][0]!r}", stacklevel=2)
        sd, sid = sd[~long], sid[~long]
    sd = sd.astype("U16")
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


# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------

def build(dia_path, orbits_path, out_path, ssobject_path=None, workers=1, read_workers=8,
          slice_days=7, chunk_factor=64, report_path=None, verbose=True):
    """Build ``out_path`` (``nearbysso.parquet``) from the DiaSources at
    ``dia_path`` and the ``mpc_orbits`` Parquet at ``orbits_path`` (or an
    ``ORBIT_DTYPE`` array, e.g. for tests). ``ssObjectId`` comes from the
    SSObject table at ``ssobject_path`` (null for objects without a row,
    and everywhere without one).

    ``workers`` forked processes run the orbit pass, and ``read_workers``
    the per-slice passes (each holds one slice of DiaSources; they bound
    the memory). The output depends on neither, nor on ``slice_days``.
    Writes the run report as JSON to ``report_path`` (default: next to the
    output, ``<stem>.report.json``) and returns it.

    An existing output and report are removed first, and both are written
    to temporary files renamed into place, so a failed run leaves neither
    (and never a stale one).
    """
    T0 = time.perf_counter()
    out_path = str(out_path)
    if report_path is None:
        stem = out_path[:-len(".parquet")] if out_path.endswith(".parquet") else out_path
        report_path = stem + ".report.json"
    for path in (out_path, report_path):
        if os.path.lexists(path):
            os.remove(path)
    rep = dict(inputs=dict(dia=str(dia_path), ssobject=None if ssobject_path is None else str(ssobject_path),
                           orbits="<array>" if isinstance(orbits_path, np.ndarray) else str(orbits_path)),
               workers=workers, read_workers=read_workers, slice_days=slice_days, timings={})
    tim = rep["timings"]
    peak = rep["peak_rss_gb"] = {}

    def log(*a):
        if verbose:
            print(*a, flush=True)

    # Orbits and the ephemeris, once ---------------------------------------
    t = time.perf_counter()
    ephem = open_ephem()
    stats = {}
    if isinstance(orbits_path, np.ndarray):
        orbits = orbits_path
        if orbits.dtype != ORBIT_DTYPE:
            raise TypeError("orbits: expected an ORBIT_DTYPE array")
        if orbits.size > 1 and not (orbits["designation"][1:] > orbits["designation"][:-1]).all():
            raise ValueError("orbits: must be sorted by designation, without duplicates")
        stats.update(kept=int(orbits.size), has_cov_false=int((~orbits["has_cov"]).sum()))
    else:
        orbits = _orbits.load_orbits(orbits_path, ephem=ephem, verbose=verbose, stats=stats)
    rep["orbits"] = stats
    tim["load_orbits"] = time.perf_counter() - t

    # Slices ---------------------------------------------------------------
    t = time.perf_counter()
    read_stats = {}
    nights, tmin, tmax, nsrc = night_ranges(dia_path, stats=read_stats)
    slices = plan_slices(nights, tmin, tmax, slice_days, nsrc)
    ns = len(slices)
    tim["plan_slices"] = time.perf_counter() - t
    log(f"{nsrc.sum():,} DiaSources in {nights.size} nights, {ns} slice(s) of {slice_days} d "
        f"({tim['plan_slices']:.1f} s)")
    rw = max(1, min(read_workers, ns))
    threads = max(1, _visits.DEFAULT_THREADS // rw)
    _W.update(dia_path=dia_path, slices=slices, threads=threads)
    one = [(s, s + 1) for s in range(ns)]

    # Pass 1: visits -------------------------------------------------------
    t = time.perf_counter()
    try:
        res = _map(_visits_slice, one, rw, "pass 1: visits", weights=[sl["n"] for sl in slices],
                   unit="slices")
    finally:
        _W.clear()
    per_slice = [r for chunk, _ in res for r in chunk]
    peak["pass1_worker"] = max((r[1] for r in res), default=0.0)
    vis_parts = [r[0] for r in per_slice]
    visits = np.concatenate(vis_parts) if vis_parts else np.zeros(0, VISIT_DTYPE)
    vstart = np.r_[0, np.cumsum([v.size for v in vis_parts])].astype(np.int64)
    n_dia = sum(r[1] for r in per_slice)
    rep["slices"] = [dict(nights=[sl["night_lo"], sl["night_hi"]], dia=r[1], dia_dropped=sl["n"] - r[1],
                          visits=int(r[0].size), read=r[2], build_visits=r[3])
                     for sl, r in zip(slices, per_slice)]
    del res, per_slice, vis_parts
    vindex = _visits.VisitIndex(visits)
    ts = sample_times(vindex)
    obs_ts = observer_at(ts)
    times = Time(visits["t_tai_mjd"], format="mjd", scale="tai").tdb
    tim["pass1_visits"] = time.perf_counter() - t
    log(f"pass 1: {visits.size:,} visits in {nights.size} nights, {ts.size} coarse samples "
        f"({tim['pass1_visits']:.1f} s)")

    # Pass 2: the orbits ---------------------------------------------------
    t = time.perf_counter()
    weights = chunk_weights(orbits, ts.min(), ts.max()) if ts.size else np.ones(orbits.size)
    order, chunks = orbit_schedule(weights, chunk_factor * workers if workers > 1 else 1)
    if not ts.size:
        chunks = []
    use_pool = _pooled(workers, len(chunks))
    # (workers open their own ephemeris; in this process, reuse ours)
    _W.update(orbits=orbits, ts=ts, obs_ts=obs_ts, visits=visits, times=times, vindex=vindex,
              order=order, ephem=None if use_pool else ephem)
    log(f"pass 2: {orbits.size:,} orbits in {len(chunks)} chunks on {workers if use_pool else 1} worker(s)")
    try:
        res = _map(_orbit_chunk, chunks, workers, "pass 2: orbits",
                   weights=[float(weights[order[a:b]].sum()) for a, b in chunks], unit="orbits")
    finally:
        _W.clear()
    counts = dict.fromkeys(_COUNTS, 0)
    stage_cpu = dict.fromkeys(_STAGES, 0.0)
    errors, examples = Counter(), {}
    # (examples; the numbers are in counts)
    failed = {}
    for k in ("coarse_all", "coarse_partial", "exception"):
        idx = np.sort(np.concatenate([r[5][k] for r in res])) if res else np.zeros(0, np.int64)
        failed[k] = orbits["designation"][idx[:_N_EXAMPLES]].tolist()
    for _, c, err, exs, stt, _, _ in res:
        for k, v in c.items():
            counts[k] += v
        for k, v in stt.items():
            stage_cpu[k] += v
        errors.update(err)
        for k, v in exs.items():
            lst = examples.setdefault(k, [])
            lst.extend(v[:_N_EXAMPLES - len(lst)])
    peak["pass2_worker"] = max((r[6] for r in res), default=0.0)
    pchunks = [r[0] for r in res]
    del res
    tim["pass2_orbits"] = time.perf_counter() - t
    t = time.perf_counter()
    preds, poff = sort_predictions(pchunks, visits.size)
    del pchunks
    tim["sort_predictions"] = time.perf_counter() - t
    log(f"pass 2: {counts['with_candidates']:,} orbits with candidates, {counts['candidate_visits']:,} "
        f"candidate visits, {preds.size:,} eligible predictions ({preds.nbytes / 2**30:.2f} GB); "
        f"{tim['pass2_orbits']:.1f} s + sort {tim['sort_predictions']:.1f} s")

    # Pass 3: matching -----------------------------------------------------
    t = time.perf_counter()
    _W.update(dia_path=dia_path, slices=slices, threads=threads, visits=visits, vstart=vstart, preds=preds,
              poff=poff)
    try:
        res = _map(_match_slice, one, rw, "pass 3: matching", weights=[sl["n"] for sl in slices],
                   unit="slices")
    finally:
        _W.clear()
    per_slice = [r for chunk, _, _ in res for r in chunk]
    t3 = sum((r[1] for r in res), np.zeros(3))
    peak["pass3_worker"] = max((r[2] for r in res), default=0.0)
    del res
    for st, r in zip(rep["slices"], per_slice):
        st.update(matches=r[4], nearest=int(r[0].size))
    ids = np.concatenate([r[0] for r in per_slice]) if per_slice else np.zeros(0, np.int64)
    k = np.concatenate([r[1] for r in per_slice]) if per_slice else np.zeros(0, np.int64)
    sep = np.concatenate([r[2] for r in per_slice]) if per_slice else np.zeros(0)
    rank = np.concatenate([r[3] for r in per_slice]) if per_slice else np.zeros(0, np.int16)
    n_matches = int(sum(r[4] for r in per_slice))
    del per_slice
    tim["pass3_matching"] = time.perf_counter() - t
    tim["pass3_cpu"] = dict(read=float(t3[0]), dia_index=float(t3[1]), match=float(t3[2]))

    # Reduce (a diaSourceId is in one slice, unless the input repeats it) --
    t = time.perf_counter()
    sel = nearest(ids, sep, k)
    ids, k, sep, rank = ids[sel], k[sel], sep[sel], rank[sel]
    p = preds[k]
    rows = np.zeros(k.size, dtype=NEARBYSSO_DTYPE)
    rows["diaSourceId"] = ids
    rows["designation"] = orbits["designation"][p["orbit"]]
    rows["ephRa"] = p["ra"]
    rows["ephDec"] = p["dec"]
    rows["ephOffset"] = sep
    rows["diaDistanceRank"] = rank
    rows["ephVmag"] = p["vmag"]
    rows["ephRateRa"] = p["rate_ra"]
    rows["ephRateDec"] = p["rate_dec"]
    rows["ephRaErr"] = p["ra_err"]
    rows["ephDecErr"] = p["dec_err"]
    rows["ephRa_ephDec_Cov"] = p["ra_dec_cov"]
    sso_id, has_sso = ssobject_ids(ssobject_path, rows["designation"])
    rows["ssObjectId"] = sso_id
    tim["reduce"] = time.perf_counter() - t

    t = time.perf_counter()
    tmp = f"{out_path}.tmp-{os.getpid()}"
    try:
        write_parquet(rows, has_sso, tmp)
    except BaseException:
        if os.path.lexists(tmp):
            os.remove(tmp)
        raise
    tim["write"] = time.perf_counter() - t

    tim["worker_cpu"] = stage_cpu
    tim["total"] = time.perf_counter() - T0
    peak["parent"] = _peak_rss_gb()
    rep.update(
        dia_rows_read=read_stats.get("rows", 0), dia_null_dropped=read_stats.get("null_dropped", 0),
        dia_dropped=int(sum(st["dia_dropped"] for st in rep["slices"])),
        dia_sources=int(n_dia), nights=int(nights.size), visits=int(visits.size),
        counts=counts, failed_orbits=failed,
        exceptions=dict(errors), exception_examples=examples,
        predictions=int(preds.size), predictions_gb=preds.nbytes / 2**30,
        matches_before_nearest=n_matches, output_rows=int(rows.size), with_ssobject=int(has_sso.sum()),
    )
    tmp_rep = f"{report_path}.tmp-{os.getpid()}"
    try:
        with open(tmp_rep, "w") as fh:
            json.dump(rep, fh, indent=1)
        os.replace(tmp, out_path)
        os.replace(tmp_rep, report_path)
    finally:
        for path in (tmp, tmp_rep):
            if os.path.lexists(path):
                os.remove(path)
    if verbose:
        _print_report(rep)
    return rep


def _print_report(rep):
    c, tim, f, pk = rep["counts"], rep["timings"], rep["failed_orbits"], rep["peak_rss_gb"]
    o = rep["orbits"]
    rms = o.get("normalized_rms")
    rms = f"; normalized_rms p50 {rms['p50']:.3f}, p95 {rms['p95']:.3f}" if rms else ""
    print(f"orbits: {o['kept']:,} (has_cov false {o['has_cov_false']:,}{rms}); "
          f"coarse failed: {c['coarse_all_fail']:,} entirely, {c['coarse_partial_fail']:,} partly "
          f"(e.g. {f['coarse_all'] + f['coarse_partial']}); "
          f"exceptions: {rep['exceptions'] or 'none'}")
    for k, v in rep["exception_examples"].items():
        print(f"  {k}: {v}")
    print(f"DiaSources: {rep['dia_rows_read']:,} rows read ({rep['dia_null_dropped']:,} with a null visit or "
          f"time, {rep['dia_dropped']:,} dropped by read_dia), {rep['dia_sources']:,} used, in "
          f"{rep['nights']:,} nights, {rep['visits']:,} visits; step-cap stops: {c['step_cap_stops']:,}; "
          f"nights skipped by candidates: {c['nights_skipped']:,}")
    print(f"{c['with_candidates']:,} orbits with candidates; {c['sigma_gated_nights']:,} orbit-nights "
          f"gated by sigma; {c['candidate_visits']:,} candidate visits = precise evaluations; "
          f"{c['eligible']:,} eligible, {c['sigma_rejected']:,} rejected by sigma; "
          f"{rep['matches_before_nearest']:,} matches; "
          f"{rep['output_rows']:,} rows (nearest; {rep['with_ssobject']:,} with an ssObjectId)")
    print("timings [s]: " + ", ".join(f"{k} {v:.1f}" for k, v in tim.items() if not isinstance(v, dict))
          + "; pass 2 worker CPU: " + ", ".join(f"{k} {v:.1f}" for k, v in tim["worker_cpu"].items())
          + "; pass 3 worker CPU: " + ", ".join(f"{k} {v:.1f}" for k, v in tim["pass3_cpu"].items()))
    print("peak RSS [GB]: " + ", ".join(f"{k} {v:.1f}" for k, v in pk.items()), flush=True)


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
    parser.add_argument("--slice-days", type=int, default=7,
                        help="Read the DiaSources in slices of this many nights' dates (default: "
                             "%(default)s). The output does not depend on it.")
    parser.add_argument(
        "--workers", type=int, default=min(64, len(os.sched_getaffinity(0))),
        help="Number of worker processes for the per-orbit pass (default: min(64, usable CPUs)). "
             "1 runs serially, with no process pool. The output does not depend on it.",
    )
    parser.add_argument(
        "--read-workers", type=int, default=8,
        help="Number of worker processes reading and indexing slices of DiaSources, each holding one "
             "slice; bounds the memory (default: %(default)s). The output does not depend on it.",
    )
    parser.add_argument(
        "--chunk-factor", type=int, default=64,
        help="With --workers > 1, split the orbits into about this many chunks per worker, the "
             "costliest (NEOs) first, to balance the load (default: %(default)s).",
    )
    parser.add_argument("--reraise", action="store_true",
                        help="Re-raise exceptions instead of exiting gracefully (for debugging)")
    args = parser.parse_args()
    for name in ("workers", "read_workers", "chunk_factor", "slice_days"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")

    try:
        build(args.dia, args.orbits, args.output, ssobject_path=args.ssobject, workers=args.workers,
              read_workers=args.read_workers, slice_days=args.slice_days, chunk_factor=args.chunk_factor)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.reraise:
            raise
        sys.exit(1)


if __name__ == "__main__":
    main()
