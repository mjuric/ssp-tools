"""NearbySSO rows on detectors, and their shutter-corrected times
(docs/design/nearbysso-unmatched.md, Approach steps 1-2, 5-7; the
contract's "Unmatched predictions, detector placement and corrected times").

``wcs_footprints`` gives the visits' footprints for candidate selection
(step 1; the visits pass, ``build._visits_slice``). The pieces the matching
pass (``build._match_slice``) runs per slice:

- ``open_store``: the slice's nights of the visit-table WCS store, read
  through ``visit_table.locate.WcsStore`` and only for the slice's visits.
  A night not in the store, a visit not in a night, a detector without a
  WCS: all "no WCS", never inferred. ``wcs_snapshot`` reads the store's
  snapshot for the run report; nothing checks its freshness.
- ``place_rows``: each row's prediction, at its visit's time, on a detector
  with ``VisitLocator.place`` (detector, x, y, edge distance, Jacobian).
- ``header_midpoints`` and ``visit_time_states``: per visit, whether the
  PPDB's DiaSource times are the uncorrected header midpoint (every one
  within ``MAX_HEADER_MISMATCH_S`` of (MJD-BEG + MJD-END)/2 from the
  correction table's exposure log), corrected, or can't be told (no header
  midpoint: no table, a night outside the table's coverage or not built, a
  visit not in its exposure log).
- ``row_times``: each row's time and ``midpointMjdTai_flag``, by
  ``TIME_CAUSES``.
- ``move_pixels``: (x, y) and the edge distance moved linearly with the
  Jacobian, by the move of the prediction to the row's time.
"""

from __future__ import annotations

import copy
import json
import os

import numpy as np

from ..ssobservation_contract import MAX_HEADER_MISMATCH_S
from ._contract import PLACEMENT_MARGIN_ARCSEC

#: The production WCS store (visit-table's ``wcs/``, refreshed hourly by
#: ssp-daily). Named through the ssp_daily_data symlink, never resolved.
DEFAULT_WCS_STORE = "/sdf/home/m/mjuric/ssp_daily_data/visit-table/wcs"

#: shutter_timing's limit: positions further than this [px, per axis]
#: outside their detector's pixel bounds get no corrected time (OMITTED);
#: shutter_timing.corrections.OFF_DETECTOR_OMIT_PIX (copied, so that this
#: module imports without shutter_timing; a test checks they agree).
OFF_DETECTOR_OMIT_PIX = 100.0

# Per visit: what its PPDB DiaSource times are (visit_time_states).
#: No header midpoint to compare with (see header_midpoints).
VISIT_UNVERIFIED = 0
#: Every DiaSource within MAX_HEADER_MISMATCH_S of the header midpoint.
VISIT_UNCORRECTED = 1
#: Some DiaSource further than that: the PPDB records corrected times.
VISIT_CORRECTED = 2

#: Why a row has the time it has (``time_cause``; codes are the indices).
#: The first two are not flagged: the corrected midpoint at the row's
#: predicted pixel from our table ("corrected"), or, in a visit whose PPDB
#: times are corrected, the matched DiaSource's own time ("ppdb"). The rest
#: are ``midpointMjdTai_flag`` True, the row keeping its visit's time (a
#: matched row in an unverified visit: its DiaSource's own time, which is
#: the visit's while the PPDB records uncorrected times):
#:
#: - no_table: the build has no correction table;
#: - outside_coverage: the night is before the table's first night;
#: - not_built: the night is not built, or the visit is not in its
#:   exposure log;
#: - omitted: the table has no time there (a skipped or failed exposure, a
#:   detector not in the table, a NaN header midpoint);
#: - beyond_100px: more than OFF_DETECTOR_OMIT_PIX off the detector;
#: - no_wcs: not placed (the visit has no WCS, or the prediction is more
#:   than PLACEMENT_MARGIN_ARCSEC from every detector with one), so no pixel
#:   to look up.
TIME_CAUSES = ("corrected", "ppdb", "no_table", "outside_coverage", "not_built", "omitted", "beyond_100px",
               "no_wcs")
(CAUSE_CORRECTED, CAUSE_PPDB, CAUSE_NO_TABLE, CAUSE_OUTSIDE_COVERAGE, CAUSE_NOT_BUILT, CAUSE_OMITTED,
 CAUSE_BEYOND_100PX, CAUSE_NO_WCS) = range(len(TIME_CAUSES))
#: Causes from here on are flagged (midpointMjdTai_flag True).
FIRST_FLAGGED = CAUSE_NO_TABLE

#: Per NearbySSO row, aligned with its rows (``build.build(...,
#: return_rows=True)``): what the sidecar (NEARBYSSO_SIDECAR_DTYPE) and the
#: next NearbySSO's ``visit``/``detector`` need, plus diagnostics.
ROW_PLACE_DTYPE = np.dtype([
    ("visit", "i8"),                  # the visit id
    ("detector", "i2"),               # placed detector; the DiaSource's without a WCS
    ("dia_detector", "i2"),           # the DiaSource's detector (-1: none, or no such column)
    ("eph_x", "f8"),                  # [px] at the row's time; NaN without a WCS
    ("eph_y", "f8"),
    ("ephDetectorEdgeDist", "f4"),    # [arcsec] signed, positive inside; NaN without a WCS
    ("midpointMjdTai", "f8"),         # TAI MJD the row's values are evaluated at
    ("midpointMjdTai_flag", "?"),     # time_cause >= FIRST_FLAGGED
    ("time_cause", "u1"),             # index into TIME_CAUSES
    ("has_wcs", "?"),                 # placed: a WCS, and within the margin of a detector
    ("move_px", "f4"),                # [px] |(eph_x, eph_y) - the placement at the visit's time|
])


# --------------------------------------------------------------------------
# The WCS store
# --------------------------------------------------------------------------

def wcs_snapshot(store_dir):
    """The store's snapshot: the largest ``fromSnapshot`` of its nights in
    ``_state.json`` (None if there is no state file, or no night)."""
    try:
        with open(os.path.join(store_dir, "_state.json")) as f:
            state = json.load(f)
    except (OSError, ValueError):
        return None
    snaps = [n.get("fromSnapshot") for n in state.get("nights", {}).values()]
    snaps = [int(s) for s in snaps if s is not None]
    return max(snaps) if snaps else None


def wcs_paths(store_dir, nights):
    """The store's files of ``nights`` (day_obs) that exist."""
    paths = (os.path.join(store_dir, f"day_obs={int(n)}.parquet") for n in np.unique(nights))
    return [p for p in paths if os.path.exists(p)]


def open_store(store_dir, nights, visits):
    """A ``WcsStore`` of ``visits`` from the files of ``nights``; None
    without a store, or if none of the nights is in it."""
    if store_dir is None:
        return None
    paths = wcs_paths(store_dir, nights)
    if not paths:
        return None
    from visit_table.locate import WcsStore
    return WcsStore(paths, visits=np.asarray(visits, np.int64))


def wcs_footprints(store_dir, visits, ra, dec, margin_arcsec=PLACEMENT_MARGIN_ARCSEC):
    """Set, in place, the ``center`` and ``radius`` of each visit of
    ``visits`` (VISIT_DTYPE, ``build_visits`` of the visit-sorted
    DiaSources at ``ra``, ``dec`` [deg]) that has a WCS to its WCS
    footprint (``visit_table.locate.footprints``: the centre of its
    detectors' corners and the radius to the farthest one, rejected
    detectors left out, as ``WcsStore.locator``) plus ``margin_arcsec``
    (design step 1). Where some of the visit's DiaSources lie farther out
    (on a detector without a WCS), the radius grows to enclose them too, so
    that no prediction a DiaSource could match is lost. Visits without a
    WCS keep their DiaSource footprint. Returns (per visit: whether it has a
    WCS footprint, whether its DiaSources widened it)."""
    from .visits import _unit_vectors

    n = len(visits)
    has = np.zeros(n, bool)
    widened = np.zeros(n, bool)
    if store_dir is None or not n:
        return has, widened
    paths = wcs_paths(store_dir, visits["night"])
    if not paths:
        return has, widened
    from visit_table.locate import footprints
    fp = footprints(paths, visits=np.asarray(visits["visit"], np.int64))
    fid = fp["visitId"].to_numpy()
    if not fid.size:
        return has, widened
    j = np.minimum(np.searchsorted(visits["visit"], fid), n - 1)
    hit = visits["visit"][j] == fid
    j = j[hit]
    has[j] = True
    center = visits["center"].copy()
    center[j] = _unit_vectors(fp["ra"].to_numpy()[hit], fp["dec"].to_numpy()[hit])
    radius = np.radians(fp["radius"].to_numpy()[hit] + margin_arcsec / 3600.0)
    # the farthest DiaSource of each visit from its (new) centre (the chord,
    # accurate at small angles; 1e-9 rad for rounding, as build_visits)
    starts, ends = visits["dia_start"], visits["dia_end"]
    uv = _unit_vectors(np.asarray(ra, np.float64), np.asarray(dec, np.float64))
    uv -= np.repeat(center, ends - starts, axis=0)
    chord = np.maximum.reduceat(np.linalg.norm(uv, axis=1), starts)
    dia_r = 2.0 * np.arcsin(np.minimum(chord / 2.0, 1.0)) + 1e-9
    widened[j] = dia_r[j] > radius
    visits["center"][j] = center[j]
    visits["radius"][j] = np.maximum(radius, dia_r[j])
    return has, widened


def place_rows(store, visit, ra, dec, margin_arcsec=PLACEMENT_MARGIN_ARCSEC):
    """Place each row's prediction (``ra``, ``dec`` [deg], at its visit's
    time) on the detectors of its ``visit`` (``VisitLocator.place``).
    Returns a dict of arrays: ``has_wcs``, ``detector`` (-1 without a WCS),
    ``x``, ``y``, ``edge`` [arcsec], ``jacobian`` (N, 2, 2) [px/deg, RA in
    plain degrees] and the placed detector's ``bbox`` (N, 4) (NaN without a
    WCS), and ``beyond_margin`` (bool). A visit the store doesn't have
    (KeyError) has no WCS; so has a prediction more than ``margin_arcsec``
    from every detector with a WCS (edge distance < -margin_arcsec; owner,
    2026-10-11): it is not placed, and flagged ``beyond_margin``."""
    n = len(visit)
    out = dict(has_wcs=np.zeros(n, bool), detector=np.full(n, -1, np.int64), x=np.full(n, np.nan),
               y=np.full(n, np.nan), edge=np.full(n, np.nan), jacobian=np.full((n, 2, 2), np.nan),
               bbox=np.full((n, 4), np.nan), beyond_margin=np.zeros(n, bool))
    if store is None or not n:
        return out
    visit = np.asarray(visit, np.int64)
    order = np.argsort(visit, kind="stable")
    vs = visit[order]
    cut = np.flatnonzero(np.r_[True, vs[1:] != vs[:-1], True])
    for a, b in zip(cut[:-1], cut[1:]):
        i = order[a:b]
        try:
            loc = store.locator(int(vs[a]))
        except KeyError:
            continue
        pl = loc.place(ra[i], dec[i])
        far = ~(pl.edgeDistance >= -margin_arcsec)
        out["beyond_margin"][i[far]] = True
        k, i = ~far, i[~far]
        out["has_wcs"][i] = True
        out["detector"][i] = pl.detector[k]
        out["x"][i], out["y"][i], out["edge"][i] = pl.x[k], pl.y[k], pl.edgeDistance[k]
        out["jacobian"][i] = pl.jacobian[k]
        by_id = np.argsort(loc.detector, kind="stable")
        j = by_id[np.searchsorted(loc.detector[by_id], pl.detector[k])]
        out["bbox"][i] = loc.bbox[j]
    return out


def off_detector_px(x, y, bbox):
    """How far [px] (x, y) is outside ``bbox`` (minX, minY, maxX, maxY),
    the larger of the two axes; <= 0 inside. NaN for NaN input."""
    b = np.asarray(bbox)
    return np.maximum(np.maximum(b[:, 0] - x, x - b[:, 2]), np.maximum(b[:, 1] - y, y - b[:, 3]))


def signed_bbox_px(x, y, bbox):
    """Signed distance [px] of (x, y) to the edge of ``bbox``, positive
    inside."""
    b = np.asarray(bbox)
    dx = np.minimum(x - b[:, 0], b[:, 2] - x)
    dy = np.minimum(y - b[:, 1], b[:, 3] - y)
    inside = (dx >= 0) & (dy >= 0)
    out = np.hypot(np.maximum(-dx, 0.0), np.maximum(-dy, 0.0))
    return np.where(inside, np.minimum(dx, dy), -out)


def move_pixels(x, y, edge, jacobian, bbox, ra0, dec0, ra1, dec1):
    """(x, y) and the edge distance [arcsec] of predictions placed at
    (``ra0``, ``dec0``), moved linearly to (``ra1``, ``dec1``) [deg]: (x, y)
    by ``jacobian`` @ (dRA, dDec) (RA in plain degrees, the difference
    wrapped across 0/360), and the edge distance by the change of the signed
    pixel distance to ``bbox``'s edge, at the local pixel scale (the
    Jacobian's, isotropic). Not re-placed: the detector stays. Rows that
    don't move keep their values bitwise; NaN stays NaN."""
    dra = (np.asarray(ra1, np.float64) - ra0 + 180.0) % 360.0 - 180.0
    ddec = np.asarray(dec1, np.float64) - dec0
    moved = (dra != 0.0) | (ddec != 0.0)
    x1, y1, e1 = np.array(x, np.float64), np.array(y, np.float64), np.array(edge, np.float64)
    if not moved.any():
        return x1, y1, e1
    m = np.flatnonzero(moved)
    j = jacobian[m]
    x1[m] = x1[m] + j[:, 0, 0] * dra[m] + j[:, 0, 1] * ddec[m]
    y1[m] = y1[m] + j[:, 1, 0] * dra[m] + j[:, 1, 1] * ddec[m]
    with np.errstate(invalid="ignore", divide="ignore"):
        det = np.abs(j[:, 0, 0] * j[:, 1, 1] - j[:, 0, 1] * j[:, 1, 0]) / np.cos(np.radians(dec0[m]))
        arcsec_per_px = 3600.0 / np.sqrt(det)
    b = bbox[m]
    before = signed_bbox_px(np.asarray(x)[m], np.asarray(y)[m], b)
    e1[m] = e1[m] + (signed_bbox_px(x1[m], y1[m], b) - before) * arcsec_per_px
    return x1, y1, e1


# --------------------------------------------------------------------------
# Times
# --------------------------------------------------------------------------

def header_midpoints(corr, visits):
    """Per visit (ids): the header midpoint (MJD-BEG + MJD-END)/2 [TAI MJD]
    from the correction table's exposure log (``Corrections.night_log``),
    NaN where there is none, and why there is none (a TIME_CAUSES code:
    no_table without ``corr``; outside_coverage for a night before the
    table's first; not_built for a night not built or a visit not in its
    log; omitted for a logged visit with a NaN header midpoint)."""
    visits = np.asarray(visits, np.int64)
    header = np.full(visits.size, np.nan)
    cause = np.full(visits.size, CAUSE_NO_TABLE, np.uint8)
    if corr is None or not visits.size:
        return header, cause
    day = visits // 100000
    for d in np.unique(day).tolist():
        sel = np.flatnonzero(day == d)
        if corr.first_night is None or d < corr.first_night:
            cause[sel] = CAUSE_OUTSIDE_COVERAGE
            continue
        cause[sel] = CAUSE_NOT_BUILT
        v, cols = corr.night_log(d)
        if not len(v):
            continue
        i = np.minimum(np.searchsorted(v, visits[sel]), len(v) - 1)
        hit = v[i] == visits[sel]
        h = cols["header_mid_mjd_tai"][i[hit]]
        header[sel[hit]] = h
        cause[sel[hit]] = np.where(np.isfinite(h), CAUSE_CORRECTED, CAUSE_OMITTED)
    return header, cause


def visit_time_states(t_src, starts, ends, header):
    """Per visit (its DiaSources ``t_src[starts[v]:ends[v]]``, TAI MJD,
    non-empty): VISIT_UNVERIFIED where ``header`` is NaN, VISIT_UNCORRECTED
    where every DiaSource is within MAX_HEADER_MISMATCH_S of ``header``,
    else VISIT_CORRECTED; and the largest |t_src - header| [s] (NaN where
    unverified)."""
    starts = np.asarray(starts, np.int64)
    ends = np.asarray(ends, np.int64)
    if not starts.size:
        return np.zeros(0, np.uint8), np.zeros(0)
    n = ends - starts
    if (n <= 0).any():
        raise ValueError("visit_time_states: a visit without DiaSources")
    idx = np.concatenate([np.arange(a, b) for a, b in zip(starts, ends)]) \
        if not (starts[1:] == ends[:-1]).all() else np.arange(starts[0], ends[-1])
    d = np.abs(np.asarray(t_src, np.float64)[idx] - np.repeat(header, n)) * 86400.0
    dev = np.maximum.reduceat(d, np.r_[0, np.cumsum(n)[:-1]])
    state = np.full(starts.size, VISIT_CORRECTED, np.uint8)
    with np.errstate(invalid="ignore"):
        state[dev <= MAX_HEADER_MISMATCH_S] = VISIT_UNCORRECTED
    state[~np.isfinite(header)] = VISIT_UNVERIFIED
    dev[~np.isfinite(header)] = np.nan
    return state, dev


def row_times(state, header_cause, t_visit, t_src, has_wcs, visit, detector, x, y, bbox, corr):
    """Each row's time [TAI MJD] and TIME_CAUSES code (the contract's
    "Times"), from its visit's ``state`` (visit_time_states) and
    ``header_cause`` (header_midpoints), the visit's time ``t_visit``, its
    DiaSource's ``t_src`` (NaN for a row without one), and its placement
    (``has_wcs``, ``detector``, ``x``, ``y``, ``bbox``):

    - matched rows in a VISIT_CORRECTED visit: their DiaSource's time
      ("ppdb");
    - rows of a VISIT_UNVERIFIED visit: their DiaSource's time, else the
      visit's, flagged with ``header_cause``;
    - the others (VISIT_UNCORRECTED, and unmatched rows of corrected
      visits): the corrected midpoint at (visit, detector, x, y) from
      ``corr`` (``ssp.export.submittable.Corrections``, its guard off: the
      visit test above replaces it); where there is none, the visit's time,
      flagged: no_wcs (not placed), beyond_100px, omitted, not_built,
      outside_coverage.
    """
    import pyarrow as pa

    from ..export import submittable as S

    n = len(state)
    t_src = np.asarray(t_src, np.float64)
    t = np.array(t_visit, np.float64)
    cause = np.full(n, CAUSE_CORRECTED, np.uint8)
    matched = np.isfinite(t_src)

    own = (state == VISIT_CORRECTED) & matched
    t[own] = t_src[own]
    cause[own] = CAUSE_PPDB

    unv = state == VISIT_UNVERIFIED
    t[unv & matched] = t_src[unv & matched]
    cause[unv] = header_cause[unv]

    look = ~own & ~unv
    cause[look & ~has_wcs] = CAUSE_NO_WCS
    look &= has_wcs
    if look.any():
        if corr is None:            # (unreachable: without a table every visit is unverified)
            raise ValueError("row_times: rows to look up but no correction table")
        # (a copy with its own, empty, lookup state: Corrections.lookup skips
        # rows it has looked up before; the night logs stay shared)
        corr = copy.copy(corr)
        corr.t, corr.dvis, corr.dshift = np.zeros(0), np.zeros(0), np.zeros(0)
        corr.status = np.zeros(0, np.uint8)
        i = np.flatnonzero(look)
        cand = pa.table(dict(visit=pa.array(np.asarray(visit, np.int64)[i]),
                             detector=pa.array(np.asarray(detector, np.int64)[i]),
                             x=pa.array(np.asarray(x, np.float64)[i]),
                             y=pa.array(np.asarray(y, np.float64)[i]),
                             midpointMjdTai=pa.array(t[i])))
        corr.lookup(cand, np.arange(i.size))
        st = corr.status[:i.size]
        ok = np.isin(st, S.APPLIED)
        t[i[ok]] = corr.t[:i.size][ok]
        c = np.full(i.size, CAUSE_OMITTED, np.uint8)
        c[ok] = CAUSE_CORRECTED
        c[st == S.NOT_BUILT] = CAUSE_NOT_BUILT
        c[st == S.OUTSIDE_COVERAGE] = CAUSE_OUTSIDE_COVERAGE
        with np.errstate(invalid="ignore"):
            far = (st == S.OMITTED) & (off_detector_px(np.asarray(x)[i], np.asarray(y)[i], bbox[i])
                                      > OFF_DETECTOR_OMIT_PIX)
        c[far] = CAUSE_BEYOND_100PX
        unknown = ~np.isin(st, (*S.APPLIED, S.OMITTED, S.NOT_BUILT, S.OUTSIDE_COVERAGE))
        if unknown.any():
            raise ValueError(f"row_times: unexpected correction statuses {sorted(set(st[unknown].tolist()))}")
        cause[i] = c
    return t, cause
