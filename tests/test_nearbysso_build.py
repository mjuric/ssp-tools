"""WP4: ``ssp.nearbysso.build`` (the pipeline, the slicing and the reduction).

The end-to-end tests build a small synthetic DiaSource catalog around the
predicted positions of the WP2 test orbits
(``tests/data/nearbysso_orbits.json``) and run ``build`` on it. They are
skipped unless ``SSP_ASSIST_PLANETS`` and ``SSP_ASSIST_ASTEROIDS`` are set,
and need no network.
"""

import json

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from astropy.time import Time

from ssp.nearbysso import build as B
from ssp.nearbysso import visits as V
from ssp.nearbysso._contract import (MATCH_RADIUS_ARCSEC, MATCH_RADIUS_COMET_ARCSEC, NEARBYSSO_DTYPE,
                                     ORBIT_DTYPE, VISIT_DTYPE, match_radius)

from test_nearbysso_propagate import HAVE_ASSIST, load_orbit_rows

needs_assist = pytest.mark.skipif(not HAVE_ASSIST, reason="SSP_ASSIST_PLANETS / SSP_ASSIST_ASTEROIDS not set")


def _write_dia(path, visit, t, ra, dec, sid=None):
    n = len(visit)
    sid = np.arange(1, n + 1, dtype=np.int64) if sid is None else sid
    pq.write_table(pa.table(dict(diaSourceId=np.asarray(sid, np.int64), visit=np.asarray(visit, np.int64),
                                 midpointMjdTai=np.asarray(t, float), ra=np.asarray(ra, float),
                                 dec=np.asarray(dec, float))), path, row_group_size=7)


# ---------------------------------------------------------------------------
# No ASSIST: slicing, sampling, the reduction and the writer
# ---------------------------------------------------------------------------

def test_night_ranges_unsorted(tmp_path):
    rng = np.random.default_rng(0)
    visit = np.array([2025093000001, 2025100100002, 2025093000003, 2025100100001] * 5)
    t = np.where(visit // 100000 == 20250930, 60949.0, 60950.0) + rng.uniform(0.0, 0.3, visit.size)
    perm = rng.permutation(visit.size)
    _write_dia(tmp_path / "d.parquet", visit[perm], t[perm], np.zeros(20), np.zeros(20))
    nights, tmin, tmax, n = B.night_ranges(tmp_path / "d.parquet")
    assert nights.tolist() == [20250930, 20251001]
    assert n.tolist() == [10, 10]
    for k, nt in enumerate(nights):
        s = visit // 100000 == nt
        assert tmin[k] == t[s].min() and tmax[k] == t[s].max()


def test_plan_slices_by_date():
    # day_obs across a month boundary: counted in dates, not in ids
    nights = np.array([20250929, 20250930, 20251001, 20251003])
    tmin = np.array([60948.0, 60949.0, 60950.0, 60952.0])
    tmax = tmin + 0.4
    assert [(s["night_lo"], s["night_hi"]) for s in B.plan_slices(nights, tmin, tmax, 1)] == [
        (20250929, 20250929), (20250930, 20250930), (20251001, 20251001), (20251003, 20251003)]
    assert [(s["night_lo"], s["night_hi"]) for s in B.plan_slices(nights, tmin, tmax, 2)] == [
        (20250929, 20250930), (20251001, 20251001), (20251003, 20251003)]
    assert len(B.plan_slices(nights, tmin, tmax, 30)) == 1
    s = B.plan_slices(nights, tmin, tmax, 2)[0]
    assert s["t_lo"] == 60948.0 and s["t_hi"] > 60949.4


def test_slices_never_split_a_night(tmp_path):
    """Each DiaSource is in exactly one slice, and each slice holds whole
    nights: also for a night crossing UTC midnight, and for nights whose
    time ranges overlap (the night cut after the time-bounded read)."""
    rng = np.random.default_rng(1)
    rows = []
    # night 1 crosses MJD midnight; night 2 overlaps night 3 in time
    # (odd, but possible)
    for night, lo, hi in ((20250929, 60948.9, 60949.3), (20250930, 60949.9, 60950.5),
                          (20251001, 60950.4, 60950.9)):
        for j in range(4):
            tv = rng.uniform(lo, hi)
            rows += [(night * 100000 + j + 1, tv)] * 3
    visit, t = map(np.array, zip(*rows))
    sid = rng.permutation(len(visit)) + 100
    _write_dia(tmp_path / "d.parquet", visit, t, np.zeros(len(t)), np.zeros(len(t)), sid)
    nights, tmin, tmax, _ = B.night_ranges(tmp_path / "d.parquet")
    for days in (1, 2, 30):
        seen = []
        for sl in B.plan_slices(nights, tmin, tmax, days):
            dia = B._read_slice(tmp_path / "d.parquet", sl)
            n = dia["visit"] // 100000
            assert ((n >= sl["night_lo"]) & (n <= sl["night_hi"])).all()
            for nt in np.unique(n):       # whole nights
                assert (n == nt).sum() == (visit // 100000 == nt).sum()
            seen.append(dia["diaSourceId"])
        seen = np.concatenate(seen)
        assert np.array_equal(np.sort(seen), np.sort(sid))


def test_sample_times_bracket_every_visit():
    vis = np.zeros(7, dtype=VISIT_DTYPE)
    vis["visit"] = [2025093000001, 2025093000002, 2025093000003, 2025100100001,
                    2025100300001, 2025100300002, 2025100300003]
    vis["night"] = vis["visit"] // 100000
    vis["t"] = [9400.0, 9400.1, 9400.3, 9401.2, 9403.0, 9403.0 + 1e-4, 9403.2]
    vis["center"] = [1.0, 0.0, 0.0]
    vi = V.VisitIndex(vis)
    ts = B.sample_times(vi)
    assert ts.size == 3 * vi.nights.size
    assert (np.diff(ts) > 0).all()
    for v in vis:
        k = np.flatnonzero(vi.nights == v["night"])[0]
        own = ts[3 * k:3 * k + 3]
        assert own[1] == vi.night_t[k]
        assert own[0] <= v["t"] <= own[2]
        assert own[2] - own[1] >= B.MIN_HALF_SPAN_DAYS - 1e-12   # never a repeated time


def test_nearest_tie_break():
    dia_id = np.array([5, 5, 5, 3, 3, 9])
    sep = np.array([1.0, 0.5, 0.5, 2.0, 2.0, 4.0])
    orbit = np.array([0, 7, 2, 4, 1, 3])   # orbit index order is designation order
    sel = B.nearest(dia_id, sep, orbit)
    assert dia_id[sel].tolist() == [3, 5, 9]
    assert orbit[sel].tolist() == [1, 2, 3]
    assert B.nearest(np.zeros(0, np.int64), np.zeros(0), np.zeros(0, np.int64)).size == 0


def test_distance_rank():
    """1-based per prediction, by separation, ties to the lower
    diaSourceId; in the input's order."""
    pred = np.array([4, 4, 4, 4, 1, 1, 9])
    dia_id = np.array([50, 20, 30, 10, 20, 10, 7])
    sep = np.array([1.0, 0.5, 0.5, 3.0, 2.0, 2.0, 4.9])
    r = B.distance_rank(pred, dia_id, sep)
    assert r.dtype == np.int16
    assert r.tolist() == [3, 1, 2, 4, 2, 1, 1]
    # independent of the order of the matches
    perm = np.random.default_rng(5).permutation(pred.size)
    np.testing.assert_array_equal(B.distance_rank(pred[perm], dia_id[perm], sep[perm]), r[perm])
    assert B.distance_rank(np.zeros(0, np.int64), np.zeros(0, np.int64), np.zeros(0)).dtype == np.int16
    # repeated diaSourceIds count once, at their smallest separation: A at
    # 1", its exact twin, B at 2" (rank 2, not 3); A again at 3" and C at
    # 2.5"; every copy of A gets A's rank
    pred = np.array([0, 0, 0, 0, 0])
    dia_id = np.array([7, 7, 3, 7, 5])     # A=7, B=3, C=5
    sep = np.array([1.0, 1.0, 2.0, 3.0, 2.5])
    assert B.distance_rank(pred, dia_id, sep).tolist() == [1, 1, 2, 1, 3]


def _east(ra, dec, arcsec):
    return ra + arcsec / 3600.0 / np.cos(np.radians(dec)), dec


def _rank_synth(tmp_path):
    """Two nights. Night 1, one visit: P0 (orbit 0) and P1 (orbit 1, 3"
    east of P0), and DiaSources around both, some nearer to P1. Night 2,
    one visit, one prediction (orbit 2): two DiaSources at the same place
    (a tie), one nearer, one farther, one outside the radius, and repeated
    diaSourceIds (an exact twin of the nearest, a farther copy of one of
    the tie), which count once. Returns the
    path, the predictions and the expected (diaSourceId -> (orbit, rank))."""
    ra0, dec0 = 10.0, 20.0
    v1, v2 = 2025093000001, 2025100100001
    # (diaSourceId, visit, offset east of P0 ["]; night 2: of P2)
    src = [(105, v1, 1.0),     # P0 1.0 (rank 2), P1 2.0 (rank 2): P0's
           (101, v1, 2.6),     # P0 2.6 (rank 3), P1 0.4 (rank 1): P1's
           (104, v1, -0.5),    # P0 0.5 (rank 1), P1 3.5 (rank 4): P0's
           (102, v1, -4.0),    # P0 4.0 (rank 4), P1 7.0 (out): P0's
           (103, v1, 6.0),     # P0 6.0 (out), P1 3.0 (rank 3): P1's
           (106, v1, 9.0),     # out of both
           (300, v2, 2.0), (200, v2, 2.0),   # a tie: 200 ranks 2, 300 ranks 3
           (250, v2, -1.0),    # rank 1
           (250, v2, -1.0),    #   its exact twin: counted once
           (200, v2, 4.8),     #   a farther copy of 200: counted once, at 2.0
           (150, v2, 4.5),     # rank 4
           (151, v2, 5.5)]     # out
    p = np.zeros(3, dtype=B.PRED_DTYPE)
    p["visit"] = [0, 0, 1]
    p["orbit"] = [0, 1, 2]
    p["ra"][0], p["dec"][0] = ra0, dec0
    p["ra"][1], p["dec"][1] = _east(ra0, dec0, 3.0)
    p["ra"][2], p["dec"][2] = 200.0, -30.0
    rows = []
    for sid, v, off in src:
        k = 0 if v == v1 else 2
        ra, dec = _east(p["ra"][k], p["dec"][k], off)
        rows.append((sid, v, 60949.1 if v == v1 else 60950.2, ra, dec))
    sid, visit, t, ra, dec = map(np.array, zip(*rows))
    _write_dia(tmp_path / "rank.parquet", visit, t, ra, dec, sid)
    want = {105: (0, 2), 101: (1, 1), 104: (0, 1), 102: (0, 4), 103: (1, 3),
            300: (2, 3), 200: (2, 2), 250: (2, 1), 150: (2, 4)}
    return tmp_path / "rank.parquet", p, want


def _pass3(path, preds, slice_days, read_workers, orbit_radius=None, full=False):
    """Pass 3 and the parent's reduction, as ``build`` runs them, for
    given predictions (the visits indexed over all nights in order), with
    ``orbit_radius`` [arcsec] per orbit (default: 5" for all). ``full``
    also returns the separations."""
    if orbit_radius is None:
        orbit_radius = np.full(int(preds["orbit"].max()) + 1, MATCH_RADIUS_ARCSEC)
    nights, tmin, tmax, nsrc = B.night_ranges(path)
    slices = B.plan_slices(nights, tmin, tmax, slice_days, nsrc)
    vis_parts = []
    for sl in slices:
        dia = B._read_slice(path, sl)
        u, start = np.unique(dia["visit"], return_index=True)
        v = np.zeros(u.size, dtype=VISIT_DTYPE)
        v["visit"], v["night"] = u, u // 100000
        v["dia_start"], v["dia_end"] = start, np.r_[start[1:], dia["visit"].size]
        vis_parts.append(v)
    visits = np.concatenate(vis_parts)
    vstart = np.r_[0, np.cumsum([v.size for v in vis_parts])].astype(np.int64)
    p, poff, _ = B.sort_predictions([preds.copy()], visits.size)
    B._W.update(dia_path=path, slices=slices, threads=1, visits=visits, vstart=vstart, preds=p, poff=poff,
                orbit_radius=np.asarray(orbit_radius, float))
    try:
        res = B._map(B._match_slice, [(s, s + 1) for s in range(len(slices))], read_workers, "test",
                     unit="slices")
    finally:
        B._W.clear()
    per = [r for chunk, _, _ in res for r in chunk]
    ids, k, sep, rank = (np.concatenate([r[j] for r in per]) for j in range(4))
    sel = B.nearest(ids, sep, k)
    if full:
        return ids[sel], p["orbit"][k[sel]], rank[sel], sep[sel]
    return ids[sel], p["orbit"][k[sel]], rank[sel], len(slices)


def test_rank_around_predictions(tmp_path, monkeypatch):
    """diaDistanceRank counts every DiaSource within the radius of the
    row's object's prediction, including those whose nearest object is
    another; it is the same for any slicing, workers and match batches."""
    path, preds, want = _rank_synth(tmp_path)
    ids, orbit, rank, ns = _pass3(path, preds, 30, 1)
    assert ns == 1
    assert rank.dtype == np.int16
    assert {int(i): (int(o), int(r)) for i, o, r in zip(ids, orbit, rank)} == want
    assert ids.tolist() == sorted(want)
    for days, rw in ((1, 1), (1, 2)):
        i2, o2, r2, ns = _pass3(path, preds, days, rw)
        assert ns == 2
        np.testing.assert_array_equal(i2, ids)
        np.testing.assert_array_equal(o2, orbit)
        np.testing.assert_array_equal(r2, rank)
    monkeypatch.setattr(B, "_MATCH_BATCH", 1)
    i3, o3, r3, _ = _pass3(path, preds, 30, 1)
    np.testing.assert_array_equal(i3, ids)
    np.testing.assert_array_equal(r3, rank)


# ---------------------------------------------------------------------------
# Comets and ISOs: a 15" match radius (docs/design/comet-radius.md)
# ---------------------------------------------------------------------------

def _comet_synth(tmp_path):
    """One visit, four groups of predictions far apart; orbits 0, 2, 4 are
    comets (15"), 1, 3, 5 asteroids (5").

    - C0 alone: DiaSources 3", 8", 14" and 16" east (ids 11-14): the pool
      at 15" ranks the first three 1, 2, 3; the fourth is out.
    - A1 alone: a DiaSource 10" east (id 21): out.
    - C2, with A3 6" east of it: id 31 10" east of C2 (4" from A3: A3's),
      id 32 2" west of C2 (C2's, rank 1 of C2's pool {32, 31}).
    - C4, with A5 7.5" east of it: id 41 3" east of C4 (4.5" from A5):
      C4's.
    Returns the path, the predictions, the per-orbit radii and the expected
    {diaSourceId: (orbit, rank, separation ["])}."""
    v1 = 2025093000001
    ctr = {0: (10.0, 20.0), 1: (50.0, 20.0), 2: (100.0, -10.0), 4: (150.0, 40.0)}
    p = np.zeros(6, dtype=B.PRED_DTYPE)
    p["orbit"] = np.arange(6)
    for o, (ra, dec) in ctr.items():
        p["ra"][o], p["dec"][o] = ra, dec
    p["ra"][3], p["dec"][3] = _east(*ctr[2], 6.0)
    p["ra"][5], p["dec"][5] = _east(*ctr[4], 7.5)
    src = [(11, 0, 3.0), (12, 0, 8.0), (13, 0, 14.0), (14, 0, 16.0), (21, 1, 10.0),
           (31, 2, 10.0), (32, 2, -2.0), (41, 4, 3.0)]
    sid = np.array([s_[0] for s_ in src])
    ra, dec = np.array([_east(*ctr[o], off) for _, o, off in src]).T
    _write_dia(tmp_path / "comet.parquet", np.full(sid.size, v1), np.full(sid.size, 60949.1), ra, dec, sid)
    radius = match_radius(np.array(["C/2020 A1", "2020 AA1", "P/2002 T6", "2020 AB1", "I/2017 U1",
                                    "2020 AC1"]))
    want = {11: (0, 1, 3.0), 12: (0, 2, 8.0), 13: (0, 3, 14.0), 31: (3, 1, 4.0), 32: (2, 1, 2.0),
            41: (4, 1, 3.0)}
    return tmp_path / "comet.parquet", p, radius, want


def test_comet_radius_matching(tmp_path, monkeypatch):
    """A comet matches at 10" and 14", an asteroid not at 10"; the comet's
    rank pool is its 15"; the nearest object wins by arcsec whatever the
    radii (asteroid at 4" over a comet at 10"; a comet at 3" over an
    asteroid at 4.5")."""
    path, preds, radius, want = _comet_synth(tmp_path)
    assert radius.tolist() == [15.0, 5.0, 15.0, 5.0, 15.0, 5.0]
    ids, orbit, rank, sep = _pass3(path, preds, 30, 1, orbit_radius=radius, full=True)
    got = {int(i): (int(o), int(r), round(float(s_), 3)) for i, o, r, s_ in zip(ids, orbit, rank, sep)}
    assert got == want
    # one prediction per DiaIndex call, so every batch has a single radius
    monkeypatch.setattr(B, "_MATCH_BATCH", 1)
    ids1, orbit1, rank1, sep1 = _pass3(path, preds, 30, 1, orbit_radius=radius, full=True)
    np.testing.assert_array_equal(ids1, ids)
    np.testing.assert_array_equal(orbit1, orbit)
    np.testing.assert_array_equal(rank1, rank)
    np.testing.assert_array_equal(sep1, sep)
    # all at 5": the comet rows within 5" stay, those beyond go
    ids5, orbit5, rank5, _ = _pass3(path, preds, 30, 1)
    assert dict(zip(ids5.tolist(), zip(orbit5.tolist(), rank5.tolist()))) == {
        11: (0, 1), 31: (3, 1), 32: (2, 1), 41: (4, 1)}


def test_match_per_radius_equals_filtered_single_radius(tmp_path):
    """``match_per_radius`` returns exactly what one 15" ``DiaIndex.match``
    returns, filtered to each prediction's own radius (same order, same
    floats), and its 5" predictions' matches are exactly a 5" call's."""
    rng = np.random.default_rng(5)
    n = 3000
    visit = np.repeat([2025093000001, 2025093000002], n // 2)
    ra = np.where(visit == visit[0], 30.0, 31.0) + rng.uniform(-0.01, 0.01, n)
    dec = rng.uniform(-0.01, 0.01, n)
    _write_dia(tmp_path / "d.parquet", visit, np.full(n, 60949.1), ra, dec)
    nights, tmin, tmax, nsrc = B.night_ranges(tmp_path / "d.parquet")
    (sl,) = B.plan_slices(nights, tmin, tmax, 30, nsrc)
    dia = B._read_slice(tmp_path / "d.parquet", sl)
    u, start = np.unique(dia["visit"], return_index=True)
    vis = np.zeros(u.size, dtype=VISIT_DTYPE)
    vis["visit"], vis["night"] = u, u // 100000
    vis["dia_start"], vis["dia_end"] = start, np.r_[start[1:], dia["visit"].size]
    di = V.DiaIndex(dia, vis, threads=1)
    m = 400
    vi = rng.integers(0, 2, m)
    pra = np.where(vi == 0, 30.0, 31.0) + rng.uniform(-0.01, 0.01, m)
    pdec = rng.uniform(-0.01, 0.01, m)
    rad = np.where(rng.random(m) < 0.3, MATCH_RADIUS_COMET_ARCSEC, MATCH_RADIUS_ARCSEC)
    k, row, sep = B.match_per_radius(di, vi, pra, pdec, rad)
    k15, row15, sep15 = di.match(vi, pra, pdec, MATCH_RADIUS_COMET_ARCSEC)
    keep = sep15 <= rad[k15]
    np.testing.assert_array_equal(k, k15[keep])
    np.testing.assert_array_equal(row, row15[keep])
    np.testing.assert_array_equal(sep, sep15[keep])
    assert (sep > MATCH_RADIUS_ARCSEC).any() and (rad[k] == MATCH_RADIUS_ARCSEC).any()
    a = np.flatnonzero(rad == MATCH_RADIUS_ARCSEC)
    ka, rowa, sepa = di.match(vi[a], pra[a], pdec[a], MATCH_RADIUS_ARCSEC)
    sub = rad[k] == MATCH_RADIUS_ARCSEC
    np.testing.assert_array_equal(k[sub], a[ka])
    np.testing.assert_array_equal(row[sub], rowa)
    np.testing.assert_array_equal(sep[sub], sepa)
    # a single radius: exactly one DiaIndex.match call
    for r in (MATCH_RADIUS_ARCSEC, MATCH_RADIUS_COMET_ARCSEC):
        got = B.match_per_radius(di, vi, pra, pdec, np.full(m, r))
        for x, y in zip(got, di.match(vi, pra, pdec, r)):
            np.testing.assert_array_equal(x, y)
    assert all(x.size == 0 for x in B.match_per_radius(di, vi[:0], pra[:0], pdec[:0], rad[:0]))


def test_candidate_margin_covers_comet_radius():
    """The candidate margin grows by the comet radius's extra 10": a
    stationary object 95" outside a visit's edge (beyond the 90" margin)
    is a candidate for a comet, not for an asteroid; asteroids keep
    exactly DEFAULT_CANDIDATE_MARGIN_ARCSEC."""
    from test_nearbysso_visits import great_circle_path, make_track, unit

    assert B.candidate_margin("2020 AA1") == V.DEFAULT_CANDIDATE_MARGIN_ARCSEC
    assert B.candidate_margin("A/2017 U1") == V.DEFAULT_CANDIDATE_MARGIN_ARCSEC
    for d in ("C/2020 A1", "P/2002 T6", "D/1993 F2", "I/2017 U1"):
        assert B.candidate_margin(d) == V.DEFAULT_CANDIDATE_MARGIN_ARCSEC + 10.0
    vis = np.zeros(1, dtype=VISIT_DTYPE)
    vis["night"] = 20250501
    vis["visit"] = vis["night"] * 100000
    vis["t"] = 26356.0
    vis["center"] = unit(np.array([100.0]), np.array([0.0]))
    vis["radius"] = np.radians(1.75)
    vi = V.VisitIndex(vis)
    pos = great_circle_path(np.array(100.0 + 1.75 + 95 / 3600), np.array(0.0), 90.0, 0.0, 26356.0)
    tr = make_track(pos, np.array([26356.0]), delta=1e6)
    assert list(vi.candidates(tr, B.candidate_margin("2020 AA1"))) == []
    assert list(vi.candidates(tr, B.candidate_margin("C/2020 A1"))) == [0]


def test_write_parquet_null_ssobjectid(tmp_path):
    rows = np.zeros(3, dtype=NEARBYSSO_DTYPE)
    rows["diaSourceId"] = [1, 2, 3]
    rows["designation"] = ["2007 VY347", "2025 PM", "2026 DF62"]
    rows["ssObjectId"] = [11, 0, 33]
    has = np.array([True, False, True])
    B.write_parquet(rows, has, tmp_path / "n.parquet")
    t = pq.read_table(tmp_path / "n.parquet")
    assert t.column_names == list(NEARBYSSO_DTYPE.names)
    from ssp.schema_ppdb import NearbySSODtype
    assert t.column_names == list(NearbySSODtype.names)          # the schema's order
    assert t.schema.field("diaDistanceRank").type == pa.int16()
    assert not t.schema.field("diaDistanceRank").nullable
    assert t["ssObjectId"].to_pylist() == [11, None, 33]
    assert t.schema.field("ssObjectId").type == pa.int64()
    assert t.schema.field("designation").type == pa.string()
    assert t.schema.field("ephOffset").type == pa.float32()
    assert t.schema.field("ephRa").type == pa.float64()
    assert t["designation"].to_pylist() == rows["designation"].tolist()
    # the tail angles: float32, nullable, NaN written as NULL
    for c in ("ephAntiSunPA", "ephAntiMotionPA"):
        assert t.schema.field(c).type == pa.float32() and t.schema.field(c).nullable
        assert t[c].null_count == 0
    assert not t.schema.field("ephRaErr").nullable
    rows["ephAntiSunPA"] = [1.5, np.nan, 359.0]
    B.write_parquet(rows, has, tmp_path / "n2.parquet")
    t = pq.read_table(tmp_path / "n2.parquet")
    assert t["ephAntiSunPA"].to_pylist() == [1.5, None, 359.0]


def test_ssobject_ids(tmp_path):
    pq.write_table(pa.table(dict(designation=["2025 PM", "2007 VY347", "X"], ssObjectId=[7, 8, None],
                                 other=[1, 2, 3])), tmp_path / "sso.parquet")
    desig = np.array(["2007 VY347", "2026 DF62", "2025 PM", "X"])
    ids, found = B.ssobject_ids(tmp_path / "sso.parquet", desig)
    assert found.tolist() == [True, False, True, False]
    assert ids[found].tolist() == [8, 7]
    ids, found = B.ssobject_ids(None, np.array(["2025 PM"]))
    assert not found.any()


# ---------------------------------------------------------------------------
# Rows at their own DiaSource time (docs/design/shutter-timing.md)
# ---------------------------------------------------------------------------

def _preds_moving(n, rng):
    """Predictions moving at a few deg/day anywhere on the sky, with rates
    of change."""
    p = np.zeros(n, dtype=B.PRED_DTYPE)
    p["ra"] = rng.uniform(0, 360, n)
    p["dec"] = rng.uniform(-89.9, 89.9, n)
    p["rate_ra"] = rng.normal(0, 2.0, n)          # [deg/day]
    p["rate_dec"] = rng.normal(0, 2.0, n)
    p["vmag"] = 20.0
    p["ra_err"], p["dec_err"], p["ra_dec_cov"] = 1e-4, 2e-4, -1e-9
    p["anti_sun_pa"] = rng.uniform(0, 360, n)
    p["anti_sun_pa"][:3] = 359.9999                # (wraps past 360)
    p["anti_motion_pa"] = np.nan                   # (undefined: stays NaN)
    d = np.zeros(n, dtype=B.DOT_DTYPE)
    d["ra_err_dot"], d["dec_err_dot"], d["ra_dec_cov_dot"] = 1e-3, -2e-3, 1e-8
    d["rate_ra_dot"], d["rate_dec_dot"] = rng.normal(0, 1e4, n), rng.normal(0, 1e4, n)
    d["vmag_dot"] = 1.0
    d["anti_sun_pa_dot"] = 1e2
    return p, d


def test_at_source_time_unchanged_at_the_visit_time():
    """Rows at the visit's time get the prediction and separation bitwise."""
    rng = np.random.default_rng(5)
    p, d = _preds_moving(50, rng)
    sep = rng.uniform(0, 5, 50)
    t = np.full(50, 60950.123456789)
    for dots in (d, None):
        out = B.at_source_time(p, dots, t, t.copy(), p["ra"], p["dec"], sep)
        for c in ("ra", "dec") + B._MOVED:
            np.testing.assert_array_equal(out[c], p[c])
        np.testing.assert_array_equal(out["sep"], sep)
        assert (out["dt"] == 0).all()


def test_at_source_time_moves_along_the_rates():
    """Rows at another time: moved by |rate| dt in the direction of the
    rates at dt/2 (on the sky, at any declination), the separation measured
    from there, and the other values moved by their rates of change; rows
    at the visit time in the same call are untouched."""
    from ssp.util import sky_separation_arcsec
    rng = np.random.default_rng(6)
    n = 2000
    p, d = _preds_moving(n, rng)
    t_v = np.full(n, 60950.5)
    dt = rng.uniform(-3.0, 3.0, n) / 86400.0
    dt[::5] = 0.0
    dt[1:3] = 2.0 / 86400.0                        # (359.9999 deg + 2.3e-3 deg)
    ra_s, dec_s = p["ra"] + 1e-4, p["dec"] - 1e-4
    sep0 = sky_separation_arcsec(p["ra"], p["dec"], ra_s, dec_s)
    out = B.at_source_time(p, d, t_v, t_v + dt, ra_s, dec_s, sep0)
    same = dt == 0.0
    for c in ("ra", "dec") + B._MOVED:
        np.testing.assert_array_equal(out[c][same], p[c][same])
    np.testing.assert_array_equal(out["sep"][same], sep0[same])
    m = ~same
    dt = out["dt"]
    np.testing.assert_array_equal(dt[m], (t_v + np.where(m, dt, 0.0) - t_v)[m])
    r_ra = p["rate_ra"].astype(np.float64) + d["rate_ra_dot"].astype(np.float64) * dt / 2
    r_dec = p["rate_dec"].astype(np.float64) + d["rate_dec_dot"].astype(np.float64) * dt / 2
    moved = sky_separation_arcsec(p["ra"], p["dec"], out["ra"], out["dec"])
    want = np.hypot(r_ra, r_dec) * np.abs(dt) * 3600.0
    np.testing.assert_allclose(moved[m], want[m], rtol=1e-8, atol=1e-9)
    # the direction: the east and north components of the move, on the
    # tangent plane at the prediction (gnomonic, which keeps directions)
    a0, d0 = np.radians(p["ra"]), np.radians(p["dec"])
    a1, d1 = np.radians(out["ra"]), np.radians(out["dec"])
    cosc = np.sin(d0) * np.sin(d1) + np.cos(d0) * np.cos(d1) * np.cos(a1 - a0)
    xi = np.degrees(np.cos(d1) * np.sin(a1 - a0) / cosc) * 3600.0
    eta = np.degrees((np.cos(d0) * np.sin(d1) - np.sin(d0) * np.cos(d1) * np.cos(a1 - a0)) / cosc) * 3600.0
    np.testing.assert_allclose(xi[m], (r_ra * dt * 3600.0)[m], rtol=0, atol=1e-6)
    np.testing.assert_allclose(eta[m], (r_dec * dt * 3600.0)[m], rtol=0, atol=1e-6)
    np.testing.assert_array_equal(out["sep"][m], sky_separation_arcsec(out["ra"], out["dec"], ra_s, dec_s)[m])
    assert ((out["ra"] >= 0) & (out["ra"] < 360)).all()
    # the rest, with their rates of change
    for c, want, rtol in (("ra_err", 1e-4 + 1e-3 * dt, 1e-7), ("dec_err", 2e-4 - 2e-3 * dt, 1e-7),
                          ("ra_dec_cov", -1e-9 + 1e-8 * dt, 1e-6), ("vmag", 20.0 + dt, 1e-7),
                          ("rate_ra", p["rate_ra"] + d["rate_ra_dot"].astype(np.float64) * dt, 1e-6),
                          ("rate_dec", p["rate_dec"] + d["rate_dec_dot"].astype(np.float64) * dt, 1e-6)):
        np.testing.assert_allclose(out[c][m], want[m].astype(np.float32), rtol=rtol, err_msg=c)
    pa = out["anti_sun_pa"].astype(np.float64)
    assert ((pa >= 0) & (pa < 360)).all()
    dpa = (pa - p["anti_sun_pa"] + 180.0) % 360.0 - 180.0
    np.testing.assert_allclose(dpa[m], (1e2 * dt)[m], rtol=0, atol=3e-5)
    assert (pa[1:3] < 1.0).all()
    assert np.isnan(out["anti_motion_pa"]).all()
    # without rates of change (None): moved along the rates alone
    out0 = B.at_source_time(p, None, t_v, t_v + dt, ra_s, dec_s, sep0)
    moved0 = sky_separation_arcsec(p["ra"], p["dec"], out0["ra"], out0["dec"])
    rate = np.hypot(p["rate_ra"].astype(np.float64), p["rate_dec"].astype(np.float64))
    np.testing.assert_allclose(moved0[m], (rate * np.abs(dt) * 3600.0)[m], rtol=1e-8, atol=1e-9)
    np.testing.assert_array_equal(out0["vmag"], p["vmag"])


# ---------------------------------------------------------------------------
# End to end (ASSIST)
# ---------------------------------------------------------------------------

DAY_OBS = (20250920, 20250921, 20250922)
DUP = "2007 VY347b"      # an exact copy of 2007 VY347's orbit: ties it everywhere
FIRST_LAST = "2007 VY347"   # has the first and the last visit of every night
# 2003 LN6's covariance is scaled so that its sigma (0.027-0.030" unscaled,
# growing) is 9.995" at the middle night's coarse sample: eligible on the
# first night, gated out by candidates on the last, and on the middle one
# its visits after the sample exceed 10" (rejected by the sigma cut). (2026
# DF62, a 3-day arc, is never eligible: sigma ~3400".)
WIDE = "2003 LN6"
T_END = 0.3           # [day] a night's last visit, after its first


def _mjd0(night):
    """The TAI MJD of a night's first visit."""
    return Time(f"{night // 10000}-{night // 100 % 100:02d}-{night % 100:02d}").mjd + 1.0


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


@pytest.fixture(scope="module")
def orbits(ephem):
    from ssp.ephem_assist import MJD_J2000
    from ssp.nearbysso import propagate
    rows = load_orbit_rows(ephem)
    t = np.array([Time(_mjd0(DAY_OBS[1]) + T_END / 2, format="mjd", scale="tai").tdb.mjd - MJD_J2000])
    tr = propagate.coarse(rows[WIDE], t, B.observer_at(t), ephem)
    rows[WIDE]["cov0"] *= (9.995 / tr.sigma_major[0]) ** 2
    dup = rows["2007 VY347"].copy()
    dup["designation"] = DUP
    out = np.array(sorted([*rows.values(), dup], key=lambda r: str(r["designation"])), dtype=ORBIT_DTYPE)
    return out


def _offset(ra, dec, east, north):
    """(ra, dec) [deg] moved by (east, north) [deg] on the tangent plane."""
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    c = SkyCoord(ra * u.deg, dec * u.deg).spherical_offsets_by(east * u.deg, north * u.deg)
    return float(c.ra.deg), float(c.dec.deg)


@pytest.fixture(scope="module")
def synth(tmp_path_factory, orbits, ephem):
    """Per night and orbit (but DUP), three visits:

    - two with the field centred 0.3 deg from the orbit's prediction, with
      a source 0.8" north of it (hit), a decoy 2.5" east and 200 random
      sources within 1.5 deg;
    - one at the midpoint of the night (where the coarse sample is), with
      the field centred 1 deg from the prediction on the side away from the
      geometric (coarse) position, the hit 0.8" further out, as the field's
      outermost source: the coarse position is then outside the field, by
      about its light-time offset, which only the candidate margin covers.

    FIRST_LAST also has the first and last visits of each night. Returns
    (dia path, dia DataFrame, truth: diaSourceId -> designation, kind,
    visit)."""
    from ssp.ephem_assist import compute_ephemerides_one

    rng = np.random.default_rng(42)
    d = tmp_path_factory.mktemp("synth")
    rows, truth = [], []
    sid = 1000
    fl = orbits[orbits["designation"] == FIRST_LAST][0]
    for night in DAY_OBS:
        mjd0 = _mjd0(night)
        t_end = T_END
        plan = [(fl, 0.0, "field")]
        seq = 0
        for o in orbits:
            if o["designation"] == DUP:
                continue
            for j in range(2):
                seq += 1
                plan.append((o, t_end - 0.02 * seq - 0.005 * j, "field"))   # (WIDE's late)
            plan.append((o, t_end / 2, "edge"))
        plan.append((fl, t_end, "field"))
        for vseq, (o, dt, kind) in enumerate(plan, start=1):
            visit = night * 100000 + vseq
            t = mjd0 + dt
            e = compute_ephemerides_one(str(o["designation"]), Time([t], format="mjd", scale="tai"),
                                        None, ephem, row=o)
            ra0, dec0 = float(e.ra_deg[0]) % 360.0, float(e.dec_deg[0])
            cd = np.cos(np.radians(dec0))
            if kind == "field":
                cra, cdec, rmax = ra0 + 0.3 / cd, dec0, 1.5
                src = [(ra0, dec0 + 0.8 / 3600, "hit"), (ra0 + 2.5 / 3600 / cd, dec0, "decoy")]
            else:
                g = e.xx[:, 0] - e.obs[:, 0]           # the geometric direction
                gra = np.degrees(np.arctan2(g[1], g[0]))
                gdec = np.degrees(np.arcsin(g[2] / np.linalg.norm(g)))
                u = np.array([((gra - ra0 + 180.0) % 360.0 - 180.0) * cd, gdec - dec0])
                u /= np.linalg.norm(u)                 # (east, north), towards it
                cra, cdec = _offset(ra0, dec0, -1.0 * u[0], -1.0 * u[1])
                rmax = 0.95
                src = [(*_offset(ra0, dec0, 0.8 / 3600 * u[0], 0.8 / 3600 * u[1]), "hit")]
            r = rmax * np.sqrt(rng.uniform(0, 1, 200))
            phi = rng.uniform(0, 2 * np.pi, 200)
            ccd = np.cos(np.radians(cdec))
            src += [(cra + r[k] * np.cos(phi[k]) / ccd, cdec + r[k] * np.sin(phi[k]), "bg")
                    for k in range(200)]
            for ra, dec, what in src:
                sid += 1
                rows.append((sid, visit, t, ra % 360.0, dec))
                truth.append((sid, str(o["designation"]), what, visit))
    rows.append((sid + 1, rows[0][1], rows[0][2], np.nan, 0.0))    # dropped by read_dia
    truth.append((sid + 1, "", "bad", rows[0][1]))
    dia = pd.DataFrame(rows, columns=["diaSourceId", "visit", "midpointMjdTai", "ra", "dec"])
    # A repeated diaSourceId (bad input) in another night: the first night's
    # first hit, again 1.5" from the prediction on the last night's first
    # visit. It loses to the original (0.8"), in the reduction across slices.
    hit0 = truth[0]
    assert hit0[2] == "hit"
    last_first = min(v for _, _, _, v in truth if v // 100000 == DAY_OBS[-1])
    r = next(x for x, tr in zip(rows, truth) if tr[3] == last_first and tr[2] == "hit")
    rep_row = pd.DataFrame([(hit0[0], r[1], r[2], r[3], r[4] + 0.7 / 3600)], columns=dia.columns)
    path = d / "dia.parquet"
    pq.write_table(pa.Table.from_pandas(pd.concat([dia, rep_row]).sample(frac=1.0, random_state=1),
                                        preserve_index=False), path, row_group_size=500)
    truth = pd.DataFrame(truth, columns=["diaSourceId", "designation", "kind", "visit"])
    truth = truth.set_index("diaSourceId")
    return path, dia.set_index("diaSourceId"), truth


@pytest.fixture(scope="module")
def expected(synth, orbits, ephem):
    """The expected rows, independently of build: every hit and decoy of
    an orbit whose night passes the candidates' sigma gate (the coarse
    sample at night_t) and whose own sigma at the visit (``ellipse_at``
    with the precise topocentric vector) is <= 10". Returns a DataFrame by
    diaSourceId: designation and the ellipse, and the sigma at every
    (orbit, visit) of the synthetic set."""
    from ssp.ephem_assist import compute_ephemerides_one
    from ssp.nearbysso import propagate
    from ssp.nearbysso._contract import SIGMA_MAX_ARCSEC

    path, dia, truth = synth
    vis = V.build_visits(V.read_dia(path))
    vi = V.VisitIndex(vis)
    ts = B.sample_times(vi)
    obs = B.observer_at(ts)
    by_desig = {str(o["designation"]): o for o in orbits}
    out, sig = [], []
    for (desig, visit), grp in truth[truth["kind"].isin(["hit", "decoy"])].groupby(["designation", "visit"]):
        o = by_desig[desig]
        tr = propagate.coarse(o, ts, obs, ephem)
        v = vis[vis["visit"] == visit][0]
        k = 3 * int(np.flatnonzero(vi.nights == v["night"])[0]) + 1     # the night_t sample
        assert ts[k] == vi.night_t[k // 3]
        gate = bool(tr.ok[k]) and tr.sigma_major[k] <= SIGMA_MAX_ARCSEC
        e = compute_ephemerides_one(desig, Time([v["t_tai_mjd"]], format="mjd", scale="tai"), None, ephem,
                                    row=o, obs_pos=v["obs_pos"][:, None], obs_vel=v["obs_vel"][:, None])
        ra_err, dec_err, cov, smaj = propagate.ellipse_at(tr, np.array([v["t"]]), topo_pos=e.topo_pos.T)
        sig.append((desig, visit, float(smaj[0]), gate))
        if gate and smaj[0] <= SIGMA_MAX_ARCSEC:
            for i in grp.index:
                out.append((i, desig, float(ra_err[0]), float(dec_err[0]), float(cov[0])))
    exp = pd.DataFrame(out, columns=["diaSourceId", "designation", "ra_err", "dec_err", "cov"])
    sig = pd.DataFrame(sig, columns=["designation", "visit", "sigma", "gate"])
    return exp.set_index("diaSourceId").sort_index(), sig


def _run(tmp_path, synth, orbits, name, **kw):
    out = tmp_path / f"{name}.parquet"
    rep = B.build(synth[0], orbits, out, verbose=False, **kw)
    return out, rep


@needs_assist
def test_expected_set_is_a_real_test(expected):
    """The synthetic set exercises both gates: WIDE is eligible at some
    visits and not at others, with sigma between 5" and 10" at some."""
    _, sig = expected
    w = sig[sig["designation"] == WIDE]
    ok = w["gate"] & (w["sigma"] <= 10.0)
    assert ok.any() and (~ok).any()
    assert ((w["sigma"] > 10.0) & w["gate"]).any()        # rejected at the visit, not the night
    assert ((w["sigma"] > 5.0) & ok).any()
    assert not (sig[sig["designation"] == "2026 DF62"]["gate"]).any()


@needs_assist
def test_end_to_end(tmp_path, synth, orbits, ephem, expected):
    path, dia, truth = synth
    exp, _ = expected
    sso = tmp_path / "ssobject.parquet"
    pq.write_table(pa.table(dict(designation=["2007 VY347", "2025 PM"], ssObjectId=[1234, 5678])), sso)
    out, rep = _run(tmp_path, synth, orbits, "e2e", workers=1, ssobject_path=sso)
    res = pq.read_table(out).to_pandas().set_index("diaSourceId")
    assert list(res.columns) == list(NEARBYSSO_DTYPE.names)[1:]
    assert res.index.is_monotonic_increasing and res.index.is_unique
    assert rep["output_rows"] == len(res) and rep["exceptions"] == {}
    assert rep["dia_rows_read"] == len(dia) + 1 and rep["dia_null_dropped"] == 0    # (+ the repeat)
    assert rep["dia_dropped"] == 1 and rep["dia_sources"] == len(dia)
    assert rep["counts"]["step_cap_stops"] == 0 and rep["counts"]["nights_skipped"] == 0
    assert rep["counts"]["orbits"] == orbits.size and rep["counts"]["sigma_rejected"] > 0
    assert rep["counts"]["sigma_gated_nights"] >= len(DAY_OBS)       # 2026 DF62, at least
    assert json.load(open(tmp_path / "e2e.report.json"))["output_rows"] == len(res)

    # exactly the expected rows (the duplicate orbit ties 2007 VY347 and
    # loses the tie by designation), and their ellipses
    assert list(res.index) == list(exp.index)
    assert (res["designation"] == exp["designation"]).all()
    assert DUP not in set(res["designation"])
    for col, e in (("ephRaErr", "ra_err"), ("ephDecErr", "dec_err"), ("ephRa_ephDec_Cov", "cov")):
        np.testing.assert_allclose(res[col], exp[e].astype(np.float32), rtol=1e-5, err_msg=col)
    # diaDistanceRank, by brute force: every DiaSource of the visit within
    # the radius of the row's prediction (ties to the lower diaSourceId),
    # in the input as read (with the repeated diaSourceId), counting each
    # distinct diaSourceId once, at its smallest separation
    from ssp.util import sky_separation_arcsec
    assert res["diaDistanceRank"].dtype == np.int16
    full = pq.read_table(path).to_pandas()
    for i, r in res.iterrows():
        v = truth.loc[i, "visit"]
        same = full[full["visit"] == v]
        s = sky_separation_arcsec(r["ephRa"], r["ephDec"], same["ra"].to_numpy(), same["dec"].to_numpy())
        near = s <= 5.0
        nid, ns = same["diaSourceId"].to_numpy()[near], s[near]
        best = {}
        for j, x in zip(nid.tolist(), ns.tolist()):
            best[j] = min(x, best.get(j, np.inf))
        ranked = sorted(best, key=lambda j: (best[j], j))
        assert r["diaDistanceRank"] == 1 + ranked.index(i), (i, v)
    # hits, decoys, and a decoy behind the repeated diaSourceId (a distinct
    # DiaSource of that visit, as its other copy is in another night)
    assert set(res["diaDistanceRank"]) == {1, 2, 3}
    hits = truth.loc[res.index, "kind"] == "hit"
    np.testing.assert_allclose(res.loc[hits, "ephOffset"], 0.8, atol=1e-3)
    np.testing.assert_allclose(res.loc[~hits, "ephOffset"], 2.5, atol=1e-3)
    # every night's first and last visit is found
    vis = truth.loc[res.index, "visit"]
    for night in DAY_OBS:
        v = truth["visit"][truth["visit"] // 100000 == night]
        assert v.min() in set(vis) and v.max() in set(vis)
    # ssObjectId: only for the objects with an SSObject row
    sso_id = res["ssObjectId"].to_numpy(dtype=float, na_value=np.nan)
    want = res["designation"].map({"2007 VY347": 1234.0, "2025 PM": 5678.0}).to_numpy(dtype=float)
    np.testing.assert_array_equal(sso_id, want)

    # the eph* values are SSSource's, for the same orbit and DiaSource: to
    # integrator noise (it depends on the set of times integrated through,
    # here all the candidate visits, there only the matched ones), 3.6 uas
    from ssp.sssource import WORK_DTYPE, compute_sssource_entry
    from ssp.util import observatory_barycentric_posvel
    import astropy.units as u
    mpcorb = pd.DataFrame({k: orbits[k] for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd",
                                                  "h", "g")}, index=orbits["designation"].astype(str))
    for desig, grp in res.groupby("designation"):
        ids = grp.index.to_numpy()
        de = np.zeros(len(ids), dtype=[(c, "f8") for c in ("midpointMjdTai", "ra", "dec")])
        for c in de.dtype.names:
            de[c] = dia.loc[ids, c].to_numpy()
        sss = np.zeros(len(ids), dtype=WORK_DTYPE)
        sss["designation"] = desig
        tt = Time(de["midpointMjdTai"], format="mjd", scale="tai")
        rp, vp = observatory_barycentric_posvel("X05", tt)
        assoc = np.zeros(len(ids), dtype=[("dia_index", "i8"), ("obs_pos", "f8", 3), ("obs_vel", "f8", 3)])
        assoc["dia_index"] = np.arange(len(ids))
        assoc["obs_pos"] = rp.to_value(u.au).T
        assoc["obs_vel"] = vp.to_value(u.km / u.s).T
        compute_sssource_entry(sss, assoc, mpcorb, de, ephem)
        np.testing.assert_allclose(grp["ephRa"], sss["ephRa"], rtol=0, atol=1e-9)
        np.testing.assert_allclose(grp["ephDec"], sss["ephDec"], rtol=0, atol=1e-9)
        for c in ("ephOffset", "ephRateRa", "ephRateDec"):
            np.testing.assert_allclose(grp[c], sss[c].astype(np.float32), rtol=1e-6, atol=1e-6,
                                       err_msg=f"{desig} {c}")
        # (V goes through SSSource's float32 columns: identical, as the
        # noise is far below their precision)
        np.testing.assert_array_equal(grp["ephVmag"], sss["ephVmag"], err_msg=f"{desig} ephVmag")
        # the tail angles, computed the same way (equal unless the integrator
        # noise straddles a float32 rounding boundary)
        for c in ("ephAntiSunPA", "ephAntiMotionPA"):
            got = grp[c].to_numpy()
            assert got.dtype == np.float32 and np.isfinite(got).all() and ((got >= 0) & (got < 360)).all()
            d = (got.astype(np.float64) - sss[c] + 180.0) % 360.0 - 180.0
            assert np.abs(d).max() < 1e-4, f"{desig} {c}"


@needs_assist
def test_serial_parallel_and_slice_sizes_identical(tmp_path, synth, orbits, monkeypatch):
    a, rep_a = _run(tmp_path, synth, orbits, "serial", workers=1, read_workers=1)
    assert pq.read_metadata(a).num_rows > 0
    ref = a.read_bytes()
    b, _ = _run(tmp_path, synth, orbits, "parallel", workers=3, read_workers=2, chunk_factor=2)
    b2, _ = _run(tmp_path, synth, orbits, "parallel64", workers=2, read_workers=2)   # (chunks of 1 orbit)
    assert b2.read_bytes() == ref
    assert b.read_bytes() == ref
    # one slice per night: each slice's first and last visits have rows
    # (test_end_to_end), so a slice losing either shows here
    c, rep_c = _run(tmp_path, synth, orbits, "sliced", workers=2, read_workers=3, slice_days=1)
    assert len(rep_a["slices"]) == 1 and len(rep_c["slices"]) == len(DAY_OBS)
    assert c.read_bytes() == ref
    assert rep_a["counts"] == rep_c["counts"]
    # one DiaIndex.match call per prediction
    with monkeypatch.context() as mp:
        mp.setattr(B, "_MATCH_BATCH", 1)
        d, _ = _run(tmp_path, synth, orbits, "unbatched", workers=1, slice_days=2)
    assert d.read_bytes() == ref


def test_sort_predictions():
    """Chunks in any order (the schedule permutes the orbits) come out
    sorted by (visit, orbit), with each visit's offsets."""
    rng = np.random.default_rng(3)
    p = np.zeros(300, dtype=B.PRED_DTYPE)
    p["visit"] = rng.integers(0, 7, p.size)
    p["orbit"] = rng.permutation(p.size)
    p["ra"] = rng.uniform(0, 360, p.size)
    want = p[np.lexsort((p["orbit"], p["visit"]))]
    cuts = np.sort(rng.choice(np.arange(1, p.size), 5, replace=False))
    perm = rng.permutation(p.size)
    chunks = np.split(p[perm], cuts)
    out, off, dots = B.sort_predictions(chunks, 9)
    assert np.array_equal(out, want) and dots is None
    assert chunks == []
    np.testing.assert_array_equal(off, np.searchsorted(out["visit"], np.arange(10)))
    # the rates of change, aligned with the predictions, follow them
    d = np.zeros(p.size, dtype=B.DOT_DTYPE)
    d["vmag_dot"] = p["ra"]
    chunks, dchunks = np.split(p[perm], cuts), np.split(d[perm], cuts)
    out2, off2, dots = B.sort_predictions(chunks, 9, dchunks)
    assert np.array_equal(out2, want) and dchunks == []
    np.testing.assert_array_equal(dots["vmag_dot"], want["ra"].astype(np.float32))
    with pytest.raises(ValueError):
        B.sort_predictions([p], 9, [d[:-1]])


def test_orbit_schedule():
    """The NEOs first, stably, in chunks covering every orbit once."""
    o = np.zeros(1000, dtype=ORBIT_DTYPE)
    o["q"] = np.where(np.arange(1000) % 10 == 3, 0.9, 2.5)
    o["epoch"] = 9000.0
    w = B.chunk_weights(o, 9000.0, 9365.0)
    order, chunks = B.orbit_schedule(w, 64)
    neo = np.flatnonzero(o["q"] < B.NEO_Q_AU)
    np.testing.assert_array_equal(order[:neo.size], neo)
    np.testing.assert_array_equal(np.sort(order), np.arange(1000))
    assert chunks[0][0] == 0 and chunks[-1][1] == 1000
    assert all(a[1] == b[0] for a, b in zip(chunks, chunks[1:]))
    # the NEO chunks are the small ones
    assert chunks[0][1] - chunks[0][0] < chunks[-1][1] - chunks[-1][0]


@needs_assist
def test_bad_orbits_are_counted(tmp_path, synth, orbits, expected, monkeypatch):
    """Exceptions of any type, in any stage, lose that orbit alone."""
    from ssp.nearbysso import propagate
    fail = {"coarse": ("2025 PM", FloatingPointError), "candidates": ("2025 FA22", KeyError),
            "precise": (WIDE, RuntimeError), "ellipse": ("2007 VY347", ZeroDivisionError)}
    cur = {}
    real_coarse, real_cand = propagate.coarse, V.VisitIndex.candidates
    real_eph, real_ell = B.compute_ephemerides_one, propagate.ellipse_at

    def check(stage, desig):
        if fail[stage][0] == desig:
            raise fail[stage][1]("injected")

    def coarse(orbit, *a, **k):
        cur["d"] = str(orbit["designation"])
        check("coarse", cur["d"])
        return real_coarse(orbit, *a, **k)

    def candidates(self, *a, **k):
        check("candidates", cur["d"])
        return real_cand(self, *a, **k)

    def eph(desig, *a, **k):
        check("precise", desig)
        return real_eph(desig, *a, **k)

    def ellipse_at(*a, **k):
        check("ellipse", cur["d"])
        return real_ell(*a, **k)

    monkeypatch.setattr(propagate, "coarse", coarse)
    monkeypatch.setattr(V.VisitIndex, "candidates", candidates)
    monkeypatch.setattr(B, "compute_ephemerides_one", eph)
    monkeypatch.setattr(propagate, "ellipse_at", ellipse_at)
    out, rep = _run(tmp_path, synth, orbits, "flaky", workers=2)
    assert rep["exceptions"] == {e.__name__: 1 for _, e in fail.values()}
    assert rep["counts"]["exceptions"] == len(fail)
    assert sorted(rep["failed_orbits"]["exception"]) == sorted(d for d, _ in fail.values())
    for d, e in fail.values():
        assert rep["exception_examples"][e.__name__] == [f"{d}: {e('injected')}"]
    # the rest is as without the faults, but 2007 VY347's rows go to its twin
    res = pq.read_table(out).to_pandas().set_index("diaSourceId")
    exp, _ = expected
    exp = exp[~exp["designation"].isin([d for d, _ in fail.values()]) | (exp["designation"] == "2007 VY347")]
    assert list(res.index) == list(exp.index)
    assert (res["designation"] == exp["designation"].replace("2007 VY347", DUP)).all()


@needs_assist
def test_failed_run_leaves_no_output(tmp_path, synth, orbits, monkeypatch):
    out = tmp_path / "n.parquet"
    rep = tmp_path / "n.report.json"
    out.write_text("stale")
    rep.write_text("stale")

    def boom(*a, **k):
        raise OSError("disk full")
    monkeypatch.setattr(B, "write_parquet", boom)
    with pytest.raises(OSError, match="disk full"):
        B.build(synth[0], orbits, out, verbose=False)
    assert sorted(p.name for p in tmp_path.iterdir()) == []


def _die(a, b):
    import os
    os._exit(3)


def test_dead_worker_names_the_pass():
    from concurrent.futures.process import BrokenProcessPool
    from ssp import util
    if util.fork_context() is None:
        pytest.skip("no fork")
    with pytest.raises(BrokenProcessPool, match=r"\[pass 9: test\] a worker process died"):
        util.run_chunks(_die, [(0, 1), (1, 2)], 2, "pass 9: test", unit="slices")


@needs_assist
def test_precise_pass_gets_nongrav(tmp_path, synth, orbits, monkeypatch):
    """The precise pass integrates each orbit with its own non-gravitational
    parameters (ssp.nongrav.from_orbit), and gravity-only orbits with none."""
    from ssp import nongrav
    o = orbits.copy()
    k = int(np.flatnonzero(o["designation"] == WIDE)[0])
    o["ng_model"][k] = "yarkovsky"
    o["ng_A"][k] = (0.0, -2.9e-14, 0.0)
    o["ng_fitted"][k] = (False, True, False)
    o["cov_full"][k] = 0.0
    o["cov_full"][k, :6, :6] = o["cov0"][k]
    seen = {}
    real_eph = B.compute_ephemerides_one

    def eph(desig, *a, **kw):
        seen.setdefault(desig, []).append(kw.get("nongrav"))
        return real_eph(desig, *a, **kw)

    monkeypatch.setattr(B, "compute_ephemerides_one", eph)
    _run(tmp_path, synth, o, "ng", workers=1)
    assert WIDE in seen and len(seen) > 1
    for desig, ngs in seen.items():
        for ng in ngs:
            if desig == WIDE:
                assert ng.model == "yarkovsky" and ng.A[1] == -2.9e-14
                assert ng.fitted.tolist() == [False, True, False]
            else:
                assert ng is nongrav.NONE or not ng.model


@pytest.fixture(scope="module")
def synth_own_times(tmp_path_factory, synth):
    """The synthetic set with per-source times, as shutter-corrected
    DiaSources will carry: each source moved by up to +-0.24 s from its
    visit's time (a fifth of them not at all), and the sources of 30%
    of the visits (as header-timed ones, whose corrections reach ~2 s) by up
    to +-3 s; positions unchanged."""
    t = pq.read_table(synth[0])
    rng = np.random.default_rng(11)
    dt = rng.uniform(-0.24, 0.24, t.num_rows)
    dt[rng.uniform(size=t.num_rows) < 0.2] = 0.0
    visit = t["visit"].to_numpy()
    uv = np.unique(visit)
    wide = np.isin(visit, uv[rng.uniform(size=uv.size) < 0.3])
    dt[wide] = rng.uniform(-3.0, 3.0, int(wide.sum()))
    dt[np.argmax(dt)] = 3.0
    tt = t["midpointMjdTai"].to_numpy() + dt / 86400.0
    t = t.set_column(t.schema.get_field_index("midpointMjdTai"), "midpointMjdTai", pa.array(tt))
    out = tmp_path_factory.mktemp("own") / "dia.parquet"
    pq.write_table(t, out, row_group_size=500)
    return out


@needs_assist
def test_rows_at_their_own_time(tmp_path, synth, synth_own_times, orbits, ephem):
    """Sources with their own times: the same rows (the match is decided at
    the visit's time), each row's published values evaluated at its own
    DiaSource's time, as an exact evaluation there gives them; and the same
    output for any workers and slicing."""
    from ssp import nongrav
    from ssp.ephem_assist import MJD_J2000, compute_ephemerides_one, tail_position_angles
    from ssp.nearbysso import propagate
    from ssp.photfit import hg_V_mag
    from ssp.util import sky_separation_arcsec

    ref = pq.read_table(_run(tmp_path, synth, orbits, "ref", workers=1)[0]).to_pandas()
    ref = ref.set_index("diaSourceId")
    out, rep = _run(tmp_path, (synth_own_times,), orbits, "own", workers=1)
    res = pq.read_table(out).to_pandas().set_index("diaSourceId")
    out2, _ = _run(tmp_path, (synth_own_times,), orbits, "own2", workers=2, read_workers=2, slice_days=1)
    assert out2.read_bytes() == out.read_bytes()
    assert list(res.index) == list(ref.index)
    np.testing.assert_array_equal(res["designation"], ref["designation"])
    np.testing.assert_array_equal(res["diaDistanceRank"], ref["diaDistanceRank"])

    dia = pq.read_table(synth_own_times).to_pandas()
    vis = V.build_visits(V.read_dia(synth_own_times))
    ts = B.sample_times(V.VisitIndex(vis))
    obs = B.observer_at(ts)
    by_desig = {str(o["designation"]): o for o in orbits}
    # (the repeated diaSourceId loses to its first copy, as in test_end_to_end)
    first = dia.sort_values("visit", kind="stable").drop_duplicates("diaSourceId").set_index("diaSourceId")
    n_moved, max_move, rate_change = 0, 0.0, 0.0
    for desig, grp in res.groupby("designation"):
        o = by_desig[desig]
        src = first.loc[grp.index]
        tt = Time(src["midpointMjdTai"].to_numpy(), format="mjd", scale="tai")
        e = compute_ephemerides_one(desig, tt, None, ephem, row=o, nongrav=nongrav.from_orbit(o))
        ra = e.ra_deg % 360.0
        moved = sky_separation_arcsec(ref.loc[grp.index, "ephRa"].to_numpy(),
                                      ref.loc[grp.index, "ephDec"].to_numpy(), grp["ephRa"].to_numpy(),
                                      grp["ephDec"].to_numpy())
        n_moved += int((moved > 0).sum())
        max_move = max(max_move, float(moved.max()))
        # the position: to the integrator noise of the two evaluations (see
        # test_end_to_end), against moves of up to tens of mas
        err = sky_separation_arcsec(ra, e.dec_deg, grp["ephRa"].to_numpy(), grp["ephDec"].to_numpy())
        assert err.max() < 2e-5, (desig, err.max())
        off = sky_separation_arcsec(ra, e.dec_deg, src["ra"].to_numpy(), src["dec"].to_numpy())
        np.testing.assert_allclose(grp["ephOffset"], off, rtol=0, atol=2e-5, err_msg=desig)
        # the ellipse, as ellipse_at gives it at the row's time
        tr = propagate.coarse(o, ts, obs, ephem)
        ell = propagate.ellipse_at(tr, tt.tdb.mjd - MJD_J2000, topo_pos=e.topo_pos.T)
        for c, x in zip(("ephRaErr", "ephDecErr", "ephRa_ephDec_Cov"), ell[:3]):
            np.testing.assert_allclose(grp[c], x.astype(np.float32), rtol=2e-7, err_msg=f"{desig} {c}")
        # the rates, V and the tail angles: to float32 rounding
        for c, x in (("ephRateRa", e.mu_lon), ("ephRateDec", e.mu_lat)):
            np.testing.assert_allclose(grp[c], x.astype(np.float32), rtol=3e-7, err_msg=f"{desig} {c}")
        helio, topo = e.helio_pos.astype(np.float32), e.topo_pos.astype(np.float32)
        v = hg_V_mag(e.H, e.G, np.sqrt((helio ** 2).sum(0)), np.sqrt((topo ** 2).sum(0)), e.phase_angle)
        assert (np.abs(grp["ephVmag"] - v) <= 2 * np.spacing(grp["ephVmag"].to_numpy())).all(), desig
        for c, x in zip(("ephAntiSunPA", "ephAntiMotionPA"),
                        tail_position_angles(e.helio_pos, e.helio_vel, e.topo_pos)):
            got = grp[c].to_numpy()
            d = (got.astype(np.float64) - x + 180.0) % 360.0 - 180.0
            assert (np.abs(d) <= np.spacing(got)).all(), (desig, c, np.abs(d).max())
        rel = grp["ephRateDec"] / ref.loc[grp.index, "ephRateDec"] - 1.0
        rate_change = max(rate_change, float(np.abs(rel).max()))
    # a real test: most rows moved, some by more than the tolerances above
    st = rep["source_times"]
    assert n_moved > 0.6 * len(res) and max_move > 1e-3, (n_moved, len(res), max_move)
    assert rate_change > 1e-5
    # (the report's moves are from the visit-level prediction, at the visit's
    # median time; the moves above, from the original visit time)
    assert st["rows_at_own_time"] > 0.6 * len(res) and st["rows_beyond_max_dt"] == 0
    assert 2.0 < st["max_abs_dt_s"] <= 3.0 + 1e-6 and st["max_move_arcsec"] > 1e-3
