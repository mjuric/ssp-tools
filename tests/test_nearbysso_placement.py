"""NearbySSO rows on detectors, and their times (WP U2 of
docs/design/nearbysso-unmatched.md): the per-visit uncorrected/corrected
test of the PPDB's times, each row's time and ``midpointMjdTai_flag`` by
cause, visits without a WCS, and the pixel correction.

The matching pass (``build._match_slice``) runs on given predictions (no
ASSIST) against a synthetic WCS store (cubic models, written in
visit-table's ``wcs/day_obs=<night>.parquet`` layout) and a format-2 copy
of the read-only fixture correction table (as tests/test_submittable_shutter.py
converts it). Skipped without the fixture or shutter_timing. The rates of
change at every candidate visit are checked on the synthetic orbits of
tests/test_nearbysso_build.py (needs ASSIST).
"""

import json
import os
import shutil
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp.nearbysso import build as B
from ssp.nearbysso import placement as P
from ssp.nearbysso._contract import MATCH_RADIUS_ARCSEC, VISIT_DTYPE

pytest.importorskip("shutter_timing.corrections")
pytest.importorskip("visit_table.locate")
from visit_table.locate import POWERS, WcsStore, gnomonic

FIXTURE = Path(os.environ.get("SSP_SHUTTER_FIXTURE",
                              "/sdf/data/rubin/user/mjuric/shutter-timing/fixtures/correction-table-v1"))
needs_fixture = pytest.mark.skipif(not (FIXTURE / "table" / "manifest.json").exists(),
                                   reason=f"no fixture correction table at {FIXTURE}")

# The fixture table's visits (its README): header midpoints [MJD TAI]
V_OK = 2026010500249           # logged, built: corrected times
V_OK2 = 2026010500250          # logged, built
V_FAILED = 2026010500251       # logged with a NaN header midpoint (corrupt raw)
V_ABSENT = 2026010500999       # night built, visit not in its log
V_NOWCS = 2026071200100        # logged, built; we give it no WCS
V_OUTSIDE = 2025070100001      # before the table's first night (20250810)
HEADER = {V_OK: 61046.25468065427, V_OK2: 61046.25486987154, V_NOWCS: 61233.98278583563}

S = 1.0 / 86400.0
ARCSEC = 1.0 / 3600.0
PIX_PER_DEG = 18000.0          # 0.2"/px
BBOX = (-0.5, -0.5, 4071.5, 3999.5)
SCALE = 0.1                    # [deg] the models' normalization
DET_SPACING = 0.232            # [deg] between detector centres (gaps of ~21", ~100 px)
DETECTORS = (94, 95, 96)


# ---------------------------------------------------------------------------
# A synthetic WCS store
# ---------------------------------------------------------------------------

def _inv_gnomonic(xi, eta, ra0, dec0):
    xi, eta, ra0, dec0 = map(np.radians, (xi, eta, ra0, dec0))
    z = np.sin(dec0) + eta * np.cos(dec0)
    w = np.cos(dec0) - eta * np.sin(dec0)
    return np.degrees(ra0 + np.arctan2(xi, w)) % 360.0, np.degrees(np.arctan2(z, np.hypot(xi, w)))


def _coeffs(k):
    """Cubic coefficients (x, y) in POWERS order: a centred, slightly
    rotated, sheared and curved 0.2"/px detector (k varies them)."""
    cx, cy = np.zeros(len(POWERS)), np.zeros(len(POWERS))
    t = {pw: i for i, pw in enumerate(POWERS)}
    lin = SCALE * PIX_PER_DEG
    cx[t[0, 0]], cy[t[0, 0]] = (BBOX[0] + BBOX[2]) / 2, (BBOX[1] + BBOX[3]) / 2
    cx[t[1, 0]], cx[t[0, 1]] = lin, 0.01 * lin * (1 + k)
    cy[t[1, 0]], cy[t[0, 1]] = -0.012 * lin, lin * (1.0 + 0.002 * k)
    cx[t[2, 0]], cx[t[1, 1]], cy[t[0, 2]], cx[t[3, 0]] = 6.0, 3.0, -4.0, 1.5
    return cx, cy


def _terms(u, v):
    return np.stack([u ** i * v ** j for i, j in POWERS], axis=-1)


class Visit:
    """Three detectors in a row at (ra0, dec0)."""

    def __init__(self, visit, ra0, dec0, detectors=DETECTORS):
        self.visit, self.ra0, self.dec0 = visit, ra0, dec0
        self.det = {}
        for k, d in enumerate(detectors):
            tra, tdec = _inv_gnomonic((k - 1) * DET_SPACING, 0.0, ra0, dec0)
            self.det[d] = (float(tra), float(tdec), *_coeffs(k))

    def sky2pix(self, d, ra, dec):
        tra, tdec, cx, cy = self.det[d]
        xi, eta = gnomonic(np.asarray(ra, float), np.asarray(dec, float), tra, tdec)
        t = _terms(xi / SCALE, eta / SCALE)
        return t @ cx, t @ cy

    def pix2sky(self, d, x, y):
        tra, tdec, cx, cy = self.det[d]
        x, y = np.atleast_1d(np.asarray(x, float)), np.atleast_1d(np.asarray(y, float))
        u = (x - cx[0]) / cx[4]
        v = (y - cy[0]) / cy[1]
        for _ in range(30):
            t = _terms(u, v)
            fx, fy = t @ cx - x, t @ cy - y
            h = 1e-7
            tu, tv = _terms(u + h, v), _terms(u, v + h)
            a, b = (tu @ cx - t @ cx) / h, (tv @ cx - t @ cx) / h
            c, e = (tu @ cy - t @ cy) / h, (tv @ cy - t @ cy) / h
            det = a * e - b * c
            u, v = u - (e * fx - b * fy) / det, v - (-c * fx + a * fy) / det
        return _inv_gnomonic(u * SCALE, v * SCALE, tra, tdec)

    def rows(self, rejected=False):
        out = []
        for d, (tra, tdec, cx, cy) in self.det.items():
            bx = [BBOX[0], BBOX[2], BBOX[2], BBOX[0]]
            by = [BBOX[1], BBOX[1], BBOX[3], BBOX[3]]
            ra, dec = self.pix2sky(d, bx, by)
            out.append(dict(visitId=self.visit, detector=d, bbox=list(BBOX),
                            corners=np.column_stack([ra, dec]).ravel().tolist(), tangent=[tra, tdec],
                            scale=SCALE, cx=cx.tolist(), cy=cy.tolist(), rejected=rejected))
        return out


def write_store(path, visits, snapshot=7, rejected=()):
    """The store of ``visits``, with every detector of the visits in
    ``rejected`` rejected."""
    path.mkdir(parents=True, exist_ok=True)
    by_night = {}
    for v in visits:
        by_night.setdefault(v.visit // 100000, []).extend(v.rows(rejected=v.visit in rejected))
    schema = pa.schema([("visitId", pa.int64()), ("detector", pa.int64()),
                        ("bbox", pa.list_(pa.field("element", pa.float64()), 4)),
                        ("corners", pa.list_(pa.float64())), ("tangent", pa.list_(pa.float64())),
                        ("scale", pa.float64()), ("cx", pa.list_(pa.float64())),
                        ("cy", pa.list_(pa.float64())),
                        ("rejected", pa.bool_())])
    for night, rows in by_night.items():
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path / f"day_obs={night}.parquet")
    state = dict(modelVersion="cubic-gnomonic-1", formatVersion=1,
                 nights={str(n): dict(fromSnapshot=snapshot - (i % 2)) for i, n in enumerate(by_night)})
    (path / "_state.json").write_text(json.dumps(state))
    return path


# ---------------------------------------------------------------------------
# The correction table and the matching pass
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def table(tmp_path_factory):
    if not (FIXTURE / "table" / "manifest.json").exists():
        pytest.skip(f"no fixture correction table at {FIXTURE}")
    from shutter_timing.table import convert

    d = tmp_path_factory.mktemp("ct")
    v1 = d / "v1"
    shutil.copytree(FIXTURE / "table", v1)
    v1.chmod(0o755)
    for p in v1.iterdir():
        p.chmod(0o644)
    convert(v1, d / "table")
    return d / "table"


def corrected(table, visit, det, x, y):
    from shutter_timing.corrections import corrected_midpoints
    r = corrected_midpoints(np.atleast_1d(visit), np.atleast_1d(det), np.atleast_1d(x), np.atleast_1d(y),
                            table_dir=table, require_uniform=True)
    return r.t_mid_mjd_tai, r.status


def _north(ra, dec, arcsec):
    return ra, dec + arcsec * ARCSEC


def run_pass3(tmp_path, dia, preds, store, table, dots=None, name="dia"):
    """``build._match_slice`` on the DiaSources ``dia`` (rows of (id, visit,
    t, ra, dec, detector)) in one slice, for ``preds`` (PRED_DTYPE, their
    ``visit`` the index of the sorted distinct visits); then the parent's
    reduction. Returns (ids, pub, place, visit stats, visits)."""
    from ssp.export.submittable import Corrections

    path = tmp_path / f"{name}.parquet"
    sid, visit, t, ra, dec, det = map(np.array, zip(*dia))
    pq.write_table(pa.table(dict(diaSourceId=sid.astype(np.int64), visit=visit.astype(np.int64),
                                 midpointMjdTai=t.astype(float), ra=ra.astype(float), dec=dec.astype(float),
                                 detector=det.astype(np.int64))), path)
    nights, tmin, tmax, nsrc = B.night_ranges(path)
    slices = B.plan_slices(nights, tmin, tmax, 10000, nsrc)
    assert len(slices) == 1
    d = B._read_slice(path, slices[0])
    u, start = np.unique(d["visit"], return_index=True)
    vis = np.zeros(u.size, dtype=VISIT_DTYPE)
    vis["visit"], vis["night"] = u, u // 100000
    vis["dia_start"], vis["dia_end"] = start, np.r_[start[1:], d["visit"].size]
    vis["t_tai_mjd"] = [np.median(d["midpointMjdTai"][a:b]) for a, b in zip(vis["dia_start"], vis["dia_end"])]
    p, poff, dd = B.sort_predictions([preds.copy()], vis.size,
                                     None if dots is None else [dots.copy()])
    corr = None if table is None else Corrections(table, guard=None)
    B._W.update(dia_path=path, slices=slices, threads=1, visits=vis, vstart=np.array([0, vis.size]), preds=p,
                poff=poff, orbit_radius=np.full(int(p["orbit"].max()) + 1, MATCH_RADIUS_ARCSEC), dots=dd,
                dia_extra=("detector",), corrections=corr, wcs_store=None if store is None else str(store))
    try:
        (res,), _, _ = B._match_slice(0, 1)
    finally:
        B._W.clear()
    ids, k, sep, rank, _, _, pub, place, vstats = res
    o = np.argsort(ids, kind="stable")
    return ids[o], pub[o], place[o], vstats, vis, p[k[o]]


# ---------------------------------------------------------------------------
# Unit tests: the visit test, no table
# ---------------------------------------------------------------------------

def test_visit_time_states():
    """Uncorrected iff every DiaSource is within 1 ms of the header midpoint
    (inclusive); NaN header: unverified."""
    h = np.array([100.0, 100.0, 100.0, np.nan, 200.0])
    t = np.array([100.0, 100.0 + 0.9e-3 * S,         # visit 0: uncorrected
                  100.0, 100.0 - 1.1e-3 * S,         # visit 1: one source 1.1 ms off
                  100.0 + 0.2 * S,                   # visit 2: shutter-corrected
                  50.0, 50.0,                        # visit 3: no header
                  200.0 + 0.999e-3 * S])             # visit 4: just within 1 ms
    starts = np.array([0, 2, 4, 5, 7])
    ends = np.array([2, 4, 5, 7, 8])
    state, dev = P.visit_time_states(t, starts, ends, h)
    assert state.tolist() == [P.VISIT_UNCORRECTED, P.VISIT_CORRECTED, P.VISIT_CORRECTED, P.VISIT_UNVERIFIED,
                              P.VISIT_UNCORRECTED]
    np.testing.assert_allclose(dev[:3], [0.9e-3, 1.1e-3, 0.2], rtol=1e-4)
    assert np.isnan(dev[3])


def test_omit_limit_matches_shutter_timing():
    from shutter_timing.corrections import OFF_DETECTOR_OMIT_PIX
    assert P.OFF_DETECTOR_OMIT_PIX == OFF_DETECTOR_OMIT_PIX


def test_wcs_snapshot(tmp_path):
    store = write_store(tmp_path / "wcs", [Visit(V_OK, 150.0, -10.0), Visit(V_OUTSIDE, 20.0, 5.0)],
                        snapshot=9)
    assert P.wcs_snapshot(store) == 9
    assert P.wcs_snapshot(tmp_path / "nowhere") is None


# ---------------------------------------------------------------------------
# The matching pass with a WCS store and the correction table
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def scene(tmp_path_factory):
    """Visits, the WCS store (no WCS for V_NOWCS), DiaSources and their
    predictions. Each DiaSource is 0.5" north of its prediction; every
    prediction moves fast (to make the moves measurable)."""
    d = tmp_path_factory.mktemp("scene")
    vis = {V_OUTSIDE: Visit(V_OUTSIDE, 20.0, 5.0), V_OK: Visit(V_OK, 150.0, -10.0),
           V_OK2: Visit(V_OK2, 150.5, -10.0), V_FAILED: Visit(V_FAILED, 151.0, -10.0),
           V_ABSENT: Visit(V_ABSENT, 151.5, -10.0), V_NOWCS: Visit(V_NOWCS, 300.0, 30.0)}
    store = write_store(d / "wcs", [v for k, v in vis.items() if k != V_NOWCS])
    t_vis = {V_OUTSIDE: 60857.1, V_OK: HEADER[V_OK], V_OK2: HEADER[V_OK2], V_FAILED: 61046.26,
             V_ABSENT: 61046.27, V_NOWCS: HEADER[V_NOWCS]}
    # (name, visit, detector, x, y, dt [s] of the DiaSource from its visit)
    gap_x = BBOX[2] + 52.0          # between 94 and 95, ~52 px off each
    plan = [("outside", V_OUTSIDE, 95, 1500.0, 1800.0, 0.0),
            ("on", V_OK, 94, 1000.0, 1500.0, 0.0),
            ("gap", V_OK, 94, gap_x, 2500.0, 0.0),
            ("far", V_OK, 96, BBOX[2] + 600.0, 2000.0, 0.0),      # ~120" off: beyond the margin
            ("near", V_OK, 96, BBOX[2] + 125.0, 2000.0, 0.0),     # ~25": in it, > 100 px off
            ("edge", V_OK, 95, 3.0, 2000.0, 0.0),
            ("ppdb1", V_OK2, 94, 2000.0, 2000.0, 0.0),
            ("ppdb2", V_OK2, 95, 500.0, 3000.0, 0.0),
            ("failed", V_FAILED, 95, 2000.0, 2000.0, 0.0),
            ("early", V_ABSENT, 94, 3000.0, 1000.0, -0.3),
            ("mid", V_ABSENT, 95, 2000.0, 20.0, 0.0),
            ("late", V_ABSENT, 96, 4060.0, 3990.0, 0.3),
            ("nowcs", V_NOWCS, 94, 2000.0, 2000.0, 0.0)]
    order = sorted(vis)
    p = np.zeros(len(plan), dtype=B.PRED_DTYPE)
    dia, names = [], []
    for i, (name, v, det, x, y, dt) in enumerate(plan):
        ra, dec = vis[v].pix2sky(det, x, y)
        p["visit"][i], p["orbit"][i] = order.index(v), i
        p["ra"][i], p["dec"][i] = ra[0], dec[0]
        p["rate_ra"][i], p["rate_dec"][i] = 150.0, -90.0     # [deg/day]: 2"/s, 1"/s
        p["ra_err"][i] = p["dec_err"][i] = 1e-5
        sra, sdec = _north(ra[0], dec[0], 0.5)
        dia.append((1000 + i, v, t_vis[v] + dt * S, sra, sdec, det))
        names.append(name)
    return dict(vis=vis, store=store, dia=dia, preds=p, names=names, t_vis=t_vis)


def _by_name(scene, ids, arr):
    return {scene["names"][int(i) - 1000]: a for i, a in zip(ids, arr)}


def _cause(c):
    return P.TIME_CAUSES[int(c)]


@needs_fixture
def test_times_and_causes(tmp_path, scene, table):
    """Each row's time and flag by cause; times bit-identical to
    corrected_midpoints at the placement's pixels; the visit statistics."""
    ids, pub, place, vstats, vis, p = run_pass3(tmp_path, scene["dia"], scene["preds"], scene["store"], table)
    assert len(ids) == len(scene["dia"])
    pl = _by_name(scene, ids, place)
    pp = _by_name(scene, ids, p)
    want = dict(outside="outside_coverage", on="corrected", gap="corrected", far="no_wcs",
                near="beyond_100px",
                edge="corrected", ppdb1="corrected", ppdb2="corrected", failed="omitted", early="not_built",
                mid="not_built", late="not_built", nowcs="no_wcs")
    assert {n: _cause(r["time_cause"]) for n, r in pl.items()} == want
    for n, r in pl.items():
        assert r["midpointMjdTai_flag"] == (want[n] not in ("corrected", "ppdb")), n
    # times: the corrected midpoint at the visit-time placement, bitwise
    for n in ("on", "gap", "edge", "ppdb1", "ppdb2"):
        r = pl[n]
        loc = WcsStore([str(scene["store"] / "day_obs=20260105.parquet")]).locator(int(r["visit"]))
        q = loc.place(pp[n]["ra"][None], pp[n]["dec"][None])
        t, st = corrected(table, r["visit"], q.detector, q.x, q.y)
        assert r["detector"] == q.detector[0]
        assert st[0] in (0, 1) and r["midpointMjdTai"] == t[0], n
        assert r["midpointMjdTai"] != scene["t_vis"][int(r["visit"])], n      # (a real correction)
    assert pl["gap"]["eph_x"] > BBOX[2] or pl["gap"]["eph_x"] < BBOX[0]      # off its detector
    t, st = corrected(table, V_OK, pl["gap"]["detector"], pl["gap"]["eph_x"], pl["gap"]["eph_y"])
    assert st[0] == 1                                                          # DEGRADED, not flagged
    # the flagged: the visit's time, or (unverified) the DiaSource's
    for n in ("far", "near", "nowcs", "outside", "failed", "mid"):
        assert pl[n]["midpointMjdTai"] == scene["t_vis"][int(pl[n]["visit"])], n
    for n, dt in (("early", -0.3), ("late", 0.3)):
        assert pl[n]["midpointMjdTai"] == scene["t_vis"][V_ABSENT] + dt * S
    # the visits
    assert vstats["visits"] == 6 and vstats["uncorrected"] == 3 and vstats["corrected"] == 0
    assert vstats["unverified"] == dict(outside_coverage=1, not_built=1, omitted=1)
    assert vstats["without_wcs"] == 1 and vstats["wcs_files"] == 2


@needs_fixture
def test_no_wcs_visit(tmp_path, scene, table):
    """A visit without a WCS: its DiaSource's detector, NaN placement, the
    visit's time, flagged no_wcs; the same with no store at all, where
    every row is unplaced."""
    for store in (scene["store"], None):
        ids, pub, place, vstats, _, _ = run_pass3(tmp_path, scene["dia"], scene["preds"], store, table)
        pl = _by_name(scene, ids, place)
        r = pl["nowcs"]
        assert not r["has_wcs"] and r["detector"] == r["dia_detector"] == 94
        assert np.isnan(r["eph_x"]) and np.isnan(r["eph_y"]) and np.isnan(r["ephDetectorEdgeDist"])
        assert _cause(r["time_cause"]) == "no_wcs" and r["midpointMjdTai_flag"]
        assert r["midpointMjdTai"] == HEADER[V_NOWCS]
        if store is None:
            assert not place["has_wcs"].any()
            np.testing.assert_array_equal(place["detector"], place["dia_detector"])
            assert all(_cause(c) in ("no_wcs", "outside_coverage", "not_built", "omitted")
                       for c in place["time_cause"])
            assert vstats["without_wcs"] == 6


@needs_fixture
def test_fabricated_corrected_times(tmp_path, scene, table):
    """The visit test on a copy of the input whose DiaSource times in V_OK2
    are our table's corrected times at their pixels: that visit is
    corrected, its rows take their DiaSource's own times ("ppdb", not
    flagged), and those agree with the uncorrected input's lookups at the
    predicted pixels (0.5" away) to well within a millisecond. Other
    visits are unaffected."""
    ids0, _, place0, vstats0, _, _ = run_pass3(tmp_path, scene["dia"], scene["preds"], scene["store"], table)
    dia = []
    for row in scene["dia"]:
        sid, v, t, ra, dec, det = row
        if v == V_OK2:
            x, y = scene["vis"][v].sky2pix(det, ra, dec)
            tc, st = corrected(table, v, det, x, y)
            assert st[0] == 0
            row = (sid, v, float(tc[0]), ra, dec, det)
        dia.append(row)
    ids, pub, place, vstats, _, _ = run_pass3(tmp_path, dia, scene["preds"], scene["store"], table,
                                              name="corr")
    assert vstats["corrected"] == 1 and vstats["uncorrected"] == 2
    assert vstats["corrected_first"][0] == V_OK2 and vstats["corrected_first"][1] > 1e-3
    pl, pl0 = _by_name(scene, ids, place), _by_name(scene, ids0, place0)
    src = {s: t for s, _, t, *_ in dia}
    for n in ("ppdb1", "ppdb2"):
        assert _cause(pl[n]["time_cause"]) == "ppdb" and not pl[n]["midpointMjdTai_flag"]
        assert pl[n]["midpointMjdTai"] == src[1000 + scene["names"].index(n)]
        assert abs(pl[n]["midpointMjdTai"] - pl0[n]["midpointMjdTai"]) < 1e-4 * S
        assert pl[n]["midpointMjdTai"] != pl0[n]["midpointMjdTai"]        # (two routes, not one)
    for n in pl:
        if n not in ("ppdb1", "ppdb2"):
            assert pl[n].tobytes() == pl0[n].tobytes(), n


@needs_fixture
def test_pixel_correction(tmp_path, scene, table):
    """eph_x/eph_y and the edge distance, moved linearly to the row's time,
    against a fresh placement at the moved position (same detector)."""
    ids, pub, place, _, _, p = run_pass3(tmp_path, scene["dia"], scene["preds"], scene["store"], table)
    stores = {n: WcsStore([str(f)]) for n, f in ((n, scene["store"] / f"day_obs={n}.parquet")
                                                for n in (20250701, 20260105))}
    n_moved = n_big = n_near = 0
    for i in range(len(ids)):
        r = place[i]
        if not r["has_wcs"]:
            continue
        loc = stores[int(r["visit"]) // 100000].locator(int(r["visit"]))
        q = loc.place(pub["ra"][i:i + 1], pub["dec"][i:i + 1])
        q0 = loc.place(p["ra"][i:i + 1], p["dec"][i:i + 1])
        move = np.hypot(q.x[0] - q0.x[0], q.y[0] - q0.y[0])
        assert q.detector[0] == r["detector"]
        assert abs(r["eph_x"] - q.x[0]) < 1e-3 and abs(r["eph_y"] - q.y[0]) < 1e-3, (i, move)
        # the edge distance follows the bbox's edge (the true one), place()'s
        # the polygon's (straight between the corners): near an edge they
        # differ by its bow, <= 0.025" (visit-table); deep inside, where the
        # nearest bbox edge may not be the polygon's, by up to twice the move
        # (0.2"/px). (+ float32 rounding)
        err = abs(r["ephDetectorEdgeDist"] - q.edgeDistance[0])
        tol = 0.025 if abs(q.edgeDistance[0]) < 100.0 else 0.025 + 2 * move * 0.2
        assert err < tol, (i, err, move)
        change = abs(q.edgeDistance[0] - q0.edgeDistance[0])
        assert abs(q.edgeDistance[0]) > 100.0 or move < 1.0 or err < 0.1 * change
        n_near += abs(q.edgeDistance[0]) < 100.0 and move > 5.0
        np.testing.assert_allclose(r["move_px"], move, rtol=1e-3, atol=1e-6)
        if pub["dt"][i] == 0.0:
            assert r["eph_x"] == q0.x[0] and r["eph_y"] == q0.y[0]
        else:
            n_moved += 1
            n_big += move > 5.0
    assert n_moved >= 6 and n_big >= 2 and n_near >= 1


def test_move_pixels_wraps_ra():
    """The RA difference is wrapped across 0/360."""
    jac = np.array([[[100.0, 0.0], [0.0, 100.0]]] * 2)
    bbox = np.array([BBOX] * 2)
    x, y, e = P.move_pixels(np.array([10.0, 10.0]), np.array([20.0, 20.0]), np.array([1.0, 1.0]), jac, bbox,
                            np.array([359.999, 0.001]), np.zeros(2), np.array([0.001, 359.999]), np.zeros(2))
    np.testing.assert_allclose(x, [10.2, 9.8])
    np.testing.assert_allclose(y, [20.0, 20.0])


# ---------------------------------------------------------------------------
# The rates of change, at every candidate visit (ASSIST)
# ---------------------------------------------------------------------------

from test_nearbysso_build import needs_assist, orbits, ephem, synth  # noqa: E402,F401


@needs_assist
def test_rates_of_change_at_every_candidate_visit(tmp_path, synth, orbits, monkeypatch):  # noqa: F811
    """Every eligible prediction gets its rates of change, also where no
    source carries its own time (none does in ``synth``), and they are
    not all zero."""
    real = B._rates_of_change_at
    got = []

    def spy(orbit, track, e, kd, vt, ell0, w, eph):
        out = real(orbit, track, e, kd, vt, ell0, w, eph)
        got.append(out[0])
        return out

    monkeypatch.setattr(B, "_rates_of_change_at", spy)
    rep = B.build(synth[0], orbits, tmp_path / "n.parquet", verbose=False, workers=1)
    assert rep["visits_own_times"] == 0
    dots = np.concatenate(got)
    assert dots.size == rep["counts"]["eligible"] > 0
    assert (dots["rate_ra_dot"] != 0).mean() > 0.99 and (dots["vmag_dot"] != 0).mean() > 0.99


@needs_assist
@needs_fixture
def test_build_wiring(tmp_path, synth, orbits, table):  # noqa: F811
    """build() with a WCS store and a correction table that cover none of
    the synthetic nights: the same NearbySSO, every row unplaced and
    flagged not_built (nights after the table's first, not built), the
    report's placement counts, and the per-row placement aligned with the
    rows."""
    store = write_store(tmp_path / "wcs", [Visit(V_OK, 150.0, -10.0)], snapshot=3)
    ref = tmp_path / "ref.parquet"
    B.build(synth[0], orbits, ref, verbose=False, workers=1)
    out = tmp_path / "out.parquet"
    rep, rows, place = B.build(synth[0], orbits, out, verbose=False, workers=1, wcs_store=store,
                               correction_table=table, return_rows=True)
    assert out.read_bytes() == ref.read_bytes()
    n = rep["output_rows"]
    assert len(rows) == len(place) == n > 0
    pl = rep["placement"]
    assert pl["matched_rows"] == n and pl["matched_without_wcs"] == n and pl["wcs_snapshot"] == 3
    assert pl["uncorrected_rows"]["not_built"] == n and pl["ppdb_corrected_visits"] == 0
    assert place["midpointMjdTai_flag"].all() and not place["has_wcs"].any()
    t = pq.read_table(synth[0]).to_pandas().sort_values("visit", kind="stable")
    t = t.drop_duplicates("diaSourceId").set_index("diaSourceId")      # (the repeated id: its first)
    np.testing.assert_array_equal(place["visit"], t.loc[rows["diaSourceId"], "visit"].to_numpy())
    np.testing.assert_array_equal(place["midpointMjdTai"],
                                  t.loc[rows["diaSourceId"], "midpointMjdTai"].to_numpy())


@needs_fixture
def test_row_times_reuses_corrections(table):
    """One Corrections serves every slice of a worker: a second row_times
    call with it looks its own rows up (Corrections.lookup skips rows it
    has looked up before)."""
    from ssp.export.submittable import Corrections

    corr = Corrections(table, guard=None)
    bbox = np.array([BBOX] * 2)

    def times(det, x):
        n = len(x)
        return P.row_times(np.full(n, P.VISIT_UNCORRECTED), np.zeros(n, np.uint8), np.full(n, HEADER[V_OK]),
                           np.full(n, HEADER[V_OK]), np.ones(n, bool), np.full(n, V_OK), np.full(n, det),
                           np.asarray(x, float), np.full(n, 2000.0), bbox, corr)

    t1, c1 = times(94, [100.0, 3900.0])
    t2, c2 = times(140, [3900.0, 100.0])
    want, st = corrected(table, V_OK, 140, [3900.0, 100.0], 2000.0)
    assert (st == 0).all() and (c2 == P.CAUSE_CORRECTED).all()
    np.testing.assert_array_equal(t2, want)
    assert (t1 != t2).all()


@needs_fixture
def test_beyond_margin_is_no_wcs(tmp_path, scene, table):
    """A matched row whose prediction is more than PLACEMENT_MARGIN_ARCSEC
    from every detector with a WCS is a row without a WCS (owner,
    2026-10-11): its DiaSource's detector, NaN placement, the visit's time,
    flagged no_wcs, counted; one within the margin keeps the nearest
    detector's placement."""
    from ssp.nearbysso._contract import PLACEMENT_MARGIN_ARCSEC

    ids, pub, place, vstats, _, p = run_pass3(tmp_path, scene["dia"], scene["preds"], scene["store"], table)
    pl, pp = _by_name(scene, ids, place), _by_name(scene, ids, p)
    loc = WcsStore([str(scene["store"] / "day_obs=20260105.parquet")]).locator(V_OK)
    far, near = (loc.place(pp[n]["ra"][None], pp[n]["dec"][None]) for n in ("far", "near"))
    assert far.edgeDistance[0] < -PLACEMENT_MARGIN_ARCSEC < near.edgeDistance[0] < -20.0
    r = pl["far"]
    assert not r["has_wcs"] and r["detector"] == r["dia_detector"] == 96
    assert np.isnan(r["eph_x"]) and np.isnan(r["eph_y"]) and np.isnan(r["ephDetectorEdgeDist"])
    assert _cause(r["time_cause"]) == "no_wcs" and r["midpointMjdTai_flag"]
    assert r["midpointMjdTai"] == HEADER[V_OK]
    r = pl["near"]
    assert r["has_wcs"] and r["detector"] == near.detector[0] and r["eph_x"] == near.x[0]
    assert abs(r["ephDetectorEdgeDist"] - near.edgeDistance[0]) < 1e-4
    assert vstats["beyond_margin"] == 1
    # exactly at the margin: kept (the margin is inclusive)
    out = P.place_rows(WcsStore([str(scene["store"] / "day_obs=20260105.parquet")]), np.array([V_OK] * 2),
                       pp["near"]["ra"][None].repeat(2), pp["near"]["dec"][None].repeat(2),
                       margin_arcsec=-near.edgeDistance[0])
    assert out["has_wcs"].all() and not out["beyond_margin"].any()


@needs_fixture
def test_correction_provenance_from_forked_workers(tmp_path, scene, table):
    """The lookups run in the pass-3 workers (forked, read_workers > 1):
    the report's correction-table provenance records what they read."""
    from ssp.export.submittable import Corrections

    from ssp.nearbysso._contract import MATCH_RADIUS_ARCSEC as R

    path = tmp_path / "dia.parquet"
    sid, visit, t, ra, dec, det = map(np.array, zip(*scene["dia"]))
    pq.write_table(pa.table(dict(diaSourceId=sid.astype(np.int64), visit=visit.astype(np.int64),
                                 midpointMjdTai=t.astype(float), ra=ra.astype(float), dec=dec.astype(float),
                                 detector=det.astype(np.int64))), path)
    nights, tmin, tmax, nsrc = B.night_ranges(path)
    slices = B.plan_slices(nights, tmin, tmax, 1, nsrc)
    assert len(slices) >= 3
    parts = []
    for sl in slices:
        d = B._read_slice(path, sl)
        u, start = np.unique(d["visit"], return_index=True)
        v = np.zeros(u.size, dtype=VISIT_DTYPE)
        v["visit"], v["night"] = u, u // 100000
        v["dia_start"], v["dia_end"] = start, np.r_[start[1:], d["visit"].size]
        v["t_tai_mjd"] = [np.median(d["midpointMjdTai"][a:b]) for a, b in zip(v["dia_start"], v["dia_end"])]
        parts.append(v)
    vis = np.concatenate(parts)
    vstart = np.r_[0, np.cumsum([v.size for v in parts])].astype(np.int64)
    p, poff, _ = B.sort_predictions([scene["preds"].copy()], vis.size)
    corr = Corrections(table, guard=None)
    B._W.update(dia_path=path, slices=slices, threads=1, visits=vis, vstart=vstart, preds=p, poff=poff,
                orbit_radius=np.full(int(p["orbit"].max()) + 1, R), dots=None, dia_extra=("detector",),
                corrections=corr, wcs_store=str(scene["store"]))
    try:
        assert B._pooled(2, len(slices))
        res = B._map(B._match_slice, [(s, s + 1) for s in range(len(slices))], 2, "test", unit="slices")
    finally:
        B._W.clear()
    vstats = [r[8] for chunk, _, _ in res for r in chunk]
    assert corr.calibrations == set()                   # (this process looked nothing up)
    prov = B.merged_provenance(corr, vstats)
    want = Corrections(table, guard=None)
    from shutter_timing.corrections import corrected_midpoints
    r = corrected_midpoints([V_OK], [94], [2000.0], [2000.0], table_dir=table)
    assert prov["table_format"] == r.table_format == 2
    assert prov["calibration_id"] == r.calibration_id != "unknown"
    assert prov["package_version"] == r.package_version != "unknown"
    assert want.provenance()["calibration_id"] == "unknown"     # (no lookup: nothing recorded)


@needs_fixture
def test_visit_with_only_rejected_detectors_has_no_wcs(tmp_path, scene, table):
    """A visit whose detectors are all rejected has no locator: its rows
    are unplaced and the visit is counted without a WCS."""
    store = write_store(tmp_path / "wcs", [v for k, v in scene["vis"].items() if k != V_NOWCS],
                        rejected=(V_OK2,))
    ids, pub, place, vstats, _, _ = run_pass3(tmp_path, scene["dia"], scene["preds"], store, table)
    pl = _by_name(scene, ids, place)
    for n in ("ppdb1", "ppdb2"):
        assert not pl[n]["has_wcs"] and _cause(pl[n]["time_cause"]) == "no_wcs"
    assert vstats["without_wcs"] == 2
