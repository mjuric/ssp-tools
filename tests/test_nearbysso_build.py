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
from ssp.nearbysso._contract import NEARBYSSO_DTYPE, ORBIT_DTYPE, VISIT_DTYPE

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


def test_write_parquet_null_ssobjectid(tmp_path):
    rows = np.zeros(3, dtype=NEARBYSSO_DTYPE)
    rows["diaSourceId"] = [1, 2, 3]
    rows["designation"] = ["2007 VY347", "2025 PM", "2026 DF62"]
    rows["ssObjectId"] = [11, 0, 33]
    has = np.array([True, False, True])
    B.write_parquet(rows, has, tmp_path / "n.parquet")
    t = pq.read_table(tmp_path / "n.parquet")
    assert t.column_names == list(NEARBYSSO_DTYPE.names)
    assert t["ssObjectId"].to_pylist() == [11, None, 33]
    assert t.schema.field("ssObjectId").type == pa.int64()
    assert t.schema.field("designation").type == pa.string()
    assert t.schema.field("ephOffset").type == pa.float32()
    assert t.schema.field("ephRa").type == pa.float64()
    assert t["designation"].to_pylist() == rows["designation"].tolist()


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
    chunks = np.split(p[rng.permutation(p.size)], cuts)
    out, off = B.sort_predictions(chunks, 9)
    assert np.array_equal(out, want)
    assert chunks == []
    np.testing.assert_array_equal(off, np.searchsorted(out["visit"], np.arange(10)))


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
