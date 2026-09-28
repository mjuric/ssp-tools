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


@pytest.fixture(scope="module")
def ephem():
    from ssp.ephem_assist import open_ephem
    return open_ephem()


@pytest.fixture(scope="module")
def orbits(ephem):
    rows = load_orbit_rows(ephem)
    dup = rows["2007 VY347"].copy()
    dup["designation"] = DUP
    out = np.array(sorted([*rows.values(), dup], key=lambda r: str(r["designation"])), dtype=ORBIT_DTYPE)
    return out


@pytest.fixture(scope="module")
def synth(tmp_path_factory, orbits, ephem):
    """Per night and orbit, two visits centred 0.3 deg from the orbit's
    prediction, each with a source 0.8" from it, a decoy 2.5" away and 200
    random sources within 1.5 deg. Returns (dia path, dia DataFrame, truth)."""
    from ssp.ephem_assist import compute_ephemerides_one

    rng = np.random.default_rng(42)
    d = tmp_path_factory.mktemp("synth")
    rows, truth = [], []
    sid = 1000
    for night in DAY_OBS:
        mjd0 = Time(f"{night // 10000}-{night // 100 % 100:02d}-{night % 100:02d}").mjd + 1.0
        seq = 0
        for o in orbits:
            if o["designation"] == DUP:
                continue
            for j in range(2):
                seq += 1
                visit = night * 100000 + seq
                t = mjd0 + 0.03 * seq + 0.01 * j
                e = compute_ephemerides_one(str(o["designation"]), Time([t], format="mjd", scale="tai"),
                                            None, ephem, row=o)
                ra0, dec0 = float(e.ra_deg[0]) % 360.0, float(e.dec_deg[0])
                cd = np.cos(np.radians(dec0))
                cra, cdec = ra0 + 0.3 / cd, dec0
                r = 1.5 * np.sqrt(rng.uniform(0, 1, 200))
                phi = rng.uniform(0, 2 * np.pi, 200)
                src = [(ra0, dec0 + 0.8 / 3600, "hit"), (ra0 + 2.5 / 3600 / cd, dec0, "decoy")]
                src += [(cra + r[k] * np.cos(phi[k]) / cd, cdec + r[k] * np.sin(phi[k]), "bg")
                        for k in range(200)]
                for ra, dec, kind in src:
                    sid += 1
                    rows.append((sid, visit, t, ra % 360.0, dec))
                    truth.append((sid, str(o["designation"]), kind))
    rows.append((sid + 1, rows[0][1], rows[0][2], np.nan, 0.0))    # dropped by read_dia
    truth.append((sid + 1, "", "bad"))
    dia = pd.DataFrame(rows, columns=["diaSourceId", "visit", "midpointMjdTai", "ra", "dec"])
    path = d / "dia.parquet"
    pq.write_table(pa.Table.from_pandas(dia.sample(frac=1.0, random_state=1), preserve_index=False), path,
                   row_group_size=500)
    truth = pd.DataFrame(truth, columns=["diaSourceId", "designation", "kind"]).set_index("diaSourceId")
    return path, dia.set_index("diaSourceId"), truth


def _run(tmp_path, synth, orbits, name, **kw):
    out = tmp_path / f"{name}.parquet"
    rep = B.build(synth[0], orbits, out, verbose=False, **kw)
    return out, rep


@needs_assist
def test_end_to_end(tmp_path, synth, orbits, ephem):
    path, dia, truth = synth
    sso = tmp_path / "ssobject.parquet"
    pq.write_table(pa.table(dict(designation=["2007 VY347", "2003 LN6"], ssObjectId=[1234, 5678])), sso)
    out, rep = _run(tmp_path, synth, orbits, "e2e", workers=1, ssobject_path=sso)
    res = pq.read_table(out).to_pandas().set_index("diaSourceId")
    assert list(res.columns) == list(NEARBYSSO_DTYPE.names)[1:]
    assert res.index.is_monotonic_increasing and res.index.is_unique
    assert rep["output_rows"] == len(res) and rep["exceptions"] == {}
    assert rep["dia_dropped"] == 1 and rep["dia_sources"] == len(dia) - 1
    assert rep["counts"]["step_cap_stops"] == 0 and rep["counts"]["nights_skipped"] == 0
    assert json.load(open(tmp_path / "e2e.report.json"))["output_rows"] == len(res)

    # no background source is near a prediction
    assert (truth.loc[res.index, "kind"] != "bg").all()
    # the long-arc orbits (tiny sigma) are found in every visit, hit and decoy
    for desig in ("2007 VY347", "2003 LN6"):
        want = truth.index[(truth["designation"] == desig) & (truth["kind"] != "bg")]
        assert set(want) <= set(res.index), desig
    # every row is the object whose sources they are (the duplicate orbit
    # ties 2007 VY347 exactly, and loses the tie by designation)
    assert (res["designation"] == truth.loc[res.index, "designation"]).all()
    assert DUP not in set(res["designation"])
    hits = truth.loc[res.index, "kind"] == "hit"
    np.testing.assert_allclose(res.loc[hits, "ephOffset"], 0.8, atol=1e-3)
    np.testing.assert_allclose(res.loc[~hits, "ephOffset"], 2.5, atol=1e-3)
    assert (res["ephRaErr"] > 0).all() and (res["ephDecErr"] > 0).all()
    assert np.isfinite(res["ephRa_ephDec_Cov"]).all()
    # ssObjectId: only for the objects with an SSObject row
    sso_id = res["ssObjectId"].to_numpy(dtype=float, na_value=np.nan)
    want = res["designation"].map({"2007 VY347": 1234.0, "2003 LN6": 5678.0}).to_numpy(dtype=float)
    np.testing.assert_array_equal(sso_id, want)

    # the eph* values are SSSource's, for the same orbit and DiaSource: to
    # integrator noise (it depends on the set of times integrated through,
    # here all the candidate visits, there only the matched ones), 3.6 uas
    from ssp import schema
    from ssp.sssource import compute_sssource_entry
    from ssp.util import observatory_barycentric_posvel
    import astropy.units as u
    mpcorb = pd.DataFrame({k: orbits[k] for k in ("q", "e", "i", "node", "argperi", "peri_time", "epoch_mjd",
                                                  "h", "g")}, index=orbits["designation"].astype(str))
    for desig, grp in res.groupby("designation"):
        ids = grp.index.to_numpy()
        de = np.zeros(len(ids), dtype=[(c, "f8") for c in ("midpointMjdTai", "ra", "dec")])
        for c in de.dtype.names:
            de[c] = dia.loc[ids, c].to_numpy()
        sss = np.zeros(len(ids), dtype=schema.SSSourceDtype)
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
        for c in ("ephOffset", "ephVmag", "ephRateRa", "ephRateDec"):
            np.testing.assert_allclose(grp[c], sss[c].astype(np.float32), rtol=1e-6, atol=1e-6,
                                       err_msg=f"{desig} {c}")


@needs_assist
def test_serial_parallel_identical_and_slices_same(tmp_path, synth, orbits, monkeypatch):
    a, rep_a = _run(tmp_path, synth, orbits, "serial", workers=1)
    b, _ = _run(tmp_path, synth, orbits, "parallel", workers=3, chunk_factor=2)
    with monkeypatch.context() as mp:     # one DiaIndex.match call per orbit
        mp.setattr(B, "_MATCH_BATCH", 1)
        d, _ = _run(tmp_path, synth, orbits, "unbatched", workers=1)
    assert a.read_bytes() == d.read_bytes()
    c, rep_c = _run(tmp_path, synth, orbits, "sliced", workers=2, slice_days=1)
    assert pq.read_metadata(a).num_rows > 0
    assert a.read_bytes() == b.read_bytes()

    # one slice per night: the same rows, candidates and ellipses; the
    # precise positions agree to integrator noise (see build's docstring)
    assert len(rep_a["slices"]) == 1 and len(rep_c["slices"]) == len(DAY_OBS)
    for k in ("candidate_visits", "eligible_evals", "sigma_rejected", "matches"):   # (not per orbit)
        assert rep_a["counts"][k] == rep_c["counts"][k], k
    ta, tc = pq.read_table(a), pq.read_table(c)
    assert ta.schema == tc.schema
    for name in ta.column_names:
        x, y = ta[name].to_numpy(), tc[name].to_numpy()
        if name in ("ephRa", "ephDec"):
            np.testing.assert_allclose(x, y, rtol=0, atol=1e-9, err_msg=name)
        elif name in ("diaSourceId", "ssObjectId", "designation") or name.endswith(("Err", "Cov")):
            np.testing.assert_array_equal(x, y, err_msg=name)
        else:
            np.testing.assert_allclose(x, y, rtol=1e-6, err_msg=name)


@needs_assist
def test_one_bad_orbit_is_counted(tmp_path, synth, orbits, monkeypatch):
    real = B.propagate.coarse

    def flaky(orbit, *a, **k):
        if str(orbit["designation"]) == "2003 LN6":
            raise FloatingPointError("injected")
        return real(orbit, *a, **k)
    monkeypatch.setattr(B.propagate, "coarse", flaky)
    out, rep = _run(tmp_path, synth, orbits, "flaky", workers=2)
    assert rep["exceptions"] == {"FloatingPointError": 1}
    assert rep["exception_examples"]["FloatingPointError"] == ["2003 LN6: injected"]
    assert rep["failed_orbits"]["exception"] == dict(n=1, examples=["2003 LN6"])
    res = pq.read_table(out).to_pandas()
    assert "2003 LN6" not in set(res["designation"])
    assert "2007 VY347" in set(res["designation"])
