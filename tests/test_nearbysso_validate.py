"""Pure logic of the NearbySSO validation harness (bench/nearbysso_validate).
No network, no ASSIST files."""
import json
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import nearbysso_validate as V
from ssp import util
from ssp.ephem_assist import cometary_to_helio_ecliptic


# --------------------------------------------------------------------------
# the orbit filter and classes
# --------------------------------------------------------------------------

def _orbits(**cols):
    base = dict(designation=["2020 AB"], packed=["K20A00B"], q=[2.0], e=[0.1], i=[5.0],
                node=[10.0], argperi=[20.0], peri_time=[60000.0], epoch_mjd=[61000.0],
                h=[15.0], g=[0.15], arc_text=["2014-2024"], normalized_rms=[0.5])
    n = max(len(v) for v in cols.values()) if cols else 1
    df = pd.DataFrame({k: (cols.get(k) or v * n) for k, v in base.items()})
    return df


def test_filter_reason():
    df = _orbits(designation=["2020 AB", "P/2019 A1", "2020 AC", "2020 AD", "2020 AE", "2020 AF"],
                 packed=["K20A00B", "PK19A010", "_K20A00C", "K20A00D", "K20A00E", "K20A00F"],
                 q=[2.0, 2.0, 2.0, np.nan, 2.0, 2.0],
                 arc_text=["3 days", "2014-2024", "2014-2024", "2014-2024", "2 days", None])
    r = V.filter_reason(df)
    # "_K20A00C" (extended packed format) is an asteroid, not a comet
    assert list(r) == ["", "comet", "", "missing_elements", "short_arc", "null_arc"]
    for arc, expect in (("0 days", "short_arc"), ("1 days", "short_arc"), ("0", ""), ("30 days", "")):
        assert V.filter_reason(df.iloc[:1].assign(arc_text=[arc]))[0] == expect
    lk = V.reason_lookup(df)
    assert list(V.reasons_for(["2020 AB", "nope"], lk)) == ["", "not_in_orbits"]


def test_arc_text_from_json():
    docs = [
        {"orbit_fit_statistics": {"nopp": 5, "arc_length_total": "2007-2021", "x": 1}},
        {"orbit_fit_statistics": {"arc_length_total": "2 days", "arc_length_sel": "1 days"}},
        {"orbit_fit_statistics": {"arc_length_total": 0, "nopp": 1}},
        {"orbit_fit_statistics": {"arc_length_total": None}},
        {"orbit_fit_statistics": {"nopp": 1}},
        {"CAR": {}, "orbit_fit_statistics": {"arc_length_total": "13 days"}},
    ]
    texts = [json.dumps(d) for d in docs] + [None, json.dumps(docs[1], separators=(",", ":"))]
    got = V.arc_text_from_json(texts)
    assert list(got) == ["2007-2021", "2 days", "0", None, None, "13 days", None, "2 days"]
    # the same through a DataFrame without arc_text
    df = _orbits().iloc[[0] * 8].reset_index(drop=True).drop(columns="arc_text").assign(mpc_orb_jsonb=texts)
    assert list(V.filter_reason(df)) == ["", "short_arc", "", "null_arc", "null_arc", "", "null_arc",
                                         "short_arc"]


def test_null_arc_excluded():
    # get-mpcorb.py: WHERE NOT ...->>'arc_length_total' IN (...) is NULL
    # for a null/absent arc (or orbit_fit_statistics null), so it's dropped
    texts = [json.dumps({"orbit_fit_statistics": None}), json.dumps({"orbit_fit_statistics": {}}),
             json.dumps({"orbit_fit_statistics": {"arc_length_total": None}}), None,
             json.dumps({"orbit_fit_statistics": {"arc_length_total": "3 days"}})]
    df = _orbits().iloc[[0] * 5].reset_index(drop=True).drop(columns="arc_text").assign(mpc_orb_jsonb=texts)
    assert list(V.filter_reason(df)) == ["null_arc"] * 4 + [""]
    # the earlier reasons take precedence
    comet = _orbits(designation=["P/2019 A1"], packed=["PK19A010"], arc_text=[None])
    assert list(V.filter_reason(comet)) == ["comet"]


def test_arc_days():
    d = V.arc_days(["3 days", "2014-2024", None, "1 day", "0"])
    assert d[0] == 3 and np.isinf(d[1]) and np.isnan(d[2]) and d[3] == 1 and np.isinf(d[4])


def test_dynamical_class():
    c = V.dynamical_class([1.0, 2.2, 5.0, 35.0, 3.5, 2.0], [0.5, 0.1, 0.02, 0.1, 0.05, 0.5])
    assert list(c) == ["neo", "main_belt", "trojan", "tno", "other", "other"]


# --------------------------------------------------------------------------
# same-orbits classification
# --------------------------------------------------------------------------

def _case():
    """Synthetic SSSource / NearbySSO / DiaSource rows, one per status."""
    ra0, dec0 = 150.0, 10.0
    arc = 1 / 3600.0
    # (diaSourceId, designation, dRA", present in nss as, note)
    rows = [
        (1, "A", 1.0, "same"),            # match
        (2, "B", 1.0, "same_bad"),        # value_mismatch
        (3, "P/C", 1.0, None),            # filtered:comet
        (4, "D", 6.0, None),              # separation
        (5, "E", 3.0, "nearer"),          # nearer_object
        (6, "F", 1.0, None),              # sigma (F has sigma 20)
        (7, "G", 1.0, None),              # unexplained
        (8, "H", 1.0, "farther"),         # wrong_nearest
        (9, "I", 1.0, None),              # sigma_unknown (NaN sigma)
    ]
    dia = pd.DataFrame({"diaSourceId": [r[0] for r in rows], "midpointMjdTai": 60800.0,
                        "ra": ra0, "dec": dec0})
    sss = pd.DataFrame({
        "diaSourceId": [r[0] for r in rows], "designation": [r[1] for r in rows],
        "ephRa": [ra0 + r[2] * arc / np.cos(np.deg2rad(dec0)) for r in rows], "ephDec": dec0,
        "ephRateRa": np.float32(0.1), "ephRateDec": np.float32(-0.05), "ephVmag": np.float32(20.0),
    })
    nss = []
    for (i, d, dra, how), s in zip(rows, sss.itertuples()):
        if how is None:
            continue
        r = dict(diaSourceId=i, designation=d, ephRa=s.ephRa, ephDec=s.ephDec, ephRateRa=s.ephRateRa,
                 ephRateDec=s.ephRateDec, ephVmag=s.ephVmag, ephOffset=np.float32(dra))
        if how == "same_bad":
            r["ephDec"] += 5 / 3.6e6       # 5 mas
        if how == "nearer":
            r.update(designation="Z", ephOffset=np.float32(0.5))
        if how == "farther":
            r.update(designation="Y", ephOffset=np.float32(4.0))
        nss.append(r)
    nss = pd.DataFrame(nss)
    lookup = {d: "" for d in "ABDEFGHIYZ"}
    lookup["P/C"] = "comet"
    sig = {"F": 20.0, "I": np.nan}

    def sigma_fn(des, t):
        return np.array([sig.get(d, 1.0) for d in des])
    return nss, sss, dia, lookup, sigma_fn


def test_compare_to_sssource_statuses():
    nss, sss, dia, lookup, sigma_fn = _case()
    rows, summ = V.compare_to_sssource(nss, sss, dia, lookup, sigma_fn)
    st = dict(zip(rows["diaSourceId"], rows["status"]))
    assert st == {1: "match", 2: "value_mismatch", 3: "filtered:comet", 4: "separation",
                  5: "nearer_object", 6: "sigma", 7: "unexplained", 8: "wrong_nearest",
                  9: "sigma_unknown"}
    assert summ["n_fail"] == 3 and summ["n_unknown"] == 1
    assert summ["n_identical"] == 1
    assert 4.9 < summ["max_d_pos_mas"] < 5.1
    disc = V.discrepancies(rows)
    assert set(disc["diaSourceId"]) == {2, 7, 8}
    assert {"sss_ephRa", "nss_ephRa", "midpointMjdTai", "dia_ra"} <= set(disc.columns)


def test_compare_sigma_only_for_unexplained():
    nss, sss, dia, lookup, _ = _case()
    seen = []

    def sigma_fn(des, t):
        seen.extend(des)
        return np.ones(len(des))
    V.compare_to_sssource(nss, sss, dia, lookup, sigma_fn)
    assert sorted(seen) == ["F", "G", "H", "I"]


def test_compare_nearer_tie_by_designation():
    nss, sss, dia, lookup, sigma_fn = _case()
    # the other object at the same separation: nearer only if it sorts first
    sss = sss[sss["diaSourceId"] == 5]
    for other, expect in (("0", "nearer_object"), ("ZZ", "wrong_nearest")):
        n = nss[nss["diaSourceId"] == 5].assign(designation=other, ephOffset=np.float32(3.0))
        lookup[other] = ""
        rows, _ = V.compare_to_sssource(n, sss, dia, lookup, sigma_fn)
        assert rows["status"].iloc[0] == expect


def test_mock_injects_detectable_faults():
    nss, sss, dia, lookup, sigma_fn = _case()
    mock, faults = V.mock_from_sssource(sss, dia, lookup, rng=1, n_drop=1, n_perturb=1)
    assert set(mock.columns) == set(V.NSS_COLUMNS)
    rows, summ = V.compare_to_sssource(mock, sss, dia, lookup, lambda d, t: np.ones(len(d)))
    st = dict(zip(rows["diaSourceId"], rows["status"]))
    for kind, i in faults.itertuples(index=False):
        assert st[i] == ("unexplained" if kind == "drop" else "value_mismatch")
    assert summ["n_fail"] == 2


# --------------------------------------------------------------------------
# DP2 designations
# --------------------------------------------------------------------------

def test_ssobjectid_roundtrip_and_reconcile():
    packed = pd.Series(["K20A00B", "00433", "a1234"], dtype="string[pyarrow]")
    ids = util.packed_ascii_to_uint64_le(packed)
    assert list(V.ssobjectid_to_packed(ids)) == ["K20A00B", "00433", "a1234"]
    ident = pd.DataFrame({"unpacked_primary_provisional_designation": ["2020 AB", "2020 AB"],
                          "unpacked_secondary_provisional_designation": ["2020 AB", "2019 XY"]})
    assert list(V.reconcile_designations(["2019 XY", "2020 AB", "2021 QQ"], ident)) == \
        ["2020 AB", "2020 AB", "2021 QQ"]


# --------------------------------------------------------------------------
# adjudication verdicts
# --------------------------------------------------------------------------

def test_adjudicate_rows():
    mas = 1 / 3.6e6
    disc = pd.DataFrame({
        "designation": ["A", "A", "B", "C"], "nss_designation": ["A", "A", None, None],
        "sss_ephRa": [10.0, 10.0, 10.0, 10.0], "sss_ephDec": [0.0, 0.0, 0.0, 0.0],
        "nss_ephRa": [10.0 + 5 * mas, 10.0 + 5 * mas, np.nan, np.nan], "nss_ephDec": [0.0] * 4,
        "dia_ra": [10.0, 10.0, 10.0 + 1 / 3600, 10.0 + 9 / 3600], "dia_dec": [0.0] * 4,
    })
    h_ra = np.array([10.0, 10.0 + 5 * mas, 10.0, 10.0])
    v = V.adjudicate_rows(disc, h_ra, np.zeros(4))["verdict"].tolist()
    assert v[0].startswith("sss_matches: NearbySSO bug")
    assert v[1].startswith("nss_matches")
    assert v[2] == "sss_matches, within radius: NearbySSO missed it"
    assert v[3] == "sss_matches, outside radius per Horizons"


# --------------------------------------------------------------------------
# Horizons chunking and etiquette (no network)
# --------------------------------------------------------------------------

def test_polite_budget_and_pacing():
    clock = [0.0]
    slept = []

    def sleep(dt):
        slept.append(dt)
        clock[0] += dt
    p = V.Polite(1.5, 2, sleep=sleep, clock=lambda: clock[0])
    p.call("a", lambda: 1)
    clock[0] += 0.5
    p.call("b", lambda: 2)
    assert slept == [pytest.approx(1.0)]
    with pytest.raises(V.QueryBudgetExceededError):
        p.call("c", lambda: 3)
    with pytest.raises(AssertionError):
        V.Polite(0.5)


def test_horizons_by_times_chunks_and_aligns():
    calls = []

    def fetch(t):
        calls.append(len(t))
        mjd = t.tai.mjd
        assert np.all(np.diff(mjd) > 0)
        return {"x": mjd.copy()}
    p = V.Polite(1.0, 100, sleep=lambda dt: None)
    t = np.concatenate([np.arange(130.0), np.arange(10.0)])[::-1] + 60000.0
    cols = V.horizons_by_times(fetch, t, p, "test")
    assert calls == [60, 60, 10] and p.n == 3
    np.testing.assert_allclose(cols["x"], t, atol=1e-8)


# --------------------------------------------------------------------------
# covariances: the cometary -> Cartesian Jacobian, CAR parsing, ellipses
# --------------------------------------------------------------------------

EL = np.array([0.15, 2.1, 60500.0, 80.0, 150.0, 12.0])  # e, q, tp [TT MJD], node, peri, i
EPOCH = 61000.0


def test_jacobian_tp_column_is_minus_velocity():
    J = V.cometary_jacobian(EL, EPOCH)
    s = V.cometary_to_state_eq(EL, EPOCH)
    # moving perihelion later by dt = moving the object back along its orbit
    np.testing.assert_allclose(J[:3, 2], -s[3:], rtol=1e-6)


def test_jacobian_node_is_rotation_about_ecliptic_pole():
    J = V.cometary_jacobian(EL, EPOCH)
    s = V.cometary_to_state_eq(EL, EPOCH)
    pole = V.R_ECL2EQ @ np.array([0.0, 0.0, 1.0])
    np.testing.assert_allclose(J[:3, 3], np.deg2rad(1) * np.cross(pole, s[:3]), rtol=1e-6, atol=1e-12)


def test_state_matches_pipeline_conversion():
    e, q, tp, node, peri, inc = EL
    X, Vv = cometary_to_helio_ecliptic(q, e, np.deg2rad(inc), np.deg2rad(node), np.deg2rad(peri),
                                       EPOCH - tp)
    s = V.cometary_to_state_eq(EL, EPOCH)
    np.testing.assert_allclose(s[:3], V.R_ECL2EQ @ X, rtol=1e-14)
    np.testing.assert_allclose(s[3:], V.R_ECL2EQ @ Vv, rtol=1e-14)
    # and the rotation is the IAU76 obliquity about x
    eps = np.arccos(V.R_ECL2EQ[1, 1])
    assert np.degrees(eps) * 3600 == pytest.approx(84381.448, abs=1e-6)


def test_cometary_cov_matches_monte_carlo():
    rng = np.random.default_rng(3)
    sig = np.array([1e-6, 1e-7, 1e-3, 1e-5, 1e-4, 1e-6])
    A = rng.normal(size=(6, 6))
    corr = A @ A.T
    corr /= np.sqrt(np.outer(np.diag(corr), np.diag(corr)))
    cov_el = corr * np.outer(sig, sig)
    cov = V.cometary_cov_to_state_cov(EL, cov_el, EPOCH)
    samples = rng.multivariate_normal(EL, cov_el, size=20000)
    states = np.array([V.cometary_to_state_eq(x, EPOCH) for x in samples])
    mc = np.cov(states.T)
    sd = np.sqrt(np.diag(cov))
    np.testing.assert_allclose(np.sqrt(np.diag(mc)), sd, rtol=0.03)
    np.testing.assert_allclose(mc / np.outer(sd, sd), cov / np.outer(sd, sd), atol=0.03)
    assert V.is_pd(cov + 1e-30 * np.eye(6)) or np.all(np.linalg.eigvalsh(cov) > -1e-25)


def test_car_cov6_parse():
    cov = {f"cov{i}{j}": float(10 * i + j + (100 if i == j else 0)) for i in range(10) for j in range(i, 10)}
    for i in range(10):
        for j in range(i, 10):
            if i >= 6 or j >= 6:
                cov[f"cov{i}{j}"] = None
    m = V.car_cov6(json.dumps({"CAR": {"covariance": cov}}))
    assert m.shape == (6, 6) and m[1, 4] == 14.0 and m[4, 1] == 14.0 and m[5, 5] == 155.0
    cov["cov05"] = None
    assert V.car_cov6(json.dumps({"CAR": {"covariance": cov}})) is None
    assert V.car_cov6(None) is None and V.car_cov6("{}") is None


def test_sbdb_orbit_parse():
    js = {
        "object": {"des": "2017 UY179", "fullname": "(2017 UY179)", "spkid": "3794545", "kind": "au"},
        "orbit": {
            "orbit_id": "12", "epoch": "2461000.5",
            "elements": [{"name": n, "value": v} for n, v in
                         (("e", "0.1"), ("q", "2.0"), ("i", "3.0"), ("om", "57.0"), ("w", "39.0"),
                          ("tp", "2460870.67"))],
            "covariance": {
                "epoch": "2460800.5", "labels": list(V.COMETARY_LABELS),
                "data": (np.eye(6) * 1e-10).tolist(),
                "elements": [{"name": n, "value": v} for n, v in
                             (("e", "0.2"), ("q", "2.1"), ("tp", "2460800.0"), ("om", "50.0"),
                              ("w", "30.0"), ("i", "4.0"))],
            },
        },
        "phys_par": [{"name": "H", "value": "18.9"}],
    }
    el, mat, info = V.sbdb_orbit(js)
    assert el["e"] == 0.2 and el["q"] == 2.1 and el["node"] == 50.0 and el["argperi"] == 30.0
    assert el["h"] == 18.9 and el["g"] == 0.15
    # TDB JD -> TT MJD: the covariance's own epoch, not the orbit's
    assert el["epoch_mjd"] == pytest.approx(60800.0, abs=1e-6)
    assert el["peri_time"] - el["epoch_mjd"] == pytest.approx(-0.5, abs=1e-6)
    assert mat.shape == (6, 6) and info["orbit_id"] == "12"
    assert V.jpl_command(info) == "'DES=2017 UY179;'"
    assert V.jpl_command({"des": "433"}) == "'433;'"


def test_ellipse_axes():
    # 3" x 1" ellipse with the major axis at PA 30 deg (east of north)
    pa = np.deg2rad(30.0)
    major = np.array([np.sin(pa), np.cos(pa)])     # (east, north)
    minor = np.array([np.cos(pa), -np.sin(pa)])
    Cm = 9 * np.outer(major, major) + 1 * np.outer(minor, minor)
    a, b, th = V.ellipse_axes(np.sqrt(Cm[0, 0]), np.sqrt(Cm[1, 1]), Cm[0, 1])
    assert a == pytest.approx(3) and b == pytest.approx(1) and th == pytest.approx(30.0)
    assert V.sigma_major_arcsec(3 / 3600, 1 / 3600, 0) == pytest.approx(3)


def test_compare_sigma_gate():
    ours = {"ra_err": np.array([1, 50, 200]) / 3600, "dec_err": np.array([0.5, 20, 100]) / 3600,
            "ra_dec_cov": np.zeros(3)}
    hz = {"SMAA_3sig": np.array([3.1, 180.0, 600.0]), "SMIA_3sig": np.array([1.5, 60, 300]),
          "Theta": np.array([0.0, 0.0, 0.0]), "RA_3sigma": np.array([3.0, 150, 600]),
          "DEC_3sigma": np.array([1.5, 60, 300])}
    r = V.compare_sigma(ours, hz)
    assert list(r["gated"]) == [True, True, False]
    assert list(r["pass"]) == [True, False, True]      # 150/180 is 17% off
    assert np.allclose(r["d_theta"], 0)


# --------------------------------------------------------------------------
# the stratified sampler
# --------------------------------------------------------------------------

def test_stratified_sample():
    n = 1000
    rng = np.random.default_rng(0)
    cls = rng.choice(["a", "b"], size=n)
    key = rng.normal(size=n)
    group = rng.integers(0, 50, size=n)
    masks = {"a": cls == "a", "b": cls == "b", "top": cls == "b", "empty": np.zeros(n, bool)}
    s = V.stratified_sample(n, masks, [("top", 1), ("a", 10), ("b", 10), ("empty", 5)],
                            keys={"top": -key}, rng=1, group=group)
    assert s["row"].is_unique
    assert s["stratum"].value_counts().to_dict() == {"a": 10, "b": 10, "top": 1}
    top = s.loc[s["stratum"] == "top", "row"].iloc[0]
    assert top == np.flatnonzero(cls == "b")[np.argmax(key[cls == "b"])]
    for st in ("a", "b"):
        rows = s.loc[s["stratum"] == st, "row"]
        assert np.all(masks[st][rows]) and len(set(group[rows])) == len(rows)
    # deterministic
    s2 = V.stratified_sample(n, masks, [("top", 1), ("a", 10), ("b", 10), ("empty", 5)],
                             keys={"top": -key}, rng=1, group=group)
    assert s.equals(s2)


def test_strata_masks():
    orbits = _orbits(designation=["N", "M", "T"], packed=["a", "b", "c"], q=[0.9, 2.2, 5.0],
                     e=[0.5, 0.1, 0.02], arc_text=["10 days", "2014-2024", "45 days"])
    nss = pd.DataFrame({"designation": ["N", "N", "M", "T"], "ephOffset": [1.0, 4.8, 1.0, 1.0],
                        "ephRaErr": np.array([1, 1, 9, 1]) / 3600, "ephDecErr": np.array([1, 1, 1, 1]) / 3600,
                        "ephRa_ephDec_Cov": 0.0, "ephRateRa": [1.0, 2.0, 0.1, 0.01],
                        "ephRateDec": 0.0})
    m, k = V.strata_masks(nss, orbits)
    assert list(m["neo"]) == [True, True, False, False]
    assert list(m["short_arc"]) == [True, True, False, False]
    assert list(m["near_radius"]) == [False, True, False, False]
    assert list(m["near_sigma"]) == [False, False, True, False]
    assert list(m["trojan"]) == [False, False, False, True]
    s = V.stratified_sample(len(nss), m, [("neo_close", 1)], keys=k)
    assert s["row"].tolist() == [1]


# --------------------------------------------------------------------------
# brute force: epoch states, visits, expected nearest
# --------------------------------------------------------------------------

def test_twobody_matches_scalar_conversion():
    rng = np.random.default_rng(5)
    n = 50
    q = rng.uniform(0.5, 40, n)
    e = np.concatenate([rng.uniform(0, 0.95, n - 5), rng.uniform(1.05, 3, 5)])
    inc, node, peri = rng.uniform(0, 180, n), rng.uniform(0, 360, n), rng.uniform(0, 360, n)
    dt = rng.uniform(-3000, 3000, n)
    X, Vv = V.twobody_state_helio_ecl(q, e, inc, node, peri, dt)
    for k in range(n):
        ref, vref = cometary_to_helio_ecliptic(q[k], e[k], np.deg2rad(inc[k]), np.deg2rad(node[k]),
                                               np.deg2rad(peri[k]), dt[k])
        np.testing.assert_allclose(X[k], ref, rtol=1e-9, atol=1e-11)
        np.testing.assert_allclose(Vv[k], vref, rtol=1e-9, atol=1e-13)


def test_derive_visits():
    dia = pd.DataFrame({"diaSourceId": [3, 1, 2, 4], "visit": [7, 5, 5, 7],
                        "midpointMjdTai": [2.0, 1.0, 1.0, 2.0],
                        "ra": [359.5, 10.0, 12.0, 0.5], "dec": [0.0, 0.0, 0.0, 0.0]})
    vis, d = V.derive_visits(dia)
    assert list(vis["visit"]) == [5, 7] and list(d["diaSourceId"]) == [1, 2, 3, 4]
    ra, dec = V.vec_to_radec(vis["center"].iloc[1])
    assert min(ra, 360 - ra) < 1e-9 and abs(dec) < 1e-9          # across the RA wrap
    assert np.degrees(vis["radius"].iloc[0]) == pytest.approx(1.0)


def test_expected_nearest_and_compare():
    pairs = pd.DataFrame({
        "diaSourceId": [1, 1, 2, 2, 3, 4, 5],
        "designation": ["B", "A", "C", "D", "E", "F", "G"],
        "sep": [1.0, 1.0, 0.5, 2.0, 6.0, 1.0, 1.0],
        "sigma": [1.0, 1.0, 30.0, 2.0, 1.0, np.nan, 1.0],
    })
    e = V.expected_nearest(pairs)
    got = dict(zip(e["diaSourceId"], e["designation"]))
    assert got == {1: "A", 2: "D", 4: "F", 5: "G"}          # tie by designation; C ineligible; E far
    assert e.set_index("diaSourceId").loc[4, "sigma_unknown"]
    nss = pd.DataFrame({"diaSourceId": [1, 2, 9], "designation": ["A", "C", "X"],
                        "ephOffset": [1.0, 0.5, 1.0]})
    c = V.compare_expected(e, nss)
    st = dict(zip(c["diaSourceId"], c["status"]))
    assert st == {1: "found", 2: "wrong_object", 4: "missing_sigma_unknown", 5: "missing", 9: "extra"}


def test_match_within():
    arc = 1 / 3600.0
    # the second prediction is 3.6" from the pole; a DiaSource 1.08" from
    # it on the other side is 4.68" away, one 1.8" from it 5.4"
    pr, pd_ = np.array([10.0, 200.0]), np.array([0.0, 89.999])
    dr = np.array([10.0 + 4.9 * arc, 10.0 + 5.1 * arc, 20.0, 20.0])
    dd = np.array([0.0, 0.0, 89.9997, 89.9995])
    k, h, sep = V.match_within(pr, pd_, dr, dd)
    got = sorted(zip(k.tolist(), h.tolist()))
    assert got == [(0, 0), (1, 2)]
    assert np.all(sep <= 5.0)
    k, h, sep = V.match_within(pr[:0], pd_[:0], dr, dd)
    assert len(k) == 0


# --------------------------------------------------------------------------
# the sigma oracle's plumbing into propagate.coarse (mocked)
# --------------------------------------------------------------------------

class _FakeEphem:
    class _P:
        x = y = z = vx = vy = vz = 0.0

    def get_particle(self, body, t):
        return self._P()


def _orbit_file(path, jsons):
    n = len(jsons)
    pd.DataFrame({
        "unpacked_primary_provisional_designation": ["2020 AB", "2020 AC"][:n],
        "packed_primary_provisional_designation": ["K20A00B", "K20A00C"][:n],
        "mpc_orb_jsonb": jsons,
        "q": [2.0, 2.5][:n], "e": [0.1, 0.2][:n], "i": [5.0, 6.0][:n], "node": [10.0, 11.0][:n],
        "argperi": [20.0, 21.0][:n], "peri_time": [60000.0, 60100.0][:n],
        "epoch_mjd": [61000.0, 61000.0][:n], "h": [15.0, 16.0][:n], "g": [0.15, 0.15][:n],
        "arc_length_total": [100.0, 100.0][:n], "normalized_rms": [0.5, 0.6][:n],
    }).to_parquet(path)


def test_sigma_oracle_plumbing(tmp_path, monkeypatch):
    from ssp.nearbysso import _contract as Ct
    from ssp.nearbysso import propagate
    cov = np.diag([1e-10, 2e-10, 3e-10, 1e-14, 2e-14, 3e-14])
    cov[0, 1] = cov[1, 0] = 5e-11
    cj = {f"cov{i}{j}": (cov[i, j] if j < 6 else None) for i in range(10) for j in range(i, 10)}
    _orbit_file(tmp_path / "o.parquet", [json.dumps({"CAR": {"covariance": cj}}), "{}"])
    seen = {}

    def coarse(orbit, t, obs_pos, ephem):
        assert np.all(np.diff(t) > 0) and obs_pos.shape == (len(t), 3)
        seen[str(orbit["designation"])] = orbit
        k = len(t)
        sig = 1.0 if orbit["has_cov"] else np.inf
        z = np.zeros(k)
        return Ct.CoarseTrack(t=t, ra=z, dec=z, rate_ra=z, rate_dec=z, ra_err=z, dec_err=z,
                              ra_dec_cov=z, sigma_major=sig * (t - t[0] + 1), ok=np.ones(k, bool),
                              delta=np.ones(k))
    monkeypatch.setattr(propagate, "coarse", coarse)
    monkeypatch.setattr(V, "observer_states", lambda m: (np.zeros((len(m), 3)), np.zeros((len(m), 3))))
    orc = V.SigmaOracle(str(tmp_path / "o.parquet"), ephem=_FakeEphem())
    s = orc(np.array(["2020 AB", "2020 AB", "2020 AC", "nope", "2020 AB"], dtype=object),
            np.array([60802.0, 60800.0, 60800.0, 60800.0, 60800.0]))
    np.testing.assert_allclose(s[[0, 1, 4]], [3.0, 1.0, 1.0])     # aligned, deduplicated
    assert np.isinf(s[2]) and np.isnan(s[3])
    ab = seen["2020 AB"]
    assert ab["has_cov"] and not seen["2020 AC"]["has_cov"]
    np.testing.assert_allclose(ab["cov0"], V.R6_ECL2EQ @ cov @ V.R6_ECL2EQ.T)
    ref = V.cometary_to_state_eq([0.1, 2.0, 60000.0, 10.0, 20.0, 5.0], 61000.0)
    np.testing.assert_allclose(ab["state0"], ref, rtol=1e-12)
    assert ab.dtype == Ct.ORBIT_DTYPE and ab["epoch"] == pytest.approx(61000.0 - 51544.5, abs=1e-5)


def test_sigma_oracle_unknown_without_wp2(tmp_path, monkeypatch):
    from ssp.nearbysso import propagate

    def coarse(*a):
        raise NotImplementedError("WP2")
    monkeypatch.setattr(propagate, "coarse", coarse)
    monkeypatch.setattr(V, "observer_states", lambda m: (np.zeros((len(m), 3)), np.zeros((len(m), 3))))
    _orbit_file(tmp_path / "o.parquet", ["{}"])
    orc = V.SigmaOracle(str(tmp_path / "o.parquet"), ephem=_FakeEphem())
    assert np.isnan(orc(np.array(["2020 AB"], dtype=object), np.array([60800.0]))).all()
    assert "not implemented" in orc.note


def test_sky_jacobian():
    rho = np.array([[0.3, -1.2, 0.4], [1.0, 0.0, 0.0]])
    J = V.sky_jacobian(rho)
    for n in range(len(rho)):
        ra0, dec0 = V.vec_to_radec(rho[n])
        for k in range(3):
            d = np.zeros(3)
            d[k] = 1e-7
            ra1, dec1 = V.vec_to_radec(rho[n] + d)
            num = np.array([np.deg2rad(ra1 - ra0) * np.cos(np.deg2rad(dec0)), np.deg2rad(dec1 - dec0)]) / 1e-7
            np.testing.assert_allclose(J[n, :, k], num, atol=1e-6)


# --------------------------------------------------------------------------
# the CLI end to end on synthetic Parquet files (no ASSIST, no network)
# --------------------------------------------------------------------------

def _write_case(tmp_path):
    nss, sss, dia, lookup, _ = _case()
    dia.assign(visit=1).to_parquet(tmp_path / "dia.parquet", row_group_size=4)
    sss.to_parquet(tmp_path / "sss.parquet")
    nss.to_parquet(tmp_path / "nss.parquet")
    des = sorted(lookup)
    orb = _orbits(designation=des, packed=[("_x" if "/" in d else "p" + d) for d in des])
    orb = orb.rename(columns={"designation": "unpacked_primary_provisional_designation",
                              "packed": "packed_primary_provisional_designation"})
    orb = orb.drop(columns="arc_text").assign(
        mpc_orb_jsonb=json.dumps({"orbit_fit_statistics": {"arc_length_total": "2014-2024"}}))
    orb.to_parquet(tmp_path / "orbits.parquet")
    return sss


def test_cli_same_orbits(tmp_path):
    _write_case(tmp_path)
    rc = V.main(["same-orbits", "--nearbysso", str(tmp_path / "nss.parquet"),
                 "--sssource", str(tmp_path / "sss.parquet"), "--dia", str(tmp_path / "dia.parquet"),
                 "--orbits", str(tmp_path / "orbits.parquet"), "--out", str(tmp_path / "r"), "--no-sigma"])
    assert rc == 1
    rows = pd.read_parquet(tmp_path / "r" / "same-orbits.parquet")
    st = dict(zip(rows["diaSourceId"], rows["status"]))
    assert st[1] == "match" and st[3] == "filtered:comet" and st[6] == "sigma_unknown"
    disc = pd.read_parquet(tmp_path / "r" / "same-orbits.discrepancies.parquet")
    assert set(disc["diaSourceId"]) == {2}          # 7 and 8 are sigma_unknown without sigma
    assert "GATE" in (tmp_path / "r" / "same-orbits.txt").read_text()


def test_cli_dp2_intersection(tmp_path):
    sss = _write_case(tmp_path)
    # DP2-style: ssObjectId only, and 'A' known to DP2 under an old designation
    packed = pd.Series(["p" + d if "/" not in d else "_x" for d in sss["designation"]],
                       dtype="string[pyarrow]")
    packed = packed.where(sss["designation"] != "A", "OLDA")
    sss.drop(columns="designation").assign(ssObjectId=util.packed_ascii_to_uint64_le(packed)).to_parquet(
        tmp_path / "dp2.parquet")
    des = sorted(set(sss["designation"]) | {"Y", "Z"})
    ident = pd.DataFrame({
        "unpacked_primary_provisional_designation": des + ["A"],
        "unpacked_secondary_provisional_designation": des + ["OLD A"],
        "packed_secondary_provisional_designation": [("_x" if "/" in d else "p" + d) for d in des] + ["OLDA"],
    })
    ident.to_parquet(tmp_path / "ident.parquet")
    rc = V.main(["dp2-intersection", "--nearbysso", str(tmp_path / "nss.parquet"),
                 "--sssource", str(tmp_path / "dp2.parquet"), "--dia", str(tmp_path / "dia.parquet"),
                 "--orbits", str(tmp_path / "orbits.parquet"), "--identifications",
                 str(tmp_path / "ident.parquet"), "--out", str(tmp_path / "r")])
    assert rc == 0                                   # reported, never gated
    rows = pd.read_parquet(tmp_path / "r" / "dp2-intersection.parquet")
    st = dict(zip(rows["diaSourceId"], rows["status"]))
    assert 3 not in st                               # the comet: not kept by us
    assert st[1] == "match" and st[2] == "value_mismatch"


def test_angle_between():
    a = np.array([[1.0, 0, 0], [1.0, 0, 0], [0, 0, 2.0]])
    b = np.array([[np.cos(1e-9), np.sin(1e-9), 0], [-1.0, 0, 0], [0, 3.0, 0]])
    np.testing.assert_allclose(V.angle_between(a, b), [1e-9, np.pi, np.pi / 2], rtol=1e-9)


def test_pluto_is_a_known_exception(tmp_path, monkeypatch):
    mas = 1 / 3.6e6
    disc = pd.DataFrame({
        "designation": ["1930 BM", "A"], "nss_designation": ["1930 BM", "A"], "diaSourceId": [1, 2],
        "midpointMjdTai": [60800.0, 60800.0], "status": "value_mismatch",
        "sss_ephRa": [10.0, 10.0], "sss_ephDec": [0.0, 0.0], "nss_ephRa": [10.0 + 50 * mas, 10.0],
        "nss_ephDec": [0.0, 0.0], "dia_ra": [10.0, 10.0], "dia_dec": [0.0, 0.0]})
    disc.to_parquet(tmp_path / "d.parquet")
    _orbit_file(tmp_path / "o.parquet", ["{}", "{}"])
    o = pd.read_parquet(tmp_path / "o.parquet")
    o["unpacked_primary_provisional_designation"] = ["1930 BM", "A"]
    o.to_parquet(tmp_path / "o.parquet")
    monkeypatch.setattr(V, "horizons_own_elements",
                        lambda row, t, polite, **kw: {"R.A.": np.full(len(t), 10.0), "DEC": np.zeros(len(t))})
    rc = V.main(["horizons-adjudicate", "--discrepancies", str(tmp_path / "d.parquet"),
                 "--orbits", str(tmp_path / "o.parquet"), "--out", str(tmp_path / "r")])
    res = pd.read_parquet(tmp_path / "r" / "horizons-adjudicate.parquet").set_index("designation")
    assert res.loc["1930 BM", "verdict"].startswith("known exception: sss_matches")
    assert res.loc["A", "verdict"] == "both_match"
    assert rc == 0                      # Pluto's 50 mas isn't a NearbySSO bug


def _sss_table():
    """The columns _read_sss reads of a widened SSSource: DiaSource and
    Source rows of several processings, a non-primary repeat, 64-bit ids
    beyond float64's exact range, and NULL diaSourceIds (Source rows)."""
    import pyarrow as pa
    import pyarrow.compute as pc
    big = 2**62 + 12345
    t = {
        "diaSourceId": pa.array([big + 1, big + 2, big + 2, None, big + 1, None], pa.int64()),
        "designation": pa.array(["2025 AA1", "2025 AA1", "2025 AA1", "2025 AA1", None, "2024 BB2"]),
        "ssObjectId": pa.array([7, 7, 7, 7, None, 9], pa.int64()),
        "processing": pc.dictionary_encode(pa.array(["AP-DS", "AP-DS", "AP-DS", "NV-S", "DP2-DS", "AP-S"])),
        "measuredOn": pc.dictionary_encode(pa.array(["difference"] * 3
                                                    + ["science", "difference", "science"])),
        "primary": pa.array([True, True, False, True, True, True]),
    }
    for c in V.EPH_COMPARED + ["ephOffset"]:
        t[c] = pa.array(np.arange(6, dtype=float))
    return pa.table(t), big


def test_read_sss_widened(tmp_path):
    import pyarrow.parquet as pq
    t, big = _sss_table()
    pq.write_table(t, tmp_path / "w.parquet")
    sss = V._read_sss(tmp_path / "w.parquet")                  # (AP-DS, the default)
    assert sss["diaSourceId"].dtype == np.int64
    assert sss["diaSourceId"].tolist() == [big + 1, big + 2]   # exact; no repeat, no Source rows
    assert sss["designation"].tolist() == ["2025 AA1", "2025 AA1"]
    # DP2-DS: its ssObjectId is NULL, a nullable Int64 (not float64)
    sss = V._read_sss(tmp_path / "w.parquet", processing="DP2-DS")
    assert sss["diaSourceId"].tolist() == [big + 1]
    assert str(sss["ssObjectId"].dtype) == "Int64" and sss["ssObjectId"].isna().all()
    # all processings: AP-DS and DP2-DS share an id (the join can't tell)
    with pytest.raises(ValueError, match="repeats across processings"):
        V._read_sss(tmp_path / "w.parquet", processing="all")


def test_read_sss_old_layout(tmp_path):
    import pyarrow.parquet as pq
    t, big = _sss_table()
    old = t.drop_columns(["processing", "measuredOn", "primary"]).slice(0, 2)
    pq.write_table(old, tmp_path / "o.parquet")
    sss = V._read_sss(tmp_path / "o.parquet")
    assert sss["diaSourceId"].dtype == np.int64 and sss["diaSourceId"].tolist() == [big + 1, big + 2]
    assert list(sss.columns) == ["diaSourceId", "designation", "ssObjectId"] + V.EPH_COMPARED + ["ephOffset"]


# --------------------------------------------------------------------------
# rank: diaDistanceRank by brute force
# --------------------------------------------------------------------------

AS = 1 / 3600.0
RANK_GATE = ("diaDistanceRank == 1 + the DiaSources of the visit nearer the prediction "
             "(ties: lower diaSourceId)")
RANK1_GATE = "rank-1 rows: no DiaSource of the visit is nearer the prediction"
ORDER_GATE = "within each (object, visit), ranks distinct and increasing with (separation, diaSourceId)"
TYPE_GATE = "diaDistanceRank is int16 (short), non-null, >= 1"


def _rank_case():
    """Visit 1: A predicted at (10, 0); DiaSources 101 (+1" Dec) and 102
    (-1" Dec) tie for A; C, predicted 1.3" north, takes 101; B, 3.2" east,
    takes 103; 104 is 7" west, near nobody. So A's row for 102 has rank 2
    (101, nearer another object, still counts, and wins the tie by id).
    Visit 2: A's two DiaSources 201 (0.5") and 202 (2"), ranks 1 and 2.
    Hand-made ranks (not from the code under test)."""
    dia = pd.DataFrame({
        "diaSourceId": [101, 102, 103, 104, 105, 201, 202, 203],
        "visit": [1, 1, 1, 1, 1, 2, 2, 2],
        "midpointMjdTai": [61000.1] * 5 + [61001.1] * 3,
        "ra": [10.0, 10.0, 10.0 + 3 * AS, 10.0 - 7 * AS, 50.0, 20.0, 20.0 + 2 * AS, 21.0],
        "dec": [AS, -AS, 0.0, 0.0, 5.0, -10.0 + 0.5 * AS, -10.0, -10.0],
    })
    pred = {"A1": (10.0, 0.0), "B1": (10.0 + 3.2 * AS, 0.0), "C1": (10.0, 1.3 * AS), "A2": (20.0, -10.0)}
    rows = [(101, "C", "C1", 1), (102, "A", "A1", 2), (103, "B", "B1", 1), (201, "A", "A2", 1),
            (202, "A", "A2", 2)]
    d = dia.set_index("diaSourceId")
    nss = pd.DataFrame({
        "diaSourceId": [r[0] for r in rows], "ssObjectId": [1] * len(rows),
        "designation": [r[1] for r in rows],
        "ephRa": [pred[r[2]][0] for r in rows], "ephDec": [pred[r[2]][1] for r in rows],
    })
    at = d.loc[nss["diaSourceId"]]
    sep = util.sky_separation_arcsec(nss["ephRa"].to_numpy(), nss["ephDec"].to_numpy(),
                                     at["ra"].to_numpy(), at["dec"].to_numpy())
    nss["ephOffset"] = sep.astype(np.float32)
    nss["diaDistanceRank"] = np.array([r[3] for r in rows], dtype=np.int16)
    return nss, dia


def _rank_files(tmp_path, nss=None, dia=None):
    n0, d0 = _rank_case()
    nss = n0 if nss is None else nss
    dia = d0 if dia is None else dia
    dia.sample(frac=1, random_state=1).to_parquet(tmp_path / "dia.parquet", row_group_size=3)
    nss.to_parquet(tmp_path / "nss.parquet")
    return str(tmp_path / "nss.parquet"), str(tmp_path / "dia.parquet")


def _rank_failed(tmp_path, nss=None, **kw):
    verdict, rep, s = V.check_rank(*_rank_files(tmp_path, nss), **kw)
    return {name for name, ok in rep.gates if not ok}, verdict, rep, s


def test_brute_force_ranks():
    nss, dia = _rank_case()
    dia = dia.sort_values(["visit", "diaSourceId"]).reset_index(drop=True)
    bf = V.brute_force_ranks(dia.set_index("diaSourceId").loc[nss["diaSourceId"], "visit"],
                             nss["ephRa"], nss["ephDec"], nss["diaSourceId"], dia)
    assert (bf["n_closer"] + 1).tolist() == nss["diaDistanceRank"].tolist()
    assert bf["n_within"].tolist() == [3, 3, 3, 2, 2]     # A1: 101, 102, 103 (3"); 104 at 7" out
    assert bf["nearest_id"].tolist() == [101, 101, 103, 201, 201]
    # a DiaSource not in that visit
    bf = V.brute_force_ranks([2], [10.0], [0.0], [101], dia)
    assert np.isnan(bf["own_sep"][0]) and bf["n_closer"][0] == -1


def test_rank_pass(tmp_path):
    failed, verdict, rep, s = _rank_failed(tmp_path)
    assert verdict == "PASS", "\n".join(rep.lines)
    assert not failed
    assert "ranks in the file: 1: 3, 2: 2" in "\n".join(rep.lines)
    assert len(s) == 5 and (s["expected_rank"] == s["diaDistanceRank"]).all()
    # a sample, and the CLI
    assert V.check_rank(*_rank_files(tmp_path), n=2)[0] == "PASS"
    nss, dia = _rank_files(tmp_path)
    assert V.main(["rank", "--nearbysso", nss, "--dia", dia, "--out", str(tmp_path / "r")]) == 0
    assert "GATE (every rank check): PASS" in (tmp_path / "r" / "rank.txt").read_text()


def test_rank_fail_value(tmp_path):
    nss, _ = _rank_case()
    nss.loc[2, "diaDistanceRank"] = 2                 # B's 103: nothing nearer B's prediction
    failed, verdict, _, _ = _rank_failed(tmp_path, nss)
    assert verdict == "FAIL" and failed == {RANK_GATE}


def test_rank_fail_tie_and_rank1(tmp_path):
    nss, _ = _rank_case()
    nss.loc[1, "diaDistanceRank"] = 1                 # the tie broken the wrong way (102 > 101)
    failed, _, _, _ = _rank_failed(tmp_path, nss)
    assert failed == {RANK_GATE, RANK1_GATE}


def test_rank_fail_order_within_object_visit(tmp_path):
    nss, _ = _rank_case()
    nss.loc[[3, 4], "diaDistanceRank"] = np.array([2, 1], np.int16)       # A's ranks in visit 2 swapped
    failed, _, _, _ = _rank_failed(tmp_path, nss)
    assert {RANK_GATE, RANK1_GATE, ORDER_GATE} <= failed
    nss.loc[[3, 4], "diaDistanceRank"] = np.array([1, 1], np.int16)       # repeated
    failed, _, _, _ = _rank_failed(tmp_path, nss)
    assert ORDER_GATE in failed


def test_rank_fail_type_and_missing(tmp_path):
    nss, _ = _rank_case()
    failed, _, _, _ = _rank_failed(tmp_path, nss.assign(diaDistanceRank=nss["diaDistanceRank"].astype("i4")))
    assert failed == {TYPE_GATE}
    nss0 = nss.copy()
    nss0.loc[0, "diaDistanceRank"] = 0
    assert TYPE_GATE in _rank_failed(tmp_path, nss0)[0]
    failed, verdict, _, s = _rank_failed(tmp_path, nss.drop(columns="diaDistanceRank"))
    assert failed == {"diaDistanceRank column present"} and s is None


def test_rank_fail_prediction_and_dia(tmp_path):
    nss, _ = _rank_case()
    nss.loc[2, "ephOffset"] = np.float32(nss.loc[2, "ephOffset"] * 1.01)
    assert _rank_failed(tmp_path, nss)[0] == {"ephOffset == its separation (rel. 1e-06)"}
    nss, _ = _rank_case()
    nss.loc[2, "ephRa"] += 6 * AS                     # 103 now > 5" from the prediction
    assert "the row's DiaSource is within 5\" of ephRa/ephDec" in _rank_failed(tmp_path, nss)[0]
    nss, _ = _rank_case()
    nss.loc[0, "diaSourceId"] = 999
    assert "every row's DiaSource is in the DiaSource file" in _rank_failed(tmp_path, nss)[0]


def test_mock_rank_and_faults(tmp_path):
    nss, dia = _rank_case()
    n_path, d_path = _rank_files(tmp_path, nss.drop(columns="diaDistanceRank"))
    out = str(tmp_path / "m.parquet")
    assert V.main(["mock-rank", "--nearbysso", n_path, "--dia", d_path, "--output", out]) == 0
    m = pd.read_parquet(out)
    assert m["diaDistanceRank"].tolist() == nss["diaDistanceRank"].tolist()
    names = list(m.columns)
    assert names.index("diaDistanceRank") == names.index("ephOffset") + 1
    assert V.check_rank(out, d_path)[0] == "PASS"
    for fault in V.RANK_FAULTS:
        V.add_ranks(n_path, d_path, out, [fault], seed=3)
        assert V.check_rank(out, d_path)[0] == "FAIL", fault
    with pytest.raises(ValueError):
        V.add_ranks(n_path, d_path, out, ["nope"])


def test_rank_repeated_dia_ids(tmp_path):
    """DiaSource rows repeating a diaSourceId (PPDB has some) are one
    DiaSource, at its smallest separation (the contract)."""
    nss, dia = _rank_case()
    twin = dia[dia["diaSourceId"] == 101]                                  # an exact repeat
    moved = dia[dia["diaSourceId"] == 203].assign(ra=20.0 + 0.2 * AS, dec=-10.0)   # 203 again, 0.2" from A2
    dia2 = pd.concat([dia, twin, moved], ignore_index=True)
    # 101 counts once for A's 102 (rank 2, not 3); 203, at its nearest row,
    # is now A2's nearest DiaSource: 201 and 202 become ranks 2 and 3
    nss.loc[[3, 4], "diaDistanceRank"] = np.array([2, 3], np.int16)
    verdict, rep, s = V.check_rank(*_rank_files(tmp_path, nss, dia2))
    text = "\n".join(rep.lines)
    assert verdict == "PASS", text
    assert "DiaSource rows repeating a diaSourceId: 2 ids (1 rows exact repeats)" in text
    assert "sampled rows whose rank involves a repeated diaSourceId: 4" in text
    bf = s.set_index("diaSourceId")
    assert bf.loc[201, "nearest_id"] == 203
    assert np.isclose(bf.loc[201, "nearest_sep"], 0.2 * np.cos(np.radians(10)))
    # a builder counting the twin twice
    nss.loc[1, "diaDistanceRank"] = 3
    failed = {n for n, ok in V.check_rank(*_rank_files(tmp_path, nss, dia2))[1].gates if not ok}
    assert failed == {RANK_GATE}
    # the mock agrees
    n_path, d_path = _rank_files(tmp_path, nss.drop(columns="diaDistanceRank"), dia2)
    V.add_ranks(n_path, d_path, str(tmp_path / "m.parquet"))
    assert pd.read_parquet(tmp_path / "m.parquet")["diaDistanceRank"].tolist() == [1, 2, 1, 2, 3]
