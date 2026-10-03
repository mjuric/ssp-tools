"""The non-grav validation harness (bench/nongrav_validate) on synthetic
inputs: every check passes on clean data and catches an injected defect.
Network-free; the one ASSIST test skips without the ephemeris files."""
import json
import os
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import nongrav_validate as V

HAVE_ASSIST = all(os.path.exists(os.environ.get(k, ""))
                  for k in ("SSP_ASSIST_PLANETS", "SSP_ASSIST_ASTEROIDS"))


# --------------------------------------------------------------------------
# the CAR parse and the classes
# --------------------------------------------------------------------------

def _car_json(extra=(), values=None, n_cov=None, arc="2014-2024", epoch=61200.0):
    names = ["x", "y", "z", "vx", "vy", "vz", *extra]
    n = len(names)
    vals = values or [1.0, 0.5, 0.1, -0.01, 0.01, 0.001] + [1e-9 * (k + 1) for k in range(len(extra))]
    cov = {}
    for i in range(n_cov or n):
        for j in range(i, n_cov or n):
            cov[f"cov{i}{j}"] = (1e-12 * (i + 1) if i == j else 1e-15)
    return json.dumps({"CAR": {"coefficient_names": names, "coefficient_values": vals, "covariance": cov},
                       "epoch_data": {"epoch": epoch, "timesystem": "TDT"},
                       "system_data": {"EclipticObliquityArcseconds": "84381.448"},
                       "orbit_fit_statistics": {"arc_length_total": arc}})


def test_car_parse_units():
    y = V.car_parse(_car_json(["yarkovski"], values=[1, 0, 0, 0, 0.017, 0, -2.5e-4]))
    assert y["kind"] == "yarkovsky" and list(y["A_index"]) == [-1, 6, -1]
    assert y["scale"][6] == 1e-10 and y["cov"].shape == (7, 7)
    st, A = V.draw_parameters(y, 4, np.random.default_rng(1))
    assert A[0, 1] == pytest.approx(-2.5e-14)            # the 1e-10 unit
    assert (A[:, 0] == 0).all() and (A[:, 2] == 0).all()
    c = V.car_parse(_car_json(["A1", "A2"]))
    assert c["kind"] == "comet" and list(c["A_index"]) == [6, 7, -1] and (c["scale"] == 1).all()
    assert V.car_parse(_car_json(["A1"], n_cov=6))["cov"] is None     # incomplete covariance
    assert V.car_parse(_car_json(["foo"]))["kind"] == "unknown"
    assert V.car_parse(None) is None and V.car_parse("{}") is None


def test_coefficient_extras_and_classify():
    js = [_car_json(), _car_json(["A1", "A2"]), _car_json(["yarkovsky"]), _car_json(["yarkovski"]),
          _car_json(["A2"]), _car_json(["srp"]), None]
    ex = V.coefficient_extras(np.array(js, dtype=object))
    assert list(ex[:6]) == [(), ("A1", "A2"), ("yarkovsky",), ("yarkovski",), ("A2",), ("srp",)]
    assert ex[6] is None
    des = ["2020 AB", "P/1991 T1", "1999 JU3", "2004 MN4", "C/2022 N2", "2001 AA", "2002 BB"]
    assert list(V.classify(des, ex)) == ["control", "comet_ng", "yarkovsky", "yarkovsky", "comet_ng",
                                         "ng_unknown", "control"]
    des2 = ["C/2025 N1", "P/2010 J5", "A/2017 U1", "S/2004 S 46", "73P-B", "1P", "2020 AB"]
    cls = V.classify(des2, [()] * 7, has_orbit=[True] * 6 + [False])
    assert list(cls) == ["comet_grav", "comet_grav", "control", "satellite", "comet_grav", "comet_grav",
                         "no_orbit"]


# --------------------------------------------------------------------------
# offsets
# --------------------------------------------------------------------------

OBJECTS = {"P/2000 A1": "comet_ng", "P/2000 B1": "comet_ng", "2000 YA": "yarkovsky",
           "2000 CA": "control", "C/2000 G1": "comet_grav"}


def _sss(n_per=6, seed=1):
    rng = np.random.default_rng(seed)
    rows = []
    k = 0
    for d, c in OBJECTS.items():
        for _ in range(n_per):
            off = {"comet_ng": 2.0, "yarkovsky": 0.08, "control": 0.06, "comet_grav": 0.3}[c]
            rows.append(dict(obsid=f"L{k:05d}", designation=d, ephRa=rng.uniform(0, 360),
                             ephDec=rng.uniform(-60, 60), ephOffset=off * rng.uniform(0.8, 1.2),
                             ephRaErr=1e-5 * rng.uniform(1, 2), ephDecErr=1e-5 * rng.uniform(1, 2),
                             ephRa_ephDec_Cov=1e-12, topoRange=rng.uniform(1, 3), elongation=90.0))
            k += 1
    return pd.DataFrame(rows)


def _write(df, path):
    sch = pa.schema([("obsid", pa.string()), ("designation", pa.string()), ("ephRa", pa.float64()),
                     ("ephDec", pa.float64()), ("ephOffset", pa.float32()), ("ephRaErr", pa.float32()),
                     ("ephDecErr", pa.float32()), ("ephRa_ephDec_Cov", pa.float32()),
                     ("topoRange", pa.float64()), ("elongation", pa.float32())])
    pq.write_table(pa.Table.from_pandas(df, schema=sch, preserve_index=False), path)
    return str(path)


def _good_new(ref):
    new = ref.copy()
    ng = new["designation"].map(OBJECTS).isin(["comet_ng", "yarkovsky"])
    new.loc[ng, "ephRa"] += 1e-5
    com = new["designation"].map(OBJECTS) == "comet_ng"
    new.loc[com, "ephOffset"] *= 0.3
    new.loc[ng, "ephRaErr"] *= 1.2
    return new


def _offsets(tmp_path, new, ref, **kw):
    r = _write(ref, tmp_path / "ref.parquet")
    n = _write(new, tmp_path / "new.parquet")
    return V.check_offsets(n, r, objects=OBJECTS, **kw)


def test_offsets_clean(tmp_path):
    ref = _sss()
    rep = _offsets(tmp_path, _good_new(ref), ref, ng_errors="changed")
    assert rep.ok, rep.text()


@pytest.mark.parametrize("defect,failed", [
    ("control_eph", "gravity-only rows bitwise identical"),
    ("control_err", "gravity-only rows bitwise identical"),
    ("comet_grav_geom", "gravity-only rows bitwise identical"),
    ("comet_worse", "comet_ng median ephOffset improves"),
    ("one_comet_worse", "no comet_ng object much worse"),
    ("yarko_worse", "no Yarkovsky object worse beyond tolerance"),
    ("ng_not_applied", "every non-grav object's ephemeris changed"),
])
def test_offsets_defects(tmp_path, defect, failed):
    ref = _sss()
    new = _good_new(ref)
    cls = new["designation"].map(OBJECTS)
    if defect == "control_eph":
        i = new.index[cls == "control"][0]
        new.loc[i, "ephRa"] = np.nextafter(new.loc[i, "ephRa"], 400.0)
    elif defect == "control_err":
        new.loc[new.index[cls == "control"][2], "ephDecErr"] *= 1.0001
    elif defect == "comet_grav_geom":
        new.loc[new.index[cls == "comet_grav"][0], "topoRange"] += 1e-12
    elif defect == "comet_worse":
        new.loc[cls == "comet_ng", "ephOffset"] = ref.loc[cls == "comet_ng", "ephOffset"] * 1.2
    elif defect == "one_comet_worse":
        sel = new["designation"] == "P/2000 B1"
        new.loc[sel, "ephOffset"] = ref.loc[sel, "ephOffset"] * 2.0
    elif defect == "yarko_worse":
        sel = cls == "yarkovsky"
        new.loc[sel, "ephOffset"] = ref.loc[sel, "ephOffset"] + 0.05
    elif defect == "ng_not_applied":
        sel = new["designation"] == "2000 YA"
        new.loc[sel, "ephRa"] = ref.loc[sel, "ephRa"]
    rep = _offsets(tmp_path, new, ref)
    assert any(f.startswith(failed) for f in rep.failed), rep.text()


def test_offsets_ng_errors(tmp_path):
    ref = _sss()
    new = _good_new(ref)
    assert "non-grav objects' error columns unchanged (--ng-errors unchanged)" in \
        _offsets(tmp_path, new, ref, ng_errors="unchanged").failed
    new2 = new.copy()
    ng = new["designation"].map(OBJECTS).isin(["comet_ng", "yarkovsky"])
    new2["ephRaErr"] = np.where(ng, ref["ephRaErr"] * 1.00001, ref["ephRaErr"])
    # a line-of-sight-level change (< ERR_CHANGE_REL) counts as unchanged
    rep = _offsets(tmp_path, new2, ref, ng_errors="changed")
    assert any(f.startswith("non-grav error columns changed") for f in rep.failed), rep.text()
    assert _offsets(tmp_path, new2, ref, ng_errors="unchanged").ok


def test_offsets_row_sets(tmp_path):
    ref = _sss()
    new = _good_new(ref).iloc[1:]
    assert "same row set (obsid)" in _offsets(tmp_path, new, ref).failed


# --------------------------------------------------------------------------
# nearbysso
# --------------------------------------------------------------------------

def _nss_case():
    """SSSource rows of three objects, the NearbySSO input, a clean
    NearbySSO, and orbits."""
    rng = np.random.default_rng(3)
    des = ["P/2000 A1"] * 4 + ["C/2000 G1"] * 3 + ["2000 CA"] * 3
    n = len(des)
    sss = pd.DataFrame({
        "designation": des, "diaSourceId": pd.array(1000 + np.arange(n), dtype="Int64"),
        "visit": 2025070100000 + np.arange(n), "midpointMjdTai": 60860.0 + np.arange(n) * 0.1,
        "ra": rng.uniform(10, 20, n), "dec": rng.uniform(-10, 10, n)})
    sss["ephRa"] = sss["ra"] + 1e-5
    sss["ephDec"] = sss["dec"]
    for c in ("ephRateRa", "ephRateDec"):
        sss[c] = rng.normal(0, 0.2, n)
    sss["ephVmag"] = 20.0
    sss["ephOffset"] = 0.036
    sss["ephRaErr"] = sss["ephDecErr"] = 1e-4
    sss["ephRa_ephDec_Cov"] = 0.0
    sss.loc[9, "diaSourceId"] = pd.NA        # a science-image row: found by (visit, ra, dec)
    dia = pd.DataFrame({"diaSourceId": 1000 + np.arange(n), "visit": sss["visit"], "ra": sss["ra"],
                        "dec": sss["dec"]})
    dia.loc[9, "diaSourceId"] = 5555
    nss = pd.DataFrame({"diaSourceId": dia["diaSourceId"], "designation": des, "ephRa": sss["ephRa"],
                        "ephDec": sss["ephDec"], "ephOffset": sss["ephOffset"], "ephVmag": 20.0,
                        "ephRateRa": sss["ephRateRa"], "ephRateDec": sss["ephRateDec"]})
    orbits = pd.DataFrame({"designation": ["P/2000 A1", "C/2000 G1", "2000 CA", "2020 FF"],
                           "q": [1.0, 2.0, 2.5, 2.0], "e": 0.5, "i": 5.0, "node": 1.0, "argperi": 2.0,
                           "peri_time": 60000.0,
                           "mpc_orb_jsonb": [_car_json(["A1", "A2"]), _car_json(), _car_json(),
                                             _car_json(arc="1 days")]})
    cmap = {"P/2000 A1": "comet_ng", "C/2000 G1": "comet_grav", "2000 CA": "control", "2020 FF": "control"}
    return sss, dia, nss, orbits, cmap


def _nss_run(sss, dia, nss, orbits, cmap, coarse_sigma=None):
    rep = V.Report("t")
    df = V.nearbysso_compare(nss, sss, dia, orbits, cmap, rep, coarse_sigma=coarse_sigma)
    V._nearbysso_gates(df, rep)
    return rep, df


def test_nearbysso_clean():
    rep, df = _nss_run(*_nss_case())
    assert rep.ok, rep.text()
    assert (df["status"] == "match").all() and len(df) == 10
    assert df["dia_id"].iloc[-1] == 5555        # matched by position


def test_nearbysso_explained_misses():
    sss, dia, nss, orbits, cmap = _nss_case()
    sss.loc[0, "ephOffset"] = 20.0                    # a comet beyond its 15" radius
    sss.loc[7, "ephOffset"] = 6.0                     # an asteroid beyond its 5" radius
    sss.loc[4, ["ephRaErr", "ephDecErr"]] = 1.0       # sigma 3600"
    nss = nss[~nss["diaSourceId"].isin([1000, 1004, 1007])]
    rep, df = _nss_run(sss, dia, nss, orbits, cmap)
    assert rep.ok, rep.text()
    assert df.loc[0, "status"] == "offset > radius" and df.loc[4, "status"] == "sigma > 10\""
    assert df.loc[7, "status"] == "offset > radius"
    assert df.loc[0, "match_radius"] == 15.0 and df.loc[7, "match_radius"] == 5.0


# the per-object match radius (docs/design/comet-radius.md): 15" for comets

BEYOND_GATE = "no NearbySSO row beyond its object's match radius"


def test_match_radius_from_contract():
    assert V.match_radius(["P/2000 A1", "C/2000 G1", "2000 CA", "A/2017 U1", "I/2017 U1"]).tolist() == \
        [15.0, 15.0, 5.0, 5.0, 15.0]
    assert V.MATCH_RADIUS_ARCSEC == 5.0 and V.MATCH_RADIUS_COMET_ARCSEC == 15.0


def test_nearbysso_comet_row_at_10_accepted():
    """A comet's NearbySSO row at 10" (inside its 15" radius) is a match."""
    sss, dia, nss, orbits, cmap = _nss_case()
    sss.loc[0, "ephOffset"] = 10.0
    nss.loc[0, "ephOffset"] = 10.0
    rep, df = _nss_run(sss, dia, nss, orbits, cmap)
    assert rep.ok, rep.text()
    assert df.loc[0, "status"] == "match"


def test_nearbysso_comet_missing_at_10_unexplained():
    """A comet's DiaSource at 10" with no NearbySSO row: an unexplained
    miss."""
    sss, dia, nss, orbits, cmap = _nss_case()
    sss.loc[0, "ephOffset"] = 10.0
    nss = nss[nss["diaSourceId"] != 1000]
    rep, df = _nss_run(sss, dia, nss, orbits, cmap)
    assert df.loc[0, "status"] == "UNEXPLAINED"
    assert "every miss explained" in rep.failed


def test_nearbysso_asteroid_row_at_10_flagged():
    """An asteroid's NearbySSO row at 10" (beyond its 5" radius) is wrong;
    its SSSource row at 10" without a NearbySSO row is explained."""
    sss, dia, nss, orbits, cmap = _nss_case()
    sss.loc[7, "ephOffset"] = 10.0
    nss.loc[7, "ephOffset"] = 10.0
    rep, _ = _nss_run(sss, dia, nss, orbits, cmap)
    assert BEYOND_GATE in rep.failed, rep.text()
    rep, df = _nss_run(sss, dia, nss[nss["diaSourceId"] != 1007], orbits, cmap)
    assert rep.ok, rep.text()
    assert df.loc[7, "status"] == "offset > radius"


def test_nearbysso_comet_at_20_beyond_radius():
    """A comet's DiaSource at 20" is explained as beyond its radius; a
    NearbySSO row for it would be wrong."""
    sss, dia, nss, orbits, cmap = _nss_case()
    sss.loc[0, "ephOffset"] = 20.0
    rep, df = _nss_run(sss, dia, nss[nss["diaSourceId"] != 1000], orbits, cmap)
    assert rep.ok, rep.text()
    assert df.loc[0, "status"] == "offset > radius"
    nss.loc[0, "ephOffset"] = 20.0
    rep, _ = _nss_run(sss, dia, nss, orbits, cmap)
    assert BEYOND_GATE in rep.failed


@pytest.mark.parametrize("defect,failed", [
    ("drop_row", "every miss explained"),
    ("satellite", "no natural satellites (S/) in NearbySSO"),
    ("filtered", "no NearbySSO row names an orbit the filter drops"),
    ("position", "matched rows agree: position"),
    ("rate", "matched rows agree: ephRateRa"),
    ("comets_missing", "most comet rows matched"),
])
def test_nearbysso_defects(defect, failed):
    sss, dia, nss, orbits, cmap = _nss_case()
    if defect == "drop_row":
        nss = nss[nss["diaSourceId"] != 1008]
    elif defect == "satellite":
        nss = pd.concat([nss, nss.iloc[[0]].assign(diaSourceId=9, designation="S/2004 S 46")])
    elif defect == "filtered":
        nss = pd.concat([nss, nss.iloc[[0]].assign(diaSourceId=9, designation="2020 FF")])
    elif defect == "position":
        nss.loc[2, "ephDec"] += 2e-6 / 3.6       # 2 mas
    elif defect == "rate":
        nss.loc[5, "ephRateRa"] += 1e-4
    elif defect == "comets_missing":
        nss = nss[nss["designation"] == "2000 CA"]
        sss.loc[sss["designation"] != "2000 CA", "ephOffset"] = 20.0   # explained, but not matched
    rep, _ = _nss_run(sss, dia, nss, orbits, cmap)
    assert any(f.startswith(failed) for f in rep.failed), rep.text()


def test_nearbysso_no_comets_in_input():
    """No comet SSSource row's DiaSource in the NearbySSO input: the comet
    gate is not applicable, not a failure."""
    sss, dia, nss, orbits, cmap = _nss_case()
    keep = (sss["designation"] == "2000 CA").to_numpy()
    sss, dia = sss[keep].reset_index(drop=True), dia[keep].reset_index(drop=True)
    nss = nss[nss["designation"] == "2000 CA"]
    rep, _ = _nss_run(sss, dia, nss, orbits, cmap)
    assert rep.ok, rep.text()
    assert "most comet rows matched: not applicable" in rep.text()


def test_nearbysso_coarse_sigma_explains():
    sss, dia, nss, orbits, cmap = _nss_case()
    nss = nss[nss["diaSourceId"] != 1008]
    rep, df = _nss_run(sss, dia, nss, orbits, cmap, coarse_sigma=lambda d, t: np.full(len(t), 12.0))
    assert rep.ok, rep.text()
    assert df.loc[8, "status"].startswith("sigma > 10\" (coarse")
    rep, _ = _nss_run(sss, dia, nss, orbits, cmap, coarse_sigma=lambda d, t: np.full(len(t), 1.0))
    assert "every miss explained" in rep.failed


def test_orbit_filter_reason():
    _, _, _, orbits, _ = _nss_case()
    o = pd.concat([orbits, pd.DataFrame({"designation": ["S/2004 S 1", "2021 XX", "2021 YY"],
                                         "q": [1.0, np.nan, 1.0], "e": 0.1, "i": 1.0, "node": 1.0,
                                         "argperi": 1.0, "peri_time": 1.0,
                                         "mpc_orb_jsonb": [_car_json(), _car_json(), _car_json(arc=None)]})])
    assert list(V.orbit_filter_reason(o)) == ["", "", "", "arc", "satellite", "missing_elements", "arc"]


def test_map_to_dia_requires_exact_ids():
    sss, dia, *_ = _nss_case()
    with pytest.raises(TypeError):
        V.map_to_dia(sss.assign(diaSourceId=sss["diaSourceId"].astype("float64")), dia)


# --------------------------------------------------------------------------
# uncertainty
# --------------------------------------------------------------------------

def test_frames():
    R = V.ecl_to_eq_matrix()
    eps = np.deg2rad(84381.448 / 3600)
    assert np.allclose(R @ [0, 0, 1], [0, -np.sin(eps), np.cos(eps)])
    assert np.allclose(R @ R.T, np.eye(3))
    st = np.array([[1.0, 0, 0, 0, 1.0, 0]])
    b = V.helio_ecl_to_bary_icrf(st, np.array([0.1, 0.2, 0.3, 0.01, 0.02, 0.03]), 84381.448)
    assert np.allclose(b[0, :3], [1.1, 0.2, 0.3])
    assert np.allclose(b[0, 3:], [0.01, 0.02 + np.cos(eps), 0.03 + np.sin(eps)])


def test_draws_state_only():
    c = V.car_parse(_car_json(["A1", "A2"]))
    st, A = V.draw_parameters(c, 2000, np.random.default_rng(2), state_only=True)
    assert np.allclose(A, [1e-9, 2e-9, 0.0])
    assert np.std(st[:, 0]) == pytest.approx(1e-6, rel=0.1)
    _, A = V.draw_parameters(c, 2000, np.random.default_rng(2))
    assert np.std(A[:, 1]) == pytest.approx(np.sqrt(8e-12), rel=0.1)


def test_tangent_offsets_recover_sigma():
    rng = np.random.default_rng(5)
    ra0, dec0 = np.deg2rad(40.0), np.deg2rad(-30.0)
    u0 = np.array([np.cos(dec0) * np.cos(ra0), np.cos(dec0) * np.sin(ra0), np.sin(dec0)])
    e_ra = np.array([-np.sin(ra0), np.cos(ra0), 0])
    e_dec = np.array([-np.sin(dec0) * np.cos(ra0), -np.sin(dec0) * np.sin(ra0), np.cos(dec0)])
    x = rng.normal(0, 2.0, 4000) / 206264.806
    y = rng.normal(0, 0.5, 4000) / 206264.806
    rho = 2.0 * (u0 + x[:, None] * e_ra + y[:, None] * e_dec)
    xi, eta = V.tangent_offsets(rho, 2.0 * u0)
    sr, sd, r, n = V.mc_ellipse(xi, eta)
    assert sr == pytest.approx(2.0, rel=0.05) and sd == pytest.approx(0.5, rel=0.05) and abs(r) < 0.1


def _mc_df(scale=1.0, kind="comet"):
    rows = []
    for k in range(6):
        rows.append(dict(designation=f"P/2000 A{k}", kind=kind, tai=60800.0 + k, dt_epoch=-300.0,
                         full_ra=0.1, full_dec=0.05, full_rho=0.3, full_bias=0.01, state_only_ra=0.2,
                         state_only_dec=0.05, coarse_ra=0.1 * scale, coarse_dec=0.05, sss_ra=0.1,
                         sss_dec=0.05))
    return pd.DataFrame(rows)


def test_uncertainty_gates():
    rep = V.Report("t")
    V._uncertainty_gates(_mc_df(1.03), rep, 1000, with_sss=True)
    assert rep.ok, rep.text()
    rep = V.Report("t")
    V._uncertainty_gates(_mc_df(0.8), rep, 1000, with_sss=True)   # A partials missing: sigma 20% low
    assert rep.failed == ["propagate.coarse + ellipse_at sigma vs the full Monte Carlo"], rep.text()
    rep = V.Report("t")
    V._uncertainty_gates(_mc_df(np.nan), rep, 1000, with_sss=False)   # no ellipse
    assert not rep.ok
    assert V.sigma_band(1000) == pytest.approx(3.5 / np.sqrt(1998) + 0.03)


def test_pick_times():
    assert list(V.pick_times([3.0, 1.0, 2.0, 2.0], 5)) == [1.0, 2.0, 3.0]
    t = V.pick_times(np.arange(100.0), 3)
    assert list(t) == [0.0, 50.0, 99.0]


@pytest.mark.skipif(not HAVE_ASSIST, reason="needs SSP_ASSIST_PLANETS/SSP_ASSIST_ASTEROIDS")
def test_integrate_draws_per_particle_params():
    ephem = V.open_ephem()
    s = np.array([1.6, 1.19, -0.02, -0.0091, 0.0114, -0.0016])
    states = np.tile(s, (3, 1))
    A = np.array([[0.0, 0, 0], [1e-8, -1e-8, 0], [0.0, 0, 0]])
    pos = V.integrate_draws(ephem, 9000.0, states, A, "comet", [8900.0, 9300.0])
    assert np.isfinite(pos).all()
    assert np.abs(pos[0] - pos[2]).max() < 1e-12            # the same particle twice
    assert np.abs(pos[1] - pos[0]).max() > 1e-6             # its own A's act on particle 1 only
    pos_y = V.integrate_draws(ephem, 9000.0, states, A, "yarkovsky", [9300.0])
    assert np.abs(pos_y[1, 0] - pos[1, 1]).max() > 1e-7     # the g(r) is per kind
