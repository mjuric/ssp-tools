"""Network-free tests of bench/jpl_compare.py (WP N5): the cache-first JPL
client, the request builders, the parsers (on excerpts of real answers),
the geometry and the non-grav model handling. Nothing here talks to JPL."""

import json
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import jpl_compare as J
from ssp import nongrav as N
from ssp.ephem_assist import cometary_to_helio_ecliptic, ecliptic_to_equatorial

# An excerpt of a Horizons observer answer (2P/Encke, JPL#K273/20, X05, TT).
HZ_OBS = """API VERSION: 1.2
*******************************************************************************
Target body name: 2P/Encke                        {source: JPL#K273/20}
Center body name: Earth (399)                     {source: DE441}
*******************************************************************************
Initial IAU76/J2000 heliocentric ecliptic osculating elements (au, days, deg.):
  EPOCH=  2460147.5 ! 2023-Jul-22.0000000 (TDB)    RMSW= n.a.
   EC= .8469532425568865   QR= .3395910296522707   TP= 2460240.0266747689
   OM= 334.0240655739339   W= 187.2820411908973    IN= 11.33709697552753
  Equivalent ICRF heliocentric cartesian coordinates (au, au/d):
   X= 1.564746628554244E+00  Y= 4.893647265026269E-01  Z= 4.882908818596594E-01
  VX=-1.431656881443231E-02 VY= 2.742739686966740E-03 VZ= 3.397424502898812E-04
Comet non-gravitational force model (AMRAT=m^2/kg;A1-A3=au/d^2;DT=days;R0=au):
   AMRAT=  0.                                      DT=  0.
   A1= 2.041002176702E-10  A2= 2.635817800183E-12  A3= 0.
 Non-standard or simulated/proxy model:
   ALN=  .1112620426   NK=  4.6142   NM=  2.15     NN=  5.093    R0=  2.808
*******************************************************************************
Date_________JDTT, , , R.A.___(ICRF), DEC____(ICRF), RA_3sigma, DEC_3sigma, SMAA_3sig, SMIA_3sig, Theta,
*******************************************************************************
$$SOE
2459417.500000000, ,m, 343.098376774, -8.254881173, 0.184, 0.173, 0.199, 0.155, 37.765, 0.0967038,
2459490.500000020,A, , 326.413642876, -13.483322598, 0.168, 0.153, 0.178, 0.141, 32.446, 0.0788637,
2460210.026675018,*, , 136.698363978, 26.823003125, 0.661, 0.385, 0.749, 0.155, -28.725, 0.3642031,
$$EOE
"""

HZ_VEC = """Target body name: 2P/Encke                        {source: JPL#K273/20}
 JDTDB, Calendar Date (TDB), X, Y, Z, VX, VY, VZ,
*******************************************************************************
$$SOE
2459417.5, 2021-Jul-22, 3.268619270133907E+00, -1.6E+00, -7.7E-01, 4.6E-03, 1.0E-03, 1.206783098308467E-03,
$$EOE
"""


def _sbdb_json(model_pars, des="2P", epoch="2460147.5"):
    el = [
        {"name": "e", "value": ".8469532425568865"},
        {"name": "q", "value": ".3395910296522707"},
        {"name": "tp", "value": "2460240.026674768663"},
        {"name": "om", "value": "334.0240655739339"},
        {"name": "w", "value": "187.2820411908973"},
        {"name": "i", "value": "11.33709697552753"},
    ]
    mp = [{"name": n, "value": v, "sigma": None, "kind": k} for n, v, k in model_pars]
    return json.dumps(
        {
            "object": {"des": des, "fullname": des, "spkid": "1", "kind": "cn", "prefix": "P"},
            "orbit": {"orbit_id": "K273/20", "epoch": epoch, "elements": el, "model_pars": mp},
        }
    )


# --- the client


class FakeNet:
    def __init__(self):
        self.urls = []
        self.t = 1000.0
        self.slept = []

    def opener(self, url):
        self.urls.append(url)
        return b'{"ok": 1}'

    def clock(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def test_request_key_ignores_parameter_order():
    assert J.request_key("sbdb", {"a": 1, "b": 2}) == J.request_key("sbdb", {"b": 2, "a": 1})
    assert J.request_key("sbdb", {"a": 1}) != J.request_key("horizons", {"a": 1})


def test_offline_client_never_fetches(tmp_path):
    net = FakeNet()
    c = J.JPLClient(str(tmp_path), offline=True, opener=net.opener)
    with pytest.raises(J.NotCachedError):
        c.get("sbdb", {"sstr": "2P"}, "sbdb 2P")
    assert net.urls == []


def test_online_client_caches_logs_paces_and_budgets(tmp_path):
    net = FakeNet()
    c = J.JPLClient(
        str(tmp_path), offline=False, opener=net.opener, sleep=net.sleep, clock=net.clock, budget=2
    )
    assert c.get("sbdb", {"sstr": "2P"}, "sbdb 2P") == '{"ok": 1}'
    assert c.get("sbdb", {"sstr": "2P"}, "sbdb 2P") == '{"ok": 1}'  # cached: not re-sent
    assert len(net.urls) == 1
    c.get("sbdb", {"sstr": "3P"}, "sbdb 3P")
    assert len(net.urls) == 2
    assert net.slept and min(net.slept) > 0 and abs(sum(net.slept) - J.MIN_INTERVAL_S) < 1e-9
    log = open(os.path.join(str(tmp_path), "requests.log")).read().splitlines()
    assert len(log) == 2 and all(ln.split("\t")[1] == "9" for ln in log)
    with pytest.raises(J.BudgetExceededError):
        c.get("sbdb", {"sstr": "4P"}, "sbdb 4P")
    # a later run reads its pacing and budget from the log
    c2 = J.JPLClient(
        str(tmp_path), offline=False, opener=net.opener, sleep=net.sleep, clock=net.clock, budget=3
    )
    assert c2.n_logged() == 2


def test_failed_request_is_logged_and_not_cached(tmp_path):
    def boom(url):
        raise RuntimeError("HTTP 400: nope")

    net = FakeNet()
    c = J.JPLClient(str(tmp_path), offline=False, opener=boom, sleep=net.sleep, clock=net.clock)
    with pytest.raises(RuntimeError):
        c.get("sbdb", {"sstr": "x"}, "sbdb x")
    assert not c.cached("sbdb", {"sstr": "x"}, "sbdb x")
    assert "HTTP 400" in open(c.log_path()).read() and c.n_logged() == 1


# --- requests


def test_horizons_commands():
    assert J.horizons_command("2P", True) == "'DES=2P;CAP;NOFRAG'"
    assert J.horizons_command("2101", False) == "'2101;'"
    assert J.horizons_command("2025 QH138", False) == "'DES=2025 QH138;'"


def test_observer_and_vector_params():
    p = J.horizons_observer_params("'2101;'", [60000.1, 60000.2])
    assert p["TLIST"] == "60000.1000000000,60000.2000000000" and p["TIME_TYPE"] == "TT"
    assert p["QUANTITIES"] == "'1,36,37'" and p["CENTER"] == "'X05'" and p["EXTRA_PREC"] == "YES"
    v = J.horizons_vectors_params("'2101;'", [60000.1])
    assert v["TIME_TYPE"] == "TDB" and v["CENTER"] == "'500@0'" and v["VEC_CORR"] == "NONE"


def test_user_params_carry_the_nongravs():
    row = {
        "q": 0.5,
        "e": 0.8,
        "i": 10.0,
        "node": 90.0,
        "argperi": 300.0,
        "peri_time": 61000.0,
        "epoch_mjd": 61200.0,
    }
    ng = N.NonGrav(np.array([1e-9, 2e-10, 0.0]), "comet", np.array([True, True, False]), None)
    p = J.horizons_user_params(row, ng, [61000.5])
    assert float(p["A1"]) == 1e-9 and float(p["A2"]) == 2e-10 and float(p["ALN"]) == 0.1112620426
    assert float(p["R0"]) == 2.808 and p["COMMAND"] == "';'" and p["ECLIP"] == "J2000"
    # TT -> TDB: TDB - TT is under 2 ms
    assert abs(float(p["EPOCH"]) - (61200.0 + 2400000.5)) < 2.5e-8
    y = N.NonGrav(np.array([0.0, -4e-10, 0.0]), "yarkovsky", np.array([False, True, False]), None)
    py = J.horizons_user_params(row, y, [61000.5])
    assert float(py["ALN"]) == 1.0 and float(py["NK"]) == 0.0 and float(py["NM"]) == 2.0
    p0 = J.horizons_user_params(row, ng, [61000.5], with_ng=False)
    assert "A1" not in p0 and "ALN" not in p0


# --- parsing


def test_parse_horizons_observer_excerpt():
    o = J.parse_horizons_observer(HZ_OBS)
    assert o["ra"][0] == 343.098376774 and o["dec"][1] == -13.483322598
    assert o["ra3s"][2] == 0.661 and o["smia3s"][0] == 0.155 and o["theta"][2] == -28.725
    assert o["jd"][0] == 2459417.5
    m = o["meta"]
    assert m["source"] == "JPL#K273/20" and m["A1"] == 2.041002176702e-10 and m["ALN"] == 0.1112620426
    assert m["DT"] == 0.0 and m["R0"] == 2.808


def test_parse_horizons_vectors_excerpt():
    v = J.parse_horizons_vectors(HZ_VEC)
    assert (
        v["jd"][0] == 2459417.5 and v["X"][0, 0] == 3.268619270133907 and v["V"][2, 0] == 1.206783098308467e-3
    )


def test_horizons_initial_elements_reproduce_its_state():
    """Our element conversion (DE440's GM_sun, IAU76 obliquity) gives the
    ICRF state Horizons prints for the same elements, to < 1 m."""
    ic = J.horizons_initial_elements(HZ_OBS)
    st = J.horizons_helio_state(HZ_OBS)
    el = ic["elements"]
    X, V = cometary_to_helio_ecliptic(
        el["q"],
        el["e"],
        np.radians(el["i"]),
        np.radians(el["om"]),
        np.radians(el["w"]),
        ic["epoch"] - el["tp"],
        mu=J.GM_SUN_DE440,
    )
    dx = np.linalg.norm(ecliptic_to_equatorial(X) - st[:3]) * J.AU_KM * 1e3
    assert dx < 1.0
    assert np.linalg.norm(ecliptic_to_equatorial(V) - st[3:]) * J.AU_KM * 1e9 / 86400 < 0.1  # um/s


def test_parse_sbdb_and_model_pars():
    sb = J.parse_sbdb(_sbdb_json([("A1", "2.04E-10", "EST"), ("A2", "2.6E-12", "EST")]))
    assert sb["des"] == "2P" and sb["epoch"] == 2460147.5 and sb["elements"]["q"] == 0.3395910296522707
    A, has_ng, has_dt, gr, kind, other = J.jpl_nongrav_summary(sb["model_pars"])
    assert has_ng and not has_dt and kind == "marsden" and other == [] and A[0] == 2.04e-10
    with pytest.raises(ValueError):
        J.parse_sbdb(json.dumps({"list": [{"pdes": "141P"}], "message": "more than one"}))


@pytest.mark.parametrize(
    "pars, kind",
    [
        (
            [
                ("A2", "-3.2E-14", "EST"),
                ("ALN", "1.", "SET"),
                ("NK", "0.", "SET"),
                ("NM", "2.", "SET"),
                ("R0", "1.", "SET"),
            ],
            "1/r2",
        ),
        (
            [
                ("A2", "-9.8E-11", "EST"),
                ("ALN", ".0408373333", "SET"),
                ("NK", "2.6", "SET"),
                ("NM", "2", "SET"),
                ("NN", "3", "SET"),
                ("R0", "5", "SET"),
            ],
            "other",
        ),
        ([("A1", "1E-9", "EST")], "marsden"),
    ],
)
def test_gr_kinds(pars, kind):
    mp = J.parse_sbdb(_sbdb_json(pars))["model_pars"]
    assert J.jpl_nongrav_summary(mp)[4] == kind


def test_nonstandard_gr_becomes_a_runtime_model():
    sb = J.parse_sbdb(
        _sbdb_json(
            [
                ("A2", "-9.8E-11", "EST"),
                ("ALN", ".04", "SET"),
                ("NK", "2.6", "SET"),
                ("NM", "2", "SET"),
                ("NN", "3", "SET"),
                ("R0", "5", "SET"),
            ]
        )
    )
    try:
        ng, note = J.jpl_nongrav(sb, "TEST")
        assert ng.model == "jpl:TEST" and N.G_OF_R[ng.model] == dict(
            alpha=0.04, nm=2.0, nn=3.0, nk=2.6, r0=5.0
        )
        assert ng.A[1] == -9.8e-11 and "other" in note
    finally:
        N.G_OF_R.pop("jpl:TEST", None)
    ng, _ = J.jpl_nongrav(J.parse_sbdb(_sbdb_json([])), "X")
    assert ng is N.NONE


def test_orbit_flags():
    mpc = N.NonGrav(np.array([1e-9, 1e-10, 0]), "comet", np.array([True, True, False]), None)
    jpl = {
        "A1": (1e-9, np.nan, "EST"),
        "A2": (1e-10, np.nan, "EST"),
        "A3": (1e-11, np.nan, "EST"),
        "DT": (9.0, np.nan, "EST"),
    }
    f = " | ".join(J.orbit_flags("P/1 X", "comet_ng", mpc, jpl, "JPL#1"))
    assert "DT" in f and "fitted A's differ" in f
    f = J.orbit_flags(
        "2025 QH138",
        "yarkovsky",
        N.NonGrav(np.array([0, -4e-10, 0]), "yarkovsky", np.array([False, True, False]), None),
        {},
        "E2026-HE1",
    )
    assert any("non-JPL" in x for x in f) and any("JPL is gravity-only" in x for x in f)
    assert J.orbit_flags("2007 GK33", "control", N.NONE, {}, "JPL#23") == []
    f = J.orbit_flags("P/1994 P1", "comet_grav", N.NONE, {"A1": (1e-9, 0, "EST")}, "JPL#K202/13")
    assert f == ["JPL fits non-gravs, MPC is gravity-only"]


# --- geometry


def test_separation_and_tangent_offsets():
    ra0, dec0 = np.array([10.0, 200.0]), np.array([-30.0, 60.0])
    d = 1.0 / 3600.0
    ra1, dec1 = ra0 + d / np.cos(np.radians(dec0)), dec0 + d
    sep = J.separation_arcsec(ra0, dec0, ra1, dec1)
    np.testing.assert_allclose(sep, np.sqrt(2), rtol=1e-4)
    e, n = J.tangent_offset_arcsec(ra0, dec0, ra1, dec1)
    np.testing.assert_allclose(e, 1.0, rtol=1e-4)
    np.testing.assert_allclose(n, 1.0, rtol=1e-4)


@pytest.mark.parametrize("conv", ["north_east", "north_west", "east_north", "east_south"])
def test_ellipse_cov_axes(conv):
    C = J.ellipse_cov(np.array([3.0]), np.array([1.0]), np.array([30.0]), conv)[0]
    w, v = np.linalg.eigh(C)
    np.testing.assert_allclose(w, [1.0, 9.0])
    major = v[:, 1] * np.sign(v[0, 1] or 1)
    ang = np.degrees(np.arctan2(major[0], major[1])) % 180  # from north through east
    expect = {"north_east": 30, "north_west": 150, "east_north": 60, "east_south": 120}[conv]
    assert abs(ang - expect) < 1e-9


def test_jpl_convention_and_sense_are_found():
    rng = np.random.default_rng(1)
    n = 200
    a, b = rng.uniform(0.2, 1, n), rng.uniform(0.06, 0.19, n)
    th = rng.uniform(-90, 90, n)
    dec = rng.uniform(-60, 60, n)
    C = J.ellipse_cov(a, b, th, "east_north")
    o = {
        "smaa3s": a,
        "smia3s": b,
        "theta": th,
        "dec": dec,
        "ra3s": np.sqrt(C[:, 0, 0]),
        "dec3s": np.sqrt(C[:, 1, 1]),
    }
    best, rms = J.jpl_convention([o])
    assert best == ("east_north", True) and rms[best] < 1e-12
    conv, frac = J.jpl_correlation_sense([(o, C / 9.0)], best)
    assert conv == ("east_north", True) and frac["east_north"] == 1.0 and frac["east_south"] == 0.0


def test_sigma_along():
    C = np.array([[[4.0, 0.0], [0.0, 1.0]]])
    assert J.sigma_along(C, np.array([1.0]), np.array([0.0]))[0] == 2.0
    assert J.sigma_along(C, np.array([0.0]), np.array([3.0]))[0] == 1.0


@pytest.mark.parametrize("q, e", [(0.34, 0.847), (2.0, 0.1), (1.35, 6.14)])
def test_state_to_cometary_round_trip(q, e):
    inc, node, argp, dt = 11.3, 334.0, 187.3, -92.5
    X, V = cometary_to_helio_ecliptic(
        q, e, np.radians(inc), np.radians(node), np.radians(argp), dt, mu=J.GM_SUN_DE440
    )
    q2, e2, i2, n2, w2, dt2 = J.state_to_cometary(X, V, J.GM_SUN_DE440)
    np.testing.assert_allclose([q2, e2, i2, n2, w2], [q, e, inc, node, argp], rtol=1e-12, atol=1e-10)
    assert abs(dt2 - dt) < 1e-7


def test_jpl_row_keeps_the_tdb_interval():
    sb = J.parse_sbdb(_sbdb_json([]))
    row = J.jpl_row(sb)
    ep_tdb, tp_tdb = sb["epoch"] - 2400000.5, sb["elements"]["tp"] - 2400000.5
    assert row["epoch_mjd"] - row["peri_time"] == pytest.approx(ep_tdb - tp_tdb, abs=1e-12)
    assert abs(float(J.tt_to_tdb(row["epoch_mjd"])) - ep_tdb) < 1e-11


def test_check2_pass_rules():
    s = pd.DataFrame(
        {
            "kind": ["grav", "grav", "comet", "comet", "yarkovsky"],
            "rate_km_yr": [0.10, 0.14, 0.25, 0.30, 0.01],
            "a_max_mas": [0.5, 1.5, 9.0, 0.1, 0.1],
            "b_max_mas": [0.5, 1.4, 9.0, 0.1, 0.1],
            "b_geom_max_mas": [0.1, 0.1, 0.1, 0.1, 0.6],
        }
    )
    thr = J.check2_pass(s)
    assert thr == pytest.approx(0.28)
    assert list(s["pass"]) == [True, True, True, False, False]


def test_check1_object_list_and_times():
    """The fixture's objects (skipped without the fixture)."""
    if not os.path.exists(os.path.join(J.FIXTURE, "objects.txt")):
        pytest.skip("no fixture")
    sel = J.check1_objects()
    cls = pd.Series(dict(sel)).value_counts()
    assert cls["comet_ng"] == 15 and cls["yarkovsky"] == 24 and cls["comet_grav"] == 5 and cls["control"] == 5
    assert J.TT_MINUS_TAI_DAY * 86400 == pytest.approx(32.184)
