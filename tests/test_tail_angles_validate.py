"""Network-free tests of bench/tail_angles_validate.py (WP T2 of
docs/design/tail-angles.md): the independent position-angle implementation,
the frame rotations, the Horizons quantity-27 request and parser (on an
excerpt of a real answer) and the SSSource/NearbySSO consistency checker
(on synthetic tables, with injected faults). Nothing here talks to JPL."""

import os
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench import jpl_compare as J
from bench import tail_angles_validate as T

# An excerpt of a Horizons answer with quantities 1, 19, 20, 24, 27
# (70P/Kojima, JPL#68, X05, TT; 2026-10-03).
HZ_PA = """API VERSION: 1.2
*******************************************************************************
Target body name: 70P/Kojima                      {source: JPL#68}
Center body name: Earth (399)                     {source: DE441}
*******************************************************************************
Initial IAU76/J2000 heliocentric ecliptic osculating elements (au, days, deg.):
  EPOCH=  2457156.5 ! 2015-May-14.0000000 (TDB)    RMSW= n.a.
   EC= .4539128537842526   QR= 2.006566090644989   TP= 2456951.1670662342
   OM= 119.2701412323078   W= 1.953402440075936    IN= 6.600118680921454
  Equivalent ICRF heliocentric cartesian coordinates (au, au/d):
   X=-2.477080116238540E+00  Y=-6.467577672651326E-01  Z= 2.800390143054585E-02
  VX=-1.545149959958162E-03 VY=-1.147911415717274E-02 VZ=-4.057898206583409E-03
Comet non-gravitational force model (AMRAT=m^2/kg;A1-A3=au/d^2;DT=days;R0=au):
   AMRAT=  0.                                      DT=  0.
   A1= 2.50734668225E-10   A2= -7.495436817408E-11 A3= 0.
 Non-standard or simulated/proxy model:
   ALN=  .1112620426   NK=  4.6142   NM=  2.15     NN=  5.093    R0=  2.808
*******************************************************************************
@HDR@
*******************************************************************************
$$SOE
@ROW1@
@ROW2@
$$EOE
*******************************************************************************
Column meaning:

 'PsAng,   PsAMV,' =
   The position angles of the extended Sun-to-target radius vector ("PsAng")
and the negative of the targets' heliocentric velocity vector ("PsAMV"), as
seen in the observers' plane-of-sky, measured counter-clockwise (east) from
reference-frame north-pole. Primarily intended for ACTIVE COMETS, "PsAng"
is an indicator of the comets' gas-tail orientation in the sky (being in the
anti-sunward direction) while "PsAMV" is an indicator of dust-tail orientation.
Units: DEGREES

Computations by ...
"""

# (the table's long lines, split for the line length)
_HDR = (
    "Date_________JDTT, , , R.A.___(ICRF), DEC____(ICRF), "
    "               r,       rdot,             delta,     deldot,     S-T-O, "
    "   PsAng,   PsAMV,"
)
_ROW1 = (
    "2460847.738924954, , , 312.965051381, -18.172043243, "
    "  5.343556744640, -0.3114640,  4.53079655999650,-18.4869239,    7.1147,  254.914, "
    "260.814,"
)
_ROW2 = (
    "2460874.594585409, , , 309.640355092, -19.284711659, "
    "  5.337028092571, -0.5303761,  4.33780368957955, -6.2815418,    2.2160, "
    " 259.163, n.a.,"
)
HZ_PA = HZ_PA.replace("@HDR@", _HDR).replace("@ROW1@", _ROW1).replace("@ROW2@", _ROW2)


def radec_unit(ra, dec):
    a, d = np.radians(ra), np.radians(dec)
    return np.array([np.cos(d) * np.cos(a), np.cos(d) * np.sin(a), np.sin(d)])


def contract_pa(w, ra, dec):
    """The contract's formula, literally (trigonometric basis)."""
    a, d = np.radians(ra), np.radians(dec)
    north = np.array([-np.sin(d) * np.cos(a), -np.sin(d) * np.sin(a), np.cos(d)])
    east = np.array([-np.sin(a), np.cos(a), np.zeros_like(a)])
    return np.degrees(np.arctan2(np.sum(w * east, 0), np.sum(w * north, 0))) % 360.0


# ---------------------------------------------------------------------------
# The position angle
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ra", [0.0, 37.0, 180.0, 359.9])
@pytest.mark.parametrize("dec", [-60.0, 0.0, 45.0])
def test_cardinal_directions(ra, dec):
    a, d = np.radians(ra), np.radians(dec)
    u = radec_unit(ra, dec)[:, None] * 3.7  # any length
    north = np.array([-np.sin(d) * np.cos(a), -np.sin(d) * np.sin(a), np.cos(d)])[:, None]
    east = np.array([-np.sin(a), np.cos(a), 0.0])[:, None]
    for w, pa_exp in ((north, 0.0), (east, 90.0), (-north, 180.0), (-east, 270.0), (north + east, 45.0)):
        got = T.position_angle(w + 5.0 * u, u)[0]  # the radial part doesn't matter
        assert abs(T.dangle(got, pa_exp)) < 1e-9, (w.ravel(), got, pa_exp)


def test_matches_the_contract_formula_and_is_in_range():
    rng = np.random.default_rng(1)
    n = 20000
    ra, dec = rng.uniform(0, 360, n), np.degrees(np.arcsin(rng.uniform(-1, 1, n)))
    u = radec_unit(ra, dec) * rng.uniform(0.01, 50, n)
    w = rng.normal(size=(3, n))
    got = T.position_angle(w, u)
    assert np.all((got >= 0) & (got < 360))
    assert np.max(np.abs(T.dangle(got, contract_pa(w, ra, dec)))) < 1e-9


def test_ra_wraparound_and_tiny_negative_angles():
    # just west of north at RA ~ 0/360: PA just below 360, never 360.0
    for ra in (359.9999999, 0.0, 1e-9):
        u = radec_unit(ra, 10.0)[:, None]
        d = np.radians(10.0)
        north = np.array(
            [-np.sin(d) * np.cos(np.radians(ra)), -np.sin(d) * np.sin(np.radians(ra)), np.cos(d)]
        )
        east = np.array([-np.sin(np.radians(ra)), np.cos(np.radians(ra)), 0.0])
        w = (north - 1e-17 * east)[:, None]
        got = T.position_angle(w, u)[0]
        assert 0.0 <= got < 360.0


def test_pole_and_opposition():
    # u at the north pole: alpha = 0, north = -x, east = +y
    u = np.array([[0.0], [0.0], [2.0]])
    assert T.position_angle(np.array([[-1.0], [0.0], [0.0]]), u)[0] == pytest.approx(0.0)
    assert T.position_angle(np.array([[0.0], [1.0], [0.0]]), u)[0] == pytest.approx(90.0)
    # south pole: north = +x
    u = np.array([[0.0], [0.0], [-1.0]])
    assert T.position_angle(np.array([[1.0], [0.0], [0.0]]), u)[0] == pytest.approx(0.0)
    # exact opposition: w along the line of sight -> NaN; NaN input -> NaN.
    # ("Exactly zero" is up to rounding: w = 2u for u = (1, 2, 3) projects
    # to ~1e-16, not 0, and gets an arbitrary angle; sky_fraction tells.)
    u = np.array([[1.0, 1.0], [0.0, 2.0], [0.0, 3.0]])
    w = np.array([[2.0, np.nan], [0.0, 0.0], [0.0, 1.0]])
    assert np.isnan(T.position_angle(w, u)).all()


def test_tail_angles_uses_minus_velocity():
    u = radec_unit(np.array([30.0]), np.array([-20.0]))
    r = np.array([[0.3], [-0.2], [0.9]])
    v = np.array([[10.0], [5.0], [-3.0]])
    a, m = T.tail_angles(r, v, u)
    assert a[0] == pytest.approx(T.position_angle(r, u)[0])
    assert T.dangle(m[0], T.position_angle(v, u)[0] + 180.0) == pytest.approx(0.0, abs=1e-9)


def test_sky_fraction():
    u = np.array([[1.0, 1.0], [0.0, 0.0], [0.0, 0.0]])
    w = np.array([[5.0, 1.0], [0.0, 1.0], [0.0, 0.0]])
    assert T.sky_fraction(w, u) == pytest.approx([0.0, np.sqrt(0.5)])


# ---------------------------------------------------------------------------
# Frames
# ---------------------------------------------------------------------------


def test_equator_of_date_rotation_is_about_a_tenth_of_a_degree_in_2026():
    t = np.array([61000.0])  # 2025-Sep
    for kind in ("mean", "true"):
        R = T.frame_matrices(t, kind)
        assert R.shape == (1, 3, 3)
        assert np.allclose(R[0] @ R[0].T, np.eye(3), atol=1e-14)
        pole_shift = np.degrees(np.arccos(R[0][2, 2]))
        assert 0.13 < pole_shift < 0.16  # ~20"/yr x 26 yr
    # the PA change at RA 90 is ~ the pole shift; at RA 0 / 180 much smaller
    w = np.array([[0.1], [0.2], [1.0]])
    for ra, lo, hi in ((90.0, 0.12, 0.17), (0.0, 0.0, 0.02)):
        u = radec_unit(np.array([ra]), np.array([0.0]))
        d = T.dangle(T.position_angle(w, u, T.frame_matrices(t, "true")), T.position_angle(w, u))
        assert lo <= abs(d[0]) <= hi, (ra, d)


def test_single_matrix_and_per_row_matrices_agree():
    rng = np.random.default_rng(3)
    t = np.full(5, 61000.0)
    w, u = rng.normal(size=(3, 5)), rng.normal(size=(3, 5))
    R = T.frame_matrices(t, "true")
    assert np.allclose(T.position_angle(w, u, R), T.position_angle(w, u, R[0]))


# ---------------------------------------------------------------------------
# Horizons
# ---------------------------------------------------------------------------


def test_request_asks_for_quantity_27_in_icrf_at_x05():
    p = T.horizons_tail_params("'DES=70P;CAP;NOFRAG'", [60847.2389249540])
    assert p["QUANTITIES"] == "'1,19,20,24,27'"
    assert p["REF_SYSTEM"] == "ICRF" and p["CENTER"] == "'X05'" and p["TIME_TYPE"] == "TT"
    assert p["TLIST"] == "60847.2389249540"


def test_parse_tail_table_excerpt():
    h = T.parse_tail_table(HZ_PA)
    assert h["jd"][0] == 2460847.738924954
    assert h["ra"][1] == pytest.approx(309.640355092)
    assert h["phase"].tolist() == [7.1147, 2.2160]
    assert h["psang"].tolist() == [254.914, 259.163]
    assert h["psamv"][0] == 260.814 and np.isnan(h["psamv"][1])
    assert h["r"][0] == pytest.approx(5.34355674464) and h["delta"][1] == pytest.approx(4.33780368957955)
    assert h["meta"]["source"] == "JPL#68" and h["meta"]["A2"] == pytest.approx(-7.495436817408e-11)
    assert "reference-frame north-pole" in h["notes"] and h["notes"].endswith("Units: DEGREES")
    assert "Computations" not in h["notes"]


def test_header_model_pars_feed_jpl_nongrav():
    from ssp import nongrav as N

    mp = T.header_model_pars(J.horizons_meta(HZ_PA))
    ng, note = J.jpl_nongrav({"model_pars": mp}, "P/1970 Y1")
    assert ng.model == "comet" and ng.A[1] == pytest.approx(-7.495436817408e-11)
    assert "marsden" in note
    assert N.NONE.model == ""


def test_offline_plan_never_fetches(tmp_path):
    calls = []
    c = J.JPLClient(str(tmp_path), offline=True, opener=lambda url: calls.append(url))
    params = T.horizons_tail_params("'DES=70P;CAP;NOFRAG'", [60847.0])
    with pytest.raises(J.NotCachedError):
        c.get("horizons", params, "pa P/1970 Y1")
    assert not calls


def test_objects_span_comets_asteroids_and_phase():
    kinds = [k for _, _, k, _ in T.OBJECTS]
    assert kinds.count("comet") >= 4 and kinds.count("asteroid") >= 4
    assert len(T.OBJECTS) <= T.BUDGET


ASSIST = os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS")


@pytest.mark.skipif(
    not (ASSIST and os.path.isdir(T.CACHE) and os.path.exists(T.REF_SSSOURCE)),
    reason="needs ASSIST data, the T2 Horizons cache and the non-grav fixture",
)
def test_cached_horizons_answer_agrees_in_icrs():
    from ssp.ephem_assist import open_ephem

    plan = T.Plan(J.JPLClient(T.CACHE, offline=True, budget=T.BUDGET), objects=T.OBJECTS[:1])
    try:
        rows, _ = T.compare_object(plan, "P/1970 Y1", "comet", open_ephem())
    except J.NotCachedError:
        pytest.skip("P/1970 Y1 not cached")
    assert np.abs(rows.d_psamv_icrs).max() < 0.002
    assert np.abs(rows.d_psang_icrs).max() < 0.002
    assert np.abs(rows.d_psamv_true).max() > 0.05  # the equator of date is clearly rejected


# ---------------------------------------------------------------------------
# The consistency checker
# ---------------------------------------------------------------------------


def make_tables(tmp_path, n=400, seed=7, mutate=None):
    """Synthetic SSSource and NearbySSO files. The 'production' angles come
    from float64 vectors (perturbed by float32 rounding-size noise, as the
    EphResult is before it's cast) and are stored as float32, next to the
    vectors stored as float32. Rows 0-1 have no orbit."""
    rng = np.random.default_rng(seed)
    ra, dec = rng.uniform(0, 360, n), np.degrees(np.arcsin(rng.uniform(-0.95, 0.95, n)))
    dist = rng.uniform(0.05, 5, n)
    topo = radec_unit(ra, dec) * dist
    helio = rng.normal(size=(3, n)) * 2.0
    # a few rows near opposition: helio almost along the line of sight
    helio[:, 10:20] = topo[:, 10:20] * 1.3 + rng.normal(size=(3, 10)) * 1e-3
    vel = rng.normal(size=(3, n)) * 20.0
    noise = lambda x: x * (1 + rng.uniform(-1, 1, x.shape) * 2.0**-24)  # noqa: E731
    sun, mot = T.tail_angles(noise(helio), noise(vel), noise(topo))
    sun, mot = sun.astype(np.float32), mot.astype(np.float32)
    orbit = np.ones(n, bool)
    orbit[:2] = False
    des = np.array([f"2026 A{i % 37}" for i in range(n)])
    ids = np.arange(n, dtype=np.int64) + 1000

    def col32(x, valid):
        return pa.array(np.asarray(x, np.float32), mask=~valid)

    s = {
        "designation": pa.array(des),
        "diaSourceId": pa.array(ids),
        "ephRa": pa.array(ra, mask=~orbit),
        "ephDec": pa.array(dec, mask=~orbit),
        "phaseAngle": col32(rng.uniform(0, 60, n), orbit),
        "ephAntiSunPA": col32(sun, orbit),
        "ephAntiMotionPA": col32(mot, orbit),
    }
    for k, c in enumerate("xyz"):
        s[f"helio_{c}"] = col32(helio[k], orbit)
        s[f"helio_v{c}"] = col32(vel[k], orbit)
        s[f"topo_{c}"] = col32(topo[k], orbit)
    keep = np.arange(2, n, 2)  # NearbySSO: every other row with an orbit, + one row of its own
    nb = {
        "diaSourceId": np.concatenate([ids[keep], [99]]),
        "designation": np.concatenate([des[keep], ["2026 ZZ"]]),
        "ephRa": np.concatenate([ra[keep], [1.0]]),
        "ephAntiSunPA": np.concatenate([sun[keep], [np.float32(12.5)]]),
        "ephAntiMotionPA": np.concatenate([mot[keep], [np.float32(13.5)]]),
    }
    if mutate:
        mutate(s, nb)
    nb = {k: pa.array(np.asarray(v, np.float32) if k.startswith("ephAnti") else v) for k, v in nb.items()}
    ps, pn = str(tmp_path / "sssource.parquet"), str(tmp_path / "nearbysso.parquet")
    pq.write_table(pa.table(s), ps)
    pq.write_table(pa.table(nb), pn)
    return ps, pn


def failures(lines):
    return [ln for ln in lines if ln.startswith("FAIL")]


def test_consistent_tables_pass(tmp_path):
    ok, lines = T.consistency(*make_tables(tmp_path))
    assert ok, "\n".join(lines)
    assert any("rows above it (near opposition)" in ln and "0 of 398" in ln for ln in lines)


def _set(name, k, value, table="s"):
    def m(s, nb):
        if table == "s":
            v = s[name].to_numpy(zero_copy_only=False).copy()
            v[k] = value
            s[name] = pa.array(v.astype(np.float32), mask=np.isnan(v))
        else:
            nb[name] = np.array(nb[name], np.float32)
            nb[name][k] = value

    return m


def _shift_n(name, k, f):
    """Replace NearbySSO row k's value x by f(x) (float32)."""

    def m(s, nb):
        nb[name] = np.array(nb[name], np.float32)
        nb[name][k] = f(nb[name][k])

    return m


@pytest.mark.parametrize(
    "mutate, expect",
    [
        (_set("ephAntiSunPA", 5, np.nan), "null/NaN on 1"),
        (_set("ephAntiMotionPA", 7, 360.0), "outside [0, 360)"),
        (_set("ephAntiSunPA", 30, -0.5), "outside [0, 360)"),
        (_shift_n("ephAntiMotionPA", 3, lambda x: x + np.float32(3e-4)), "pairs ephAntiMotionPA"),
        (_shift_n("ephAntiSunPA", 31, lambda x: np.float32(np.nan)), "pairs ephAntiSunPA"),
    ],
)
def test_injected_faults_fail(tmp_path, mutate, expect):
    ok, lines = T.consistency(*make_tables(tmp_path, mutate=mutate))
    assert not ok
    assert any(expect in ln for ln in failures(lines)), "\n".join(lines)


def test_one_ulp_between_the_tables_passes_and_is_counted(tmp_path):
    up = lambda x: np.nextafter(x, np.float32(1e9))  # noqa: E731
    ok, lines = T.consistency(*make_tables(tmp_path, mutate=_shift_n("ephAntiMotionPA", 3, up)))
    assert ok, "\n".join(lines)
    assert any(
        ln.startswith("ephAntiMotionPA: 198 of 199 matched pairs bitwise equal, 1 not") for ln in lines
    ), "\n".join(lines)
    assert any(ln.startswith("ephAntiSunPA: 199 of 199 matched pairs bitwise equal, 0 not") for ln in lines)


def test_an_angle_off_by_a_hundredth_of_a_degree_fails_the_recomputation(tmp_path):
    def m(s, nb):
        for name in ("ephAntiSunPA", "ephAntiMotionPA"):
            v = s[name].to_numpy(zero_copy_only=False).astype(np.float64)
            v[50] = (v[50] + 0.01) % 360
            s[name] = pa.array(v.astype(np.float32), mask=np.isnan(v))
            k = np.flatnonzero(np.asarray(nb["diaSourceId"]) == np.asarray(s["diaSourceId"])[50])
            nb[name] = np.array(nb[name], np.float32)
            nb[name][k] = np.float32(v[50])  # NearbySSO agrees: only the recomputation catches it

    ok, lines = T.consistency(*make_tables(tmp_path, mutate=m))
    f = failures(lines)
    assert not ok and len(f) == 2 and all("recomputed" in ln for ln in f), "\n".join(lines)


def test_wrong_conventions_fail(tmp_path):
    """+velocity instead of -velocity, and an equator-of-date pole."""

    def plus_v(s, nb):
        v = np.array([s[f"helio_v{c}"].to_numpy(zero_copy_only=False) for c in "xyz"], float)
        u = np.array([s[f"topo_{c}"].to_numpy(zero_copy_only=False) for c in "xyz"], float)
        s["ephAntiMotionPA"] = pa.array(T.position_angle(v, u).astype(np.float32))

    ok, lines = T.consistency(*make_tables(tmp_path, mutate=plus_v))
    assert not ok and any("recomputed ephAntiMotionPA" in ln for ln in failures(lines))

    def of_date(s, nb):
        h = np.array([s[f"helio_{c}"].to_numpy(zero_copy_only=False) for c in "xyz"], float)
        u = np.array([s[f"topo_{c}"].to_numpy(zero_copy_only=False) for c in "xyz"], float)
        R = T.frame_matrices(np.array([61000.0]), "true")[0]
        s["ephAntiSunPA"] = pa.array(T.position_angle(h, u, R).astype(np.float32))

    ok, lines = T.consistency(*make_tables(tmp_path, mutate=of_date))
    assert not ok and any("recomputed ephAntiSunPA" in ln for ln in failures(lines))


def test_angle_without_an_orbit_fails(tmp_path):
    def m(s, nb):
        v = s["ephAntiSunPA"].to_numpy(zero_copy_only=False).copy()
        v[0] = 10.0
        s["ephAntiSunPA"] = pa.array(v.astype(np.float32), mask=np.isnan(v))

    ok, lines = T.consistency(*make_tables(tmp_path, mutate=m))
    assert not ok and any("non-null on 1 rows without an orbit" in ln for ln in failures(lines))


def test_float32_rounding_to_360_is_caught():
    """The contract's [0, 360) is for the float64 result: a value within
    half a float32 ulp (1.5e-5 deg) of 360 rounds to 360.0f when stored."""
    x = np.float64(360.0 - 1e-5)
    assert 0 <= x < 360 and np.float32(x) == np.float32(360.0)
    v = np.array([np.float32(x)], np.float64)
    res = T.check_angle_columns(v, v, np.array([True]), "t")
    assert [ok for ok, msg in res if "outside" in msg] == [False, False]


def test_recompute_tolerance():
    tol = T.recompute_tolerance(np.array([1.0, 0.1, 1e-3, 1e-5, 0.0]), np.array([0.0, 30.0, 0.0, 0.0, 0.0]))
    assert tol[0] == T.RECOMP_FLOOR_DEG and tol[1] == T.RECOMP_FLOOR_DEG
    assert T.RECOMP_FLOOR_DEG < tol[2] < 0.05 < tol[3]
    assert np.isinf(tol[4])


def test_cli_consistency(tmp_path, capsys):
    ps, pn = make_tables(tmp_path)
    rep = tmp_path / "rep.txt"
    assert T.main(["consistency", ps, pn, "--report", str(rep)]) == 0
    assert "RESULT: PASS" in rep.read_text()
    ps, pn = make_tables(tmp_path, mutate=_set("ephAntiSunPA", 5, np.nan))
    assert T.main(["consistency", ps, pn]) == 1
