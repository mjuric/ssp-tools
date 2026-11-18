"""The delivery checks of docs/design/elongation-ssobject-nulls.md, section
3 (ssp.delivery_check): SSObservation's elongation against its topo/helio
vectors (check_elongation), and no NaN in the float columns of
NAN_FREE_TABLES (and NearbySSO's sidecar). Each fails on a broken file and
passes on a correct one. Network-free."""

import numpy as np
import pyarrow as pa
import pytest

from ssp import delivery_check as D

from nearbysso_delivery_fixture import SIDE, nearbysso_table, sidecar_table, write_nearbysso
from test_delivery_check import make_table, replace, results, ss_table, with_geometry, write, write_ss


def _set(t, name, row, value):
    vals = t[name].to_pylist()
    vals[row] = value
    return replace(t, name, pa.array(vals, t.schema.field(name).type))


def _ss_results(d):
    return {r.name: r for r in D.check_delivery(d, tables=["SSObservation"])["SSObservation"]}


# --------------------------------------------------------------------------
# elongation_from_vectors and check_elongation
# --------------------------------------------------------------------------

def test_elongation_from_vectors_known_angles():
    topo = np.array([[1.0, 0, 0], [3.0, 0, 0], [1.0, 0, 0], [0, 2.0, 0], [1.0, 1.0, 0]])
    sun = np.array([[1.0, 0, 0], [-1.0, 0, 0], [0, 0, 1.0], [0, 1.0, 1.0], [1.0, 0, 0]])
    got = D.elongation_from_vectors(topo, topo - sun)
    np.testing.assert_allclose(got, [0, 180, 90, 45, 45], rtol=0, atol=1e-12)


def test_elongation_from_observed_direction():
    """With (ra, dec), the target's direction is the observed one, not
    topo's."""
    topo = np.array([[1.0, 0, 0], [1.0, 0, 0]])
    sun = np.array([[0, 1.0, 0], [0, 0, 1.0]])
    got = D.elongation_from_vectors(topo, topo - sun, ra=[90.0, 0.0], dec=[0.0, 45.0])
    np.testing.assert_allclose(got, [0, 45], rtol=0, atol=1e-12)


def test_elongation_bad_orbit_passes(tmp_path):
    """A row whose orbit predicts it 1 deg off (topo's direction) passes:
    its elongation is of the observed position; it is counted apart."""
    t = ss_table()
    t = _set(t, "ra", 3, 1.0)          # (along the Sun's great circle)
    e = D.elongation_from_vectors(*_vectors(t, 3), ra=[1.0], dec=[0.0])[0]
    t = _set(t, "elongation", 3, float(e))
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok, r
    assert "; 1 rows differ from the angle to topo" in r.detail


def _vectors(t, i):
    topo = np.array([[t[f"topo_{a}"][i].as_py() for a in "xyz"]])
    helio = np.array([[t[f"helio_{a}"][i].as_py() for a in "xyz"]])
    return topo, helio


def test_elongation_from_vectors_aberrated():
    """With the velocities: both directions aberrated by helio_v - topo_v
    (the observatory's velocity relative to the Sun): the Sun along +x, the
    observatory moving along +y at 30 km/s."""
    v = 30.0
    b = np.degrees(v / D.C_KM_S)
    topo = np.array([[-2.0, 0, 0], [0, 2.0, 0], [1.0, 0, 0]])
    helio = topo - np.array([1.0, 0, 0])
    topo_v = np.array([[0, -v, 0]] * 3)          # (a target at rest relative to the Sun)
    helio_v = np.zeros((3, 3))
    got = D.elongation_from_vectors(topo, helio, topo_v=topo_v, helio_v=helio_v)
    np.testing.assert_allclose(got, [180 - 2 * b, 90 - b, 0], rtol=0, atol=1e-6)
    np.testing.assert_allclose(D.elongation_from_vectors(topo, helio), [180, 90, 0], rtol=0, atol=1e-12)


def _with_velocity(t, v_kms):
    """``t`` with the observatory moving at ``v_kms`` (3,) relative to the
    Sun: helio_v - topo_v = v."""
    n = len(t)
    for k, a in enumerate("xyz"):
        t = replace(t, f"helio_v{a}", pa.array(np.zeros(n), pa.float32()))
        t = replace(t, f"topo_v{a}", pa.array(np.full(n, -v_kms[k]), pa.float32()))
    return t


@pytest.mark.parametrize("apparent,ok", [(True, True), (False, False)])
def test_elongation_check_is_apparent(tmp_path, apparent, ok):
    """Near opposition, with the observatory moving at 30 km/s across the
    Sun's direction, the apparent and geometric elongations differ by ~41"
    (over the ~1" tolerance): the check takes the apparent one."""
    t = _with_velocity(with_geometry(ss_table(), elong=[179.9] * 9), [0, 30.0, 0])
    if apparent:
        topo, helio = _vectors(t, 0)
        e = D.elongation_from_vectors(topo, helio, ra=[0.0], dec=[0.0],
                                      topo_v=[[0, -30.0, 0]], helio_v=[[0, 0, 0]])[0]
        t = replace(t, "elongation", pa.array(np.full(9, e), pa.float32()))
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok == ok, r


def test_elongation_pass(tmp_path):
    write_ss(tmp_path)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok, r
    assert r.detail.startswith("9 rows with topo/helio vectors")


@pytest.mark.parametrize("off,ok", [(0.5 / 3600, True), (2.0 / 3600, False), (-0.5, False)])
def test_elongation_tolerance(tmp_path, off, ok):
    """Nearby (2-10 AU) the tolerance is ~1": 0.5" passes, 2" fails."""
    t = ss_table()
    t = _set(t, "elongation", 4, t["elongation"][4].as_py() + off)
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok == ok, r
    if not ok:
        assert r.detail.startswith("1 rows differ by more than their tolerance")
    worst = float(r.detail.split("the vectors| ")[1].split(" deg")[0])
    assert worst == pytest.approx(abs(off), abs=3e-5)


def _tol(d, dtype):
    """The tolerance [arcsec] of a row ``d`` AU away, 1 AU from the Sun
    (at 90 deg from it), for vectors stored as ``dtype``."""
    topo = np.array([[d, 0.0, 0.0]])
    return D.elongation_tolerance_arcsec(topo, topo - np.array([[0.0, 1.0, 0.0]]), D.unit_roundoff(dtype))[0]


def test_unit_roundoff():
    assert D.unit_roundoff(np.float32) == 2.0 ** -24
    assert D.unit_roundoff(np.float64) == 2.0 ** -53
    assert D.unit_roundoff(pa.float32()) == 2.0 ** -24
    assert D.unit_roundoff(pa.float64()) == 2.0 ** -53


def test_vector_unit_roundoff():
    """The stored position columns' type sets it: the largest if they
    differ (and a non-float column counts as float32)."""
    t = ss_table()
    assert D.vector_unit_roundoff(t.schema) == 2.0 ** -53
    assert D.vector_unit_roundoff(with_geometry(t, vtype=pa.float32()).schema) == 2.0 ** -24
    assert D.vector_unit_roundoff(replace(t, "helio_z", t["helio_z"].cast(pa.float32())).schema) == 2.0 ** -24
    # (a non-float position column raises: check_delivery makes that a FAIL)
    ints = pa.array(np.zeros(len(t), np.int64))
    with pytest.raises(ValueError):
        D.vector_unit_roundoff(replace(t, "topo_x", ints).schema)
    # (the velocities do not count)
    assert D.vector_unit_roundoff(replace(t, "topo_vx", t["topo_vx"].cast(pa.float16())).schema) == 2.0 ** -53


def test_elongation_tolerance_float32_vectors():
    """1" plus the float32 bound and the Sun's motion over the light-time:
    ~1" nearby, ~7" at 90 AU (where float32 vectors are good to ~4")."""
    def tol(d):
        return _tol(d, np.float32)
    assert 1.0 < tol(1.0) < 1.1
    assert 7.0 < tol(90.0) < 8.0
    assert tol(1.0) < tol(10.0) < tol(40.0) < tol(90.0)


def test_elongation_tolerance_float64_vectors():
    """With float64 vectors the rounding term vanishes: 1" plus the Sun's
    motion over both light-time legs, object-to-observer and Sun-to-object
    (at most 16 m/s x 499 s/AU x (|topo| + |helio|), seen from 1 AU), ~3"
    at 90 AU."""
    def tol(d):
        return _tol(d, np.float64)
    lt = 0.016 * 499.004784 / 149597870.7 * np.degrees(3600.0)   # [arcsec per AU of path, seen from 1 AU]
    for d in (1.0, 10.0, 90.0):
        # (|topo| = d, |helio| = hypot(d, 1); + ~1e-8" of float64 rounding)
        assert tol(d) == pytest.approx(1.0 + lt * (d + np.hypot(d, 1.0)), rel=1e-7)
    assert 1.0 < tol(1.0) < 1.03
    assert 2.9 < tol(90.0) < 3.1
    assert tol(1.0) < tol(10.0) < tol(40.0) < tol(90.0)
    # the float32 rounding term is the difference
    f32 = 2 * 2.0 ** -24 * (90.0 + np.hypot(90.0, 1.0)) * np.degrees(3600.0)
    assert _tol(90.0, np.float32) - tol(90.0) == pytest.approx(f32, rel=1e-6)


@pytest.mark.parametrize("vtype,dist,ok", [
    (pa.float32(), 2.0, False), (pa.float32(), 90.0, True),
    (pa.float64(), 2.0, False), (pa.float64(), 90.0, False)])
def test_elongation_4arcsec_off_far_and_near(tmp_path, vtype, dist, ok):
    """A 4" difference fails for a nearby object; for a distant one (90
    AU) it is within the precision of float32 vectors (a delivery from
    before sdm_schemas d6e8100; ~7.4" there), but not of float64 ones
    (~3", from light-time)."""
    t = ss_table()
    t = with_geometry(t, dist=dist, vtype=vtype)
    t = _set(t, "elongation", 0, t["elongation"][0].as_py() + 4.0 / 3600)
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok == ok, r
    assert f"rows with topo/helio vectors ({vtype})" in r.detail


@pytest.mark.parametrize("vtype", [pa.float32(), pa.float64()])
def test_elongation_2_5arcsec_off_far(tmp_path, vtype):
    """At 90 AU 2.5" is within either tolerance (~3" with float64
    vectors, from the Sun's motion over both light-time legs)."""
    t = with_geometry(ss_table(), dist=90.0, vtype=vtype)
    t = _set(t, "elongation", 0, t["elongation"][0].as_py() + 2.5 / 3600)
    write_ss(tmp_path, t)
    assert _ss_results(tmp_path)["elongation"].ok


def test_elongation_wrong_sun_fails(tmp_path):
    """The old bug's kind: the elongation from another Sun direction."""
    t = ss_table()
    e = np.asarray(t["elongation"].to_numpy(), float)
    t = replace(t, "elongation", pa.array((180 - e).astype(np.float32)))
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    # (all but the row at 90 deg)
    assert not r.ok and r.detail.startswith("8 rows differ")


def test_elongation_rows_without_vectors_skipped(tmp_path):
    t = ss_table()
    # rows without an orbit: NULL vectors (any of them), any elongation
    t = _set(t, "elongation", 6, 1.0)
    t = _set(t, "topo_x", 6, None)
    t = _set(t, "elongation", 7, 2.0)
    t = _set(t, "helio_z", 7, None)
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok, r
    assert r.detail.startswith("7 rows with topo/helio vectors")


def test_elongation_null_with_vectors_fails(tmp_path):
    t = _set(ss_table(), "elongation", 2, None)
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert not r.ok and r.detail.startswith("1 rows differ")


def test_elongation_across_parts(tmp_path):
    """Every part is checked (the fixture cuts 9 rows into 5 parts)."""
    t = _set(ss_table(), "elongation", 8, 0.0)       # (the last part)
    m = write_ss(tmp_path, t)
    assert len(m["parts"]) == 5
    r = _ss_results(tmp_path)["elongation"]
    assert not r.ok and "SSObservation.part0004.parquet row 0: 0.000000 vs 175.0000" in r.detail


def test_elongation_near_0_and_180(tmp_path):
    t = with_geometry(ss_table(), elong=[0.0, 0.001, 179.999, 180.0, 90, 1, 2, 3, 4])
    write_ss(tmp_path, t)
    assert _ss_results(tmp_path)["elongation"].ok


def test_elongation_non_float_vector_fails(tmp_path):
    t = replace(ss_table(), "topo_x", pa.array(np.arange(9), pa.int64()))
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert not r.ok and r.detail.startswith("cannot check (ValueError"), r


def test_elongation_nan_vector_fails(tmp_path):
    """A NaN (not NULL) vector component fails the check (its tolerance is
    NaN), at an infinite fraction of the tolerance."""
    t = ss_table()[:5]
    topo_x = t["topo_x"].to_numpy().copy()
    topo_x[0] = np.nan
    t = replace(t, "topo_x", pa.array(topo_x))
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert not r.ok, r
    assert r.detail.startswith("1 rows differ by more than their tolerance"), r
    assert "at most inf of the row's tolerance" in r.detail


def test_elongation_no_files():
    r = D.check_elongation([])
    assert r.ok and r.detail.startswith("0 rows with topo/helio vectors; "), r


# --------------------------------------------------------------------------
# check_offsets (F-16 of docs/design/precision-doubles.md)
# --------------------------------------------------------------------------

def _with_offsets(t, ra_off, dec_off, dec=None, sep=None):
    """``t`` with the given ephOffsetRa/Dec [arcsec] (lists, None = NULL),
    ephDec [deg] (default 0) and ephOffset (default: their exact
    great-circle separation, from a prediction at RA 10 deg)."""
    from ssp.util import sky_separation_arcsec
    n = len(t)
    dec = np.zeros(n) if dec is None else np.asarray(dec, float)
    if sep is None:
        sep = []
        for x, y, d in zip(ra_off, dec_off, dec):
            if x is None or y is None:
                sep.append(None)
            else:
                ra2 = 10.0 + x / 3600 / np.cos(np.radians(d))
                sep.append(float(sky_separation_arcsec(10.0, d, ra2, d + y / 3600)))
    t = replace(t, "ephOffsetRa", pa.array(ra_off, pa.float32()))
    t = replace(t, "ephOffsetDec", pa.array(dec_off, pa.float32()))
    t = replace(t, "ephDec", pa.array(dec, pa.float64()))
    return replace(t, "ephOffset", pa.array(sep, pa.float32()))


def _offsets_result(tmp_path, t):
    write_ss(tmp_path, t)
    return _ss_results(tmp_path)["offsets"]


#: Nine rows: 1 mas to 50", at declinations up to 89 deg.
OFF_RA = [0.001, 0.3, -1.2, 25.0, -40.0, 0.7, 3.0, -0.02, 0.5]
OFF_DEC = [0.0, -0.4, 0.9, 30.0, 10.0, -0.1, 0.0, 0.03, -0.5]
OFF_DECL = [0.0, 10.0, -30.0, 45.0, 60.0, -75.0, 85.0, -89.0, 89.0]


def test_offsets_pass(tmp_path):
    r = _offsets_result(tmp_path, _with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL))
    assert r.ok, r
    assert r.detail.startswith("9 rows with the offsets; at most "), r
    assert "; 0 rows beyond the tangent-plane regime" in r.detail


def test_offsets_unwrapped_straddle_fails(tmp_path):
    """An RA difference not wrapped across RA 0/360: 0.72" the short way
    becomes 359.9998 deg x cos(dec) ~ 1.3e6"."""
    ra = list(OFF_RA)
    ra[2] = (360 - 0.0002) * 3600 * np.cos(np.radians(OFF_DECL[2]))
    t = _with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL)
    t = replace(t, "ephOffsetRa", pa.array(ra, pa.float32()))
    r = _offsets_result(tmp_path, t)
    assert not r.ok and r.detail.startswith("1 rows outside;"), r
    assert "ephOffsetRa 1.1223" in r.detail and "at most 2 of the tolerance" in r.detail


@pytest.mark.parametrize("times,ok", [(0.5, True), (3.0, False)])
def test_offsets_few_times_tolerance(tmp_path, times, ok):
    """An ephOffset off by 3x the tolerance fails, by half of it passes
    (row 4: ~41" at dec 60, where the tolerance is ~8e-5 relative)."""
    t = _with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL)
    sep = t["ephOffset"].to_numpy().astype(float)
    tol = D.offsets_tolerance_arcsec(sep[4], OFF_DEC[4], OFF_DECL[4])
    assert 5e-5 < tol / sep[4] < 2e-4
    sep[4] += times * tol
    r = _offsets_result(tmp_path, replace(t, "ephOffset", pa.array(sep, pa.float32())))
    assert r.ok == ok, r
    if not ok:
        assert r.detail.startswith("1 rows outside;"), r


COS20 = np.cos(np.radians(20.0))


# (within the bounds; and an RA difference not wrapped)
@pytest.mark.parametrize("ra_off,ok", [(-150 * 3600 * COS20, True), (-179.9 * 3600 * COS20, True),
                                       (-200 * 3600 * COS20, False)])
def test_offsets_large_separation_bounds(tmp_path, ra_off, ok):
    """Beyond 1 deg only the bounds: |ephOffsetRa| <= 180 deg x cos(dec),
    |ephOffsetDec| <= ephOffset (here the separation of the wrapped
    difference, ~150 deg; the tangent-plane hypot is far from it)."""
    from ssp.util import sky_separation_arcsec
    t = _with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL)
    dec = np.array(OFF_DECL, float)
    dec[0] = 20.0
    ra = list(OFF_RA)
    ra[0] = ra_off
    dra = ((ra_off / 3600 / np.cos(np.radians(20.0))) + 180) % 360 - 180
    sep = t["ephOffset"].to_numpy().astype(float)
    sep[0] = sky_separation_arcsec(10.0, 20.0, 10.0 + dra, 20.0 - 2.0)
    od = list(OFF_DEC)
    od[0] = -2.0 * 3600
    t = replace(t, "ephOffsetRa", pa.array(ra, pa.float32()))
    t = replace(t, "ephOffsetDec", pa.array(od, pa.float32()))
    t = replace(t, "ephDec", pa.array(dec))
    t = replace(t, "ephOffset", pa.array(sep, pa.float32()))
    r = _offsets_result(tmp_path, t)
    assert r.ok == ok, r
    assert "1 rows beyond the tangent-plane regime" in r.detail


def test_offsets_dec_beyond_separation_fails(tmp_path):
    """Beyond 1 deg: |ephOffsetDec| > ephOffset fails."""
    t = _with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL)
    od = list(OFF_DEC)
    od[0] = 3 * 3600.0
    sep = t["ephOffset"].to_numpy().astype(float)
    sep[0] = 2 * 3600.0
    t = replace(t, "ephOffsetDec", pa.array(od, pa.float32()))
    r = _offsets_result(tmp_path, replace(t, "ephOffset", pa.array(sep, pa.float32())))
    assert not r.ok and r.detail.startswith("1 rows outside;"), r


def test_offsets_nulls_skipped(tmp_path):
    ra, de = list(OFF_RA), list(OFF_DEC)
    ra[1], de[2] = None, None
    t = _with_offsets(ss_table(), ra, de, OFF_DECL)
    # (an unwrapped value with a NULL separation is not checked either)
    t = _set(_set(t, "ephOffsetRa", 3, 1.3e6), "ephOffset", 3, None)
    r = _offsets_result(tmp_path, t)
    assert r.ok and r.detail.startswith("6 rows with the offsets"), r


@pytest.mark.parametrize("col", ["ephOffsetRa", "ephOffsetDec", "ephOffset"])
def test_offsets_nan_fails(tmp_path, col):
    t = _set(_with_offsets(ss_table(), OFF_RA, OFF_DEC, OFF_DECL), col, 4, float("nan"))
    r = _offsets_result(tmp_path, t)
    assert not r.ok and r.detail.startswith("1 rows outside;"), r
    assert "at most inf of the tolerance" in r.detail


def test_offsets_tolerance():
    """The tangent-plane terms, times OFFSETS_TOL_SAFETY (2): |tan(dec)|
    |dDec| / 2 dominates; float32 and the float64 floor otherwise."""
    tol = D.offsets_tolerance_arcsec
    assert D.OFFSETS_TOL_SAFETY == 2.0
    assert tol(0.0, 0.0, 0.0) == D.OFFSETS_TOL_FLOOR_ARCSEC
    want = 4 * 2.0 ** -24 + 1e-9 + 2 * np.radians(1 / 3600) ** 2 / 8
    assert tol(1.0, 0.0, 0.0) == pytest.approx(want, rel=1e-12)
    # 60" at dec 45, all in declination: 2 x |tan| x eta / 2 relative
    assert tol(60.0, 60.0, 45.0) / 60 == pytest.approx(2 * np.radians(60 / 3600) / 2, rel=3e-3)
    assert tol(60.0, 60.0, 80.0) > tol(60.0, 60.0, 45.0) > tol(60.0, 0.0, 45.0)


def test_offsets_missing_columns(tmp_path):
    write_ss(tmp_path)
    t = ss_table().drop_columns(["ephOffsetDec"])
    r = D.check_offsets([write(tmp_path, "x", t)])
    assert not r.ok and "lacks ['ephOffsetDec']" in r.detail


def test_elongation_missing_columns(tmp_path):
    write_ss(tmp_path)
    t = ss_table().drop_columns(["topo_y"])
    r = D.check_elongation([write(tmp_path, "x", t)])
    assert not r.ok and "lacks ['topo_y']" in r.detail


# --------------------------------------------------------------------------
# no NaN
# --------------------------------------------------------------------------

@pytest.mark.parametrize("table,col", [("SSObject", "r_H"), ("SSObject", "arc"),
                                       ("NearbySSO", None), ("SSObservation", "psfFlux")])
def test_nan_fails(tmp_path, table, col):
    t = make_table(table)
    if col is None:
        col = next(f.name for f in t.schema if pa.types.is_floating(f.type))
    rs = results(table, write(tmp_path, table, _set(t, col, 1, float("nan"))))
    assert not rs["no NaN"].ok
    assert rs["no NaN"].detail == f"NaN in {col} (1)"
    # NULL is how a value is missing: passes
    rs = results(table, write(tmp_path, table, _set(t, col, 1, None) if col != "arc" else t))
    assert rs["no NaN"].ok, rs["no NaN"]


def test_nan_inf_is_not_nan(tmp_path):
    t = _set(make_table("SSObject"), "u_HErr", 0, float("inf"))
    assert results("SSObject", write(tmp_path, "SSObject", t))["no NaN"].ok


def test_nan_every_column_counted(tmp_path):
    t = make_table("SSObject")
    for c in ("g_H", "g_HErr"):
        for i in (0, 3):
            t = _set(t, c, i, float("nan"))
    r = results("SSObject", write(tmp_path, "SSObject", t))["no NaN"]
    assert r.detail == "NaN in g_H (2); g_HErr (2)"


@pytest.mark.parametrize("table", ["mpc_orbits"])
def test_nan_mpc_tables_not_checked(tmp_path, table):
    t = make_table(table)
    col = next(f.name for f in t.schema if pa.types.is_floating(f.type))
    rs = results(table, write(tmp_path, table, _set(t, col, 1, float("nan"))))
    assert "no NaN" not in rs and all(r.ok for r in rs.values())


def test_nan_in_ssobservation_part_fails_delivery(tmp_path):
    t = _set(ss_table(), "psfFlux", 5, float("nan"))
    write_ss(tmp_path, t)
    bad = [r for r in D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"] if not r.ok]
    assert [r.name for r in bad] == ["no NaN [SSObservation.part0002.parquet]"]


def test_nan_in_nearbysso_sidecar(tmp_path):
    side = sidecar_table(nearbysso_table())
    col = next(f.name for f in side.schema if pa.types.is_floating(f.type) and f.name != "midpointMjdTai")
    vals = list(SIDE[col]) if col in SIDE else side[col].to_pylist()
    row = next(i for i, v in enumerate(vals) if v is not None)
    vals[row] = float("nan")
    side = side.set_column(side.schema.get_field_index(col), col, pa.array(vals, side.schema.field(col).type))
    rs = {r.name: r for r in D.check_nearbysso(write_nearbysso(tmp_path, side=side))}
    assert not rs["sidecar no NaN"].ok and rs["sidecar no NaN"].detail == f"NaN in {col} (1)"
    rs = {r.name: r for r in D.check_nearbysso(write_nearbysso(tmp_path))}
    assert rs["sidecar no NaN"].ok
