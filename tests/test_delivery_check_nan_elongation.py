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


def test_elongation_tolerance_grows_with_distance():
    """1" plus the float32 bound and light-time: ~1" nearby, ~6" at 90 AU
    (where the delivered float32 vectors are good to ~2")."""
    def tol(d):
        topo = np.array([[d, 0.0, 0.0]])
        return D.elongation_tolerance_arcsec(topo, topo - np.array([[0.0, 1.0, 0.0]]))[0]
    assert 1.0 < tol(1.0) < 1.1
    assert 5.0 < tol(90.0) < 7.0
    assert tol(1.0) < tol(10.0) < tol(40.0) < tol(90.0)


@pytest.mark.parametrize("dist,ok", [(2.0, False), (90.0, True)])
def test_elongation_3arcsec_off_far_and_near(tmp_path, dist, ok):
    """A 3" difference fails for a nearby object but is within the float32
    precision of a distant one's vectors."""
    t = ss_table()
    t = with_geometry(t, dist=dist)
    t = _set(t, "elongation", 0, t["elongation"][0].as_py() + 3.0 / 3600)
    write_ss(tmp_path, t)
    r = _ss_results(tmp_path)["elongation"]
    assert r.ok == ok, r


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
