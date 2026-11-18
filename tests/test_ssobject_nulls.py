"""SSObject's NULLs (docs/design/elongation-ssobject-nulls.md, section 2):
written with the Felis-derived Arrow schema, NaN as NULL in every float
column, ``{band}_slope_fit_failed`` NULL for bands without observations,
every other value bitwise as compute_ssobject has it."""

import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp import schema_ppdb, ssobject
from ssp.delivery_check import check_table
from ssp.delivery_contract import delivery_schema
from ssp.ssobject import compute_ssobject, ssobject_schema, ssobject_table, write_ssobject

from test_ssobject_parallel import _tables

BANDS = "ugrizy"


@pytest.fixture(scope="module")
def ref():
    sss, orbits = _tables(n_obj=12, seed=4)
    return compute_ssobject(sss, orbits)


def _bits(a):
    """The values' bytes (bitwise comparison of floats)."""
    a = np.ascontiguousarray(a)
    return a.view(f"u{a.dtype.itemsize}") if a.dtype.kind == "f" else a


def _check_against(obj, t):
    assert t.schema.equals(ssobject_schema())
    assert t.column_names == list(schema_ppdb.SSObjectDtype.names)
    for name in t.column_names:
        col, ref = t[name].combine_chunks(), obj[name]
        if ref.dtype.kind == "f":
            nan = np.isnan(ref)
            # NULL exactly where NaN; never a NaN left
            assert np.array_equal(col.is_null().to_numpy(zero_copy_only=False), nan), name
            assert not pc.any(pc.is_nan(col)).as_py(), name
            got = col.to_numpy(zero_copy_only=False)
            np.testing.assert_array_equal(_bits(got[~nan]), _bits(ref[~nan]), err_msg=name)
        elif name.endswith("_slope_fit_failed"):
            none = obj[name[0] + "_nObs"] == 0
            assert np.array_equal(col.is_null().to_numpy(zero_copy_only=False), none), name
            got = col.fill_null(False).to_numpy(zero_copy_only=False)
            np.testing.assert_array_equal(got[~none], ref[~none], err_msg=name)
        elif ref.dtype.kind == "U":
            assert col.null_count == 0
            assert col.to_pylist() == ref.tolist(), name
        else:
            assert col.null_count == 0, name
            np.testing.assert_array_equal(col.to_numpy(zero_copy_only=False), ref, err_msg=name)


def test_nulls_where_nan(ref):
    # the test data has what the test needs: NaN in some float columns,
    # bands without observations, and flags True with observations
    assert any(np.isnan(ref[n]).any() for n in ref.dtype.names if ref.dtype[n].kind == "f")
    assert any((ref[f"{b}_nObs"] == 0).any() for b in BANDS)
    assert any((ref[f"{b}_slope_fit_failed"] & (ref[f"{b}_nObs"] > 0)).any() for b in BANDS)
    _check_against(ref, ssobject_table(ref))


def test_nullability_is_felis():
    cols = delivery_schema()["SSObject"]
    s = ssobject_schema()
    assert s.names == [c["name"] for c in cols]
    nonnull = {c["name"] for c in cols if not c.get("nullable", True)}
    assert {f.name for f in s if not f.nullable} == nonnull
    assert len(nonnull) == 15
    assert nonnull == {"ssObjectId", "nObs", "arc",
                       *(f"{b}_{c}" for b in BANDS for c in ("nObs", "nObsUsed"))}


def test_inf_kept_and_flag_with_observations(ref):
    obj = ref.copy()
    obj["u_HErr"][0] = np.inf
    obj["g_Chi2"][1] = -np.inf
    # a band with observations but no usable point: the flag stays True
    obj["r_nObs"][2], obj["r_nObsUsed"][2], obj["r_slope_fit_failed"][2] = 3, 0, True
    obj["r_H"][2] = np.nan
    t = ssobject_table(obj)
    assert t["u_HErr"][0].as_py() == np.inf
    assert t["g_Chi2"][1].as_py() == -np.inf
    assert t["r_slope_fit_failed"][2].as_py() is True
    assert t["r_H"][2].as_py() is None
    # and nObs 0: NULL, whatever the array held
    obj["i_nObs"][3], obj["i_slope_fit_failed"][3] = 0, True
    assert ssobject_table(obj)["i_slope_fit_failed"][3].as_py() is None
    _check_against(obj, ssobject_table(obj))


@pytest.mark.parametrize("name", ["arc"])
def test_null_in_nonnullable_refused(ref, name):
    obj = ref.copy()
    obj[name][1] = np.nan
    with pytest.raises(ValueError, match=f"{name}.*NULL values in a non-nullable column"):
        ssobject_table(obj)


def test_columns_must_match(ref):
    with pytest.raises(ValueError, match=r"missing \['arc'\]"):
        ssobject_table(ref[[n for n in ref.dtype.names if n != "arc"]])


def test_written_file_passes_the_delivery_check(ref, tmp_path):
    p = tmp_path / "SSObject.parquet"
    write_ssobject(ref, p)
    t = pq.read_table(p)
    assert t.schema.equals(ssobject_schema())
    _check_against(ref, t)
    res = check_table("SSObject", p)
    assert all(r.ok for r in res), [r for r in res if not r.ok]


def test_cli_writes_nulls(tmp_path, monkeypatch, ref):
    from test_ssobject_widened import _widened
    sss, orbits = _tables(n_obj=12, seed=4)
    w = pa.Table.from_pandas(_widened(sss), preserve_index=False)
    pq.write_table(w, tmp_path / "ssobservation.parquet")
    orbits.assign(a=0.0, peri_time=0.0, mean_anomaly=0.0, h=17.0, g=0.15).to_parquet(
        tmp_path / "mpc_orbits.parquet")
    monkeypatch.setattr(sys, "argv", [
        "ssp-build-ssobject", str(tmp_path / "ssobservation.parquet"), str(tmp_path / "mpc_orbits.parquet"),
        "--output", str(tmp_path / "ssobject.parquet"), "--workers", "1", "--reraise"])
    ssobject.main()
    t = pq.read_table(tmp_path / "ssobject.parquet")
    assert t.schema.equals(ssobject_schema())
    for b in BANDS:
        nobs = t[f"{b}_nObs"].to_numpy()
        assert np.array_equal(t[f"{b}_slope_fit_failed"].is_null().to_numpy(zero_copy_only=False), nobs == 0)
    for name in t.column_names:
        if pa.types.is_floating(t.schema.field(name).type):
            assert not pc.any(pc.is_nan(t[name])).as_py(), name


def test_arc_is_float64_with_subsecond_precision(tmp_path):
    """arc is float64 (sdm_schemas d6e8100): over ten years it keeps
    sub-second precision (float32 would round it to ~21 s), in the array
    and in the written file."""
    sss, orbits = _tables(n_obj=12, seed=4)
    assert schema_ppdb.SSObjectDtype["arc"] == np.float64
    # (an object's primary rows: SSObject is computed from those)
    prim = sss["primary"].to_numpy(dtype=bool)
    oid = sss["ssObjectId"][prim].value_counts().index[0]
    rows = np.flatnonzero((sss["ssObjectId"] == oid).to_numpy() & prim)
    span = 3652.5 + 0.25 / 86400                  # ten years and a quarter second [d]
    t = sss["midpointMjdTai"].to_numpy(dtype=np.float64, copy=True)
    t[rows] = 60800.0 + np.linspace(0.0, span, rows.size)
    sss["midpointMjdTai"] = t
    obj = compute_ssobject(sss, orbits)
    arc = obj["arc"][obj["ssObjectId"] == oid][0]
    assert obj["arc"].dtype == np.float64
    assert arc == np.ptp(t[rows])
    assert abs(arc - span) * 86400 < 1e-3
    assert abs(float(np.float32(arc)) - span) * 86400 > 0.2       # (float32 loses the quarter second)
    p = tmp_path / "SSObject.parquet"
    write_ssobject(obj, p)
    w = pq.read_table(p)
    assert w.schema.field("arc").type == pa.float64()
    assert np.array_equal(w["arc"].to_numpy(), obj["arc"])
