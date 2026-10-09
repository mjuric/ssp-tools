"""SSObject on SSObservation: a NULL ssObjectId is unmatched (as 0
was), the photometry comes from SSObservation, and ssp-build-ssobject reads
only the SSObservation columns it uses (and no DiaSource file)."""

import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import ssobject
from ssp.ssobject import compute_ssobject

from test_ssobject_parallel import _assert_identical, _tables


def _widened(sss):
    """``sss`` as SSObservation has it: unmatched rows with a NULL
    ssObjectId and designation (not 0 and ''), pyarrow-backed, read back
    from Parquet, band dictionary-encoded; sorted by (ssObjectId, obsid)
    with the NULLs last."""
    t = pa.Table.from_pandas(sss, preserve_index=False)
    unmatched = pa.array((sss["ssObjectId"] == 0).to_numpy())
    t = t.set_column(t.schema.get_field_index("ssObjectId"), "ssObjectId",
                     pa.compute.if_else(unmatched, pa.scalar(None, pa.int64()), t["ssObjectId"]))
    t = t.set_column(t.schema.get_field_index("designation"), "designation",
                     pa.compute.if_else(unmatched, pa.scalar(None, pa.string()), t["designation"]))
    t = t.set_column(t.schema.get_field_index("band"), "band", pa.compute.dictionary_encode(t["band"]))
    t = t.sort_by([("ssObjectId", "ascending"), ("obsid", "descending")])
    return t.to_pandas(types_mapper=pd.ArrowDtype)


def test_null_ssobjectid_is_unmatched():
    sss, orbits = _tables(n_obj=12, seed=4)
    ref = compute_ssobject(sss, orbits)
    w = _widened(sss)
    assert w["ssObjectId"].isna().sum() == 2
    _assert_identical(ref, compute_ssobject(w, orbits))
    _assert_identical(ref, compute_ssobject(w, orbits, workers=3))
    # the NULL ssObjectId alone makes a row unmatched (not its NULL ephRa)
    w.loc[w["ssObjectId"].isna(), "ephRa"] = 10.0
    _assert_identical(ref, compute_ssobject(w, orbits))


def test_photometry_is_ssobservations():
    sss, orbits = _tables(n_obj=12, seed=7)
    ref = compute_ssobject(sss, orbits)
    oid = ref["ssObjectId"][np.argmax(ref["r_nObsUsed"])]
    rows = (sss["ssObjectId"] == oid) & (sss["band"] == "r") & sss["primary"]
    # 1 mag brighter, same magnitude errors
    f = np.where(rows, 10 ** 0.4, 1.0)
    brighter = sss.assign(psfFlux=sss["psfFlux"] * f, psfFluxErr=sss["psfFluxErr"] * f)
    obj = compute_ssobject(brighter, orbits)
    k = np.flatnonzero(obj["ssObjectId"] == oid)[0]
    assert ref["r_nObsUsed"][k] > 2
    assert np.isclose(obj["r_H"][k], ref["r_H"][k] - 1, atol=1e-5)

    missing = sss.drop(columns="psfFluxErr")
    with pytest.raises(ValueError, match="psfFluxErr"):
        compute_ssobject(missing, orbits)


@pytest.mark.parametrize("legacy", [False, True])
def test_cli_reads_only_what_it_uses(tmp_path, monkeypatch, legacy):
    sss, orbits = _tables(n_obj=6, seed=6)
    ref = compute_ssobject(sss, orbits)
    w = pa.Table.from_pandas(_widened(sss), preserve_index=False)
    # SSObservation columns compute_ssobject doesn't use, one of them
    # dictionary-encoded
    w = w.append_column("status", pa.compute.dictionary_encode(pa.array(["p"] * w.num_rows)))
    w = w.append_column("psfMag", pa.array(np.full(w.num_rows, -1.0, np.float32)))
    pq.write_table(w, tmp_path / "ssobservation.parquet")
    orbits.assign(a=0.0, peri_time=0.0, mean_anomaly=0.0, h=17.0, g=0.15).to_parquet(
        tmp_path / "mpc_orbits.parquet")

    read = []
    real = pd.read_parquet

    def spy(path, *args, **kw):
        read.append((str(path), kw.get("columns")))
        return real(path, *args, **kw)
    monkeypatch.setattr(ssobject.pd, "read_parquet", spy)
    # the older form names a dia_sources.parquet, which isn't read (here it
    # doesn't even exist)
    dia = [str(tmp_path / "dia_sources.parquet")] if legacy else []
    monkeypatch.setattr(sys, "argv", [
        "ssp-build-ssobject", str(tmp_path / "ssobservation.parquet"), *dia,
        str(tmp_path / "mpc_orbits.parquet"), "--output", str(tmp_path / "ssobject.parquet"),
        "--workers", "1", "--reraise"])
    ssobject.main()

    assert [p for p, _ in read] == [str(tmp_path / "ssobservation.parquet"),
                                    str(tmp_path / "mpc_orbits.parquet")]
    assert read[0][1] == ssobject.SSS_COLUMNS
    out = pq.read_table(tmp_path / "ssobject.parquet")
    assert out["ssObjectId"].to_pylist() == ref["ssObjectId"].tolist()
    assert np.array_equal(out["r_H"].to_numpy(), ref["r_H"], equal_nan=True)
