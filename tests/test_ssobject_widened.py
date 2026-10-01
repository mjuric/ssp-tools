"""SSObject on the widened SSSource: a NULL ssObjectId is unmatched (as 0
was), the result doesn't depend on the order of an object's SSSource rows,
and ssp-build-ssobject reads only the SSSource columns it uses."""

import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ssp import ssobject
from ssp.ssobject import compute_ssobject

from test_ssobject_parallel import _assert_identical, _tables


def _widened(sss):
    """``sss`` as the widened SSSource has it: unmatched rows with a NULL
    ssObjectId and designation (not 0 and ''), pyarrow-backed, read back
    from Parquet; sorted by (ssObjectId, obsid) with the NULLs last."""
    t = pa.Table.from_pandas(sss, preserve_index=False)
    unmatched = pa.array((sss["ssObjectId"] == 0).to_numpy())
    t = t.set_column(t.schema.get_field_index("ssObjectId"), "ssObjectId",
                     pa.compute.if_else(unmatched, pa.scalar(None, pa.int64()), t["ssObjectId"]))
    t = t.set_column(t.schema.get_field_index("designation"), "designation",
                     pa.compute.if_else(unmatched, pa.scalar(None, pa.string()), t["designation"]))
    t = t.sort_by([("ssObjectId", "ascending"), ("obsid", "descending")])  # (not dia's order)
    return t.to_pandas(types_mapper=pd.ArrowDtype)


def test_null_ssobjectid_is_unmatched():
    sss, dia, orbits = _tables(n_obj=12, seed=4)
    ref = compute_ssobject(sss, dia, orbits)
    w = _widened(sss)
    assert w["ssObjectId"].isna().sum() == 2
    _assert_identical(ref, compute_ssobject(w, dia, orbits))
    _assert_identical(ref, compute_ssobject(w, dia, orbits, workers=3))
    # the NULL ssObjectId alone makes a row unmatched (not its NULL ephRa)
    w.loc[w["ssObjectId"].isna(), "ephRa"] = 10.0
    _assert_identical(ref, compute_ssobject(w, dia, orbits))


def test_row_order_does_not_matter():
    sss, dia, orbits = _tables(n_obj=12, seed=5)
    ref = compute_ssobject(sss, dia, orbits)
    # reverse the rows within each object (keeping the objects grouped)
    shuffled = sss.iloc[np.lexsort((-np.arange(len(sss)), sss["ssObjectId"].to_numpy()))]
    assert not shuffled["obsid"].reset_index(drop=True).equals(sss["obsid"])
    _assert_identical(ref, compute_ssobject(shuffled.reset_index(drop=True), dia, orbits))


def test_cli_reads_only_what_it_uses(tmp_path, monkeypatch):
    sss, dia, orbits = _tables(n_obj=6, seed=6)
    ref = compute_ssobject(sss, dia, orbits)
    w = pa.Table.from_pandas(_widened(sss), preserve_index=False)
    # widened-SSSource columns compute_ssobject doesn't use, one of them
    # dictionary-encoded
    w = w.append_column("status", pa.compute.dictionary_encode(pa.array(["p"] * w.num_rows)))
    w = w.append_column("psfFlux", pa.array(np.full(w.num_rows, -1.0, np.float32)))
    pq.write_table(w, tmp_path / "sssource.parquet")
    dia.to_parquet(tmp_path / "dia_sources.parquet")
    orbits.assign(a=0.0, peri_time=0.0, mean_anomaly=0.0, h=17.0, g=0.15).to_parquet(
        tmp_path / "mpc_orbits.parquet")

    read = []
    real = pd.read_parquet

    def spy(path, *args, **kw):
        read.append((str(path), kw.get("columns")))
        return real(path, *args, **kw)
    monkeypatch.setattr(ssobject.pd, "read_parquet", spy)
    monkeypatch.setattr(sys, "argv", [
        "ssp-build-ssobject", str(tmp_path / "sssource.parquet"), str(tmp_path / "dia_sources.parquet"),
        str(tmp_path / "mpc_orbits.parquet"), "--output", str(tmp_path / "ssobject.parquet"),
        "--workers", "1", "--reraise"])
    ssobject.main()

    (cols,) = [c for p, c in read if p.endswith("sssource.parquet")]
    assert set(cols) <= set(ssobject.SSS_COLUMNS) and "status" not in cols and "psfFlux" not in cols
    out = pq.read_table(tmp_path / "ssobject.parquet")
    assert out["ssObjectId"].to_pylist() == ref["ssObjectId"].tolist()
    assert np.array_equal(out["r_H"].to_numpy(), ref["r_H"], equal_nan=True)
