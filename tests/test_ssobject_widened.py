"""SSObject on SSObservation: a NULL ssObjectId is unmatched (as 0
was), the photometry comes from SSObservation, and ssp-build-ssobject reads
only the SSObservation columns it uses (and no DiaSource file), from the
partitioned SSObservation (its directory or manifest) or a single file."""

import json
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import ssobject
from ssp.ssobject import compute_ssobject
from ssp.ssobservation_contract import PART_FILE_FORMAT, SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE

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


def _write_parts(t, d, n_parts=3):
    """``t`` (sorted, NULL ssObjectId last) as a partitioned SSObservation
    in ``d``: ranged parts of whole objects, then the NULL rows' part, the
    sidecar and a manifest (only the fields the reader needs)."""
    d.mkdir()
    ids = t["ssObjectId"].to_pylist()
    n_ranged = sum(i is not None for i in ids)
    step = -(-n_ranged // n_parts)
    cuts, a = [], 0
    while a < n_ranged:
        b = min(a + step, n_ranged)
        while b < n_ranged and ids[b] == ids[b - 1]:
            b += 1
        cuts.append((a, b))
        a = b
    if n_ranged < len(ids):
        cuts.append((n_ranged, len(ids)))
    parts = []
    for k, (a, b) in enumerate(cuts):
        pq.write_table(t.slice(a, b - a), d / PART_FILE_FORMAT.format(k))
        parts.append({"file": PART_FILE_FORMAT.format(k), "rows": b - a})
    pq.write_table(pa.table({"obsid": t["obsid"]}), d / SIDECAR_FILE)
    (d / SSOBSERVATION_MANIFEST_FILE).write_text(json.dumps(
        {"rows": t.num_rows, "parts": parts, "sidecar": {"file": SIDECAR_FILE}}))
    return len(parts)


@pytest.mark.parametrize("layout", ["file", "file+dia", "directory", "manifest"])
def test_cli_reads_only_what_it_uses(tmp_path, monkeypatch, layout):
    sss, orbits = _tables(n_obj=6, seed=6)
    ref = compute_ssobject(sss, orbits)
    w = pa.Table.from_pandas(_widened(sss), preserve_index=False)
    # SSObservation columns compute_ssobject doesn't use, one of them
    # dictionary-encoded
    w = w.append_column("status", pa.compute.dictionary_encode(pa.array(["p"] * w.num_rows)))
    w = w.append_column("psfMag", pa.array(np.full(w.num_rows, -1.0, np.float32)))
    if layout.startswith("file"):
        src = tmp_path / "ssobservation.parquet"
        pq.write_table(w, src)
        n_files = 1
    else:
        n_files = _write_parts(w, tmp_path / "delivery", n_parts=6)
        assert n_files >= 3         # (ranged parts and the NULL rows')
        src = tmp_path / "delivery"
        if layout == "manifest":
            src = src / SSOBSERVATION_MANIFEST_FILE
    orbits.assign(a=0.0, peri_time=0.0, mean_anomaly=0.0, h=17.0, g=0.15).to_parquet(
        tmp_path / "mpc_orbits.parquet")

    read, sss_reads = [], []
    real = pd.read_parquet

    def spy(path, *args, **kw):
        read.append((str(path), kw.get("columns")))
        return real(path, *args, **kw)
    monkeypatch.setattr(ssobject.pd, "read_parquet", spy)
    real_pq = pq.read_table

    def pq_spy(path, *args, **kw):
        if "mpc_orbits" not in str(path):
            sss_reads.append((str(path), kw.get("columns")))
        return real_pq(path, *args, **kw)
    monkeypatch.setattr(pq, "read_table", pq_spy)
    # the older form names a dia_sources.parquet, which isn't read (here it
    # doesn't even exist)
    dia = [str(tmp_path / "dia_sources.parquet")] if layout == "file+dia" else []
    monkeypatch.setattr(sys, "argv", [
        "ssp-build-ssobject", str(src), *dia,
        str(tmp_path / "mpc_orbits.parquet"), "--output", str(tmp_path / "ssobject.parquet"),
        "--workers", "1", "--reraise"])
    ssobject.main()

    # pandas reads only mpc_orbits; SSObservation, file by file, only the
    # columns compute_ssobject uses, never the sidecar
    assert [p for p, _ in read] == [str(tmp_path / "mpc_orbits.parquet")]
    assert len(sss_reads) == n_files
    assert all(cols == ssobject.SSS_COLUMNS for _, cols in sss_reads)
    assert not any(SIDECAR_FILE in p for p, _ in sss_reads)
    out = pq.read_table(tmp_path / "ssobject.parquet")
    assert out["ssObjectId"].to_pylist() == ref["ssObjectId"].tolist()
    assert np.array_equal(out["r_H"].to_numpy(), ref["r_H"], equal_nan=True)


def test_parts_read_as_the_single_file(tmp_path):
    """The DataFrame compute_ssobject gets is the same from the parts as
    from one file (as pd.read_parquet(..., dtype_backend="pyarrow") read
    it before the partitioned delivery), and so is SSObject."""
    sss, orbits = _tables(n_obj=9, seed=3)
    w = pa.Table.from_pandas(_widened(sss), preserve_index=False)
    pq.write_table(w, tmp_path / "one.parquet")
    _write_parts(w, tmp_path / "delivery")
    old = pd.read_parquet(tmp_path / "one.parquet", engine="pyarrow", dtype_backend="pyarrow",
                          columns=ssobject.SSS_COLUMNS).reset_index(drop=True)
    for src in (tmp_path / "one.parquet", tmp_path / "delivery",
                tmp_path / "delivery" / SSOBSERVATION_MANIFEST_FILE):
        new = ssobject.read_ssobservation_columns(src)
        assert (new.dtypes == old.dtypes).all() and new.equals(old), src
    _assert_identical(compute_ssobject(old, orbits),
                      compute_ssobject(ssobject.read_ssobservation_columns(tmp_path / "delivery"), orbits))
