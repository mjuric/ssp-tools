"""ssp.ssobservation_parts: reading a partitioned SSObservation."""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import ssobservation_parts as P
from ssp.ssobservation_contract import PART_FILE_FORMAT, SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE


def _write(d):
    # two ranged parts and a NULL part
    rows = [("o1", 1, 1.0, "obssubid"), ("o2", 1, 2.0, "position"), ("o3", 5, 3.0, "obssubid"),
            ("o4", None, 4.0, "obssubid_trail")]
    parts = [rows[:2], rows[2:3], rows[3:]]
    entries = []
    for k, rs in enumerate(parts):
        f = PART_FILE_FORMAT.format(k)
        pq.write_table(pa.table({"obsid": [r[0] for r in rs],
                                 "ssObjectId": pa.array([r[1] for r in rs], pa.int64()),
                                 "midpointMjdTai": [r[2] for r in rs]}), d / f)
        entries.append({"file": f, "rows": len(rs)})
    pq.write_table(pa.table({"obsid": [r[0] for r in rows], "matchMethod": [r[3] for r in rows]}),
                   d / SIDECAR_FILE)
    (d / SSOBSERVATION_MANIFEST_FILE).write_text(json.dumps(
        {"rows": len(rows), "parts": entries, "sidecar": {"file": SIDECAR_FILE}}))
    return rows


def test_read_all_in_part_order(tmp_path):
    rows = _write(tmp_path)
    t = P.read_ssobservation(tmp_path)
    assert t["obsid"].to_pylist() == [r[0] for r in rows]
    assert "matchMethod" not in t.column_names
    assert P.num_rows(tmp_path) == len(rows)
    assert P.part_paths(tmp_path / SSOBSERVATION_MANIFEST_FILE) == P.part_paths(tmp_path)


def test_internal_columns(tmp_path):
    rows = _write(tmp_path)
    t = P.read_ssobservation(tmp_path, internal=True)
    assert t["matchMethod"].to_pylist() == [r[3] for r in rows]
    t = P.read_ssobservation(tmp_path, columns=["matchMethod", "ssObjectId"], internal=True)
    assert t.column_names == ["matchMethod", "ssObjectId"]
    # with a filter the sidecar is matched on obsid, not by position
    t = P.read_ssobservation(tmp_path, columns=["obsid", "matchMethod"], internal=True,
                             filters=[("ssObjectId", "=", 5)])
    assert t.to_pylist() == [{"obsid": "o3", "matchMethod": "obssubid"}]


def test_internal_needs_flag(tmp_path):
    _write(tmp_path)
    with pytest.raises(ValueError, match="internal=True"):
        P.read_ssobservation(tmp_path, columns=["matchMethod"])


def test_either_layout(tmp_path):
    """read_table/read_schema take the partitioned SSObservation (its
    directory or manifest) or a single Parquet file, the same way."""
    (tmp_path / "d").mkdir()
    rows = _write(tmp_path / "d")
    one = tmp_path / "ssobservation.parquet"
    pq.write_table(P.read_ssobservation(tmp_path / "d"), one)
    for src in (tmp_path / "d", tmp_path / "d" / SSOBSERVATION_MANIFEST_FILE, one):
        assert P.is_partitioned(src) is (src != one)
        assert P.read_schema(src).names == ["obsid", "ssObjectId", "midpointMjdTai"]
        assert P.read_table(src)["obsid"].to_pylist() == [r[0] for r in rows]
        t = P.read_table(src, columns=["obsid"], filters=[("ssObjectId", ">=", 5)])
        assert t.to_pylist() == [{"obsid": "o3"}]
    with pytest.raises(FileNotFoundError, match="not a partitioned SSObservation"):
        P.is_partitioned(tmp_path)
