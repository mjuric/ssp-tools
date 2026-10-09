"""The delivery check (ssp.delivery_check) on small synthetic Parquet files
built from delivery_schema(). Network-free."""
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from ssp import delivery_check as D
from ssp.delivery_contract import DELIVERY_TABLES, SCHEMA_DIR, delivery_schema

N = 6


def _array(c, n=N, dictionary=False):
    dt = c["datatype"]
    if dt in D.STRING_TYPES:
        L = c.get("length") or 32
        vals = [f"{c['name'][:3]}{i}"[:L] if L >= 2 else "abcdef"[i % 6] for i in range(n)]
        arr = pa.array(vals, pa.string())
        return arr.dictionary_encode() if dictionary else arr
    if dt == "timestamp":
        return pa.array(range(n), pa.timestamp("us"))
    if dt == "boolean":
        return pa.array([i % 2 == 0 for i in range(n)])
    return pa.array(range(n), D.FELIS_ARROW[dt])


def make_table(table, n=N, dictionary=False):
    cols = delivery_schema()[table]
    return pa.table({c["name"]: _array(c, n, dictionary) for c in cols})


def write(tmp_path, table, t):
    p = tmp_path / f"{table}.parquet"
    pq.write_table(t, p)
    return p


def results(table, path, schema_dir=SCHEMA_DIR):
    return {r.name: r for r in D.check_table(table, path, schema_dir)}


def failed(rs):
    return sorted(name for name, r in rs.items() if not r.ok)


def replace(t, name, arr):
    return t.set_column(t.schema.get_field_index(name), name, arr)


# --------------------------------------------------------------------------
# passing cases
# --------------------------------------------------------------------------

@pytest.mark.parametrize("table", DELIVERY_TABLES)
def test_pass(tmp_path, table):
    rs = results(table, write(tmp_path, table, make_table(table)))
    assert failed(rs) == [], rs
    assert set(rs) == {"file", "columns", "types", "nulls", "char lengths", "primary key"}


@pytest.mark.parametrize("table", ["SSObservation", "mpc_orbits"])
def test_pass_dictionary_and_large_string(tmp_path, table):
    t = make_table(table, dictionary=True)
    assert failed(results(table, write(tmp_path, table, t))) == []
    t = make_table(table)
    s = next(c["name"] for c in delivery_schema()[table] if c["datatype"] == "char")
    t = replace(t, s, t[s].cast(pa.large_string()))
    assert failed(results(table, write(tmp_path, table, t))) == []


def test_check_delivery_all_pass(tmp_path):
    for table in DELIVERY_TABLES:
        write(tmp_path, table, make_table(table))
    out = D.check_delivery(tmp_path)
    assert list(out) == list(DELIVERY_TABLES)
    assert all(r.ok for rs in out.values() for r in rs)
    assert D.main([str(tmp_path)]) == 0


# --------------------------------------------------------------------------
# columns
# --------------------------------------------------------------------------

def test_extra_column(tmp_path):
    t = make_table("NearbySSO").append_column("bogus", pa.array(range(N)))
    rs = results("NearbySSO", write(tmp_path, "NearbySSO", t))
    assert failed(rs) == ["columns"]
    assert "extra bogus" in rs["columns"].detail


def test_missing_column(tmp_path):
    t = make_table("mpc_orbits").drop_columns(["designation"])
    rs = results("mpc_orbits", write(tmp_path, "mpc_orbits", t))
    assert failed(rs) == ["columns"]
    assert "missing designation" in rs["columns"].detail


def test_misordered_columns(tmp_path):
    t = make_table("SSObject")
    names = t.column_names
    names[1], names[2] = names[2], names[1]
    rs = results("SSObject", write(tmp_path, "SSObject", t.select(names)))
    assert failed(rs) == ["columns"]
    assert "out of order" in rs["columns"].detail
    assert "missing" not in rs["columns"].detail and "extra" not in rs["columns"].detail


def test_missing_key_column(tmp_path):
    t = make_table("SSObservation").drop_columns(["obsid"])
    rs = results("SSObservation", write(tmp_path, "SSObservation", t))
    assert failed(rs) == ["columns", "primary key"]


# --------------------------------------------------------------------------
# types
# --------------------------------------------------------------------------

def test_int64_for_float(tmp_path):
    t = make_table("SSObservation")
    t = replace(t, "ra", pa.array(range(N), pa.int64()))
    rs = results("SSObservation", write(tmp_path, "SSObservation", t))
    assert failed(rs) == ["types"]
    assert "ra: int64, want double" in rs["types"].detail


def test_float64_for_float32(tmp_path):
    t = make_table("SSObject")
    c = next(c["name"] for c in delivery_schema()["SSObject"] if c["datatype"] == "float")
    t = replace(t, c, t[c].cast(pa.float64()))
    assert failed(results("SSObject", write(tmp_path, "SSObject", t))) == ["types"]


def test_bool_for_published(tmp_path):
    t = make_table("current_identifications")
    t = replace(t, "published", pa.array([True] * N))
    rs = results("current_identifications", write(tmp_path, "current_identifications", t))
    assert failed(rs) == ["types"]
    assert "published: bool, want int32" in rs["types"].detail


def test_int_for_string_and_timestamp(tmp_path):
    t = make_table("numbered_identifications")
    t = replace(t, "iau_name", pa.array(range(N), pa.int64()))
    t = replace(t, "created_at", pa.array(range(N), pa.int64()))
    rs = results("numbered_identifications", write(tmp_path, "numbered_identifications", t))
    assert failed(rs) == ["types"]
    assert "iau_name" in rs["types"].detail and "created_at" in rs["types"].detail


def test_unknown_felis_datatype(tmp_path):
    sd = tmp_path / "schema"
    shutil.copytree(SCHEMA_DIR, sd)
    base = yaml.safe_load(open(sd / "sso_base.yaml"))
    for tab in base["tables"]:
        if tab["name"] == "NearbySSO":
            tab["columns"][-1]["datatype"] = "binary"
    yaml.safe_dump(base, open(sd / "sso_base.yaml", "w"), sort_keys=False)
    rs = results("NearbySSO", write(tmp_path, "NearbySSO", make_table("NearbySSO")), sd)
    assert failed(rs) == ["types"]
    assert "'binary' has no Arrow mapping" in rs["types"].detail


# --------------------------------------------------------------------------
# NULLs and keys
# --------------------------------------------------------------------------

def test_nulls_in_non_null_column(tmp_path):
    t = make_table("SSObservation")
    t = replace(t, "visit", pa.array([None, 1, 2, None, 4, 5], pa.int64()))
    t = replace(t, "trksub", pa.array([None] * N, pa.string()))   # nullable: fine
    rs = results("SSObservation", write(tmp_path, "SSObservation", t))
    assert failed(rs) == ["nulls"]
    assert "visit (2 NULLs)" in rs["nulls"].detail
    assert "trksub" not in rs["nulls"].detail


def test_duplicate_key(tmp_path):
    t = make_table("SSObject")
    t = replace(t, "ssObjectId", pa.array([1, 1, 2, 3, 3, 3], pa.int64()))
    rs = results("SSObject", write(tmp_path, "SSObject", t))
    assert failed(rs) == ["primary key"]
    assert "3 duplicate rows over 2 key values" in rs["primary key"].detail


def test_duplicate_string_key_dictionary(tmp_path):
    t = make_table("SSObservation", dictionary=True)
    t = replace(t, "obsid", pa.array(["a", "b", "a", "c", "d", "e"]).dictionary_encode())
    rs = results("SSObservation", write(tmp_path, "SSObservation", t))
    assert failed(rs) == ["primary key"]
    assert "'a'" in rs["primary key"].detail


def test_null_key(tmp_path):
    t = make_table("mpc_orbits")
    t = replace(t, "id", pa.array([None, 1, 2, 3, 4, 5], pa.int32()))
    rs = results("mpc_orbits", write(tmp_path, "mpc_orbits", t))
    assert failed(rs) == ["nulls", "primary key"]
    assert "NULLs in id (1)" in rs["primary key"].detail


def test_composite_primary_key(tmp_path):
    sd = tmp_path / "schema"
    shutil.copytree(SCHEMA_DIR, sd)
    ppdb = yaml.safe_load(open(sd / "ppdb.yaml"))
    for tab in ppdb["tables"]:
        if tab["name"] == "NearbySSO":
            tab["primaryKey"] = ["#NearbySSO.diaSourceId", "#NearbySSO.diaDistanceRank"]
    yaml.safe_dump(ppdb, open(sd / "ppdb.yaml", "w"), sort_keys=False)
    assert D.primary_keys(str(sd))["NearbySSO"] == ["diaSourceId", "diaDistanceRank"]
    t = make_table("NearbySSO")
    t = replace(t, "diaSourceId", pa.array([1, 1, 2, 2, 3, 3], pa.int64()))
    t = replace(t, "diaDistanceRank", pa.array([1, 2, 1, 2, 1, 2], pa.int16()))
    assert failed(results("NearbySSO", write(tmp_path, "NearbySSO", t), sd)) == []
    t = replace(t, "diaDistanceRank", pa.array([1, 1, 1, 2, 1, 2], pa.int16()))
    rs = results("NearbySSO", write(tmp_path, "NearbySSO", t), sd)
    assert failed(rs) == ["primary key"]
    assert "(1, 1)" in rs["primary key"].detail


# --------------------------------------------------------------------------
# char lengths
# --------------------------------------------------------------------------

@pytest.mark.parametrize("dictionary", [False, True])
def test_over_long_char(tmp_path, dictionary):
    t = make_table("SSObservation")
    arr = pa.array(["X", "Y", "TOOLONG", "X", "Y", "X"])   # status: char(1)
    t = replace(t, "status", arr.dictionary_encode() if dictionary else arr)
    rs = results("SSObservation", write(tmp_path, "SSObservation", t))
    assert failed(rs) == ["char lengths"]
    assert "status (1 values over 1, max 7" in rs["char lengths"].detail


def test_unused_dictionary_entry_is_not_a_value(tmp_path):
    t = make_table("SSObservation")
    dictionary = pa.array(["X", "Y", "UNUSED-AND-LONG"])
    arr = pa.DictionaryArray.from_arrays(pa.array([0, 1, 0, 1, 0, 1], pa.int32()), dictionary)
    t = replace(t, "status", arr)
    assert failed(results("SSObservation", write(tmp_path, "SSObservation", t))) == []


# --------------------------------------------------------------------------
# files and the CLI
# --------------------------------------------------------------------------

def test_missing_file(tmp_path, capsys):
    for table in DELIVERY_TABLES:
        if table != "NearbySSO":
            write(tmp_path, table, make_table(table))
    out = D.check_delivery(tmp_path)
    assert out["NearbySSO"] == [D.CheckResult("file", False, f"{tmp_path / 'NearbySSO.parquet'}: missing")]
    assert all(r.ok for t, rs in out.items() if t != "NearbySSO" for r in rs)
    assert D.main([str(tmp_path)]) == 1
    rep = capsys.readouterr().out
    assert "NearbySSO: FAIL" in rep and "NOT DELIVERABLE" in rep
    assert D.main([str(tmp_path), "--tables", "SSObservation", "SSObject"]) == 0


def test_unreadable_file(tmp_path):
    p = tmp_path / "SSObject.parquet"
    p.write_text("not parquet")
    rs = D.check_table("SSObject", p)
    assert [(r.name, r.ok) for r in rs] == [("file", False)]


def test_cli_fails_on_any_check(tmp_path, capsys):
    t = make_table("SSObject").append_column("bogus", pa.array(range(N)))
    write(tmp_path, "SSObject", t)
    assert D.main([str(tmp_path), "--tables", "SSObject"]) == 1
    assert "FAIL  columns" in capsys.readouterr().out
