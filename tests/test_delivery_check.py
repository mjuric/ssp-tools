"""The delivery check (ssp.delivery_check) on small synthetic Parquet files
built from delivery_schema(), and SSObservation's partitioned delivery
(parts, manifest, sidecar) written by hand (tests/ssobs_parts_fixture.py).
Network-free."""
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from ssp import delivery_check as D
from ssp.delivery_contract import DELIVERY_TABLES, SCHEMA_DIR, delivery_schema
from ssp.ssobservation_contract import (
    MATCH_METHODS,
    PART_FILE_FORMAT,
    SIDECAR_FILE,
    SSOBSERVATION_MANIFEST_FIELDS,
    SSOBSERVATION_MANIFEST_FILE,
)
from ssobs_parts_fixture import read_manifest, refresh, rename_part, write_manifest, write_parts

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
        return pa.array([i % 2 == 0 for i in range(n)], pa.bool_())
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


def write_delivery(d, skip=()):
    """Every delivery table: SSObservation partitioned, the rest single
    files."""
    for table in DELIVERY_TABLES:
        if table in skip:
            continue
        if table == "SSObservation":
            write_ss(d)
        else:
            write(d, table, make_table(table))


def test_check_delivery_all_pass(tmp_path):
    write_delivery(tmp_path)
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
    write_delivery(tmp_path, skip=("NearbySSO",))
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


# --------------------------------------------------------------------------
# SSObservation: the partitioned delivery (check_ssobservation_parts)
# --------------------------------------------------------------------------
#
# Nine rows: objects 1 (3 rows), 2 (2 rows), 3 (1 row), then 3 NULL
# ssObjectId rows. At part_rows 2 the contract cuts them into
#   part0000 [1, 1, 1]  part0001 [2, 2]  part0002 [3]
#   part0003 [N, N]  part0004 [N]

SID = [1, 1, 1, 2, 2, 3, None, None, None]
NS = len(SID)


def ss_table(sid=SID):
    n = len(sid)
    t = make_table("SSObservation", n)
    t = replace(t, "ssObjectId", pa.array(sid, pa.int64()))
    t = replace(t, "midpointMjdTai", pa.array([61000.0 + i for i in range(n)], pa.float64()))
    return replace(t, "obsid", pa.array([f"obs{i:03d}" for i in range(n)], pa.string()))


def sidecar_for(t):
    n = len(t)
    return pa.table({"obsid": t["obsid"],
                     "matchMethod": pa.array([MATCH_METHODS[i % 3] for i in range(n)], pa.string()),
                     "midpointMjdTai_flag_degraded": pa.array([i % 4 == 0 for i in range(n)], pa.bool_())})


def write_ss(d, t=None, **kw):
    t = ss_table() if t is None else t
    kw.setdefault("sidecar", sidecar_for(t))
    return write_parts(t, d, **kw)


def parts_failed(d):
    return sorted(r.name for r in D.check_ssobservation_parts(d) if not r.ok)


def parts_result(d, name):
    return next(r for r in D.check_ssobservation_parts(d) if r.name == name)


PARTS_CHECKS = ["manifest", "part files", "stray files", "part integrity", "part contents", "part ranges",
                "part sizes", "rows total", "sort order", "part schemas", "primary key across parts",
                "sidecar", "sidecar columns", "sidecar encoding", "sidecar nulls", "sidecar obsid"]


def test_parts_fixture_layout(tmp_path):
    m = write_ss(tmp_path)
    assert [p["rows"] for p in m["parts"]] == [3, 2, 1, 2, 1]
    assert [p["null_ssObjectId"] for p in m["parts"]] == [False, False, False, True, True]
    assert set(m) == set(SSOBSERVATION_MANIFEST_FIELDS)


def test_parts_pass(tmp_path):
    write_ss(tmp_path)
    rs = D.check_ssobservation_parts(tmp_path)
    assert [r.name for r in rs] == PARTS_CHECKS
    assert all(r.ok for r in rs), [r for r in rs if not r.ok]


@pytest.mark.parametrize("sid", [
    [1, 1, 2, 3, 3, 3],                    # no NULL rows
    [None, None, None, None, None],        # only NULL rows
    [],                                    # an empty table: one empty part
    [7] * 5 + [None],                      # one object longer than part_rows
])
def test_parts_pass_edge_cases(tmp_path, sid):
    t = ss_table(sid)
    write_ss(tmp_path, t)
    assert parts_failed(tmp_path) == []
    if not sid:
        m = read_manifest(tmp_path)
        assert len(m["parts"]) == 1 and m["parts"][0]["rows"] == 0
    # check_table on every part passes too
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert [r for r in out if not r.ok] == []


def test_parts_pass_internal_none(tmp_path):
    t = ss_table()
    write_ss(tmp_path, t, sidecar=pa.table({"obsid": t["obsid"]}))
    assert parts_failed(tmp_path) == []


def test_check_delivery_runs_check_table_per_part(tmp_path):
    write_delivery(tmp_path)
    rs = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    names = [r.name for r in rs]
    for k in range(5):
        assert f"columns [{PART_FILE_FORMAT.format(k)}]" in names
        assert f"primary key [{PART_FILE_FORMAT.format(k)}]" in names
    assert "SSObservation.parquet" not in " ".join(r.detail for r in rs)
    assert all(r.ok for r in rs)


# --- the manifest ---------------------------------------------------------

def test_parts_missing_manifest(tmp_path):
    write_ss(tmp_path)
    (tmp_path / SSOBSERVATION_MANIFEST_FILE).unlink()
    rs = D.check_ssobservation_parts(tmp_path)
    assert [(r.name, r.ok) for r in rs] == [("manifest", False)]
    assert "missing" in rs[0].detail
    # check_delivery still runs check_table on the parts it finds
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert not all(r.ok for r in out)
    assert any(r.name == "columns [SSObservation.part0000.parquet]" for r in out)


def test_parts_unreadable_manifest(tmp_path):
    write_ss(tmp_path)
    (tmp_path / SSOBSERVATION_MANIFEST_FILE).write_text("{not json")
    assert parts_failed(tmp_path) == ["manifest"]


@pytest.mark.parametrize("field", list(SSOBSERVATION_MANIFEST_FIELDS))
def test_parts_manifest_missing_field(tmp_path, field):
    m = write_ss(tmp_path)
    del m[field]
    write_manifest(tmp_path, m)
    assert "manifest" in parts_failed(tmp_path)
    assert field in parts_result(tmp_path, "manifest").detail


@pytest.mark.parametrize("field,value", [
    ("format_version", 2), ("format_version", "1"), ("table", "SSSource"), ("partition_key", "obsid"),
    ("sort", ["ssObjectId", "obsid"]), ("part_rows", 0), ("schema", {"file": "sso_base.yaml"}),
])
def test_parts_manifest_bad_value(tmp_path, field, value):
    m = write_ss(tmp_path)
    m[field] = value
    write_manifest(tmp_path, m)
    assert "manifest" in parts_failed(tmp_path)


@pytest.mark.parametrize("field", ["file", "rows", "ssObjectId_min", "null_ssObjectId", "bytes", "md5"])
def test_parts_manifest_part_missing_field(tmp_path, field):
    m = write_ss(tmp_path)
    del m["parts"][1][field]
    write_manifest(tmp_path, m)
    assert "manifest" in parts_failed(tmp_path)


@pytest.mark.parametrize("field", ["file", "key", "columns", "rows", "bytes", "md5"])
def test_parts_manifest_sidecar_missing_field(tmp_path, field):
    m = write_ss(tmp_path)
    del m["sidecar"][field]
    write_manifest(tmp_path, m)
    assert "manifest" in parts_failed(tmp_path)


# --- the part files -------------------------------------------------------

def test_parts_missing_part_file(tmp_path):
    write_ss(tmp_path)
    (tmp_path / PART_FILE_FORMAT.format(1)).unlink()
    f = parts_failed(tmp_path)
    assert "part files" in f and "rows total" in f
    assert "missing on disk: SSObservation.part0001.parquet" in parts_result(tmp_path, "part files").detail
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert any(r.name == "file [SSObservation.part0001.parquet]" and not r.ok for r in out)


def test_parts_dropped_part(tmp_path):
    # dropped from the manifest and the disk, the rest renumbered and the
    # totals made consistent: the rows are gone from the parts but not
    # from the sidecar
    m = write_ss(tmp_path)
    (tmp_path / PART_FILE_FORMAT.format(1)).unlink()
    del m["parts"][1]
    for k in range(1, len(m["parts"])):
        rename_part(tmp_path, m, k, k)
    refresh(tmp_path, m)
    f = parts_failed(tmp_path)
    assert "sidecar" in f and "sidecar obsid" in f
    assert "part files" not in f


def test_parts_extra_part_file(tmp_path):
    write_ss(tmp_path)
    shutil.copy(tmp_path / PART_FILE_FORMAT.format(4), tmp_path / PART_FILE_FORMAT.format(9))
    assert parts_failed(tmp_path) == ["part files"]
    assert "not in the manifest: SSObservation.part0009.parquet" in \
        parts_result(tmp_path, "part files").detail


def test_parts_gapped_number(tmp_path):
    m = write_ss(tmp_path)
    rename_part(tmp_path, m, 4, 5)
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["part files"]
    assert "contiguous from 0" in parts_result(tmp_path, "part files").detail


def test_parts_swapped_order(tmp_path):
    m = write_ss(tmp_path)
    # part0000 <-> part0001, files renamed so that the names stay contiguous
    (tmp_path / PART_FILE_FORMAT.format(0)).rename(tmp_path / "tmp")
    (tmp_path / PART_FILE_FORMAT.format(1)).rename(tmp_path / PART_FILE_FORMAT.format(0))
    (tmp_path / "tmp").rename(tmp_path / PART_FILE_FORMAT.format(1))
    p0, p1 = m["parts"][0], m["parts"][1]
    p0["file"], p1["file"] = p1["file"], p0["file"]
    m["parts"][0], m["parts"][1] = p1, p0
    write_manifest(tmp_path, m)
    f = parts_failed(tmp_path)
    assert {"part ranges", "sort order", "sidecar obsid"} <= set(f)
    assert "part files" not in f and "part integrity" not in f


def test_parts_null_part_not_last(tmp_path):
    t = ss_table()
    # the NULL rows moved before object 3: a NULL part, then a ranged one
    order = [0, 1, 2, 3, 4, 6, 7, 5]
    t2 = t.take(order)
    side = sidecar_for(t).take(order)
    write_ss(tmp_path, t2, sidecar=side, splits=[(0, 3, False), (3, 5, False), (5, 7, True), (7, 8, False)])
    f = parts_failed(tmp_path)
    assert "part ranges" in f and "sort order" in f
    assert "NULL part before a ranged part" in parts_result(tmp_path, "part ranges").detail


# --- ranges and contents --------------------------------------------------

def test_parts_object_split(tmp_path):
    write_ss(tmp_path, splits=[(0, 2, False), (2, 5, False), (5, 6, False), (6, 8, True), (8, 9, True)])
    f = parts_failed(tmp_path)
    assert "part ranges" in f
    assert "ssObjectId 1 split across" in parts_result(tmp_path, "part ranges").detail


def test_parts_overlapping_ranges(tmp_path):
    t = ss_table([1, 3, 2, 4, None])
    write_ss(tmp_path, t, splits=[(0, 2, False), (2, 4, False), (4, 5, True)])
    f = parts_failed(tmp_path)
    assert "part ranges" in f
    assert "overlap" in parts_result(tmp_path, "part ranges").detail


def test_parts_nulls_in_ranged_part(tmp_path):
    write_ss(tmp_path, splits=[(0, 3, False), (3, 5, False), (5, 7, False), (7, 9, True)])
    f = parts_failed(tmp_path)
    assert "part contents" in f
    assert "1 NULL ssObjectId rows in a ranged part" in parts_result(tmp_path, "part contents").detail


def test_parts_null_part_with_ids(tmp_path):
    write_ss(tmp_path, splits=[(0, 3, False), (3, 5, False), (5, 7, True), (7, 9, True)])
    assert "part contents" in parts_failed(tmp_path)


@pytest.mark.parametrize("field,value", [("ssObjectId_min", 0), ("ssObjectId_max", 2),
                                         ("ssObjectId_max", None)])
def test_parts_wrong_range(tmp_path, field, value):
    m = write_ss(tmp_path)
    m["parts"][0][field] = value
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["part contents"]


def test_parts_null_part_with_a_range(tmp_path):
    m = write_ss(tmp_path)
    m["parts"][3]["ssObjectId_min"] = m["parts"][3]["ssObjectId_max"] = 4
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["part contents"]


def test_parts_wrong_null_flag(tmp_path):
    m = write_ss(tmp_path)
    m["parts"][4]["null_ssObjectId"] = False
    write_manifest(tmp_path, m)
    assert "part contents" in parts_failed(tmp_path)


# --- sizes ----------------------------------------------------------------

def test_parts_ranged_part_too_small(tmp_path):
    m = write_ss(tmp_path)
    m["part_rows"] = 4                            # part0000 has 3 rows, part0001 2
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["part sizes"]
    assert "3 rows < part_rows 4" in parts_result(tmp_path, "part sizes").detail


def test_parts_ranged_part_not_closed_at_first_boundary(tmp_path):
    # objects 1 and 2 in one part: it had 3 >= part_rows rows before object 2
    write_ss(tmp_path, splits=[(0, 5, False), (5, 6, False), (6, 8, True), (8, 9, True)])
    assert parts_failed(tmp_path) == ["part sizes"]
    assert "first object boundary" in parts_result(tmp_path, "part sizes").detail


def test_parts_null_part_wrong_size(tmp_path):
    write_ss(tmp_path, splits=[(0, 3, False), (3, 5, False), (5, 6, False), (6, 7, True), (7, 9, True)])
    assert parts_failed(tmp_path) == ["part sizes"]


def test_parts_empty_part(tmp_path):
    write_ss(tmp_path, splits=[(0, 3, False), (3, 5, False), (5, 6, False), (6, 6, False), (6, 8, True),
                               (8, 9, True)])
    assert "part contents" in parts_failed(tmp_path)


# --- integrity ------------------------------------------------------------

@pytest.mark.parametrize("field,value", [("md5", "0" * 32), ("bytes", 12345), ("rows", 4)])
def test_parts_wrong_integrity(tmp_path, field, value):
    m = write_ss(tmp_path)
    m["parts"][0][field] = value
    write_manifest(tmp_path, m)
    f = parts_failed(tmp_path)
    assert "part integrity" in f
    assert set(f) <= {"part integrity", "rows total"}


def test_parts_part_rewritten_after_manifest(tmp_path):
    # a part's contents changed (same rows), the manifest not updated
    write_ss(tmp_path)
    p = tmp_path / PART_FILE_FORMAT.format(1)
    t = pq.read_table(p)
    pq.write_table(t, p, compression="snappy")
    assert parts_failed(tmp_path) == ["part integrity"]


def test_parts_wrong_total(tmp_path):
    m = write_ss(tmp_path)
    m["rows"] = NS + 1
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["rows total"]


# --- order, schema, key ---------------------------------------------------

def test_parts_out_of_order_across_boundary(tmp_path):
    # the NULL rows (sorted by midpointMjdTai) swapped across the boundary
    # of the two NULL parts: each part is in order on its own
    t = ss_table()
    order = [0, 1, 2, 3, 4, 5, 6, 8, 7]
    write_ss(tmp_path, t.take(order), sidecar=sidecar_for(t).take(order))
    assert parts_failed(tmp_path) == ["sort order"]
    assert "first at its first row" in parts_result(tmp_path, "sort order").detail


def test_parts_out_of_order_within_part(tmp_path):
    t = ss_table()
    order = [1, 0, 2, 3, 4, 5, 6, 7, 8]
    write_ss(tmp_path, t.take(order), sidecar=sidecar_for(t).take(order))
    assert parts_failed(tmp_path) == ["sort order"]


def test_parts_schema_differs(tmp_path):
    write_ss(tmp_path)
    p = tmp_path / PART_FILE_FORMAT.format(2)
    t = pq.read_table(p)
    t = replace(t, "ra", t["ra"].cast(pa.float32()))
    pq.write_table(t, p, compression="zstd")
    refresh(tmp_path)
    assert parts_failed(tmp_path) == ["part schemas"]
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert [r.name for r in out if not r.ok] == ["part schemas", "types [SSObservation.part0002.parquet]"]


def test_parts_obsid_duplicated_across_parts(tmp_path):
    t = ss_table()
    obsid = t["obsid"].to_pylist()
    obsid[7] = obsid[0]                          # unique within each part
    t = replace(t, "obsid", pa.array(obsid))
    write_ss(tmp_path, t)
    assert parts_failed(tmp_path) == ["primary key across parts"]
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert [r.name for r in out if not r.ok] == ["primary key across parts"]
    assert "'obs000'" in parts_result(tmp_path, "primary key across parts").detail


# --- the sidecar ----------------------------------------------------------

def test_parts_missing_sidecar(tmp_path):
    write_ss(tmp_path)
    (tmp_path / SIDECAR_FILE).unlink()
    assert parts_failed(tmp_path) == ["sidecar"]
    assert "missing" in parts_result(tmp_path, "sidecar").detail


def _residecar(d, side, columns=None):
    pq.write_table(side, d / SIDECAR_FILE, compression="zstd")
    m = read_manifest(d)
    if columns is not None:
        m["sidecar"]["columns"] = columns
    refresh(d, m)


def test_parts_sidecar_missing_row(tmp_path):
    write_ss(tmp_path)
    _residecar(tmp_path, sidecar_for(ss_table()).slice(0, NS - 1))
    f = parts_failed(tmp_path)
    assert f == ["sidecar", "sidecar obsid"]


def test_parts_sidecar_shuffled(tmp_path):
    write_ss(tmp_path)
    _residecar(tmp_path, sidecar_for(ss_table()).take([0, 1, 3, 2, 4, 5, 6, 7, 8]))
    assert parts_failed(tmp_path) == ["sidecar obsid"]
    assert "first at row 2" in parts_result(tmp_path, "sidecar obsid").detail


@pytest.mark.parametrize("col,arr", [
    ("matchMethod", pa.array(list(range(NS)), pa.int64())),
    ("midpointMjdTai_flag_degraded", pa.array(["x"] * NS)),
    ("obsid", pa.array(list(range(NS)), pa.int64())),
])
def test_parts_sidecar_wrong_type(tmp_path, col, arr):
    write_ss(tmp_path)
    side = sidecar_for(ss_table())
    side = side.set_column(side.column_names.index(col), col, arr)
    _residecar(tmp_path, side)
    assert "sidecar columns" in parts_failed(tmp_path)


@pytest.mark.parametrize("col", ["matchMethod", "midpointMjdTai_flag_degraded", "obsid"])
def test_parts_sidecar_null(tmp_path, col):
    write_ss(tmp_path)
    side = sidecar_for(ss_table())
    vals = side[col].to_pylist()
    vals[4] = None
    side = side.set_column(side.column_names.index(col), col, pa.array(vals, side[col].type))
    _residecar(tmp_path, side)
    f = parts_failed(tmp_path)
    assert "sidecar nulls" in f
    assert set(f) <= {"sidecar nulls", "sidecar obsid"}


def test_parts_sidecar_column_delivered(tmp_path):
    write_ss(tmp_path)
    side = sidecar_for(ss_table()).append_column("ra", pa.array([1.0] * NS))
    _residecar(tmp_path, side, ["matchMethod", "midpointMjdTai_flag_degraded", "ra"])
    assert parts_failed(tmp_path) == ["sidecar columns"]
    assert "ra is also in the delivered schema" in parts_result(tmp_path, "sidecar columns").detail


def test_parts_sidecar_unknown_column(tmp_path):
    write_ss(tmp_path)
    side = sidecar_for(ss_table()).append_column("bogus", pa.array([1.0] * NS))
    _residecar(tmp_path, side, ["matchMethod", "midpointMjdTai_flag_degraded", "bogus"])
    assert parts_failed(tmp_path) == ["sidecar columns"]


def test_parts_sidecar_columns_disagree_with_manifest(tmp_path):
    write_ss(tmp_path)
    m = read_manifest(tmp_path)
    m["sidecar"]["columns"] = ["midpointMjdTai_flag_degraded", "matchMethod"]
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["sidecar columns"]


@pytest.mark.parametrize("field,value", [("md5", "0" * 32), ("bytes", 1), ("rows", NS + 1)])
def test_parts_sidecar_integrity(tmp_path, field, value):
    m = write_ss(tmp_path)
    m["sidecar"][field] = value
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == ["sidecar"]


def test_parts_cli(tmp_path, capsys):
    write_delivery(tmp_path)
    assert D.main([str(tmp_path)]) == 0
    assert "SSObservation: PASS" in capsys.readouterr().out
    (tmp_path / SIDECAR_FILE).unlink()
    assert D.main([str(tmp_path), "--tables", "SSObservation"]) == 1


# --- review round 1: stray files, the sidecar's name, manifest types ------

@pytest.mark.parametrize("name", ["SSObservation.parquet", "SSObservation.part0001.parquet.bak",
                                  "SSObservation.manifest.json.old", "ssobservation.part0009.parquet",
                                  "SSOBSERVATION_internal.parquet", "SSObservation_internal.parquet~"])
def test_parts_stray_file(tmp_path, name):
    write_ss(tmp_path)
    write(tmp_path, "SSObject", make_table("SSObject"))          # other tables' files: fine
    (tmp_path / name).write_bytes(b"stale")
    assert parts_failed(tmp_path) == ["stray files"]
    assert name in parts_result(tmp_path, "stray files").detail


@pytest.mark.parametrize("name", ["../SSObservation_internal.parquet", "sub/SSObservation_internal.parquet",
                                  "other.parquet"])
def test_parts_sidecar_name(tmp_path, name):
    d = tmp_path / "d"
    m = write_ss(d)
    target = d / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(d / SIDECAR_FILE, target)
    m["sidecar"]["file"] = name
    write_manifest(d, m)
    f = parts_failed(d)
    assert "manifest" in f and "sidecar" in f
    assert "want 'SSObservation_internal.parquet'" in parts_result(d, "sidecar").detail


def _set(m, path, value):
    *head, last = path
    for k in head:
        m = m[k]
    m[last] = value


@pytest.mark.parametrize("path,value", [
    (("parts", 0, "rows"), True), (("parts", 0, "bytes"), "123"), (("parts", 0, "rows"), 3.0),
    (("parts", 0, "md5"), "z" * 32), (("parts", 0, "md5"), 12345), (("parts", 0, "md5"), "abc"),
    (("parts", 0, "null_ssObjectId"), 0), (("parts", 0, "ssObjectId_min"), "1"),
    (("parts", 0, "ssObjectId_max"), 1.0), (("parts", 0, "file"), 7),
    (("format_version",), True), (("part_rows",), True), (("rows",), "9"), (("rows",), True),
    (("created_utc",), "yesterday"), (("created_utc",), "2026-10-08T12:00:00+02:00"),
    (("created_utc",), "2026-10-08T12:00:00"), (("created_utc",), None),
    (("ssp_tools_commit",), 12345), (("ssp_tools_commit",), "not-a-commit"),
    (("schema", "source"), "nonsense"), (("schema", "file"), "other.yaml"), (("schema", "md5"), "deadbeef"),
    (("sidecar", "key"), "obsId"), (("sidecar", "rows"), True), (("sidecar", "md5"), None),
    (("sidecar", "columns"), "matchMethod"), (("parts",), []), (("parts",), {}),
])
def test_parts_manifest_types(tmp_path, path, value):
    m = write_ss(tmp_path)
    _set(m, path, value)
    write_manifest(tmp_path, m)
    assert "manifest" in parts_failed(tmp_path)


def test_parts_manifest_empty_parts_message(tmp_path):
    m = write_ss(tmp_path)
    m["parts"] = []
    write_manifest(tmp_path, m)
    assert "parts is not a non-empty list" in parts_result(tmp_path, "manifest").detail


def test_parts_manifest_good_values(tmp_path):
    m = write_ss(tmp_path)
    m["ssp_tools_commit"] = "0123456789abcdef0123456789abcdef01234567"
    m["schema"]["md5"] = "0123456789abcdef0123456789abcdef"
    m["created_utc"] = "2026-10-08T12:00:00.123456+00:00"
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == []


def test_parts_manifest_extra_fields_allowed(tmp_path):
    # unknown fields are allowed at every level (forward compatibility)
    m = write_ss(tmp_path)
    m["bogus"] = 1
    m["parts"][0]["extra"] = "x"
    m["sidecar"]["what"] = 2
    m["schema"]["ref"] = "abc"
    write_manifest(tmp_path, m)
    assert parts_failed(tmp_path) == []


def test_parts_manifest_bad_file_entry_not_filtered(tmp_path):
    # an extra parts entry whose file is not a string: FAILs, not ignored
    m = write_ss(tmp_path)
    m["parts"].append({"file": 7, "rows": 0, "ssObjectId_min": None, "ssObjectId_max": None,
                       "null_ssObjectId": True, "bytes": 0, "md5": "0" * 32})
    write_manifest(tmp_path, m)
    f = parts_failed(tmp_path)
    assert "manifest" in f and "part files" in f


# --- NaN sort keys, the sidecar's encoding --------------------------------

def test_parts_nan_midpoint(tmp_path):
    t = ss_table()
    mjd = t["midpointMjdTai"].to_pylist()
    mjd[1] = float("nan")
    write_ss(tmp_path, replace(t, "midpointMjdTai", pa.array(mjd, pa.float64())))
    assert parts_failed(tmp_path) == ["sort order"]
    assert "1 NaN or NULL midpointMjdTai" in parts_result(tmp_path, "sort order").detail


@pytest.mark.parametrize("kw", [dict(compression="snappy"), dict(use_dictionary=False)])
def test_parts_sidecar_encoding(tmp_path, kw):
    write_ss(tmp_path)
    pq.write_table(sidecar_for(ss_table()), tmp_path / SIDECAR_FILE, **{"compression": "zstd", **kw})
    refresh(tmp_path)
    assert parts_failed(tmp_path) == ["sidecar encoding"]


def test_parts_sidecar_encoding_empty_table(tmp_path):
    write_ss(tmp_path, ss_table([]))
    assert parts_result(tmp_path, "sidecar encoding").ok


# --- crashes become FAILs -------------------------------------------------

def _rewrite_part(d, k, col, arr):
    p = d / PART_FILE_FORMAT.format(k)
    t = pq.read_table(p)
    t = t.set_column(t.column_names.index(col), col, arr)
    pq.write_table(t, p, compression="zstd")
    refresh(d)


def _no_crash(d):
    rs = D.check_ssobservation_parts(d)
    out = D.check_delivery(d, tables=["SSObservation"])["SSObservation"]
    return sorted(r.name for r in rs if not r.ok), out


def test_parts_uint64_ssobjectid_no_crash(tmp_path):
    write_ss(tmp_path)
    _rewrite_part(tmp_path, 1, "ssObjectId", pa.array([2 ** 63 + 5] * 2, pa.uint64()))
    f, out = _no_crash(tmp_path)
    assert "part contents" in f
    assert "ssObjectId is uint64" in parts_result(tmp_path, "part contents").detail
    assert not all(r.ok for r in out)


def test_parts_string_ssobjectid_no_crash(tmp_path):
    write_ss(tmp_path)
    for k, n in enumerate([3, 2, 1, 2, 1]):
        _rewrite_part(tmp_path, k, "ssObjectId", pa.array([None if k > 2 else str(k)] * n, pa.string()))
    f, out = _no_crash(tmp_path)
    assert "part contents" in f
    assert not all(r.ok for r in out)


def test_parts_sidecar_duplicated_column_no_crash(tmp_path):
    write_ss(tmp_path)
    side = sidecar_for(ss_table())
    side = pa.Table.from_arrays([side["obsid"], side["matchMethod"], side["matchMethod"]],
                                names=["obsid", "matchMethod", "matchMethod"])
    pq.write_table(side, tmp_path / SIDECAR_FILE, compression="zstd")
    m = read_manifest(tmp_path)
    m["sidecar"]["columns"] = ["matchMethod"]
    refresh(tmp_path, m)
    f, out = _no_crash(tmp_path)
    assert "sidecar columns" in f
    assert "duplicated columns ['matchMethod']" in parts_result(tmp_path, "sidecar columns").detail


# --- the sort tie-break and the cutting rules' edges ----------------------

def test_parts_sort_tie_break_on_obsid(tmp_path):
    # rows 0 and 1: same ssObjectId and midpointMjdTai, so obsid decides
    t = ss_table()
    mjd = t["midpointMjdTai"].to_pylist()
    mjd[1] = mjd[0]
    t = replace(t, "midpointMjdTai", pa.array(mjd, pa.float64()))
    write_ss(tmp_path / "ok", t)
    assert parts_failed(tmp_path / "ok") == []                  # obs000 < obs001: in order
    order = [1, 0, 2, 3, 4, 5, 6, 7, 8]
    write_ss(tmp_path / "bad", t.take(order), sidecar=sidecar_for(t).take(order))
    assert parts_failed(tmp_path / "bad") == ["sort order"]


def test_parts_sort_tie_break_across_boundary(tmp_path):
    # the NULL rows with equal midpointMjdTai, obsid descending across the
    # boundary of the two NULL parts
    t = ss_table()
    mjd = t["midpointMjdTai"].to_pylist()
    mjd[6:] = [61010.0] * 3
    t = replace(t, "midpointMjdTai", pa.array(mjd, pa.float64()))
    write_ss(tmp_path / "ok", t)
    assert parts_failed(tmp_path / "ok") == []
    order = [0, 1, 2, 3, 4, 5, 6, 8, 7]
    write_ss(tmp_path / "bad", t.take(order), sidecar=sidecar_for(t).take(order))
    assert parts_failed(tmp_path / "bad") == ["sort order"]


SID6 = [1, 1, 2, 2, 3, None]


def test_parts_closure_exact_boundary(tmp_path):
    # part0000 has objects 1 and 2: exactly part_rows (2) rows before its
    # last object, so it should have closed after object 1
    write_ss(tmp_path, ss_table(SID6), splits=[(0, 4, False), (4, 5, False), (5, 6, True)])
    assert parts_failed(tmp_path) == ["part sizes"]


def test_parts_closure_last_ranged_part(tmp_path):
    # the last ranged part [2, 2, 3] should have closed after object 2
    write_ss(tmp_path, ss_table(SID6), splits=[(0, 2, False), (2, 5, False), (5, 6, True)])
    assert parts_failed(tmp_path) == ["part sizes"]
    assert "SSObservation.part0001.parquet: not closed" in parts_result(tmp_path, "part sizes").detail


def test_parts_small_middle_ranged_part(tmp_path):
    # part0001 [2] (1 row < 2) is neither the first nor the last ranged part
    write_ss(tmp_path, ss_table([1, 1, 2, 3, 3, None]),
             splits=[(0, 2, False), (2, 3, False), (3, 5, False), (5, 6, True)])
    assert parts_failed(tmp_path) == ["part sizes"]
    assert "SSObservation.part0001.parquet: 1 rows < part_rows" in parts_result(tmp_path, "part sizes").detail


def test_parts_last_null_part_too_big(tmp_path):
    write_ss(tmp_path, splits=[(0, 3, False), (3, 5, False), (5, 6, False), (6, 9, True)])
    assert parts_failed(tmp_path) == ["part sizes"]
    assert "the last NULL part must have 1..2" in parts_result(tmp_path, "part sizes").detail


def test_parts_empty_ranged_part_after_full(tmp_path):
    write_ss(tmp_path, ss_table([1, 1]), splits=[(0, 2, False), (2, 2, False)])
    assert parts_failed(tmp_path) == ["part contents"]


def test_check_delivery_no_parts(tmp_path):
    out = D.check_delivery(tmp_path, tables=["SSObservation"])["SSObservation"]
    assert [(r.name, r.ok) for r in out] == [("manifest", False), ("parts", False)]


def test_parts_empty_table_flagged_null(tmp_path):
    # the empty table's single part flagged as a NULL part: both the
    # contents (an empty NULL part) and the empty-table rule FAIL
    write_ss(tmp_path, ss_table([]), splits=[(0, 0, True)])
    f = parts_failed(tmp_path)
    assert "part contents" in f and "part sizes" in f
    assert "a NULL part with no rows" in parts_result(tmp_path, "part contents").detail
    assert "null_ssObjectId false" in parts_result(tmp_path, "part sizes").detail
