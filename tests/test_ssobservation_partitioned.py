"""The SSObservation builder's partitioned output (docs/design/
ssobservation-delivery.md): the parts and how they are cut, the manifest,
and the configurable internal columns in the sidecar. On the synthetic
inputs of test_ssobservation_widened (offline)."""

import datetime
import hashlib
import json
import pathlib
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp import ssobservation, ssobservation_parts
from ssp.ssobservation import build_ssobservation, part_bounds, ssobservation_schema, write_partitioned
from ssp.ssobservation_contract import (
    MANIFEST_FORMAT_VERSION, PART_FIELDS, PART_FILE_FORMAT, PART_ROWS_DEFAULT, SIDECAR_FIELDS,
    SIDECAR_FILE, SIDECAR_KEY, SSOBSERVATION_INTERNAL_DEFAULT, SSOBSERVATION_INTERNAL_DTYPE,
    SSOBSERVATION_MANIFEST_FIELDS, SSOBSERVATION_MANIFEST_FILE, SSOBSERVATION_SORT, SSObservationDtype,
)

from test_ssobservation_widened import ROWS, _same, make_inputs, offline  # noqa: F401 (a fixture)

#: The synthetic inputs' rows: per object with an orbit (A 4, B 3, D 2),
#: and with a NULL ssObjectId (C's 2, without an orbit, and 3 'I' rows).
N_ROWS = len(ROWS)
N_NULL = 5
OBJECT_ROWS = sorted([4, 3, 2])


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def _ref_bounds(ids, part_rows):
    """The contract's cutting rules, row by row (a brute-force reference)."""
    if not ids:
        return [(0, 0)]
    bounds, start = [], 0
    for i in range(1, len(ids) + 1):
        end_of_table = i == len(ids)
        if end_of_table:
            bounds.append((start, i))
            break
        prev, cur = ids[i - 1], ids[i]
        if prev is not None and cur is None:            # the NULL rows start
            bounds.append((start, i))
            start = i
        elif prev is not None and cur != prev and i - start >= part_rows:   # an object boundary
            bounds.append((start, i))
            start = i
        elif prev is None and i - start == part_rows:
            bounds.append((start, i))
            start = i
    return bounds


def _built(path, **kw):
    """Build the synthetic inputs in ``path``; returns the manifest."""
    path.mkdir(exist_ok=True)
    make_inputs(path)
    return build_ssobservation(path, path, **kw)


def _parts(path):
    return [pq.read_table(p) for p in ssobservation_parts.part_paths(path)]


def _md5(p):
    return hashlib.md5(p.read_bytes()).hexdigest()


def _check_parts(path, part_rows):
    """Check the parts in ``path`` against the cutting rules, from the
    files themselves; returns them."""
    parts = _parts(path)
    m = ssobservation_parts.read_manifest(path)
    assert [p["file"] for p in m["parts"]] == [PART_FILE_FORMAT.format(k) for k in range(len(parts))]
    assert sorted(f.name for f in path.glob("SSObservation.part*")) == [p["file"] for p in m["parts"]]
    seen_null, prev_max = False, None
    ranged = [t for t in parts if t.num_rows and t["ssObjectId"].null_count == 0]
    for k, t in enumerate(parts):
        assert t.schema.equals(ssobservation_schema())
        ids = t["ssObjectId"]
        if t.num_rows and ids.null_count:
            assert ids.null_count == t.num_rows                       # NULL parts are all NULL
            assert t.num_rows == part_rows or k == len(parts) - 1      # cut every part_rows
            seen_null = True
            continue
        assert not seen_null                                          # NULL parts last
        if not t.num_rows:
            assert len(parts) == 1                                    # only the empty table's
            continue
        lo, hi = pc.min(ids).as_py(), pc.max(ids).as_py()
        assert prev_max is None or lo > prev_max                      # disjoint, ascending
        prev_max = hi
        # closes at the first boundary at or after part_rows: dropping its
        # last object leaves fewer than part_rows rows
        last = ids[-1].as_py()
        n_last = pc.sum(pc.equal(ids, last)).as_py()
        assert t.num_rows - n_last < part_rows
        if t is not ranged[-1]:
            assert t.num_rows >= part_rows
    return parts


# --------------------------------------------------------------------------
# part_bounds
# --------------------------------------------------------------------------

@pytest.mark.parametrize("ids,part_rows,want", [
    ([], 3, [(0, 0)]),                                                  # empty table
    ([1, 1, 1, 2, 2, 3], 2, [(0, 3), (3, 5), (5, 6)]),                  # part_rows < an object
    ([1, 1, 1, 2, 2, 3], 1, [(0, 3), (3, 5), (5, 6)]),
    ([1, 2, 3, 4, 5], 2, [(0, 2), (2, 4), (4, 5)]),                     # no NULLs
    ([1, 2, 3, 4, 5], 100, [(0, 5)]),
    ([None] * 5, 2, [(0, 2), (2, 4), (4, 5)]),                          # only NULLs
    ([None] * 4, 2, [(0, 2), (2, 4)]),
    ([1, 1, 2, None, None, None], 2, [(0, 2), (2, 3), (3, 5), (5, 6)]),  # NULLs over several parts
    ([1, 1, 2, None, None, None], 100, [(0, 3), (3, 6)]),               # the NULL part on its own
])
def test_part_bounds(ids, part_rows, want):
    got = part_bounds(pa.array(ids, pa.int64()), part_rows)
    assert got == want
    assert got == _ref_bounds(ids, part_rows)


def test_part_bounds_against_brute_force():
    rng = np.random.default_rng(1)
    for _ in range(500):
        n_obj = rng.integers(0, 12)
        ids = [int(o) for o in np.sort(rng.choice(10**6, n_obj, replace=False))
               for _ in range(rng.integers(1, 6))]
        ids += [None] * int(rng.integers(0, 8))
        part_rows = int(rng.integers(1, 12))
        got = part_bounds(pa.array(ids, pa.int64()), part_rows)
        assert got == _ref_bounds(ids, part_rows), (ids, part_rows)


def test_part_bounds_refuses():
    with pytest.raises(ValueError, match="at least 1"):
        part_bounds(pa.array([1, 2], pa.int64()), 0)
    with pytest.raises(ValueError, match="NULL rows are not last"):
        part_bounds(pa.array([1, None, 2], pa.int64()), 1)


# --------------------------------------------------------------------------
# The built parts
# --------------------------------------------------------------------------

@pytest.mark.parametrize("part_rows", [1, 2, 3, 4, 5, 6, 9, 10, 100])
def test_cut(tmp_path, offline, part_rows):  # noqa: F811
    m = _built(tmp_path, part_rows=part_rows)
    parts = _check_parts(tmp_path, part_rows)
    assert sum(t.num_rows for t in parts) == N_ROWS == m["rows"]
    null_rows = [t.num_rows for t in parts if t["ssObjectId"].null_count]
    assert null_rows == [min(part_rows, N_NULL - k) for k in range(0, N_NULL, part_rows)]
    # the whole table, cut as the reference cuts it
    ids = pa.concat_tables(parts)["ssObjectId"].to_pylist()
    assert [p["rows"] for p in m["parts"]] == [e - s for s, e in _ref_bounds(ids, part_rows)]


def test_several_ranged_parts(tmp_path, offline):  # noqa: F811
    # part_rows smaller than every object: one object per part
    m = _built(tmp_path, part_rows=1)
    ranged = [p for p in m["parts"] if not p["null_ssObjectId"]]
    assert sorted(p["rows"] for p in ranged) == OBJECT_ROWS
    assert all(p["ssObjectId_min"] == p["ssObjectId_max"] for p in ranged)
    assert len(m["parts"]) == len(OBJECT_ROWS) + N_NULL


def test_concatenated_parts_equal_unpartitioned(tmp_path, offline):  # noqa: F811
    _built(tmp_path / "one", part_rows=10**9)
    assert len(ssobservation_parts.part_paths(tmp_path / "one")) == 2    # (ranged, NULL)
    for part_rows in (1, 2, 4):
        d = tmp_path / f"p{part_rows}"
        _built(d, part_rows=part_rows)
        a = ssobservation_parts.read_ssobservation(tmp_path / "one", internal=True)
        b = ssobservation_parts.read_ssobservation(d, internal=True)
        assert a.schema.equals(b.schema)
        for c in a.column_names:
            assert _same(a[c].combine_chunks(), b[c].combine_chunks()), (part_rows, c)
        assert pq.read_table(d / SIDECAR_FILE).equals(pq.read_table(tmp_path / "one" / SIDECAR_FILE))


def _subset(tmp_path, mask_fn, part_rows):
    """write_partitioned on a subset of a built table (rows where
    ``mask_fn(table)``) into tmp_path/sub; returns the manifest."""
    src = tmp_path / "src"
    _built(src, part_rows=10**9)
    t = ssobservation_parts.read_ssobservation(src)
    side = pq.read_table(src / SIDECAR_FILE)
    keep = mask_fn(t)
    out = tmp_path / "sub"
    out.mkdir()
    return write_partitioned(t.filter(keep), side.filter(keep), out, part_rows=part_rows), out


def test_no_null_rows(tmp_path, offline):  # noqa: F811
    m, out = _subset(tmp_path, lambda t: pc.is_valid(t["ssObjectId"]), 3)
    assert not any(p["null_ssObjectId"] for p in m["parts"])
    assert m["rows"] == N_ROWS - N_NULL
    _check_parts(out, 3)


def test_only_null_rows(tmp_path, offline):  # noqa: F811
    m, out = _subset(tmp_path, lambda t: pc.is_null(t["ssObjectId"]), 2)
    assert [p["rows"] for p in m["parts"]] == [2, 2, 1]
    assert all(p["null_ssObjectId"] and p["ssObjectId_min"] is None and p["ssObjectId_max"] is None
               for p in m["parts"])
    _check_parts(out, 2)


def test_empty_table(tmp_path, offline):  # noqa: F811
    m, out = _subset(tmp_path, lambda t: pa.array(np.zeros(t.num_rows, bool)), 2)
    assert m["rows"] == 0
    assert m["parts"] == [{"file": PART_FILE_FORMAT.format(0), "rows": 0, "ssObjectId_min": None,
                           "ssObjectId_max": None, "null_ssObjectId": False,
                           "bytes": (out / PART_FILE_FORMAT.format(0)).stat().st_size,
                           "md5": _md5(out / PART_FILE_FORMAT.format(0))}]
    t = ssobservation_parts.read_ssobservation(out, internal=True)
    assert t.num_rows == 0 and t.schema.remove_metadata().names[:len(SSObservationDtype.names)] == list(
        SSObservationDtype.names)
    assert pq.read_table(out / SIDECAR_FILE).num_rows == 0


def test_rebuild_removes_stale_files(tmp_path, offline):  # noqa: F811
    _built(tmp_path, part_rows=1)
    stale = ["ssobservation.parquet", "SSObservation.manifest.json.tmp", "SSObservation.part0099.parquet",
             "SSOBSERVATION.notes.txt", "ssObservation_internal.old.parquet"]
    for name in stale:
        (tmp_path / name).write_text("stale")
    (tmp_path / "ssobservation_dir").mkdir()                 # (a directory is left alone)
    (tmp_path / "other.parquet").write_text("kept")
    old_sidecar = (tmp_path / SIDECAR_FILE).stat().st_mtime_ns
    m = _built(tmp_path, part_rows=10**9)
    names = sorted(f.name for f in tmp_path.iterdir() if f.name.lower().startswith("ssobservation"))
    assert names == sorted([p["file"] for p in m["parts"]]
                           + [SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE, "ssobservation_dir"])
    assert len(m["parts"]) == 2
    assert (tmp_path / "other.parquet").read_text() == "kept"
    assert (tmp_path / SIDECAR_FILE).stat().st_mtime_ns >= old_sidecar


def test_failed_rebuild_leaves_no_manifest(tmp_path, offline, monkeypatch):  # noqa: F811
    _built(tmp_path, part_rows=1)
    assert (tmp_path / SSOBSERVATION_MANIFEST_FILE).exists() and (tmp_path / SIDECAR_FILE).exists()
    calls = []
    orig = ssobservation.write_ssobservation

    def failing(table, path):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("disk full")
        orig(table, path)

    monkeypatch.setattr(ssobservation, "write_ssobservation", failing)
    with pytest.raises(OSError, match="disk full"):
        build_ssobservation(tmp_path, tmp_path, part_rows=1)
    assert not (tmp_path / SSOBSERVATION_MANIFEST_FILE).exists()
    assert not (tmp_path / SIDECAR_FILE).exists()
    assert sorted(f.name for f in tmp_path.glob("SSObservation*")) == [PART_FILE_FORMAT.format(0)]


def test_manifest_removed_first(tmp_path, offline, monkeypatch):  # noqa: F811
    # a removal that stops after the first file leaves no manifest
    _built(tmp_path, part_rows=1)
    removed = []
    orig = pathlib.Path.unlink

    def unlink(self, missing_ok=False):
        if removed:
            raise OSError("interrupted")
        removed.append(self.name)
        orig(self, missing_ok=missing_ok)

    monkeypatch.setattr(pathlib.Path, "unlink", unlink)
    with pytest.raises(OSError, match="interrupted"):
        build_ssobservation(tmp_path, tmp_path, part_rows=1)
    assert removed == [SSOBSERVATION_MANIFEST_FILE]
    assert not (tmp_path / SSOBSERVATION_MANIFEST_FILE).exists()


def _built_table(tmp_path):
    src = tmp_path / "src"
    _built(src, part_rows=10**9)
    return ssobservation_parts.read_ssobservation(src), pq.read_table(src / SIDECAR_FILE)


def _refused(tmp_path, t, side, match):
    out = tmp_path / "out"
    out.mkdir(exist_ok=True)
    (out / SSOBSERVATION_MANIFEST_FILE).write_text("{}")
    with pytest.raises(ValueError, match=match):
        write_partitioned(t, side, out, part_rows=2)
    # (refused before anything is removed or written)
    assert (out / SSOBSERVATION_MANIFEST_FILE).read_text() == "{}"
    assert not list(out.glob("SSObservation.part*"))


def test_write_refuses_mismatched_sidecar(tmp_path, offline):  # noqa: F811
    t, side = _built_table(tmp_path)
    _refused(tmp_path, t, side.slice(1), "sidecar has 13 rows")
    perm = list(range(1, side.num_rows)) + [0]
    _refused(tmp_path, t, side.take(perm), "sequence differs")
    swapped = side.set_column(0, SIDECAR_KEY, pa.array(side[SIDECAR_KEY].to_pylist()[::-1]))
    _refused(tmp_path, t, swapped, "sequence differs")
    _refused(tmp_path, t, side.select(side.column_names[1:] + [SIDECAR_KEY]), "first column")


def test_write_refuses_unsorted_ssobjectid(tmp_path, offline):  # noqa: F811
    t, side = _built_table(tmp_path)
    n_ranged = N_ROWS - N_NULL
    # the last object's rows first: ssObjectId descends
    perm = list(range(n_ranged - 1, n_ranged)) + list(range(n_ranged - 1)) + list(range(n_ranged, N_ROWS))
    _refused(tmp_path, t.take(perm), side.take(perm), "not in ascending order")
    # a NULL row among the ranged ones
    perm = [N_ROWS - 1] + list(range(N_ROWS - 1))
    _refused(tmp_path, t.take(perm), side.take(perm), "NULL rows are not last")


# --------------------------------------------------------------------------
# The manifest
# --------------------------------------------------------------------------

@pytest.mark.parametrize("part_rows,n_ranged", [(3, 3), (6, 2)])
def test_manifest(tmp_path, offline, part_rows, n_ranged):  # noqa: F811
    m = _built(tmp_path, part_rows=part_rows)
    on_disk = json.loads((tmp_path / SSOBSERVATION_MANIFEST_FILE).read_text())
    assert on_disk == m
    assert list(m) == list(SSOBSERVATION_MANIFEST_FIELDS)
    assert m["table"] == "SSObservation"
    assert m["format_version"] == MANIFEST_FORMAT_VERSION
    assert m["partition_key"] == "ssObjectId"
    assert m["sort"] == list(SSOBSERVATION_SORT)
    assert m["part_rows"] == part_rows
    assert m["rows"] == N_ROWS == sum(p["rows"] for p in m["parts"])

    schema_file = ssobservation.REPO_ROOT / "tests" / "data" / "sdm_schemas" / "sso_base.yaml"
    assert m["schema"] == {"source": "lsst/sdm_schemas tickets/DM-55375", "file": "sso_base.yaml",
                           "md5": _md5(schema_file)}
    head = subprocess.run(["git", "-C", str(ssobservation.REPO_ROOT), "rev-parse", "HEAD"],
                          capture_output=True, text=True)
    assert m["ssp_tools_commit"] == (head.stdout.strip() if head.returncode == 0 else None)
    t = datetime.datetime.strptime(m["created_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.UTC)
    assert abs((datetime.datetime.now(datetime.UTC) - t).total_seconds()) < 600

    for p in m["parts"]:
        assert list(p) == list(PART_FIELDS)
        f = tmp_path / p["file"]
        assert p["bytes"] == f.stat().st_size
        assert p["md5"] == _md5(f)
        t = pq.read_table(f)
        assert p["rows"] == t.num_rows
        ids = t["ssObjectId"]
        assert p["null_ssObjectId"] == (ids.null_count > 0)
        if p["null_ssObjectId"]:
            assert p["ssObjectId_min"] is None and p["ssObjectId_max"] is None
        else:
            assert p["ssObjectId_min"] == pc.min(ids).as_py()
            assert p["ssObjectId_max"] == pc.max(ids).as_py()
            assert isinstance(p["ssObjectId_min"], int)
    n_null = -(-N_NULL // part_rows)
    assert [p["null_ssObjectId"] for p in m["parts"]] == [False] * n_ranged + [True] * n_null
    # (with part_rows 6, a part holds two objects)
    assert any(p["ssObjectId_min"] < p["ssObjectId_max"] for p in m["parts"][:n_ranged]) == (part_rows == 6)

    s = m["sidecar"]
    assert list(s) == list(SIDECAR_FIELDS)
    assert s["file"] == SIDECAR_FILE and s["key"] == SIDECAR_KEY
    assert s["columns"] == list(SSOBSERVATION_INTERNAL_DEFAULT)
    assert s["rows"] == N_ROWS
    assert s["bytes"] == (tmp_path / SIDECAR_FILE).stat().st_size
    assert s["md5"] == _md5(tmp_path / SIDECAR_FILE)
    md = pq.ParquetFile(tmp_path / SIDECAR_FILE).metadata
    assert all(md.row_group(0).column(k).compression == "ZSTD" for k in range(md.num_columns))


def test_manifest_without_source_tree(tmp_path, offline, monkeypatch):  # noqa: F811
    monkeypatch.setattr(ssobservation, "REPO_ROOT", tmp_path / "nowhere")
    m = _built(tmp_path / "b")
    assert m["schema"]["md5"] is None
    assert m["ssp_tools_commit"] is None


def test_source_commit(tmp_path, monkeypatch):
    def git(*args, cwd=tmp_path / "repo"):
        return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                              check=True, capture_output=True, text=True).stdout.strip()

    (tmp_path / "repo").mkdir()
    git("init", "-q")
    git("commit", "-q", "--allow-empty", "-m", "x")
    sha = git("rev-parse", "HEAD")
    (tmp_path / "repo" / "site-packages").mkdir()
    monkeypatch.setattr(ssobservation, "REPO_ROOT", tmp_path / "repo")
    assert ssobservation.source_commit() == sha
    # an installed copy inside another checkout: not that checkout's commit
    monkeypatch.setattr(ssobservation, "REPO_ROOT", tmp_path / "repo" / "site-packages")
    assert ssobservation.source_commit() is None
    # a worktree (.git is a file)
    git("worktree", "add", "-q", "--detach", str(tmp_path / "wt"))
    assert (tmp_path / "wt" / ".git").is_file()
    monkeypatch.setattr(ssobservation, "REPO_ROOT", tmp_path / "wt")
    assert ssobservation.source_commit() == sha


def test_default_part_rows(tmp_path, offline):  # noqa: F811
    m = _built(tmp_path)
    assert m["part_rows"] == PART_ROWS_DEFAULT
    assert [p["rows"] for p in m["parts"]] == [N_ROWS - N_NULL, N_NULL]


@pytest.mark.parametrize("bad", [0, -1, 1.5, True])
def test_part_rows_refused(tmp_path, offline, bad):  # noqa: F811
    with pytest.raises(ValueError, match="part_rows"):
        build_ssobservation(tmp_path / "nowhere", tmp_path, part_rows=bad)


# --------------------------------------------------------------------------
# The internal columns
# --------------------------------------------------------------------------

@pytest.mark.parametrize("cols", [
    SSOBSERVATION_INTERNAL_DEFAULT,
    ("midpointMjdTai_flag_degraded", "matchMethod"),     # reordered
    ("matchMethod",),
    ("midpointMjdTai_flag_degraded",),
    (),
])
def test_internal_columns(tmp_path, offline, cols):  # noqa: F811
    _built(tmp_path / "ref")
    ref = ssobservation_parts.read_ssobservation(tmp_path / "ref", internal=True)
    m = _built(tmp_path / "b", internal_columns=cols)
    side = pq.read_table(tmp_path / "b" / SIDECAR_FILE)
    assert side.column_names == [SIDECAR_KEY, *cols]
    assert m["sidecar"]["columns"] == list(cols)
    for f in side.schema:
        assert not f.nullable, f.name
        assert f.type == ssobservation.arrow_type(f.name), f.name
    for c in cols:
        assert side[c].equals(ref[c]), c
    t = ssobservation_parts.read_ssobservation(tmp_path / "b")
    assert t.column_names == list(SSObservationDtype.names)
    assert side[SIDECAR_KEY].equals(t[SIDECAR_KEY])


def test_internal_columns_not_produced(tmp_path, offline, monkeypatch):  # noqa: F811
    # an unconfigured column isn't produced at all: its value check doesn't
    # run, and it isn't read
    dia, _ = make_inputs(tmp_path, match_method=True)
    mm = dia["matchMethod"].to_pylist()
    mm[0] = "telepathy"
    make_inputs(tmp_path, match_method=True, matchMethod=pa.array(mm))
    with pytest.raises(ValueError, match="telepathy"):
        build_ssobservation(tmp_path, tmp_path, internal_columns=("matchMethod",))
    read = []
    orig = pq.ParquetFile.read

    def spy(self, columns=None, **kw):
        read.extend(columns or [])
        return orig(self, columns=columns, **kw)

    monkeypatch.setattr(pq.ParquetFile, "read", spy)
    build_ssobservation(tmp_path, tmp_path, internal_columns=("midpointMjdTai_flag_degraded",))
    assert "matchMethod" not in read and "midpointMjdTai_flag_degraded" in read
    side = pq.read_table(tmp_path / SIDECAR_FILE)
    assert side.column_names == [SIDECAR_KEY, "midpointMjdTai_flag_degraded"]
    read.clear()
    build_ssobservation(tmp_path, tmp_path, internal_columns=())
    assert not set(read) & set(SSOBSERVATION_INTERNAL_DTYPE)


def test_match_inputs_not_needed_without_match_method(tmp_path, offline):  # noqa: F811
    # without matchMethod configured, neither it nor match/obssubid (what it
    # is derived from) is needed in dia_sources.parquet
    dia, _ = make_inputs(tmp_path)
    assert "matchMethod" not in dia.column_names
    pq.write_table(dia.drop_columns(["match", "obssubid"]), tmp_path / "dia_sources.parquet")
    with pytest.raises(ValueError, match="lacks"):
        build_ssobservation(tmp_path, tmp_path)
    m = build_ssobservation(tmp_path, tmp_path, internal_columns=("midpointMjdTai_flag_degraded",))
    assert m["rows"] == N_ROWS


def test_internal_columns_derived_match_method(tmp_path, offline):  # noqa: F811
    # (inputs predating matchMethod: derived only when configured)
    make_inputs(tmp_path, shutter=False)
    build_ssobservation(tmp_path, tmp_path, internal_columns=("matchMethod",))
    assert set(pq.read_table(tmp_path / SIDECAR_FILE)["matchMethod"].to_pylist()) <= {
        "obssubid", "obssubid_trail", "position"}
    build_ssobservation(tmp_path, tmp_path, internal_columns=("midpointMjdTai_flag_degraded",))
    side = pq.read_table(tmp_path / SIDECAR_FILE)
    assert side.column_names == [SIDECAR_KEY, "midpointMjdTai_flag_degraded"]
    assert not pc.any(side["midpointMjdTai_flag_degraded"]).as_py()


@pytest.mark.parametrize("cols,match", [
    (("telepathy",), "not columns the build can make internal"),
    (("matchMethod", "ra"), "in the delivered SSObservation schema"),
    (("matchMethod", "matchMethod"), "more than once"),
    ("matchMethod", "not a string"),
])
def test_internal_columns_refused_before_reading(tmp_path, offline, cols, match):  # noqa: F811
    # (the input directory doesn't exist: the check comes before any read)
    with pytest.raises(ValueError, match=match):
        build_ssobservation(tmp_path / "nowhere", tmp_path, internal_columns=cols)


def test_internal_column_in_both_refused(monkeypatch):
    # a column back in the delivered schema can't stay internal
    monkeypatch.setattr(ssobservation, "_NAMES", (*SSObservationDtype.names, "matchMethod"))
    with pytest.raises(ValueError, match="in the delivered SSObservation schema"):
        ssobservation.check_internal_columns(("matchMethod",))
    with pytest.raises(ValueError, match="in the delivered SSObservation schema"):
        ssobservation.check_internal_columns(("ra",))


# --------------------------------------------------------------------------
# The command line
# --------------------------------------------------------------------------

def _main(monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["ssp-build-ssobservation", *args])
    ssobservation.main()


def test_cli(tmp_path, offline, monkeypatch):  # noqa: F811
    make_inputs(tmp_path)
    d = str(tmp_path)
    _main(monkeypatch, "--input-dir", d, "--output-dir", d, "--workers", "1", "--part-rows", "2",
          "--internal-columns", " midpointMjdTai_flag_degraded , matchMethod")
    m = ssobservation_parts.read_manifest(tmp_path)
    assert m["part_rows"] == 2
    assert m["sidecar"]["columns"] == ["midpointMjdTai_flag_degraded", "matchMethod"]
    _main(monkeypatch, "--input-dir", d, "--output-dir", d, "--workers", "1", "--internal-columns", "")
    m = ssobservation_parts.read_manifest(tmp_path)
    assert m["sidecar"]["columns"] == [] and m["part_rows"] == PART_ROWS_DEFAULT
    assert pq.read_table(tmp_path / SIDECAR_FILE).column_names == [SIDECAR_KEY]
    _main(monkeypatch, "--input-dir", d, "--output-dir", d, "--workers", "1")
    assert ssobservation_parts.read_manifest(tmp_path)["sidecar"]["columns"] == list(
        SSOBSERVATION_INTERNAL_DEFAULT)


@pytest.mark.parametrize("args,match", [(["--part-rows", "0"], "--part-rows must be at least 1"),
                                        (["--internal-columns", "ra"], "internal columns ['ra']"),
                                        (["--internal-columns", "nope"], "internal columns ['nope']")])
def test_cli_refuses(tmp_path, monkeypatch, capsys, args, match):
    with pytest.raises(SystemExit) as e:
        _main(monkeypatch, "--input-dir", str(tmp_path / "nowhere"), "--output-dir", str(tmp_path), *args)
    assert e.value.code != 0
    assert match in capsys.readouterr().err
    assert not (tmp_path / SSOBSERVATION_MANIFEST_FILE).exists()


def test_parse_internal_columns():
    assert ssobservation.parse_internal_columns("") == ()
    assert ssobservation.parse_internal_columns("  ") == ()
    assert ssobservation.parse_internal_columns("a,b") == ("a", "b")
    assert ssobservation.parse_internal_columns(" b , a ") == ("b", "a")
