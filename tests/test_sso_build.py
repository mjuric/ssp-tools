"""ssp-build-sso (ssp/sso_build.py), stage 2 of the SSO delivery.

Offline unit tests of the manifest validation, the ``mpc`` shaping rules,
the report and ``--from`` (with stand-in steps); and, when the USDF subset
fixture and ASSIST are present, an end-to-end build on it, plus one on a
hand-made INPUTS_DIR of other file names, column orders, types and extra
columns, which must deliver the same tables.
"""

import datetime
import json
import os
import re
import shutil
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from ssp import sso_build as B
from ssp.delivery_contract import (
    BUILD_STEPS,
    DELIVERY_TABLES,
    INPUT_FILES,
    REPORT_FIELDS,
    REQUIRED_INPUT_COLUMNS,
    SHUTTER_INPUT_COLUMNS,
    delivery_schema,
)

SCHEMA = delivery_schema()
UTC = datetime.timezone.utc


# ---------------------------------------------------------------------------
# Synthetic inputs
# ---------------------------------------------------------------------------

def _value(col, i):
    dt = col["datatype"]
    if dt in ("char", "text"):
        return '{"i": %d}' % i if B.is_json_column(col) else f"{col['name'][:4]}{i}"
    if dt == "timestamp":
        return datetime.datetime(2025, 1, 1) + datetime.timedelta(seconds=i)
    if dt == "boolean":
        return bool(i % 2)
    return i + 1


def mpc_input(table, n=5, extras=True):
    """A synthetic MPC input table: the schema's columns (less the derived
    ones) in reverse order, plus the extra columns the MPC has."""
    cols = {}
    for c in reversed(SCHEMA[table]):
        if (table, c["name"]) in B.MPC_DERIVED:
            continue
        cols[c["name"]] = pa.array([_value(c, i) for i in range(n)], B.arrow_type(c))
    if extras:
        cols["identifier_ids" if table == "current_identifications" else "extra_col"] = pa.array(
            [f"{{x{i}}}" for i in range(n)])
        if table == "numbered_identifications":
            cols["numbered_publication_references"] = pa.array([None] * n, pa.string())
            cols["named_publication_references"] = pa.array([None] * n, pa.string())
    return pa.table(cols)


def write_inputs(d, n=4, files=None, **replace):
    """A complete INPUTS_DIR of small synthetic files, each with only the
    required columns (the MPC tables: the schema's), and its manifest."""
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    names = {}
    for name, (fname, _) in INPUT_FILES.items():
        if name in replace:
            t = replace[name]
        elif name in B.MPC_TABLES:
            t = mpc_input(name, n)
        else:
            t = pa.table({c: pa.array(np.arange(n)) for c in REQUIRED_INPUT_COLUMNS[name]})
        fname = (files or {}).get(name, fname)
        (d / fname).parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(t, d / fname)
        names[name] = fname
    return B.write_manifest(d, names, producer="test")


# ---------------------------------------------------------------------------
# The manifest
# ---------------------------------------------------------------------------

def test_valid_manifest(tmp_path):
    m = write_inputs(tmp_path)
    assert B.validate_manifest(tmp_path) == m
    assert set(m["files"]) == set(INPUT_FILES)


def _edit_manifest(d, fn):
    p = Path(d) / "manifest.json"
    m = json.loads(p.read_text())
    fn(m)
    p.write_text(json.dumps(m))


@pytest.mark.parametrize("edit, match", [
    (lambda m: m["files"]["obs_sbn"].update(rows=99), "obs_sbn: 4 rows, the manifest says 99"),
    (lambda m: m["files"]["mpc_orbits"].update(md5="0" * 32), "mpc_orbits: md5"),
    (lambda m: m["files"].pop("ppdb_dia_sources"), "lacks input 'ppdb_dia_sources'"),
    (lambda m: m["files"]["dia_sources"].update(file="nope.parquet"), "nope.parquet does not exist"),
    (lambda m: m.pop("mpc_snapshot_utc"), "manifest lacks 'mpc_snapshot_utc'"),
    (lambda m: m["files"]["obs_sbn"].pop("md5"), r"obs_sbn: manifest entry lacks \['md5'\]"),
])
def test_manifest_failures(tmp_path, edit, match):
    write_inputs(tmp_path)
    _edit_manifest(tmp_path, edit)
    with pytest.raises(B.ManifestError, match=match):
        B.validate_manifest(tmp_path)


def test_manifest_missing(tmp_path):
    with pytest.raises(B.ManifestError, match="no manifest.json"):
        B.validate_manifest(tmp_path)


def test_manifest_required_columns(tmp_path):
    t = pa.table({c: pa.array([1, 2]) for c in REQUIRED_INPUT_COLUMNS["dia_sources"] if c != "measuredOn"})
    write_inputs(tmp_path, dia_sources=t)
    with pytest.raises(B.ManifestError, match=r"dia_sources: lacks required columns \['measuredOn'\]"):
        B.validate_manifest(tmp_path)


def test_manifest_partial_shutter_columns(tmp_path):
    # the shutter correction's columns: all or none (shutter-timing.md)
    cols = REQUIRED_INPUT_COLUMNS["dia_sources"] + ["midpointMjdTaiVisit", "midpointMjdTai_flag"]
    write_inputs(tmp_path, dia_sources=pa.table({c: pa.array([1, 2]) for c in cols}))
    with pytest.raises(B.ManifestError, match=r"dia_sources: has some of the shutter-correction columns, but "
                                              r"lacks \['midpointMjdTai_flag_degraded', 'obstime_basis'\]"):
        B.validate_manifest(tmp_path)
    cols = REQUIRED_INPUT_COLUMNS["dia_sources"] + list(SHUTTER_INPUT_COLUMNS)
    write_inputs(tmp_path, dia_sources=pa.table({c: pa.array(np.arange(4)) for c in cols}))
    B.validate_manifest(tmp_path)


def test_manifest_every_problem_listed(tmp_path):
    write_inputs(tmp_path)
    _edit_manifest(tmp_path, lambda m: (m["files"]["obs_sbn"].update(rows=1),
                                        m["files"]["mpc_orbits"].update(md5="x")))
    with pytest.raises(B.ManifestError) as e:
        B.validate_manifest(tmp_path)
    assert "obs_sbn" in str(e.value) and "mpc_orbits" in str(e.value)


def test_manifest_dia_sources_out_of_step(tmp_path):
    """dia_sources built from another obs_sbn: refused."""
    write_inputs(tmp_path)
    _edit_manifest(tmp_path, lambda m: m["files"]["dia_sources"].update(obs_sbn_md5="f" * 32))
    with pytest.raises(B.ManifestError, match="dia_sources was built from a obs_sbn of md5 f{32}, not this"):
        B.validate_manifest(tmp_path)


def test_manifest_dia_sources_md5_from_source(tmp_path):
    """Manifests before obs_sbn_md5: the md5 in the source string."""
    m = write_inputs(tmp_path)
    obs = m["files"]["obs_sbn"]["md5"]

    def old_style(md5):
        def edit(m):
            m["files"]["dia_sources"].pop("obs_sbn_md5", None)
            m["files"]["dia_sources"]["source"] = f"extract-submitted-sources on obs_sbn (md5 {md5})"
        return edit

    _edit_manifest(tmp_path, old_style(obs))
    B.validate_manifest(tmp_path)
    _edit_manifest(tmp_path, old_style("0" * 32))
    with pytest.raises(B.ManifestError, match="out of step"):
        B.validate_manifest(tmp_path)
    _edit_manifest(tmp_path, lambda m: m["files"]["dia_sources"].update(source="somewhere"))
    with pytest.raises(B.ManifestError, match="does not record the md5 of the obs_sbn"):
        B.validate_manifest(tmp_path)


def test_manifest_any_file_names(tmp_path):
    """The manifest's 'file' may be any path under INPUTS_DIR."""
    m = write_inputs(tmp_path, files={"obs_sbn": "mpc/X05-observations.parquet"})
    assert B.validate_manifest(tmp_path)["files"]["obs_sbn"]["file"] == "mpc/X05-observations.parquet"
    assert B.input_path(tmp_path, m, "obs_sbn") == tmp_path / "mpc" / "X05-observations.parquet"


def test_invalid_inputs_report(tmp_path):
    write_inputs(tmp_path / "in")
    _edit_manifest(tmp_path / "in", lambda m: m["files"]["obs_sbn"].update(rows=1))
    rep = B.build(tmp_path / "in", tmp_path / "run", log=lambda *a: None)
    assert rep["deliverable"] is False and "obs_sbn: 4 rows" in rep["error"]
    assert all(s["status"] == "skipped" for s in rep["steps"].values())
    assert rep["inputs"]["producer"] == "test"           # (as read)
    assert json.loads((tmp_path / "run" / "report.json").read_text()) == rep
    assert B.main([str(tmp_path / "in"), str(tmp_path / "run2")]) == 1


# ---------------------------------------------------------------------------
# Step mpc: the shaping rules
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("table", B.MPC_TABLES)
def test_shape_mpc_table(tmp_path, table):
    src = tmp_path / "in.parquet"
    t = mpc_input(table, n=300)
    pq.write_table(t, src, row_group_size=70)
    dst = tmp_path / f"{table}.parquet"
    assert B.shape_mpc_table(table, src, dst, SCHEMA, batch_rows=128, log=lambda *a: None) == 300
    out = pq.read_table(dst)
    # exactly the schema's columns, in order, with its types
    assert out.schema.names == [c["name"] for c in SCHEMA[table]]
    assert out.schema == B.arrow_schema(SCHEMA[table])
    for c in SCHEMA[table]:
        src_name = B.MPC_DERIVED.get((table, c["name"]), c["name"])
        assert out[c["name"]].equals(t[src_name].cast(out.schema.field(c["name"]).type)), c["name"]
    assert pq.ParquetFile(dst).metadata.row_group(0).column(0).compression == "ZSTD"
    assert not os.path.exists(f"{dst}.tmp")


def test_mpc_designation_and_dropped_columns():
    names = {t: [c["name"] for c in SCHEMA[t]] for t in B.MPC_TABLES}
    assert names["mpc_orbits"][1] == "designation"
    assert "identifier_ids" not in names["current_identifications"]
    assert not {"numbered_publication_references", "named_publication_references"} & set(
        names["numbered_identifications"])
    b = mpc_input("mpc_orbits", 3).to_batches()[0]
    out = B.shape_batch("mpc_orbits", b, SCHEMA["mpc_orbits"])
    assert out["designation"].equals(out["unpacked_primary_provisional_designation"])


def test_published_is_int():
    col = next(c for c in SCHEMA["current_identifications"] if c["name"] == "published")
    assert B.cast_column("p", pa.array([0, 1, 2, 4], pa.int64()), col).type == pa.int32()
    assert B.cast_column("p", pa.array(["0", "4", None]), col).to_pylist() == [0, 4, None]
    with pytest.raises(B.ShapeError, match="cannot cast"):
        B.cast_column("p", pa.array([0.5]), col)
    with pytest.raises(B.ShapeError, match="cannot cast"):
        B.cast_column("p", pa.array([2**40]), col)


def _col(datatype, nullable=True, name="x", **kw):
    return dict(name=name, datatype=datatype, nullable=nullable, **kw)


def test_cast_rules():
    s = _col("char")
    assert B.cast_column("w", pa.array(["a", None], pa.large_string()), s).type == pa.string()
    assert B.cast_column("w", pa.array(["a", "b", "a"]).dictionary_encode(), s).to_pylist() == ["a", "b", "a"]
    with pytest.raises(B.ShapeError, match="cannot cast int64 to a string"):
        B.cast_column("w", pa.array([1]), s)
    b = _col("boolean")
    assert B.cast_column("w", pa.array([0, 1, None]), b).to_pylist() == [False, True, None]
    with pytest.raises(B.ShapeError, match="other than 0 and 1"):
        B.cast_column("w", pa.array([0, 2]), b)
    with pytest.raises(B.ShapeError, match="NULL values in a non-nullable"):
        B.cast_column("w", pa.array([1, None]), _col("int", nullable=False))
    with pytest.raises(B.ShapeError, match="cannot cast"):
        B.cast_column("w", pa.array([{"a": 1}]), _col("double"))
    with pytest.raises(B.ShapeError, match="overflow"):
        B.cast_column("w", pa.array([1e300]), _col("float"))


def test_cast_timestamps():
    ts = _col("timestamp", precision=6)
    t = datetime.datetime(2024, 5, 6, 7, 8, 9, 123456)
    want = pa.array([t, None], pa.timestamp("us"))
    assert B.cast_column("w", pa.array([t, None], pa.timestamp("us", "UTC")), ts).equals(want)
    assert B.cast_column("w", pa.array([t, None], pa.timestamp("ns")), ts).equals(want)
    assert B.cast_column("w", pa.array(["2024-05-06 07:08:09.123456", None]), ts).equals(want)
    assert B.cast_column("w", pa.array(["2024-05-06T07:08:09.123456+00:00", None]), ts).equals(want)
    with pytest.raises(B.ShapeError, match="cannot cast"):    # (sub-microsecond: not safe)
        B.cast_column("w", pa.array([1], pa.timestamp("ns")), ts)
    with pytest.raises(B.ShapeError, match="outside 1990 .. 2100"):   # (seconds read as us)
        B.cast_column("w", pa.array([1_700_000_000], pa.int64()).cast(pa.timestamp("us")), ts)
    with pytest.raises(B.ShapeError, match="cannot cast double"):
        B.cast_column("w", pa.array([1.0]), ts)


@pytest.mark.parametrize("workers", [1, 3])
@pytest.mark.parametrize("bad, match", [('{"a": 1', "invalid JSON"), ('"{}"', "JSON str, not an object")])
def test_shape_bad_json(tmp_path, workers, bad, match):
    t = mpc_input("mpc_orbits", n=50)
    i = t.schema.get_field_index("mpc_orb_jsonb")
    vals = t["mpc_orb_jsonb"].to_pylist()
    vals[37] = bad
    t = t.set_column(i, "mpc_orb_jsonb", pa.array(vals))
    pq.write_table(t, tmp_path / "in.parquet")
    dst = tmp_path / "out.parquet"
    with pytest.raises(B.ShapeError, match=f"mpc_orbits.mpc_orb_jsonb: row 37: {match}"):
        B.shape_mpc_table("mpc_orbits", tmp_path / "in.parquet", dst, SCHEMA, workers=workers,
                          batch_rows=16, log=lambda *a: None)
    assert not dst.exists() and not os.path.exists(f"{dst}.tmp")


def test_shape_missing_column(tmp_path):
    t = mpc_input("mpc_orbits").drop_columns(["unpacked_primary_provisional_designation", "h"])
    pq.write_table(t, tmp_path / "in.parquet")
    with pytest.raises(B.ShapeError,
                       match=r"lacks columns \['unpacked_primary_provisional_designation', 'h'\]"):
        B.shape_mpc_table("mpc_orbits", tmp_path / "in.parquet", tmp_path / "out.parquet", SCHEMA,
                          log=lambda *a: None)
    assert not (tmp_path / "out.parquet").exists()


def test_step_mpc_subprocess_fails_clearly(tmp_path):
    """The mpc step, run by the driver: a bad cast fails it, with the reason
    in its log."""
    t = mpc_input("current_identifications")
    t = t.set_column(t.schema.get_field_index("published"), "published", pa.array([0.5, 1, 2, 3, 4]))
    write_inputs(tmp_path / "in", current_identifications=t)
    rep = B.build(tmp_path / "in", tmp_path / "run", log=lambda *a: None)
    assert rep["steps"]["mpc"]["status"] == "failed"
    assert [rep["steps"][s]["status"] for s in BUILD_STEPS[1:]] == ["skipped"] * 4
    log = (tmp_path / "run" / rep["steps"]["mpc"]["log"]).read_text()
    assert "current_identifications.published: cannot cast double to int32" in log
    assert not (tmp_path / "run" / "delivery" / "current_identifications.parquet").exists()


# ---------------------------------------------------------------------------
# The driver, with stand-in builder steps: the report and --from
# ---------------------------------------------------------------------------

FAKE = """
import json, sys, pyarrow as pa, pyarrow.parquet as pq
out, fail, n = sys.argv[1], sys.argv[2] == "1", int(sys.argv[3])
print("fake step writing", out)
if fail:
    sys.exit("fake failure")
if out.endswith(".json"):
    checks = json.loads(sys.argv[4])
    json.dump({k: {"status": v, "report": "checks/fake.txt"} for k, v in checks.items()}, open(out, "w"))
    sys.exit(int(sys.argv[5]))
pq.write_table(pa.table({"x": list(range(n))}), out)
"""


@pytest.fixture
def fake_steps(monkeypatch):
    """Replace the builder and check steps with stand-ins; the mpc step is
    real. ``fails`` lists the steps that fail; ``rows`` the rows written;
    ``checks`` the check results the fake check step writes, and
    ``check_exit`` its exit code."""
    state = dict(fails=set(), rows=3, calls=[], checks={c: "PASS" for c in B.EXPECTED_CHECKS},
                 check_exit=0)
    real = B.step_command

    def step_command(step, inputs_dir, run_dir, manifest, workers):
        state["calls"].append(step)
        if step == "mpc":
            return real(step, inputs_dir, run_dir, manifest, workers)
        work = Path(run_dir).resolve() / "work" / step
        fail = "1" if step in state["fails"] else "0"
        if step == "check":
            (Path(run_dir) / "checks").mkdir(exist_ok=True)
            out = Path(run_dir).resolve() / "checks" / "results.json"
            return [sys.executable, "-c", FAKE, str(out), fail, str(state["rows"]),
                    json.dumps(state["checks"]), str(state["check_exit"])], {}
        table = B.STEP_TABLES[step][0]
        out = work / f"{table}.parquet"
        return ([sys.executable, "-c", FAKE, str(out), fail, str(state["rows"])],
                {out: Path(run_dir).resolve() / "delivery" / f"{table}.parquet"})

    monkeypatch.setattr(B, "step_command", step_command)
    return state


def _quiet(*a):
    pass


def test_report_all_ok(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    rep = B.build(tmp_path / "in", tmp_path / "run", workers=2, log=_quiet)
    assert set(REPORT_FIELDS) - {"upload"} <= set(rep)
    assert rep["deliverable"] is True
    assert list(rep["steps"]) == list(BUILD_STEPS)
    for s in BUILD_STEPS:
        e = rep["steps"][s]
        assert e["status"] == "ok" and e["wall_s"] >= 0 and e["max_rss_gb"] > 0
        assert (tmp_path / "run" / e["log"]).exists()
        datetime.datetime.fromisoformat(e["started_utc"])
    assert set(rep["tables"]) == set(DELIVERY_TABLES)
    for t, e in rep["tables"].items():
        p = tmp_path / "run" / e["file"]
        assert e["file"] == f"delivery/{t}.parquet"
        assert e["md5"] == B._md5(p) and e["bytes"] == p.stat().st_size
    assert rep["tables"]["mpc_orbits"]["rows"] == 4 and rep["tables"]["SSSource"]["rows"] == 3
    assert rep["checks"] == {c: {"status": "PASS", "report": "checks/fake.txt"} for c in B.EXPECTED_CHECKS}
    assert rep["input_paths"]["obs_sbn"] == str((tmp_path / "in" / "obs_sbn.parquet").resolve())
    assert all(rep["steps"][s]["ssp_tools_commit"] == rep["ssp_tools_commit"] for s in BUILD_STEPS)
    assert rep["inputs"] == B.read_manifest(tmp_path / "in")
    assert json.loads((tmp_path / "run" / "report.json").read_text()) == rep
    delivered = sorted(os.listdir(tmp_path / "run" / "delivery"))
    assert delivered == sorted(f"{t}.parquet" for t in DELIVERY_TABLES)


def test_failed_check_not_deliverable(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    fake_steps["checks"]["delivery:SSObject"] = "FAIL"
    fake_steps["check_exit"] = 1
    rep = B.build(tmp_path / "in", tmp_path / "run", log=_quiet)
    assert rep["steps"]["check"]["status"] == "failed"
    assert rep["checks"]["delivery:SSObject"]["status"] == "FAIL"
    assert rep["deliverable"] is False


@pytest.mark.parametrize("checks", [
    "one FAIL",                 # (M3: the check results, not just the step's exit code)
    "sssource missing",         # (an expected check that never ran)
    "extra FAIL",
])
def test_deliverable_needs_every_check(tmp_path, fake_steps, checks):
    """A check step that exits 0 is not enough: every expected check, by
    name, must PASS."""
    write_inputs(tmp_path / "in")
    if checks == "one FAIL":
        fake_steps["checks"]["delivery:NearbySSO"] = "FAIL"
    elif checks == "sssource missing":
        for c in ("sssource:conformance", "sssource:offsets"):
            del fake_steps["checks"][c]
    else:
        fake_steps["checks"]["something:else"] = "FAIL"
    rep = B.build(tmp_path / "in", tmp_path / "run", log=_quiet)
    assert rep["steps"]["check"]["status"] == "ok"
    assert rep["deliverable"] is False


def test_deliverable_rules():
    """_deliverable on hand-made reports (M3, M19)."""
    ok = dict(steps={s: dict(status="ok") for s in BUILD_STEPS},
              checks={c: dict(status="PASS") for c in B.EXPECTED_CHECKS},
              tables={t: {} for t in DELIVERY_TABLES})
    assert B._deliverable(ok) is True
    for edit in (lambda r: r["tables"].pop("NearbySSO"),
                 lambda r: r["checks"]["delivery:SSSource"].update(status="FAIL"),
                 lambda r: r["checks"].pop("sssource:offsets"),
                 lambda r: r.update(checks={}),
                 lambda r: r["steps"]["ssobject"].update(status="skipped")):
        r = json.loads(json.dumps(ok))
        edit(r)
        assert B._deliverable(r) is False


def test_failed_step_then_from(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    fake_steps["fails"] = {"ssobject"}
    rep = B.build(tmp_path / "in", run, log=_quiet)
    st = {s: e["status"] for s, e in rep["steps"].items()}
    assert st == dict(mpc="ok", sssource="ok", ssobject="failed", nearbysso="skipped", check="skipped")
    assert "exited 1" in rep["steps"]["ssobject"]["error"]
    assert "fake failure" in (run / "logs" / "ssobject.log").read_text()
    assert rep["deliverable"] is False and set(rep["tables"]) == set(B.MPC_TABLES) | {"SSSource"}
    # the report on disk is the failed run's
    assert json.loads((run / "report.json").read_text())["steps"]["ssobject"]["status"] == "failed"

    # --from a step after the failed one: refused
    with pytest.raises(ValueError, match="step ssobject did not succeed"):
        B.build(tmp_path / "in", run, from_step="nearbysso", log=_quiet)

    # --from ssobject: the earlier outputs and entries are kept
    sss = run / "delivery" / "SSSource.parquet"
    mtime = sss.stat().st_mtime_ns
    fake_steps["fails"] = set()
    fake_steps["calls"].clear()
    rep2 = B.build(tmp_path / "in", run, from_step="ssobject", log=_quiet)
    assert fake_steps["calls"] == ["ssobject", "nearbysso", "check"]
    assert rep2["deliverable"] is True
    for s in ("mpc", "sssource"):
        assert rep2["steps"][s] == rep["steps"][s]
    assert rep2["tables"]["SSSource"] == rep["tables"]["SSSource"]
    assert sss.stat().st_mtime_ns == mtime


def test_from_refuses_other_inputs(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    B.build(tmp_path / "in", tmp_path / "run", log=_quiet)
    write_inputs(tmp_path / "in2", n=5)
    with pytest.raises(ValueError, match="inputs differ"):
        B.build(tmp_path / "in2", tmp_path / "run", from_step="check", log=_quiet)
    with pytest.raises(ValueError, match="no .*report.json"):
        B.build(tmp_path / "in", tmp_path / "new", from_step="check", log=_quiet)
    assert B.main([str(tmp_path / "in"), str(tmp_path / "new"), "--from", "check"]) == 2


def test_from_removes_later_outputs(tmp_path, fake_steps):
    """A rerun from a step removes that step's and the later steps' old
    outputs first, so a failure cannot leave stale tables delivered."""
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    fake_steps["fails"] = {"sssource"}
    rep = B.build(tmp_path / "in", run, from_step="sssource", log=_quiet)
    assert rep["steps"]["sssource"]["status"] == "failed"
    left = sorted(os.listdir(run / "delivery"))
    assert left == sorted(f"{t}.parquet" for t in B.MPC_TABLES)
    assert set(rep["tables"]) == set(B.MPC_TABLES) and rep["checks"] == {}
    assert not (run / "checks").exists()


def test_fresh_run_clears_old_outputs(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    write_inputs(tmp_path / "in", current_identifications=mpc_input("current_identifications").drop_columns(
        ["published"]))
    rep = B.build(tmp_path / "in", run, log=_quiet)
    assert rep["steps"]["mpc"]["status"] == "failed" and rep["tables"] == {}
    assert os.listdir(run / "delivery") == []


def test_check_real_delivery_check_fails(tmp_path, monkeypatch):
    """The check step runs ssp.delivery_check: an empty delivery fails
    every table."""
    monkeypatch.setattr(B, "have_sssource_validate", lambda: False)
    (tmp_path / "delivery").mkdir()
    assert B.step_check(tmp_path, log=_quiet) is False
    res = json.loads((tmp_path / "checks" / "results.json").read_text())
    assert set(res) == set(B.EXPECTED_CHECKS)
    assert all(v["status"] == "FAIL" for v in res.values())
    assert "run from a source checkout" in (tmp_path / "checks" / "sssource-offsets.txt").read_text()


def test_check_without_bench_fails(tmp_path, monkeypatch):
    """Without bench/, the SSSource checks FAIL as not available, so the
    delivery is not deliverable even if the delivery check passes."""
    from collections import namedtuple
    R = namedtuple("CheckResult", "name ok detail")
    monkeypatch.setattr(B, "check_delivery",
                        lambda d, **k: {t: [R("x", True, "ok")] for t in DELIVERY_TABLES})
    monkeypatch.setattr(B, "have_sssource_validate", lambda: False)
    (tmp_path / "delivery").mkdir()
    assert B.step_check(tmp_path, log=_quiet) is False
    res = json.loads((tmp_path / "checks" / "results.json").read_text())
    assert set(res) == set(B.EXPECTED_CHECKS)
    failed = [k for k, v in res.items() if v["status"] == "FAIL"]
    assert failed == ["sssource:conformance", "sssource:offsets"]


def test_check_delivery_results(tmp_path, monkeypatch):
    """The per-table results of check_delivery, as the contract defines it."""
    from collections import namedtuple
    R = namedtuple("CheckResult", "name ok detail")

    def check_delivery(delivery_dir, schema_dir=None, tables=DELIVERY_TABLES):
        return {t: [R("columns", True, "ok"), R("pk", t != "SSObject", "dup")] for t in tables}

    monkeypatch.setattr(B, "check_delivery", check_delivery)
    monkeypatch.setattr(B, "have_sssource_validate", lambda: False)
    (tmp_path / "delivery").mkdir()
    assert B.step_check(tmp_path, log=_quiet) is False
    res = json.loads((tmp_path / "checks" / "results.json").read_text())
    assert set(res) == set(B.EXPECTED_CHECKS)
    assert [t for t in DELIVERY_TABLES if res[f"delivery:{t}"]["status"] == "FAIL"] == ["SSObject"]
    assert "FAIL  pk            dup" in (tmp_path / "checks" / "delivery-SSObject.txt").read_text()


def test_from_refuses_missing_kept_table(tmp_path, fake_steps):
    """(M9)"""
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    (run / "delivery" / "SSSource.parquet").unlink()
    with pytest.raises(ValueError, match="SSSource, from step sssource, is missing"):
        B.build(tmp_path / "in", run, from_step="ssobject", log=_quiet)


def test_from_refuses_changed_kept_table(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    pq.write_table(pa.table({"x": [9, 9, 9, 9]}), run / "delivery" / "SSSource.parquet")
    with pytest.raises(ValueError, match="SSSource.parquet has changed since step sssource"):
        B.build(tmp_path / "in", run, from_step="check", log=_quiet)


def test_from_mixed_commits(tmp_path, fake_steps, monkeypatch):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    monkeypatch.setattr(B, "ssp_tools_commit", lambda: "OLD")
    B.build(tmp_path / "in", run, log=_quiet)
    monkeypatch.setattr(B, "ssp_tools_commit", lambda: "NEW")
    with pytest.raises(ValueError, match="built by other code"):
        B.build(tmp_path / "in", run, from_step="nearbysso", log=_quiet)
    rep = B.build(tmp_path / "in", run, from_step="nearbysso", allow_mixed_commits=True, log=_quiet)
    assert rep["deliverable"] is True and rep["ssp_tools_commit"] == "NEW"
    assert rep["mixed_commits"] == dict(mpc="OLD", sssource="OLD", ssobject="OLD", nearbysso="NEW",
                                        check="NEW")
    assert rep["steps"]["sssource"]["ssp_tools_commit"] == "OLD"
    assert rep["steps"]["check"]["ssp_tools_commit"] == "NEW"


def test_inputs_changed_during_build(tmp_path, fake_steps, monkeypatch):
    """An input replaced after validation fails the check step."""
    write_inputs(tmp_path / "in")
    write_inputs(tmp_path / "other", n=7)
    inner = B.step_command

    def sc(step, *a, **k):
        if step == "sssource":
            shutil.copy(tmp_path / "other" / "dia_sources.parquet", tmp_path / "in" / "dia_sources.parquet")
        return inner(step, *a, **k)

    monkeypatch.setattr(B, "step_command", sc)
    rep = B.build(tmp_path / "in", tmp_path / "run", log=_quiet)
    assert rep["steps"]["nearbysso"]["status"] == "ok"
    assert rep["steps"]["check"]["status"] == "failed"
    assert "inputs changed during the build: ['dia_sources']" in rep["steps"]["check"]["error"]
    assert rep["checks"] == {} and rep["deliverable"] is False


def _uploaded(run, dry_run=False):
    r = json.loads((run / "report.json").read_text())
    r["upload"] = {"dry_run": dry_run, "object_prefix": "20261001T000000000", "message_id": "1"}
    r["uploads"] = [dict(r["upload"])]
    (run / "report.json").write_text(json.dumps(r))
    return r


@pytest.mark.parametrize("from_step", [None, "check"])
def test_uploaded_run_dir_refused(tmp_path, fake_steps, from_step):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    before = _uploaded(run)
    with pytest.raises(ValueError, match="was uploaded"):
        B.build(tmp_path / "in", run, from_step=from_step, log=_quiet)
    assert json.loads((run / "report.json").read_text()) == before
    assert B.main([str(tmp_path / "in"), str(run)]) == 2
    rep = B.build(tmp_path / "in", run, from_step=from_step, force_rebuild=True, log=_quiet)
    assert rep["deliverable"] is True
    assert rep["upload"] == before["upload"] and rep["uploads"] == before["uploads"]


def test_dry_run_upload_not_refused(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    B.build(tmp_path / "in", run, log=_quiet)
    before = _uploaded(run, dry_run=True)
    rep = B.build(tmp_path / "in", run, log=_quiet)
    assert rep["deliverable"] is True and rep["uploads"] == before["uploads"]


def test_stray_delivery_files_removed(tmp_path, fake_steps):
    write_inputs(tmp_path / "in")
    run = tmp_path / "run"
    (run / "delivery" / "junk").mkdir(parents=True)
    (run / "delivery" / "Extra.parquet").write_text("x")
    msgs = []
    rep = B.build(tmp_path / "in", run, log=msgs.append)
    assert rep["deliverable"] is True
    assert sorted(os.listdir(run / "delivery")) == sorted(f"{t}.parquet" for t in DELIVERY_TABLES)
    assert sum("WARNING: removing" in m for m in msgs) == 2


@pytest.mark.parametrize("edit, match", [
    (lambda m: m["files"]["obs_sbn"].update(rows="4"), "rows '4' is not a row count"),
    (lambda m: m["files"]["obs_sbn"].update(md5=None), "md5 None is not an md5"),
    (lambda m: m["files"]["obs_sbn"].update(file=7), "file 7 is not a path"),
    (lambda m: m["files"]["obs_sbn"].update(file="."), "is not a readable Parquet file|does not exist"),
])
def test_malformed_manifest_replaces_stale_report(tmp_path, edit, match):
    write_inputs(tmp_path / "in")
    _edit_manifest(tmp_path / "in", edit)
    (tmp_path / "run").mkdir()
    (tmp_path / "run" / "report.json").write_text(json.dumps({"deliverable": True, "old": 1}))
    assert B.main([str(tmp_path / "in"), str(tmp_path / "run")]) == 1
    rep = json.loads((tmp_path / "run" / "report.json").read_text())
    assert rep["deliverable"] is False and "old" not in rep
    assert re.search(match, rep["error"])


def test_hashing_oserror_is_a_manifest_error(tmp_path, monkeypatch):
    write_inputs(tmp_path / "in")

    def boom(path, blocksize=0):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(B, "_md5", boom)
    rep = B.build(tmp_path / "in", tmp_path / "run", log=_quiet)
    assert rep["deliverable"] is False and "PermissionError" in rep["error"]


@pytest.mark.parametrize("edit, match", [
    (lambda m: m["files"]["mpc_orbits"].update(extracted_utc="2020-01-01T00:00:00Z"),
     "mpc_orbits: extracted_utc"),
    (lambda m: m.update(mpc_snapshot_utc="2020-01-01T00:00:00Z"), "must come from one snapshot"),
    (lambda m: m.update(mpc_snapshot_utc="yesterday"), "not an ISO 8601 time"),
])
def test_manifest_one_mpc_snapshot(tmp_path, edit, match):
    write_inputs(tmp_path)
    _edit_manifest(tmp_path, edit)
    with pytest.raises(B.ManifestError, match=match):
        B.validate_manifest(tmp_path)


def test_manifest_snapshot_z_and_offset_equal(tmp_path):
    m = write_inputs(tmp_path)
    t = B._parse_utc(m["mpc_snapshot_utc"])
    _edit_manifest(tmp_path, lambda m: m.update(mpc_snapshot_utc=t.strftime("%Y-%m-%dT%H:%M:%SZ")))
    B.validate_manifest(tmp_path)


def test_recorded_md5_rules():
    a, b, c = "a" * 32, "b" * 32, "c" * 32
    e = {"source": f"rebuilt; old_obs_sbn (md5 {a}) replaced by obs_sbn (md5 {b})"}
    assert B.recorded_parent_md5(e, "obs_sbn") == b
    assert B.recorded_parent_md5({"obs_sbn_md5": "", "source": f"x obs_sbn (md5 {c})"}, "obs_sbn") == ""


def test_manifest_empty_obs_sbn_md5_refused(tmp_path):
    m = write_inputs(tmp_path)
    md5 = m["files"]["obs_sbn"]["md5"]
    _edit_manifest(tmp_path, lambda m: m["files"]["dia_sources"].update(
        obs_sbn_md5="", source=f"on obs_sbn (md5 {md5})"))
    with pytest.raises(B.ManifestError, match="out of step"):
        B.validate_manifest(tmp_path)


def test_json_nan_rejected():
    assert B._check_json(['{"a": 1}', None, '{"a": NaN}'], 10) == (12, "invalid JSON: NaN is not JSON")
    assert B._check_json(['{"a": -Infinity}'], 0)[1] == "invalid JSON: -Infinity is not JSON"


# ---------------------------------------------------------------------------
# End to end on the subset fixture (USDF), with ASSIST
# ---------------------------------------------------------------------------

FIXTURE = Path("/sdf/data/rubin/user/mjuric/sssource-widened/fixtures/2026-09-30/subset/in")
HAVE_ASSIST = bool(os.environ.get("SSP_ASSIST_PLANETS") and os.environ.get("SSP_ASSIST_ASTEROIDS"))
needs_fixture = pytest.mark.skipif(not (FIXTURE.is_dir() and HAVE_ASSIST),
                                   reason="needs the 2026-09-30 subset fixture (USDF) and ASSIST")
E2E_WORKERS = 4


def subset_tables():
    """The subset fixture's inputs, with the MPC tables cut to its objects,
    and a ppdb_dia_sources of its DiaSource rows."""
    obs = pq.read_table(FIXTURE / "obs_sbn.parquet")
    dia = pq.read_table(FIXTURE / "dia_sources.parquet")
    prov = set(pc.unique(obs["provid"].drop_null()).to_pylist())
    num = pq.read_table(FIXTURE / "numbered_identifications.parquet")
    prov |= set(num.filter(pc.is_in(num["permid"], value_set=pc.unique(obs["permid"].drop_null())))[
        "unpacked_primary_provisional_designation"].to_pylist())
    cur = pq.read_table(FIXTURE / "current_identifications.parquet")
    keep = pc.unique(cur.filter(pc.is_in(cur["unpacked_secondary_provisional_designation"],
                                         value_set=pa.array(sorted(prov))))[
        "unpacked_primary_provisional_designation"])

    def cut(t):
        return t.filter(pc.is_in(t["unpacked_primary_provisional_designation"], value_set=keep))

    pf = pq.ParquetFile(FIXTURE / "mpc_orbits.parquet")
    orb = pa.Table.from_batches([cut(b) for b in pf.iter_batches(batch_size=200_000)], schema=pf.schema_arrow)
    ppdb = dia.filter(pc.equal(dia["measuredOn"], "difference")).select(
        ["diaSourceId", "visit", "midpointMjdTai", "ra", "dec"])
    return dict(obs_sbn=obs, dia_sources=dia, mpc_orbits=orb, current_identifications=cut(cur),
                numbered_identifications=cut(num), ppdb_dia_sources=ppdb)


@pytest.fixture(scope="module")
def e2e(tmp_path_factory):
    d = tmp_path_factory.mktemp("sso")
    tables = subset_tables()
    (d / "in").mkdir()
    for name, t in tables.items():
        pq.write_table(t, d / "in" / INPUT_FILES[name][0])
    B.write_manifest(d / "in", {n: INPUT_FILES[n][0] for n in INPUT_FILES}, producer="test subset")
    rep = B.build(d / "in", d / "run", workers=E2E_WORKERS)
    return d, tables, rep


def _expect_checks(rep):
    """The check step's results: every check passes."""
    ch = rep["checks"]
    assert ch["sssource:conformance"]["status"] == "PASS"
    assert ch["sssource:offsets"]["status"] == "PASS"
    assert all(ch[f"delivery:{t}"]["status"] == "PASS" for t in DELIVERY_TABLES), ch
    assert rep["steps"]["check"]["status"] == "ok" and rep["deliverable"] is True


@needs_fixture
def test_e2e_subset(e2e):
    d, tables, rep = e2e
    for s in BUILD_STEPS[:-1]:
        assert rep["steps"][s]["status"] == "ok", (s, rep["steps"][s])
    _expect_checks(rep)
    assert set(rep["tables"]) == set(DELIVERY_TABLES)
    assert rep["tables"]["SSSource"]["rows"] == tables["dia_sources"].num_rows
    assert rep["tables"]["mpc_orbits"]["rows"] == tables["mpc_orbits"].num_rows
    assert rep["tables"]["NearbySSO"]["rows"] > 0
    for t in DELIVERY_TABLES:
        s = pq.read_schema(d / "run" / "delivery" / f"{t}.parquet")
        assert s.names == [c["name"] for c in SCHEMA[t]], t
    orb = pq.read_table(d / "run" / "delivery" / "mpc_orbits.parquet")
    assert orb["designation"].equals(orb["unpacked_primary_provisional_designation"])
    # NearbySSO's ssObjectId is SSObject's, by designation (M13: --ssobject)
    sso = pq.read_table(d / "run" / "delivery" / "SSObject.parquet", columns=["designation", "ssObjectId"])
    ids = dict(zip(sso["designation"].to_pylist(), sso["ssObjectId"].to_pylist()))
    nss = pq.read_table(d / "run" / "delivery" / "NearbySSO.parquet", columns=["designation", "ssObjectId"])
    pairs = list(zip(nss["designation"].to_pylist(), nss["ssObjectId"].to_pylist()))
    assert sum(des in ids for des, _ in pairs) > 0
    for des, sid in pairs:
        assert sid == ids.get(des), (des, sid)
    assert (d / "run" / "work" / "sssource" / "in" / "obs_sbn.parquet").is_symlink()


@needs_fixture
def test_e2e_from_check(e2e):
    d, _, rep = e2e
    sss = d / "run" / "delivery" / "SSSource.parquet"
    mtime = sss.stat().st_mtime_ns
    rep2 = B.build(d / "in", d / "run", from_step="check", workers=E2E_WORKERS)
    for s in BUILD_STEPS[:-1]:
        assert rep2["steps"][s] == rep["steps"][s]
    assert rep2["tables"] == rep["tables"] and sss.stat().st_mtime_ns == mtime
    _expect_checks(rep2)


def _reverse(t, extra=True):
    """Columns in reverse order, plus an extra column."""
    t = t.select(t.schema.names[::-1])
    if extra:
        t = t.append_column("not_in_the_contract", pa.array(np.arange(t.num_rows, dtype=np.float64)))
    return t


def _restyle_mpc(t):
    """The MPC table as another source might write it: 64-bit integers,
    large strings, UTC timestamps."""
    cols = {}
    for f in t.schema:
        a = t[f.name]
        if pa.types.is_int32(f.type):
            a = a.cast(pa.int64())
        elif pa.types.is_string(f.type):
            a = a.cast(pa.large_string())
        elif pa.types.is_timestamp(f.type):
            a = a.cast(pa.timestamp("us", "UTC"))
        cols[f.name] = a
    return _reverse(pa.table(cols))


@needs_fixture
def test_e2e_non_clickhouse_source(e2e, tmp_path):
    """Inputs from another source: other file names and directories, other
    column orders and types, extra columns, rows in another order. Only the
    contract matters: the delivered tables are the same."""
    d, tables, rep = e2e
    src = tmp_path / "in"
    files = dict(obs_sbn="mpc/rubin_observations.parquet", mpc_orbits="mpc/orbits-snapshot.parquet",
                 current_identifications="mpc/ids_current.parquet",
                 numbered_identifications="mpc/ids_numbered.parquet",
                 dia_sources="measurements/submitted.parquet", ppdb_dia_sources="ppdb/DiaSource-5col.parquet")
    for name, f in files.items():
        (src / f).parent.mkdir(parents=True, exist_ok=True)
        t = tables[name]
        t = _restyle_mpc(t) if name in B.MPC_TABLES else _reverse(t, extra=name != "dia_sources")
        if name in ("ppdb_dia_sources", "obs_sbn"):
            t = t.take(np.random.default_rng(1).permutation(t.num_rows))
        pq.write_table(t, src / f, row_group_size=1000)
    B.write_manifest(src, files, producer="a hand-made source", ppdb_dia_sources_source="a CSV dump")
    rep2 = B.build(src, tmp_path / "run", workers=E2E_WORKERS)
    for s in BUILD_STEPS[:-1]:
        assert rep2["steps"][s]["status"] == "ok", (s, rep2["steps"][s])
    _expect_checks(rep2)
    for t in DELIVERY_TABLES:
        a = pq.read_table(d / "run" / "delivery" / f"{t}.parquet")
        b = pq.read_table(tmp_path / "run" / "delivery" / f"{t}.parquet")
        assert a.schema == b.schema, t
        if t == "NearbySSO":           # (its row order follows the DiaSources')
            a, b = (x.sort_by("diaSourceId") for x in (a, b))
        assert a.to_pandas().equals(b.to_pandas()), t     # (NaN == NaN)
