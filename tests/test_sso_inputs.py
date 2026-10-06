"""Tests for ssp.sso_inputs (stage 1, extract), offline: the three sources
(MPC replica, extract-submitted-sources, ppdb.DiaSource) are stubbed."""

import collections
import datetime
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import sso_inputs as M
from ssp.delivery_contract import INPUT_FILES, MANIFEST_FIELDS, MANIFEST_FILE, MPC_SNAPSHOT, \
    REQUIRED_INPUT_COLUMNS

SNAP = datetime.datetime(2026, 10, 1, 6, 25, 3, 123456, tzinfo=datetime.timezone.utc)


def table(name, n, extra=True, drop=()):
    cols = {c: pa.array([f"{c}{i}" for i in range(n)]) for c in REQUIRED_INPUT_COLUMNS[name]
            if c not in drop}
    if extra:
        cols["someExtraColumn"] = pa.array(range(n))
    return pa.table(cols)


ROWS = {"obs_sbn": 5, "mpc_orbits": 4, "current_identifications": 3, "numbered_identifications": 2,
        "dia_sources": 5, "ppdb_dia_sources": 7}


EXPORT_MPC = M.export_mpc


@pytest.fixture
def stubs(monkeypatch):
    """Stub the sources; returns a dict of call counts and controls."""
    calls = {"mpc": 0, "dia_sources": 0, "ppdb_dia_sources": 0, "fail": None, "drop": {},
             "rows": collections.ChainMap({}, ROWS)}   # overrides over ROWS

    def export_mpc(tmp, args):
        calls["mpc"] += 1
        if calls["fail"] == "mpc":
            pq.write_table(table("obs_sbn", 1), tmp / "obs_sbn.parquet")   # a partial export
            raise RuntimeError("connection lost")
        for name in MPC_SNAPSHOT:
            pq.write_table(table(name, calls["rows"][name], drop=calls["drop"].get(name, ())),
                           tmp / INPUT_FILES[name][0])
        return SNAP

    def extract_dia_sources(obs_path, out_path, args):
        calls["dia_sources"] += 1
        calls["obs_rows"] = pq.ParquetFile(obs_path).metadata.num_rows
        calls["workers"] = args.workers
        if calls["fail"] == "dia_sources":
            raise RuntimeError("ClickHouse timeout")
        n = calls["obs_rows"]
        pq.write_table(table("dia_sources", n, drop=calls["drop"].get("dia_sources", ())), out_path)
        pq.write_table(pa.table({"obsid": pa.array([], pa.string())}),
                       str(out_path)[:-8] + ".unresolved.parquet")
        calls["correction_table"] = args.correction_table
        return calls.get("shutter")

    def export_ppdb(out_path, args):
        calls["ppdb_dia_sources"] += 1
        if calls["fail"] == "ppdb_dia_sources":
            raise RuntimeError("ClickHouse down")
        pq.write_table(table("ppdb_dia_sources", ROWS["ppdb_dia_sources"]), out_path)

    monkeypatch.setattr(M, "export_mpc", export_mpc)
    monkeypatch.setattr(M, "extract_dia_sources", extract_dia_sources)
    monkeypatch.setattr(M, "export_ppdb", export_ppdb)
    return calls


def run(tmp_path, *argv, d="inputs"):
    return M.run(M.build_parser().parse_args([str(tmp_path / d), *argv]))


def manifest(tmp_path, d="inputs"):
    return json.loads((tmp_path / d / MANIFEST_FILE).read_text())


def test_full_run_manifest(tmp_path, stubs):
    m = run(tmp_path)
    assert m == manifest(tmp_path)
    assert list(m) == list(MANIFEST_FIELDS) + ["shutter_timing"]
    assert m["shutter_timing"] is None     # the stub extracts without a correction table
    assert m["mpc_snapshot_utc"] == "2026-10-01T06:25:03Z"
    assert m["producer"].startswith("ssp-extract-sso-inputs ")
    assert "(" in m["producer"] and m["producer"].endswith(")")
    datetime.datetime.strptime(m["created_utc"], "%Y-%m-%dT%H:%M:%SZ")
    assert list(m["files"]) == list(INPUT_FILES)
    out = tmp_path / "inputs"
    for name, (fname, _) in INPUT_FILES.items():
        e = m["files"][name]
        assert set(e) == {"file", "rows", "md5", "source", "extracted_utc"} | (
            {"obs_sbn_md5"} if name == "dia_sources" else set())
        assert e["file"] == fname
        assert e["rows"] == ROWS[name]
        assert e["md5"] == M.md5sum(out / fname)
        datetime.datetime.strptime(e["extracted_utc"], "%Y-%m-%dT%H:%M:%SZ")
        assert set(REQUIRED_INPUT_COLUMNS[name]) <= set(pq.read_schema(out / fname).names)
    for name in MPC_SNAPSHOT:
        assert m["files"][name]["extracted_utc"] == m["mpc_snapshot_utc"]
        assert "mpcorb-db.slac.stanford.edu" in m["files"][name]["source"]
    assert "WHERE stn='X05'" in m["files"]["obs_sbn"]["source"]
    assert m["files"]["obs_sbn"]["md5"] in m["files"]["dia_sources"]["source"]
    assert m["files"]["dia_sources"]["obs_sbn_md5"] == m["files"]["obs_sbn"]["md5"]
    assert "ppdb.DiaSource" in m["files"]["ppdb_dia_sources"]["source"]
    assert stubs["workers"] == 8
    # the side file is kept; nothing partial is left behind
    assert (out / "dia_sources.unresolved.parquet").exists()
    assert not (out / M.PARTIAL).exists()


def test_existing_manifest_needs_force(tmp_path, stubs, capsys):
    run(tmp_path)
    with pytest.raises(M.ExtractError, match="--force"):
        run(tmp_path)
    with pytest.raises(SystemExit) as e:
        M.main([str(tmp_path / "inputs")])
    assert e.value.code == 1
    assert "--force" in capsys.readouterr().err
    run(tmp_path, "--force")
    assert stubs["mpc"] == 2


@pytest.mark.parametrize("step", ["mpc", "dia_sources", "ppdb_dia_sources"])
def test_failure_writes_no_manifest(tmp_path, stubs, step):
    stubs["fail"] = step
    with pytest.raises(RuntimeError):
        run(tmp_path)
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


def assert_valid(tmp_path, m, d="inputs"):
    """INPUTS_DIR's files match manifest ``m``, which is its manifest."""
    assert manifest(tmp_path, d) == m
    for e in m["files"].values():
        assert M.md5sum(tmp_path / d / e["file"]) == e["md5"]


@pytest.mark.parametrize("step", ["mpc", "dia_sources", "ppdb_dia_sources"])
def test_failed_force_rerun_keeps_old_inputs(tmp_path, stubs, step):
    first = run(tmp_path)
    stubs["fail"] = step
    stubs["rows"]["obs_sbn"] = 6      # so a new obs_sbn would differ
    with pytest.raises(RuntimeError):
        run(tmp_path, "--force")
    assert_valid(tmp_path, first)
    # ... and can be redone
    stubs["fail"] = None
    m = run(tmp_path, "--force", "--only", step)
    assert_valid(tmp_path, m)


def test_failed_mpc_leaves_no_partial_file_in_place(tmp_path, stubs):
    stubs["fail"] = "mpc"
    with pytest.raises(RuntimeError):
        run(tmp_path)
    assert not (tmp_path / "inputs" / "obs_sbn.parquet").exists()


def test_missing_required_column_fails(tmp_path, stubs):
    stubs["drop"] = {"mpc_orbits": ["mpc_orb_jsonb"]}
    with pytest.raises(M.ExtractError, match="mpc_orb_jsonb"):
        run(tmp_path)
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


def test_reuse_dia_sources(tmp_path, stubs):
    """A fixture directory without a manifest: obs_sbn and dia_sources
    reused together from it are taken to belong together."""
    fx = tmp_path / "fixture"
    fx.mkdir()
    for n in MPC_SNAPSHOT:
        pq.write_table(table(n, ROWS[n]), fx / INPUT_FILES[n][0])
    src = fx / "dia_sources.parquet"
    pq.write_table(table("dia_sources", 11), src)
    reuse = [a for n in MPC_SNAPSHOT + ("dia_sources",)
             for a in ("--reuse", f"{n}={fx / INPUT_FILES[n][0]}")]
    m = run(tmp_path, *reuse, "--mpc-snapshot-utc", "2026-10-01T06:25:00Z")
    assert (stubs["dia_sources"], stubs["mpc"], stubs["ppdb_dia_sources"]) == (0, 0, 1)
    e = m["files"]["dia_sources"]
    assert e["rows"] == 11
    assert e["source"].startswith(f"reused {src}")
    assert e["obs_sbn_md5"] == m["files"]["obs_sbn"]["md5"]
    assert e["md5"] == M.md5sum(src) == M.md5sum(tmp_path / "inputs" / "dia_sources.parquet")


def test_reuse_mpc_takes_snapshot_from_manifest(tmp_path, stubs):
    run(tmp_path, d="old")
    old = manifest(tmp_path, "old")
    reuse = [a for n in MPC_SNAPSHOT for a in ("--reuse", f"{n}={tmp_path / 'old' / INPUT_FILES[n][0]}")]
    m = run(tmp_path, *reuse)
    assert stubs["mpc"] == 1          # only the first run
    assert stubs["dia_sources"] == 2  # rebuilt from the reused obs_sbn
    assert m["mpc_snapshot_utc"] == old["mpc_snapshot_utc"]
    for n in MPC_SNAPSHOT:
        assert m["files"][n]["md5"] == old["files"][n]["md5"]
        assert m["files"][n]["extracted_utc"] == old["files"][n]["extracted_utc"]
        assert "reused" in m["files"][n]["source"] and "originally" in m["files"][n]["source"]


def test_reuse_mpc_without_manifest_needs_snapshot(tmp_path, stubs):
    src = tmp_path / "fixture"
    src.mkdir()
    for n in MPC_SNAPSHOT:
        pq.write_table(table(n, ROWS[n]), src / INPUT_FILES[n][0])
    reuse = [a for n in MPC_SNAPSHOT for a in ("--reuse", f"{n}={src / INPUT_FILES[n][0]}")]
    with pytest.raises(M.ExtractError, match="--mpc-snapshot-utc"):
        run(tmp_path, *reuse)
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()
    m = run(tmp_path, *reuse, "--mpc-snapshot-utc", "2026-10-01T06:25:00Z")
    assert m["mpc_snapshot_utc"] == "2026-10-01T06:25:00Z"
    assert stubs["mpc"] == 0


def test_reuse_partial_mpc_snapshot_refused(tmp_path, stubs):
    src = tmp_path / "x.parquet"
    pq.write_table(table("mpc_orbits", 3), src)
    with pytest.raises(M.ExtractError, match="one snapshot"):
        run(tmp_path, "--reuse", f"mpc_orbits={src}")


def test_reuse_bad_args(tmp_path, stubs):
    with pytest.raises(M.ExtractError, match="NAME=PATH"):
        run(tmp_path, "--reuse", "nonsense")
    with pytest.raises(M.ExtractError, match="no such file"):
        run(tmp_path, "--reuse", f"dia_sources={tmp_path / 'missing.parquet'}")


def test_reuse_missing_column_fails(tmp_path, stubs):
    src = tmp_path / "bad.parquet"
    pq.write_table(table("dia_sources", 3, drop=["measuredOn"]), src)
    with pytest.raises(M.ExtractError, match="measuredOn"):
        run(tmp_path, "--reuse", f"dia_sources={src}")
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


def test_reuse_own_path(tmp_path, stubs):
    first = run(tmp_path)
    p = tmp_path / "inputs" / "ppdb_dia_sources.parquet"
    m = run(tmp_path, "--force", "--reuse", f"ppdb_dia_sources={p}")
    assert stubs["ppdb_dia_sources"] == 1
    assert m["files"]["ppdb_dia_sources"]["md5"] == first["files"]["ppdb_dia_sources"]["md5"]
    assert "originally" in m["files"]["ppdb_dia_sources"]["source"]
    assert_valid(tmp_path, m)


def test_only_mpc_also_runs_dia_sources(tmp_path, stubs):
    run(tmp_path)
    stubs["rows"]["obs_sbn"] = 6      # the MPC moved on
    m = run(tmp_path, "--force", "--only", "mpc")
    assert (stubs["mpc"], stubs["dia_sources"], stubs["ppdb_dia_sources"]) == (2, 2, 1)
    assert m["files"]["dia_sources"]["rows"] == 6
    assert m["files"]["dia_sources"]["obs_sbn_md5"] == m["files"]["obs_sbn"]["md5"]
    assert_valid(tmp_path, m)


def test_skip_dia_sources_with_new_obs_refused(tmp_path, stubs):
    first = run(tmp_path)
    with pytest.raises(M.ExtractError, match="new obs_sbn"):
        run(tmp_path, "--force", "--skip", "dia_sources")
    assert stubs["mpc"] == 1
    assert_valid(tmp_path, first)


def test_reuse_dia_sources_with_fresh_mpc(tmp_path, stubs):
    # no record of the obs_sbn it was built from: refused before anything runs
    src = tmp_path / "my_dia.parquet"
    pq.write_table(table("dia_sources", 11), src)
    with pytest.raises(M.ExtractError, match="which obs_sbn"):
        run(tmp_path, "--reuse", f"dia_sources={src}")
    assert stubs["mpc"] == 0
    # from a run whose obs_sbn the fresh export reproduces: accepted
    old = run(tmp_path, d="old")
    old_dia = tmp_path / "old" / "dia_sources.parquet"
    m = run(tmp_path, "--reuse", f"dia_sources={old_dia}")
    assert m["files"]["dia_sources"]["obs_sbn_md5"] == old["files"]["obs_sbn"]["md5"]
    assert stubs["dia_sources"] == 1
    # ... but not once the MPC has moved on
    stubs["rows"]["obs_sbn"] = 6
    with pytest.raises(M.ExtractError, match="built from obs_sbn md5"):
        run(tmp_path, "--force", "--reuse", f"dia_sources={old_dia}")
    assert_valid(tmp_path, m)


def test_carried_dia_sources_must_match_obs(tmp_path, stubs):
    first = run(tmp_path)
    mf = tmp_path / "inputs" / MANIFEST_FILE
    bad = json.loads(mf.read_text())
    bad["files"]["dia_sources"]["obs_sbn_md5"] = "0" * 32
    mf.write_text(json.dumps(bad))
    with pytest.raises(M.ExtractError, match="built from obs_sbn md5"):
        run(tmp_path, "--force", "--only", "ppdb_dia_sources")
    assert stubs["ppdb_dia_sources"] == 1
    # an older manifest without the field: the md5 in its source is used
    del bad["files"]["dia_sources"]["obs_sbn_md5"]
    mf.write_text(json.dumps(bad))
    m = run(tmp_path, "--force", "--only", "ppdb_dia_sources")
    assert m["files"]["dia_sources"]["obs_sbn_md5"] == first["files"]["obs_sbn"]["md5"]


def test_previous_manifest_kept_aside_and_used(tmp_path, stubs):
    first = run(tmp_path)
    m = run(tmp_path, "--force", "--only", "ppdb_dia_sources")
    prev = tmp_path / "inputs" / M.PREVIOUS_MANIFEST
    assert json.loads(prev.read_text()) == first
    # without manifest.json (a crash while committing), the carry-overs
    # come from the previous manifest
    (tmp_path / "inputs" / MANIFEST_FILE).unlink()
    m2 = run(tmp_path, "--only", "ppdb_dia_sources")
    for n in MPC_SNAPSHOT + ("dia_sources",):
        assert m2["files"][n] == m["files"][n]


def test_mpc_snapshot_utc_validated(tmp_path, stubs):
    with pytest.raises(M.ExtractError, match="ISO 8601"):
        run(tmp_path, "--mpc-snapshot-utc", "yesterday")
    with pytest.raises(M.ExtractError, match="time zone"):
        run(tmp_path, "--mpc-snapshot-utc", "2026-10-01T06:25:00")
    assert M.parse_utc("2026-10-01T08:25:00+02:00") == "2026-10-01T06:25:00Z"


def test_reused_snapshot_needs_matching_side_manifest(tmp_path, stubs):
    run(tmp_path, d="old")
    old = tmp_path / "old"
    pq.write_table(table("mpc_orbits", 9), old / "mpc_orbits.parquet")   # no longer what it describes
    reuse = [a for n in MPC_SNAPSHOT for a in ("--reuse", f"{n}={old / INPUT_FILES[n][0]}")]
    with pytest.raises(M.ExtractError, match="--mpc-snapshot-utc"):
        run(tmp_path, *reuse)


def test_lock(tmp_path, stubs):
    import fcntl
    d = tmp_path / "inputs"
    d.mkdir()
    with open(d / M.LOCK_FILE, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        with pytest.raises(M.ExtractError, match="in use"):
            run(tmp_path)
    run(tmp_path)


def test_export_mpc_one_transaction(tmp_path, stubs, monkeypatch):
    """export_mpc: one export_in_transaction call, with the four exports in
    MPC_SNAPSHOT order, and the CSVs in INPUTS_DIR/.partial."""
    from ssp.export import postgres as P

    seen = []

    def fake(dsn, exports, tmp_dir=None, **kw):
        seen.append((dsn, exports, tmp_dir))
        for e in exports:
            name = next(n for n, (f, _) in INPUT_FILES.items() if e["out"].endswith("/" + f))
            pq.write_table(table(name, ROWS[name]), e["out"])
        return SNAP

    monkeypatch.setattr(P, "export_in_transaction", fake)
    monkeypatch.setattr(M, "export_mpc", EXPORT_MPC)
    m = run(tmp_path)
    assert len(seen) == 1
    dsn, exports, tmp_dir = seen[0]
    assert "host=mpcorb-db.slac.stanford.edu" in dsn and "dbname=mpc_sbn" in dsn and "user=rubin" in dsn
    assert [e["sql"] for e in exports] == [
        "SELECT * FROM obs_sbn WHERE stn='X05'", "SELECT * FROM mpc_orbits",
        "SELECT * FROM current_identifications", "SELECT * FROM numbered_identifications"]
    assert [e["out"] for e in exports] == [str(tmp_path / "inputs" / M.PARTIAL / INPUT_FILES[n][0])
                                           for n in MPC_SNAPSHOT]
    assert tmp_dir == tmp_path / "inputs" / M.PARTIAL
    assert m["mpc_snapshot_utc"] == "2026-10-01T06:25:03Z"


def test_only_redoes_one_step(tmp_path, stubs):
    first = run(tmp_path)
    m = run(tmp_path, "--force", "--only", "ppdb_dia_sources")
    assert (stubs["mpc"], stubs["dia_sources"], stubs["ppdb_dia_sources"]) == (1, 1, 2)
    assert m["mpc_snapshot_utc"] == first["mpc_snapshot_utc"]
    for n in MPC_SNAPSHOT + ("dia_sources",):
        assert m["files"][n] == first["files"][n]


def test_skip(tmp_path, stubs):
    run(tmp_path)
    run(tmp_path, "--force", "--skip", "mpc")
    assert (stubs["mpc"], stubs["dia_sources"], stubs["ppdb_dia_sources"]) == (1, 2, 2)


def test_skip_without_previous_fails(tmp_path, stubs):
    with pytest.raises(M.ExtractError, match="no previous file"):
        run(tmp_path, "--skip", "ppdb_dia_sources")
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


def test_skip_detects_changed_file(tmp_path, stubs):
    run(tmp_path)
    pq.write_table(table("ppdb_dia_sources", 99), tmp_path / "inputs" / "ppdb_dia_sources.parquet")
    with pytest.raises(M.ExtractError, match="no longer matches"):
        run(tmp_path, "--force", "--skip", "ppdb_dia_sources")


def test_workers_capped(tmp_path, stubs):
    with pytest.raises(SystemExit):
        M.main([str(tmp_path / "inputs"), "--workers", "9"])


class FakeCursor:
    """Enough of a psycopg2 cursor: one int column ``x``; COPY writes
    ``csv`` (then raises, if ``fail``)."""

    def __init__(self, log, csv="1\n2\n", fail=False):
        self.log, self.csv, self.fail = log, csv, fail

    def execute(self, sql):
        self.log.append(("execute", sql))
        self.description = [type("D", (), {"name": "x", "type_code": 23})()]

    def fetchone(self):
        return (SNAP,)

    def copy_expert(self, sql, f):
        self.log.append(("copy", f.name))
        f.write(self.csv.encode())
        f.flush()
        if self.fail:
            raise RuntimeError("connection lost")

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_export_in_transaction(tmp_path, monkeypatch):
    """One REPEATABLE READ, READ ONLY transaction; its first statement's
    now() is the snapshot time; every export runs on the same cursor, with
    its CSV in tmp_dir; then commit and close."""
    from ssp.export import postgres as P

    log = []
    cur = FakeCursor(log)

    class Conn:
        def set_session(self, **kw):
            log.append(("session", kw))

        def cursor(self):
            return cur

        def commit(self):
            log.append(("commit",))

        def close(self):
            log.append(("close",))

    monkeypatch.setattr(P.psycopg2, "connect", lambda dsn: (log.append(("connect", dsn)), Conn())[1])
    monkeypatch.setattr(P, "export_query_to_parquet",
                        lambda cur, sql, parquet_out, tmp_dir=None, **kw:
                        log.append(("export", id(cur), sql, tmp_dir)))
    exports = [{"sql": "SELECT 1", "out": "a"}, {"sql": "SELECT 2", "out": "b"}]
    assert P.export_in_transaction("dsn", exports, tmp_dir="T", log=lambda m: None) == SNAP
    rr = P.psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ
    assert log == [("connect", "dsn"),
                   ("session", {"isolation_level": rr, "readonly": True}),
                   ("execute", "SELECT now()"),
                   ("export", id(cur), "SELECT 1", "T"), ("export", id(cur), "SELECT 2", "T"),
                   ("commit",), ("close",)]


def test_export_query_tmp_csv(tmp_path):
    """The CSV goes to tmp_dir and is removed, also when the COPY fails."""
    from ssp.export import postgres as P

    tmpd = tmp_path / "tmp"
    tmpd.mkdir()
    log = []
    P.export_query_to_parquet(FakeCursor(log), "SELECT x", str(tmp_path / "x.parquet"), tmp_dir=tmpd)
    assert pq.read_table(tmp_path / "x.parquet")["x"].to_pylist() == [1, 2]
    assert log[-1][1].startswith(str(tmpd)) and not list(tmpd.iterdir())
    with pytest.raises(RuntimeError):
        P.export_query_to_parquet(FakeCursor(log, fail=True), "SELECT x", str(tmp_path / "y.parquet"),
                                  tmp_dir=tmpd)
    assert not list(tmpd.iterdir())


#
# The shutter-motion correction's manifest entry (WP S1)
#

SHUTTER = {"table_dir": "/t", "table_format": 1, "calibration_id": "c", "package_version": "v",
           "obstime_basis": {"visit": 4, "corrected": 1, "both": 0},
           "status": {"ok": 2, "degraded": 2, "omitted": 0, "not_built": 1}, "not_built_visits": [7]}


def test_shutter_entry_written_and_carried(tmp_path, stubs):
    stubs["shutter"] = SHUTTER
    m = run(tmp_path, "--correction-table", "/t")
    assert stubs["correction_table"] == "/t"
    assert m["shutter_timing"] == SHUTTER and manifest(tmp_path)["shutter_timing"] == SHUTTER
    assert "correction table /t" in m["files"]["dia_sources"]["source"]
    # dia_sources not rerun: its entry is carried over with it
    stubs["shutter"] = None
    m = run(tmp_path, "--force", "--only", "ppdb_dia_sources")
    assert stubs["dia_sources"] == 1 and m["shutter_timing"] == SHUTTER
    # rerun: replaced
    m = run(tmp_path, "--force", "--only", "dia_sources")
    assert stubs["dia_sources"] == 2 and m["shutter_timing"] is None


def test_shutter_entry_from_reused_dia_sources(tmp_path, stubs):
    stubs["shutter"] = SHUTTER
    run(tmp_path, d="old")
    stubs["shutter"] = None
    old = tmp_path / "old"
    reuse = [a for n in MPC_SNAPSHOT + ("dia_sources",)
             for a in ("--reuse", f"{n}={old / INPUT_FILES[n][0]}")]
    m = run(tmp_path, *reuse)
    assert stubs["dia_sources"] == 1 and m["shutter_timing"] == SHUTTER


def test_correction_defaults():
    from ssp.export.submittable import DEFAULT_CORRECTION_TABLE
    from ssp.sssource_contract import MAX_NOT_BUILT_VISITS
    a = M.build_parser().parse_args(["x"])
    assert (a.correction_table, a.max_not_built_visits) == (DEFAULT_CORRECTION_TABLE, MAX_NOT_BUILT_VISITS)


def test_correction_error_is_extract_error(tmp_path, monkeypatch):
    from ssp.export import submittable as S

    def boom(*a, **kw):
        raise S.CorrectionError("12 visits are not built")
    monkeypatch.setattr(S, "extract", boom)
    args = M.build_parser().parse_args([str(tmp_path), "--max-not-built-visits", "3"])
    args.ch_host = "h"
    with pytest.raises(M.ExtractError, match="shutter-motion correction: 12 visits"):
        M.extract_dia_sources(tmp_path / "obs.parquet", tmp_path / "dia.parquet", args)
