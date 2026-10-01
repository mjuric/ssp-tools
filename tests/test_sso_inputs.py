"""Tests for ssp.sso_inputs (stage 1, extract), offline: the three sources
(MPC replica, extract-submitted-sources, ppdb.DiaSource) are stubbed."""

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


@pytest.fixture
def stubs(monkeypatch):
    """Stub the sources; returns a dict of call counts and controls."""
    calls = {"mpc": 0, "dia_sources": 0, "ppdb_dia_sources": 0, "fail": None, "drop": {}}

    def export_mpc(tmp, args):
        calls["mpc"] += 1
        if calls["fail"] == "mpc":
            pq.write_table(table("obs_sbn", 1), tmp / "obs_sbn.parquet")   # a partial export
            raise RuntimeError("connection lost")
        for name in MPC_SNAPSHOT:
            pq.write_table(table(name, ROWS[name], drop=calls["drop"].get(name, ())),
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
    assert list(m) == list(MANIFEST_FIELDS)
    assert m["mpc_snapshot_utc"] == "2026-10-01T06:25:03Z"
    assert m["producer"].startswith("ssp-extract-sso-inputs ")
    assert "(" in m["producer"] and m["producer"].endswith(")")
    datetime.datetime.strptime(m["created_utc"], "%Y-%m-%dT%H:%M:%SZ")
    assert list(m["files"]) == list(INPUT_FILES)
    out = tmp_path / "inputs"
    for name, (fname, _) in INPUT_FILES.items():
        e = m["files"][name]
        assert set(e) == {"file", "rows", "md5", "source", "extracted_utc"}
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
    assert "no manifest written" in capsys.readouterr().err
    run(tmp_path, "--force")
    assert stubs["mpc"] == 2


@pytest.mark.parametrize("step", ["mpc", "dia_sources", "ppdb_dia_sources"])
def test_failure_writes_no_manifest(tmp_path, stubs, step):
    stubs["fail"] = step
    with pytest.raises(RuntimeError):
        run(tmp_path)
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


def test_failed_force_rerun_removes_old_manifest(tmp_path, stubs):
    run(tmp_path)
    stubs["fail"] = "ppdb_dia_sources"
    with pytest.raises(RuntimeError):
        run(tmp_path, "--force")
    assert not (tmp_path / "inputs" / MANIFEST_FILE).exists()


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
    src = tmp_path / "elsewhere" / "my_dia.parquet"
    src.parent.mkdir()
    pq.write_table(table("dia_sources", 11), src)
    m = run(tmp_path, "--reuse", f"dia_sources={src}")
    assert stubs["dia_sources"] == 0 and stubs["mpc"] == 1
    e = m["files"]["dia_sources"]
    assert e["rows"] == 11
    assert e["source"].startswith(f"reused {src}")
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


def test_export_in_transaction(tmp_path, monkeypatch):
    """One REPEATABLE READ transaction; its first statement's now() is the
    snapshot time; every export runs on the same cursor; then commit."""
    from ssp.export import postgres as P

    log = []

    class Cur:
        def execute(self, sql):
            log.append(("execute", sql))

        def fetchone(self):
            return (SNAP,)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    cur = Cur()

    class Conn:
        def set_isolation_level(self, level):
            log.append(("isolation", level))

        def cursor(self):
            return cur

        def commit(self):
            log.append(("commit",))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(P.psycopg2, "connect", lambda dsn: (log.append(("connect", dsn)), Conn())[1])
    monkeypatch.setattr(P, "export_query_to_parquet",
                        lambda cur, sql, parquet_out, **kw: log.append(("export", id(cur), sql)))
    exports = [{"sql": "SELECT 1", "out": "a"}, {"sql": "SELECT 2", "out": "b"}]
    assert P.export_in_transaction("dsn", exports, log=lambda m: None) == SNAP
    assert log == [("connect", "dsn"),
                   ("isolation", P.psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ),
                   ("execute", "SELECT now()"),
                   ("export", id(cur), "SELECT 1"), ("export", id(cur), "SELECT 2"),
                   ("commit",)]
