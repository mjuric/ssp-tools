"""ssp-upload-sso (stage 3) with fake GCS and Pub/Sub clients; no network."""
import concurrent.futures
import json
import os
import re
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import delivery_contract as C
from ssp import sso_upload as U
from ssp.ssobservation_contract import PART_FILE_FORMAT, SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE

TOPIC_PATH = "projects/ppdb-dev-5c07/topics/load-sso-topic"
#: All six tables, in the upload order (dax_ppdb SSO_TABLES).
SIX = ("SSObservation", "NearbySSO", "SSObject", "mpc_orbits", "current_identifications",
       "numbered_identifications")
SSOBS = C.UPLOAD_SSOBSERVATION_FILE


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def upload_from_filename(self, filename, if_generation_match=None):
        from google.api_core.exceptions import PreconditionFailed
        self.store.calls.append(("upload", self.name, if_generation_match))
        if self.name in self.store.fail_on:
            raise self.store.fail_on[self.name]
        if if_generation_match == 0 and self.name in self.store.objects:
            raise PreconditionFailed("exists")
        with open(filename, "rb") as f:
            self.store.objects[self.name] = f.read()

    def delete(self):
        self.store.calls.append(("delete", self.name))
        if self.name in self.store.fail_delete:
            raise RuntimeError("injected delete failure")
        del self.store.objects[self.name]

    # what the chunked (--parallel) path uses
    def exists(self):
        self.store.calls.append(("exists", self.name))
        return self.name in self.store.objects

    def reload(self):
        self.store.calls.append(("reload", self.name))
        data = self.store.objects[self.name]
        self.size, self.crc32c = len(data), _crc32c(data)


def _crc32c(data):
    import base64

    import google_crc32c
    return base64.b64encode(google_crc32c.Checksum(data).digest()).decode()


class FakeBucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name):
        return FakeBlob(self.store, name)


class FakeStorage:
    def __init__(self):
        self.objects, self.calls, self.buckets = {}, [], []
        self.fail_on, self.fail_delete = {}, set()

    def bucket(self, name):
        self.buckets.append(name)
        return FakeBucket(self)


class FakeFuture:
    def __init__(self, exc=None):
        self.exc, self.timeouts = exc, []

    def result(self, timeout=None):
        self.timeouts.append(timeout)
        if self.exc:
            raise self.exc
        return "msg-123"


class FakePublisher:
    def __init__(self, exc=None, publish_exc=None):
        self.published, self.exc, self.futures = [], exc, []
        self.publish_exc = publish_exc

    @staticmethod
    def topic_path(project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data):
        if self.publish_exc:
            raise self.publish_exc
        self.published.append((topic, data))
        self.futures.append(FakeFuture(self.exc))
        return self.futures[-1]


@pytest.fixture
def fakes(monkeypatch):
    pytest.importorskip("google.api_core.exceptions")
    st, pub = FakeStorage(), FakePublisher()
    monkeypatch.setattr(U, "_storage_client", lambda: st)
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    return st, pub


@pytest.fixture
def no_remote(monkeypatch):
    """Any attempt to make a client fails the test."""
    def boom():
        raise AssertionError("a remote client was created")
    monkeypatch.setattr(U, "_storage_client", boom)
    monkeypatch.setattr(U, "_publisher_client", boom)


# --------------------------------------------------------------------------
# A stand-in delivery
# --------------------------------------------------------------------------

#: SSObservation's parts in make_run: rows of each, and the row group size
#: they are written with (so each has more than one row group).
PART_ROWS = (7, 5)
PART_RG = 3

#: The stand-in SSObservation schema: a string key, a nullable int64, a
#: dictionary column (as the real parts have), not-null.
SSOBS_SCHEMA = pa.schema([
    pa.field("obsid", pa.string(), nullable=False),
    pa.field("ssObjectId", pa.int64()),
    pa.field("band", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("midpointMjdTai", pa.float64(), nullable=False),
])


def part_table(k, n, schema=SSOBS_SCHEMA, start=0):
    base = 100 * k + start
    cols = {"obsid": [f"o{base + i:05d}" for i in range(n)],
            "ssObjectId": [None if i == 2 else base + i // 2 for i in range(n)],
            "band": pa.array(["gri"[(base + i) % 3] for i in range(n)]).dictionary_encode(),
            "midpointMjdTai": [60000.0 + base + i / 10 for i in range(n)]}
    if "extra" in schema.names:
        cols["extra"] = list(range(n))
    return pa.table(cols, schema=schema)


def write_part(path, k, n, schema=SSOBS_SCHEMA, start=0):
    pq.write_table(part_table(k, n, schema, start), path, compression="zstd", row_group_size=PART_RG)


def _part_entry(p, rows):
    return dict(file=p.name, rows=rows, ssObjectId_min=0, ssObjectId_max=0, null_ssObjectId=False,
                bytes=p.stat().st_size, md5=U.md5_file(p))


def make_partitioned(run, part_rows=PART_ROWS):
    """A stand-in partitioned SSObservation (Parquet parts, sidecar,
    manifest) in RUN_DIR/delivery; returns its report entry."""
    d = run / C.DELIVERY_DIR
    parts = []
    for k, n in enumerate(part_rows):
        p = d / PART_FILE_FORMAT.format(k)
        write_part(p, k, n)
        parts.append(_part_entry(p, n))
    side = d / SIDECAR_FILE
    pq.write_table(pa.table({"obsid": ["o"], "matchMethod": ["x"]}), side)
    m = dict(table="SSObservation", format_version=1, rows=sum(p["rows"] for p in parts), parts=parts,
             sidecar=dict(file=SIDECAR_FILE, key="obsid", columns=["matchMethod"], rows=1,
                          bytes=side.stat().st_size, md5=U.md5_file(side)))
    (d / SSOBSERVATION_MANIFEST_FILE).write_text(json.dumps(m))
    return {"manifest": f"{C.DELIVERY_DIR}/{SSOBSERVATION_MANIFEST_FILE}",
            "manifest_md5": U.md5_file(d / SSOBSERVATION_MANIFEST_FILE),
            "parts": [f"{C.DELIVERY_DIR}/{p['file']}" for p in parts],
            "sidecar": f"{C.DELIVERY_DIR}/{SIDECAR_FILE}", "rows": m["rows"],
            "bytes": sum(p["bytes"] for p in parts)}


def make_run(tmp_path, deliverable=True, tables=C.DELIVERY_TABLES):
    run = tmp_path / "run"
    (run / C.DELIVERY_DIR).mkdir(parents=True)
    rec = {}
    for t in tables:
        if t == "SSObservation":
            rec[t] = make_partitioned(run)
            continue
        p = run / C.DELIVERY_DIR / f"{t}.parquet"
        p.write_bytes(f"PAR1 {t} PAR1".encode())
        rec[t] = {"file": f"{C.DELIVERY_DIR}/{t}.parquet", "rows": 1, "md5": U.md5_file(p),
                  "bytes": p.stat().st_size}
    report = {"inputs": {}, "ssp_tools_commit": "abc", "steps": {s: {"status": "ok"} for s in C.BUILD_STEPS},
              "tables": rec, "checks": {"schema": {"status": "PASS"}}, "deliverable": deliverable}
    (run / C.REPORT_FILE).write_text(json.dumps(report))
    return run


def rewrite_ssobservation(run, write):
    """Rewrite the delivery's SSObservation parts with ``write(k, path)``,
    and its manifest and report entry to match (a rebuilt delivery)."""
    d = run / C.DELIVERY_DIR
    mpath = d / SSOBSERVATION_MANIFEST_FILE
    m = json.loads(mpath.read_text())
    for k, e in enumerate(m["parts"]):
        p = d / e["file"]
        write(k, p)
        e.update(_part_entry(p, pq.ParquetFile(p).metadata.num_rows))
    m["rows"] = sum(e["rows"] for e in m["parts"])
    mpath.write_text(json.dumps(m))
    r = report(run)
    r["tables"]["SSObservation"].update(manifest_md5=U.md5_file(mpath), rows=m["rows"],
                                        bytes=sum(e["bytes"] for e in m["parts"]))
    (run / C.REPORT_FILE).write_text(json.dumps(r))


def report(run):
    return json.loads((run / C.REPORT_FILE).read_text())


def ordered(tables):
    return [t for t in C.UPLOAD_TABLE_ORDER if t in tables]


def objects_of(tables, prefix):
    return [f"{prefix}/{t}.parquet" for t in tables]


def message(prefix, tables, bucket="ppdb-dev-sso-ingest"):
    return json.dumps({"bucket": bucket, "object_prefix": prefix, "uploaded_tables": list(tables)})


def combined(run):
    return run / C.UPLOAD_DIR / SSOBS


def combined_record(run):
    return json.loads((run / C.UPLOAD_DIR / C.UPLOAD_SSOBSERVATION_RECORD).read_text())


def parts_of(run):
    return sorted((run / C.DELIVERY_DIR).glob("SSObservation.part*.parquet"))


PREFIX = "20260914T200401001"


@pytest.fixture
def fixed_prefix(monkeypatch):
    monkeypatch.setattr(U, "generate_prefix", lambda now=None: PREFIX)
    return PREFIX


# --------------------------------------------------------------------------
# Prefix, configs, table selection
# --------------------------------------------------------------------------

def test_prefix_format():
    now = datetime(2026, 9, 14, 20, 4, 1, 1234, tzinfo=timezone.utc)
    assert U.generate_prefix(now) == "20260914T200401001"
    assert re.fullmatch(r"\d{8}T\d{9}", U.generate_prefix())
    # exactly the reference expression
    assert U.generate_prefix(now) == now.strftime("%Y%m%dT%H%M%S%f")[:-3]


def test_prefix_pinned_to_utc(monkeypatch):
    # the same instant in another zone gives the UTC prefix
    pdt = timezone(timedelta(hours=-7))
    assert U.generate_prefix(datetime(2026, 9, 14, 13, 4, 1, 1999, tzinfo=pdt)) == "20260914T200401001"

    class FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = datetime(2026, 9, 14, 20, 4, 1, 1000, tzinfo=timezone.utc)
            return instant.astimezone(tz) if tz else datetime(2026, 9, 14, 13, 4, 1, 1000)
    monkeypatch.setattr(U, "datetime", FakeDatetime)
    assert U.generate_prefix() == "20260914T200401001"


def test_accepted_tables():
    """All six, NearbySSO included, in dax_ppdb's SSO_TABLES order."""
    assert U.UNSUPPORTED_TABLES == ()
    assert U.ACCEPTED_TABLES == SIX == C.UPLOAD_TABLE_ORDER
    assert set(SIX) == set(C.DELIVERY_TABLES)


@pytest.mark.parametrize("env, bucket, project", [("dev", "ppdb-dev-sso-ingest", "ppdb-dev-5c07"),
                                                  ("int", "ppdb-int-sso-ingest", "ppdb-int-6c62")])
def test_configs(env, bucket, project):
    _, cfg = U.load_config(env)
    assert cfg == {"bucket_name": bucket, "topic": "load-sso-topic", "project": project,
                   "tables": list(SIX)}


def test_prod_template_and_unknown_names():
    with pytest.raises(U.SSOUploadError, match="template"):
        U.load_config("prod")
    text = (U.CONFIG_DIR / "prod.yaml").read_text()
    assert all(f"#   - {t}\n" in text for t in SIX)
    assert "not uploaded until" not in text
    with pytest.raises(U.SSOUploadError, match="no config named"):
        U.load_config("nosuchenv")


def test_config_name_vs_path(tmp_path, monkeypatch):
    # a bare name is looked up in config/sso-upload/, never in the cwd
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dev").write_text("bucket_name: evil\ntopic: t\nproject: p\n")
    assert U.load_config("dev")[1]["bucket_name"] == "ppdb-dev-sso-ingest"
    # a path: *.yaml, *.yml, or with a /
    for name in ("c.yaml", "c.yml"):
        (tmp_path / name).write_text("bucket_name: b\ntopic: t\nproject: p\n")
        assert U.load_config(name)[1]["bucket_name"] == "b"
    assert U.load_config("./dev")[1]["bucket_name"] == "evil"
    with pytest.raises(U.SSOUploadError, match="no config file"):
        U.load_config("missing.yaml")


def test_config_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("bucket_name: b\ntopic: t\n")
    with pytest.raises(U.SSOUploadError, match="missing"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\ntopic: t\nproject: p\nbukcet: x\n")
    with pytest.raises(U.SSOUploadError, match="unknown"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\ntopic: t\nproject: p\ntables: [SSObservation]\n")
    assert U.load_config(str(p))[1]["tables"] == ["SSObservation"]


def test_nearbysso_in_the_default_set(no_remote, tmp_path, capsys):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    assert rec["tables"] == list(SIX)
    out = capsys.readouterr().out
    assert f"gs://ppdb-dev-sso-ingest/{rec['object_prefix']}/NearbySSO.parquet" in out
    # and with a config without tables
    cfg = tmp_path / "c.yaml"
    cfg.write_text("bucket_name: b\ntopic: t\nproject: p\n")
    assert U.run(str(cfg), run, dry_run=True)["tables"] == list(SIX)


def test_unsupported_hook_still_works(no_remote, tmp_path, monkeypatch, caplog):
    """--include-unsupported is a no-op today; with a table listed as
    unsupported, its refusal and warning still work."""
    run = make_run(tmp_path)
    assert U.run("dev", run, dry_run=True, include_unsupported=True)["tables"] == list(SIX)
    assert U.main(["dev", str(run), "--dry-run", "--include-unsupported"]) == 0
    monkeypatch.setattr(U, "UNSUPPORTED_TABLES", ("NearbySSO",))
    with pytest.raises(U.SSOUploadError, match="--include-unsupported"):
        U.run("dev", run, dry_run=True)
    with caplog.at_level("WARNING"):
        rec = U.run("dev", run, dry_run=True, include_unsupported=True)
    assert rec["tables"] == list(SIX)
    assert [r for r in caplog.records if "NearbySSO" in r.getMessage() and "fails the entire load"
            in r.getMessage()]


def test_partial_upload_refused_without_allow_partial(no_remote, tmp_path):
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="WRITE_TRUNCATE.*--allow-partial"):
        U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"])
    rec = U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"], allow_partial=True)
    assert rec["tables"] == ["SSObservation", "SSObject"]       # the upload order
    # the config's six in any order count as complete
    rec = U.run("dev", run, dry_run=True, tables=list(reversed(SIX)))
    assert rec["tables"] == list(SIX)
    with pytest.raises(U.SSOUploadError, match="not delivery tables"):
        U.run("dev", run, tables=["DiaSource"])


# --------------------------------------------------------------------------
# The combined SSObservation
# --------------------------------------------------------------------------

def test_combined_file(no_remote, tmp_path):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    path = combined(run)
    assert path.is_file() and not path.with_name(path.name + ".tmp").exists()
    parts = parts_of(run)
    assert len(parts) == len(PART_ROWS)
    pf = pq.ParquetFile(path)
    # the parts' rows, in order, with their schema; no sidecar columns
    assert pf.read().equals(pa.concat_tables(pq.read_table(p) for p in parts))
    assert pf.schema_arrow.equals(pq.ParquetFile(parts[0]).schema_arrow, check_metadata=True)
    assert pf.schema_arrow.equals(SSOBS_SCHEMA)
    assert "matchMethod" not in pf.schema_arrow.names
    # one row group per part row group, none bigger
    md = pf.metadata
    want = [pq.ParquetFile(p).metadata.row_group(i).num_rows
            for p in parts for i in range(pq.ParquetFile(p).metadata.num_row_groups)]
    assert [md.row_group(i).num_rows for i in range(md.num_row_groups)] == want
    assert len(want) > len(parts) and max(want) <= PART_RG
    assert md.row_group(0).column(0).compression == "ZSTD"
    # the record, next to it and in the report
    r = combined_record(run)
    assert r == {"file": f"{C.UPLOAD_DIR}/{SSOBS}", "rows": sum(PART_ROWS), "bytes": path.stat().st_size,
                 "md5": U.md5_file(path),
                 "manifest_md5": report(run)["tables"]["SSObservation"]["manifest_md5"]}
    assert rec["ssobservation"] == r
    assert report(run)["upload"]["ssobservation"] == r


def test_combined_row_groups_are_not_split(no_remote, tmp_path, monkeypatch):
    """Each part row group is written with a row_group_size that holds it
    whole (the writer's default would cap it at 1 Mi rows)."""
    run = make_run(tmp_path)
    calls = []
    orig = pq.ParquetWriter.write_table

    def spy(self, table, row_group_size=None):
        calls.append((table.num_rows, row_group_size))
        return orig(self, table, row_group_size=row_group_size)
    monkeypatch.setattr(pq.ParquetWriter, "write_table", spy)
    U.run("dev", run, dry_run=True)
    assert [n for n, _ in calls] == [3, 3, 1, 3, 2] and all(rgs is not None and rgs >= n for n, rgs in calls)


def test_combined_file_reused(no_remote, tmp_path, caplog, capsys):
    run = make_run(tmp_path)
    U.run("dev", run, dry_run=True)
    st = combined(run).stat()
    capsys.readouterr()
    with caplog.at_level("INFO"):
        rec = U.run("dev", run, dry_run=True)
    assert "SSObservation: reused" in capsys.readouterr().out
    assert any("reusing" in r.getMessage() for r in caplog.records)
    st2 = combined(run).stat()
    assert (st2.st_ino, st2.st_mtime_ns) == (st.st_ino, st.st_mtime_ns)
    assert rec["ssobservation"] == combined_record(run)


def test_combined_rebuilt_for_a_new_delivery(no_remote, tmp_path, caplog):
    """The delivery rebuilt (other parts, another manifest_md5): the file is
    rebuilt from the new parts."""
    run = make_run(tmp_path)
    U.run("dev", run, dry_run=True)
    old = combined_record(run)
    rewrite_ssobservation(run, lambda k, p: write_part(p, k, PART_ROWS[k] + 1, start=50))
    with caplog.at_level("INFO"):
        rec = U.run("dev", run, dry_run=True)
    assert any("building" in r.getMessage() and old["manifest_md5"] in r.getMessage()
               for r in caplog.records)
    new = combined_record(run)
    assert new["manifest_md5"] == report(run)["tables"]["SSObservation"]["manifest_md5"]
    assert new["manifest_md5"] != old["manifest_md5"]
    assert new["rows"] == sum(PART_ROWS) + len(PART_ROWS) and rec["ssobservation"] == new
    assert pq.read_table(combined(run)).equals(pa.concat_tables(pq.read_table(p) for p in parts_of(run)))


def _flip_byte(run):
    p = combined(run)
    b = bytearray(p.read_bytes())
    b[len(b) // 2] ^= 0xFF          # same size, other content
    p.write_bytes(bytes(b))


def _set_record(run, **kw):
    p = run / C.UPLOAD_DIR / C.UPLOAD_SSOBSERVATION_RECORD
    r = json.loads(p.read_text())
    r.update(kw)
    p.write_text(json.dumps(r))


@pytest.mark.parametrize("damage, why", [
    (_flip_byte, "has md5"),
    (lambda run: combined(run).write_bytes(combined(run).read_bytes()[:-10]), "bytes, its record says"),
    (lambda run: combined(run).unlink(), "is missing"),
    (lambda run: (run / C.UPLOAD_DIR / C.UPLOAD_SSOBSERVATION_RECORD).unlink(), "no "),
    (lambda run: (run / C.UPLOAD_DIR / C.UPLOAD_SSOBSERVATION_RECORD).write_text("{"), "cannot read"),
    (lambda run: _set_record(run, manifest_md5="0" * 32), "built from manifest md5 " + "0" * 32),
    (lambda run: _set_record(run, rows=1), "says 1 rows"),
])
def test_combined_rebuilt_when_damaged(no_remote, tmp_path, caplog, damage, why):
    run = make_run(tmp_path)
    U.run("dev", run, dry_run=True)
    good = combined_record(run)
    damage(run)
    with caplog.at_level("INFO"):
        U.run("dev", run, dry_run=True)
    assert any("building" in r.getMessage() and why in r.getMessage() for r in caplog.records)
    assert combined_record(run) == good          # the same file again (deterministic)
    assert U.md5_file(combined(run)) == good["md5"]


@pytest.mark.parametrize("change", ["extra column", "type", "nullability"])
def test_parts_schema_mismatch_refused(no_remote, tmp_path, change):
    run = make_run(tmp_path)
    if change == "extra column":
        other = SSOBS_SCHEMA.append(pa.field("extra", pa.int64()))
    elif change == "type":
        other = SSOBS_SCHEMA.set(3, pa.field("midpointMjdTai", pa.float32(), nullable=False))
    else:
        other = SSOBS_SCHEMA.set(3, pa.field("midpointMjdTai", pa.float64()))
    rewrite_ssobservation(run, lambda k, p: write_part(p, k, PART_ROWS[k], other if k == 1 else SSOBS_SCHEMA))
    with pytest.raises(U.SSOUploadError, match=r"part0001\.parquet has another schema"):
        U.run("dev", run, dry_run=True)
    assert not (run / C.UPLOAD_DIR / SSOBS).exists()
    assert not list((run / C.UPLOAD_DIR).glob("*.tmp"))
    assert "upload" not in report(run)


def test_combined_check_failure_leaves_nothing(no_remote, tmp_path, monkeypatch):
    """A combined file that fails its check is not put in place."""
    run = make_run(tmp_path)
    monkeypatch.setattr(U, "_read_manifest", lambda mpath, raw=None: (
        [(p["file"], p["bytes"], p["md5"]) for p in json.loads(raw or open(mpath).read())["parts"]], 999))
    r = report(run)
    r["tables"]["SSObservation"]["rows"] = 999
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="combined .* has .* rows, the manifest says 999"):
        U.run("dev", run, dry_run=True)
    assert not combined(run).exists() and not list((run / C.UPLOAD_DIR).glob("*"))   # its .tmp too


def test_combined_tmp_name_is_per_process(no_remote, tmp_path, monkeypatch):
    """The combined file is written to a temporary name unique to the
    process and call, then renamed into place."""
    seen = []
    orig = U._write_combined

    def spy(parts, out):
        seen.append(out)
        return orig(parts, out)
    monkeypatch.setattr(U, "_write_combined", spy)
    run = make_run(tmp_path)
    stale = run / C.UPLOAD_DIR / f"{SSOBS}.1.deadbeef.tmp"     # another run's
    stale.parent.mkdir()
    stale.write_bytes(b"someone else's")
    U.run("dev", run, dry_run=True)
    (out,) = seen
    assert out.parent == run / C.UPLOAD_DIR
    assert re.fullmatch(rf"{re.escape(SSOBS)}\.{os.getpid()}\.[0-9a-f]{{8}}\.tmp", out.name)
    assert not out.exists() and combined(run).is_file()
    assert stale.read_bytes() == b"someone else's"          # never touched
    assert U._tmp_name(combined(run)) != U._tmp_name(combined(run))


def test_combined_refuses_a_changed_manifest(tmp_path):
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="changed"):
        U.combine_ssobservation(run, "0" * 32)


# --------------------------------------------------------------------------
# The upload
# --------------------------------------------------------------------------

def test_upload_success_and_message(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    rec = U.run("dev", run)
    prefix = rec["object_prefix"]
    assert re.fullmatch(r"\d{8}T\d{9}", prefix)
    assert st.buckets == ["ppdb-dev-sso-ingest"]
    # one object per table, in SSO_TABLES order; no part, manifest or sidecar
    assert [c[1] for c in st.calls if c[0] == "upload"] == objects_of(SIX, prefix)
    assert sorted(st.objects) == sorted(objects_of(SIX, prefix))
    assert all(c[2] == 0 for c in st.calls if c[0] == "upload")    # if_generation_match=0
    assert st.objects[f"{prefix}/SSObservation.parquet"] == combined(run).read_bytes()
    for t in SIX[1:]:
        assert st.objects[f"{prefix}/{t}.parquet"] == (run / C.DELIVERY_DIR / f"{t}.parquet").read_bytes()
    assert len(pub.published) == 1
    topic, data = pub.published[0]
    assert topic == TOPIC_PATH
    assert data == message(prefix, SIX).encode("utf-8")
    assert data == (b'{"bucket": "ppdb-dev-sso-ingest", "object_prefix": "' + prefix.encode()
                    + b'", "uploaded_tables": ["SSObservation", "NearbySSO", "SSObject", "mpc_orbits", '
                    b'"current_identifications", "numbered_identifications"]}')
    assert tuple(json.loads(data)) == C.UPLOAD_MESSAGE_FIELDS
    assert pub.futures[0].timeouts == [U.PUBLISH_TIMEOUT_S]
    r = report(run)
    assert r["upload"] == rec and r["uploads"] == [rec]
    assert list(rec) == ["config", "bucket", "object_prefix", "tables", "message_id", "dry_run",
                         "published", "utc", "ssobservation", "parallel"]
    # parallel by default, but nothing here is big enough to go in chunks
    assert rec["parallel"] == {"chunk_bytes": 64 << 20, "workers": 16, "min_bytes": 1 << 30, "tables": []}
    assert rec["message_id"] == "msg-123" and rec["dry_run"] is False and rec["published"] is True
    assert rec["ssobservation"] == combined_record(run)


def test_upload_order_whatever_the_config_order(fakes, tmp_path, fixed_prefix):
    st, pub = fakes
    run = make_run(tmp_path)
    cfg = tmp_path / "c.yaml"
    cfg.write_text("bucket_name: b\ntopic: t\nproject: p\ntables: [" + ", ".join(reversed(SIX)) + "]\n")
    rec = U.run(str(cfg), run)
    assert rec["tables"] == list(SIX)
    assert [c[1] for c in st.calls if c[0] == "upload"] == objects_of(SIX, PREFIX)
    assert pub.published[0][1] == message(PREFIX, SIX, bucket="b").encode()
    # upload() itself orders a file map, too
    assert U.upload({"bucket_name": "b", "topic": "t", "project": "p"},
                    {"SSObject": run / C.DELIVERY_DIR / "SSObject.parquet", "SSObservation": combined(run)},
                    object_prefix="X", publish=False) == ("X", None)
    assert [c[1] for c in st.calls if c[0] == "upload"][-2:] == ["X/SSObservation.parquet",
                                                                 "X/SSObject.parquet"]
    assert len(pub.published) == 1


def test_precondition_failure_aborts_and_cleans_up(fakes, tmp_path, fixed_prefix):
    st, pub = fakes
    run = make_run(tmp_path)
    # an object already at the third table's name, as from a colliding upload
    st.objects[f"{PREFIX}/{SIX[2]}.parquet"] = b"someone else's"
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run)
    # the pre-existing object is untouched, ours are deleted, nothing published
    assert st.objects == {f"{PREFIX}/{SIX[2]}.parquet": b"someone else's"}
    assert [c[1] for c in st.calls if c[0] == "delete"] == objects_of(SIX[:2], PREFIX)
    assert pub.published == []
    assert "upload" not in report(run)


@pytest.mark.parametrize("k", range(len(SIX)))
def test_failure_on_any_table_cleans_up_the_earlier(fakes, tmp_path, fixed_prefix, k):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{SIX[k]}.parquet"] = ServiceUnavailable("injected")
    with pytest.raises(U.SSOUploadError, match="failed to upload"):
        U.run("dev", run)
    assert st.objects == {} and pub.published == []
    assert [c[1] for c in st.calls if c[0] == "delete"] == objects_of(SIX[:k], PREFIX)
    assert "upload" not in report(run)


@pytest.mark.parametrize("exc", ["auth", "other"])
def test_any_upload_error_is_wrapped(fakes, tmp_path, fixed_prefix, exc):
    from google.auth.exceptions import RefreshError
    st, _ = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{SIX[1]}.parquet"] = (
        RefreshError("token expired") if exc == "auth" else ValueError("odd"))
    with pytest.raises(U.SSOUploadError, match="failed to upload"):
        U.run("dev", run)
    assert st.objects == {}
    assert U.main(["dev", str(run)]) == 1      # reported cleanly, not a traceback


def test_client_creation_error_is_wrapped(tmp_path, monkeypatch):
    pytest.importorskip("google.auth.exceptions")
    from google.auth.exceptions import DefaultCredentialsError

    def no_creds():
        raise DefaultCredentialsError("no ADC")
    monkeypatch.setattr(U, "_storage_client", no_creds)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="GCS client"):
        U.run("dev", run)


def test_cleanup_keeps_going_and_reraises_the_original(fakes, tmp_path, fixed_prefix):
    from google.api_core.exceptions import ServiceUnavailable
    st, _ = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{SIX[3]}.parquet"] = ServiceUnavailable("injected")
    stuck = f"{PREFIX}/{SIX[0]}.parquet"
    st.fail_delete.add(stuck)
    with pytest.raises(U.SSOUploadError, match="failed to upload") as ei:
        U.run("dev", run)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    assert [c[1] for c in st.calls if c[0] == "delete"] == objects_of(SIX[:3], PREFIX)
    assert set(st.objects) == {stuck}     # the one that wouldn't delete


def test_publish_failure_cleans_up(fakes, tmp_path, monkeypatch):
    """publish() itself fails: the message never left, so the objects are
    deleted."""
    from google.api_core.exceptions import ServiceUnavailable
    st, _ = fakes
    pub = FakePublisher(publish_exc=ServiceUnavailable("injected"))
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="failed to publish"):
        U.run("dev", run)
    assert pub.published == []
    assert st.objects == {}
    assert len([c for c in st.calls if c[0] == "delete"]) == len(SIX)
    assert "upload" not in report(run)


def test_publisher_creation_failure_cleans_up(fakes, tmp_path, monkeypatch):
    st, _ = fakes

    def boom():
        raise RuntimeError("no publisher")
    monkeypatch.setattr(U, "_publisher_client", boom)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="failed to publish"):
        U.run("dev", run)
    assert st.objects == {} and "upload" not in report(run)


def test_publish_error_after_handover_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix):
    """The message was handed to Pub/Sub, then the wait failed: it may have
    gone through, so the objects are kept and the upload recorded."""
    from google.api_core.exceptions import ServiceUnavailable
    st, _ = fakes
    pub = FakePublisher(exc=ServiceUnavailable("injected"))
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.PublishUnconfirmedError, match="interrupted or failed.*KEPT") as ei:
        U.run("dev", run)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))
    assert not [c for c in st.calls if c[0] == "delete"]
    up = report(run)["upload"]
    assert up["published"] is None and up["object_prefix"] == PREFIX and "ServiceUnavailable" in up["error"]
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_publish_interrupted_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix, exc):
    """Ctrl-C (or SystemExit) while waiting for the confirmation: the
    objects are kept, the upload recorded with published null, and the
    interrupt propagates."""
    st, _ = fakes
    pub = FakePublisher(exc=exc())
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(exc):
        U.run("dev", run)
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))
    assert not [c for c in st.calls if c[0] == "delete"]
    r = report(run)
    assert r["upload"]["published"] is None and r["upload"]["object_prefix"] == PREFIX
    err = r["upload"]["error"]
    assert "interrupted" in err and "may or may not have gone through" in err
    assert r["uploads"] == [r["upload"]]
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


def test_interrupt_during_upload_still_cleans_up(fakes, tmp_path, fixed_prefix):
    """Before the message is handed over, even a KeyboardInterrupt cleans
    up."""
    st, pub = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{SIX[2]}.parquet"] = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        U.run("dev", run)
    assert st.objects == {} and pub.published == []
    assert "upload" not in report(run)


def test_publish_timeout_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix):
    st, _ = fakes
    pub = FakePublisher(exc=concurrent.futures.TimeoutError())
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.PublishUnconfirmedError, match="KEPT"):
        U.run("dev", run)
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))
    assert not [c for c in st.calls if c[0] == "delete"]
    r = report(run)
    assert r["upload"]["object_prefix"] == PREFIX and r["upload"]["message_id"] is None
    assert r["upload"]["published"] is None
    assert "wasn't confirmed" in r["upload"]["error"]
    # and a plain re-upload is refused: the message may have gone through
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


# --------------------------------------------------------------------------
# --no-publish
# --------------------------------------------------------------------------

def test_no_publish(tmp_path, monkeypatch, fixed_prefix, capsys):
    pytest.importorskip("google.api_core.exceptions")
    st = FakeStorage()
    monkeypatch.setattr(U, "_storage_client", lambda: st)

    def boom():
        raise AssertionError("a publisher was created")
    monkeypatch.setattr(U, "_publisher_client", boom)
    run = make_run(tmp_path)
    rec = U.run("dev", run, no_publish=True)
    # uploaded, kept, nothing published
    assert [c[1] for c in st.calls if c[0] == "upload"] == objects_of(SIX, PREFIX)
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))
    assert not [c for c in st.calls if c[0] == "delete"]
    out = capsys.readouterr().out
    assert f"gs://ppdb-dev-sso-ingest/{PREFIX}/" in out and "KEPT" in out
    assert message(PREFIX, SIX) in out
    assert rec["published"] is False and rec["dry_run"] is False and rec["message_id"] is None
    assert rec["object_prefix"] == PREFIX and rec["ssobservation"] == combined_record(run)
    assert report(run)["upload"] == rec


def test_no_publish_does_not_block_a_real_upload(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    nopub = U.run("dev", run, no_publish=True)
    assert pub.published == []
    real = U.run("dev", run)                 # no --force needed
    assert real["published"] is True and len(pub.published) == 1
    # a later --no-publish run doesn't replace the real upload's record, nor
    # need --force
    nopub2 = U.run("dev", run, no_publish=True)
    r = report(run)
    assert r["upload"] == real and r["uploads"] == [nopub, real, nopub2]
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


def test_no_publish_with_dry_run_is_an_error(no_remote, tmp_path):
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="--no-publish"):
        U.run("dev", run, dry_run=True, no_publish=True)
    assert U.main(["dev", str(run), "--dry-run", "--no-publish"]) == 1
    assert "upload" not in report(run)


def test_no_publish_failure_cleans_up(fakes, tmp_path, fixed_prefix):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{SIX[4]}.parquet"] = ServiceUnavailable("injected")
    with pytest.raises(U.SSOUploadError, match="failed to upload"):
        U.run("dev", run, no_publish=True)
    assert st.objects == {} and pub.published == []


# --------------------------------------------------------------------------
# Dry run, history, refusals
# --------------------------------------------------------------------------

def test_dry_run_touches_nothing_remote(no_remote, tmp_path, capsys):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    assert f"{combined(run)} -> gs://ppdb-dev-sso-ingest/{prefix}/SSObservation.parquet" in out
    for t in SIX[1:]:
        assert f"{run / C.DELIVERY_DIR / t}.parquet -> gs://ppdb-dev-sso-ingest/{prefix}/{t}.parquet" in out
    assert SIDECAR_FILE not in out and "part0000" not in out and SSOBSERVATION_MANIFEST_FILE not in out
    assert TOPIC_PATH in out
    assert message(prefix, SIX) in out
    up = report(run)["upload"]
    assert up["dry_run"] is True and up["message_id"] is None and up["published"] is False
    assert up["ssobservation"] == combined_record(run)
    assert U.main(["dev", str(run), "--dry-run"]) == 0


def test_no_ssobservation_entry_without_ssobservation(no_remote, tmp_path):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True, tables=["SSObject"], allow_partial=True)
    assert "ssobservation" not in rec and not (run / C.UPLOAD_DIR).exists()


def test_upload_history(fakes, tmp_path):
    run = make_run(tmp_path)
    dry = U.run("dev", run, dry_run=True)
    real = U.run("dev", run)
    dry2 = U.run("dev", run, dry_run=True)       # doesn't replace the real one as 'upload'
    assert report(run)["upload"] == real
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)
    again = U.run("dev", run, force=True)
    r = report(run)
    assert r["upload"] == again
    assert r["uploads"] == [dry, real, dry2, again]       # --force doesn't erase history


def test_old_records_count(no_remote, tmp_path):
    """A real upload recorded before ``published`` existed still counts."""
    run = make_run(tmp_path)
    r = report(run)
    r["upload"] = {"bucket": "b", "object_prefix": "p", "message_id": "1", "dry_run": False}
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)
    assert U.counts_as_upload({"dry_run": True}) is False


def test_refuses_not_deliverable(no_remote, tmp_path):
    run = make_run(tmp_path, deliverable=False)
    with pytest.raises(U.SSOUploadError, match="deliverable"):
        U.run("dev", run, dry_run=True)
    assert U.main(["dev", str(run)]) == 1
    assert "upload" not in report(run)


@pytest.mark.parametrize("value", ["yes", "true", 1, None])
def test_refuses_truthy_but_not_true(no_remote, tmp_path, value):
    run = make_run(tmp_path, deliverable=value)
    with pytest.raises(U.SSOUploadError, match="deliverable"):
        U.run("dev", run, dry_run=True)


def test_refuses_md5_mismatch(no_remote, tmp_path):
    run = make_run(tmp_path)
    p = run / C.DELIVERY_DIR / "SSObject.parquet"
    p.write_bytes(p.read_bytes().replace(b"PAR1", b"PARX"))     # same size, other content
    with pytest.raises(U.SSOUploadError, match="SSObject.*md5"):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)


def test_refuses_missing_file_or_record(no_remote, tmp_path):
    run = make_run(tmp_path)
    (run / C.DELIVERY_DIR / "SSObject.parquet").unlink()
    with pytest.raises(U.SSOUploadError, match="SSObject.*missing"):
        U.run("dev", run, dry_run=True)
    r = report(run)
    del r["tables"]["SSObservation"]
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="SSObservation: no parts in the report"):
        U.run("dev", run, dry_run=True, tables=["SSObservation"], allow_partial=True)


def _set_report_manifest_md5(run):
    """Make the report's manifest_md5 the manifest's as it is now (to reach
    the checks behind it)."""
    r = report(run)
    mpath = run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE
    r["tables"]["SSObservation"]["manifest_md5"] = U.md5_file(mpath)
    (run / C.REPORT_FILE).write_text(json.dumps(r))


def _edit_manifest(run, fn, report_too=True):
    """Edit the manifest; with ``report_too``, also the report's
    manifest_md5, so that the manifest's contents are what is checked."""
    p = run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE
    m = json.loads(p.read_text())
    fn(m)
    p.write_text(json.dumps(m))
    if report_too:
        _set_report_manifest_md5(run)


def _corrupt_part(run, k=1):
    p = run / C.DELIVERY_DIR / PART_FILE_FORMAT.format(k)
    b = bytearray(p.read_bytes())
    b[len(b) // 2] ^= 0xFF          # same size, other content
    p.write_bytes(bytes(b))
    return p


def _unreadable_manifest(run):
    (run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE).write_text("{")
    _set_report_manifest_md5(run)


@pytest.mark.parametrize("damage, match", [
    (_corrupt_part, r"SSObservation\.part0001\.parquet has md5 [0-9a-f]{32}, the manifest says"),
    (lambda run: (run / C.DELIVERY_DIR / PART_FILE_FORMAT.format(0)).unlink(), "part0000.parquet is missing"),
    (lambda run: (run / C.DELIVERY_DIR / PART_FILE_FORMAT.format(0)).write_bytes(b"x"),
     "part0000.parquet is 1 bytes, the manifest says"),
    (lambda run: (run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE).unlink(), "manifest.json is missing"),
    (_unreadable_manifest, "cannot read"),
    (lambda run: _edit_manifest(run, lambda m: m["parts"].pop()), "the manifest lists the parts"),
    (lambda run: _edit_manifest(run, lambda m: m.update(rows=1)), "the manifest says 1 rows"),
    (lambda run: _edit_manifest(run, lambda m: m["parts"][0].update(file=SIDECAR_FILE)), "lists the parts"),
])
def test_refuses_bad_parts(no_remote, tmp_path, damage, match):
    """Each part is checked against the manifest, and the manifest against
    the report, before anything is combined."""
    run = make_run(tmp_path)
    damage(run)
    with pytest.raises(U.SSOUploadError, match=match):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)
    assert not (run / C.UPLOAD_DIR).exists()


MANIFEST_MD5_MISMATCH = (r"SSObservation: .*SSObservation\.manifest\.json has md5 [0-9a-f]{32}, the report "
                         r"says [0-9a-f]{32}")


def test_refuses_manifest_edited_with_a_part(no_remote, tmp_path):
    """A part changed and the manifest edited to stay consistent with it
    (its md5, bytes): refused by the report's manifest_md5."""
    run = make_run(tmp_path)
    p = _corrupt_part(run, 0)
    _edit_manifest(run, lambda m: m["parts"][0].update(md5=U.md5_file(p), bytes=p.stat().st_size),
                   report_too=False)
    with pytest.raises(U.SSOUploadError, match=MANIFEST_MD5_MISMATCH):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)


def test_refuses_manifest_edited_alone(no_remote, tmp_path):
    """The manifest alone edited (a field nothing else checks): refused."""
    run = make_run(tmp_path)
    _edit_manifest(run, lambda m: m.update(created_utc="2000-01-01T00:00:00Z"), report_too=False)
    with pytest.raises(U.SSOUploadError, match=MANIFEST_MD5_MISMATCH):
        U.run("dev", run, dry_run=True)
    r = report(run)
    del r["tables"]["SSObservation"]["manifest_md5"]
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="no manifest_md5 in the report"):
        U.run("dev", run, dry_run=True)


def test_sidecar_never_uploaded(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    side = run / C.DELIVERY_DIR / SIDECAR_FILE
    with pytest.raises(U.SSOUploadError, match="never uploaded"):
        U.upload(U.load_config("dev")[1], {"SSObservation": side})
    assert st.calls == [] and pub.published == []
    file_map = U.verify_delivery(run, report(run), ["SSObservation"])
    assert file_map == {"SSObservation": parts_of(run)}
    U.run("dev", run)
    assert not any(SIDECAR_FILE in name or "part0" in name or "manifest" in name for name in st.objects)


def test_nearbysso_sidecar_never_uploaded(fakes, tmp_path):
    """NearbySSO's sidecar, next to it in the delivery, is never picked, and
    upload() refuses it."""
    from ssp.nearbysso._contract import NEARBYSSO_SIDECAR_FILE
    st, pub = fakes
    run = make_run(tmp_path)
    side = run / C.DELIVERY_DIR / NEARBYSSO_SIDECAR_FILE
    side.write_bytes(b"PAR1 sidecar PAR1")
    file_map = U.verify_delivery(run, report(run), list(C.DELIVERY_TABLES))
    assert file_map["NearbySSO"] == [run / C.DELIVERY_DIR / "NearbySSO.parquet"]
    with pytest.raises(U.SSOUploadError, match="never uploaded"):
        U.upload(U.load_config("dev")[1], {"NearbySSO": side})
    assert st.calls == [] and pub.published == []
    rec = U.run("dev", run)
    assert "NearbySSO" in rec["tables"]
    assert not any(NEARBYSSO_SIDECAR_FILE in name for name in st.objects)
    assert st.objects[f"{rec['object_prefix']}/NearbySSO.parquet"] == (
        run / C.DELIVERY_DIR / "NearbySSO.parquet").read_bytes()


def test_upload_refuses_unknown_tables(fakes, tmp_path):
    st, _ = fakes
    with pytest.raises(U.SSOUploadError, match="not upload tables"):
        U.upload(U.load_config("dev")[1], {"DiaSource": tmp_path / "x.parquet"})
    assert st.calls == []


def test_refuses_no_report(no_remote, tmp_path):
    with pytest.raises(U.SSOUploadError, match="report.json"):
        U.run("dev", tmp_path, dry_run=True)


def test_accepts_the_delivery_dir(no_remote, tmp_path):
    run = make_run(tmp_path)
    U.run("dev", run / C.DELIVERY_DIR, dry_run=True)
    assert report(run)["upload"]["dry_run"] is True
    assert combined(run).is_file()


def test_run_dir_read_only_delivery(no_remote, tmp_path):
    """The delivery is only read: nothing in it changes (it may be hard
    links to another run's files)."""
    run = make_run(tmp_path)
    d = run / C.DELIVERY_DIR
    before = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, U.md5_file(p)) for p in d.iterdir()}
    for p in d.iterdir():
        os.chmod(p, 0o444)
    U.run("dev", run, dry_run=True)
    after = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, U.md5_file(p)) for p in d.iterdir()}
    assert after == before


# --------------------------------------------------------------------------
# --parallel: chunked uploads of the big files
# (docs/design/sso-upload-parallel.md)
# --------------------------------------------------------------------------

#: The stand-in "big" files: the threshold, and mpc_orbits made bigger than
#: it (the combined SSObservation is bigger already).
BIG = 600


def make_big_run(tmp_path):
    """make_run, with SSObservation (combined) and mpc_orbits at least BIG
    bytes and every other table smaller."""
    run = make_run(tmp_path)
    p = run / C.DELIVERY_DIR / "mpc_orbits.parquet"
    p.write_bytes(b"PAR1" + bytes(range(256)) * 4 + b"PAR1")
    r = report(run)
    r["tables"]["mpc_orbits"].update(md5=U.md5_file(p), bytes=p.stat().st_size)
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    return run


class FakeMPU:
    """Stands in for upload_chunks_concurrently and the XML multipart
    listing and abort, on a FakeStorage. ``fail`` maps an object name to
    what happens: an exception to raise with the multipart upload left
    incomplete, or "corrupt" / "short" to write wrong bytes."""

    def __init__(self, store):
        self.store, self.uploads, self.calls, self.fail = store, {}, [], {}
        self.aborted, self.list_exc, self.abort_exc, self._n = [], None, None, 0

    def chunks(self, path, blob, chunk_bytes, workers):
        self.calls.append((blob.name, str(path), chunk_bytes, workers))
        self._n += 1
        uid = f"u{self._n}"
        self.uploads[uid] = blob.name
        with open(path, "rb") as f:
            data = f.read()
        how = self.fail.get(blob.name)
        if isinstance(how, BaseException):
            raise how
        if how == "corrupt":
            data = data[:-1] + bytes([data[-1] ^ 1])
        elif how == "short":
            data = data[:-1]
        # no precondition: a multipart upload overwrites
        self.store.objects[blob.name] = data
        del self.uploads[uid]

    def list(self, bucket, prefix):
        if self.list_exc:
            raise self.list_exc
        return [(n, u) for u, n in self.uploads.items() if n.startswith(prefix)]

    def abort(self, bucket, name, upload_id):
        if self.abort_exc:
            raise self.abort_exc
        assert self.uploads.pop(upload_id) == name
        self.aborted.append((name, upload_id))


@pytest.fixture
def mpu(fakes, monkeypatch):
    pytest.importorskip("google_crc32c")
    st, pub = fakes
    m = FakeMPU(st)
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", BIG)
    monkeypatch.setattr(U, "_chunks_concurrently", m.chunks)
    monkeypatch.setattr(U, "_list_multipart_uploads", m.list)
    monkeypatch.setattr(U, "_abort_multipart_upload", m.abort)
    return st, pub, m


CHUNKED = ("SSObservation", "mpc_orbits")
DEV_CFG = "bucket_name: ppdb-dev-sso-ingest\ntopic: load-sso-topic\nproject: ppdb-dev-5c07\n"


def test_parallel_defaults():
    """The owner's choice (2026-11-13): parallel by default, 64 MiB x 16."""
    assert (U.PARALLEL_CHUNK_MIB, U.PARALLEL_WORKERS, U.PARALLEL_MIN_BYTES) == (64, 16, 1 << 30)
    assert U.parallel_options() == {"chunk_bytes": 64 << 20, "workers": 16}


def test_parallel_by_default(mpu, tmp_path, fixed_prefix):
    """No flag, no config key: the big files go in chunks of 64 MiB, 16
    threads."""
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    rec = U.run("dev", run)
    assert [c[0] for c in m.calls] == objects_of(CHUNKED, PREFIX)
    assert {c[2:] for c in m.calls} == {(64 << 20, 16)}
    assert rec["parallel"] == {"chunk_bytes": 64 << 20, "workers": 16, "min_bytes": BIG,
                               "tables": list(CHUNKED)}
    assert pub.published == [(TOPIC_PATH, message(PREFIX, SIX).encode())]
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))


def test_parallel_chunks_only_the_big_files(mpu, tmp_path, fixed_prefix):
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    rec = U.run("dev", run, parallel=True, chunk_mib=64, workers=8)
    # the big two in chunks, the rest as before (if_generation_match=0)
    assert m.calls == [(f"{PREFIX}/SSObservation.parquet", str(combined(run)), 64 << 20, 8),
                       (f"{PREFIX}/mpc_orbits.parquet", str(run / C.DELIVERY_DIR / "mpc_orbits.parquet"),
                        64 << 20, 8)]
    plain = [t for t in SIX if t not in CHUNKED]
    assert [c[1:] for c in st.calls if c[0] == "upload"] == [(o, 0) for o in objects_of(plain, PREFIX)]
    # each chunked object checked not to exist first, then read back
    for t in CHUNKED:
        name = f"{PREFIX}/{t}.parquet"
        assert [c[0] for c in st.calls if c[1] == name] == ["exists", "reload"]
    # the same objects and the same message as without --parallel
    assert sorted(st.objects) == sorted(objects_of(SIX, PREFIX))
    assert st.objects[f"{PREFIX}/SSObservation.parquet"] == combined(run).read_bytes()
    for t in SIX[1:]:
        assert st.objects[f"{PREFIX}/{t}.parquet"] == (run / C.DELIVERY_DIR / f"{t}.parquet").read_bytes()
    assert pub.published == [(TOPIC_PATH, message(PREFIX, SIX).encode())]
    assert rec["parallel"] == {"chunk_bytes": 64 << 20, "workers": 8, "min_bytes": BIG,
                               "tables": list(CHUNKED)}
    assert report(run)["upload"] == rec
    assert m.uploads == {} and m.aborted == []


@pytest.mark.parametrize("how", ["cli-off", "config-off", "cli-off-over-config-on"])
def test_no_parallel_gives_the_single_stream_path(mpu, tmp_path, fixed_prefix, how):
    """--no-parallel (also over the config's parallel: true), or the
    config's parallel: false: even the big files go in one
    if_generation_match=0 upload, and the record's parallel is null."""
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    cfg = "dev"
    if how != "cli-off":
        cfg = tmp_path / "c.yaml"
        cfg.write_text(DEV_CFG + f"parallel: {'false' if how == 'config-off' else 'true'}\n")
    rec = U.run(str(cfg), run, parallel=None if how == "config-off" else False)
    assert m.calls == []
    assert [c[1:] for c in st.calls] == [(o, 0) for o in objects_of(SIX, PREFIX)]
    assert all(c[0] == "upload" for c in st.calls)
    assert "parallel" in rec and rec["parallel"] is None
    assert report(run)["upload"]["parallel"] is None
    assert pub.published == [(TOPIC_PATH, message(PREFIX, SIX).encode())]


def test_cli_parallel_over_config_off(mpu, tmp_path, fixed_prefix):
    """--parallel wins over the config's parallel: false."""
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(DEV_CFG + "parallel: false\n")
    rec = U.run(str(cfg), run, parallel=True)
    assert [c[0] for c in m.calls] == objects_of(CHUNKED, PREFIX)
    assert rec["parallel"]["tables"] == list(CHUNKED)


def test_parallel_from_the_config(mpu, tmp_path, fixed_prefix):
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(DEV_CFG + "parallel: true\n")
    rec = U.run(str(cfg), run)
    assert [c[0] for c in m.calls] == objects_of(CHUNKED, PREFIX)
    assert rec["parallel"]["chunk_bytes"] == U.PARALLEL_CHUNK_MIB << 20
    assert rec["parallel"]["workers"] == U.PARALLEL_WORKERS
    cfg.write_text("bucket_name: b\ntopic: t\nproject: p\nparallel: yes please\n")
    with pytest.raises(U.SSOUploadError, match="parallel must be true or false"):
        U.load_config(str(cfg))


def test_parallel_refuses_an_existing_object(mpu, tmp_path, fixed_prefix):
    """No precondition on a multipart upload: an object already at the name
    is found by the check, left untouched, and the upload fails cleanly."""
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    theirs = f"{PREFIX}/mpc_orbits.parquet"
    st.objects[theirs] = b"someone else's"
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run, parallel=True)
    assert st.objects == {theirs: b"someone else's"}
    assert [c[0] for c in m.calls] == [f"{PREFIX}/SSObservation.parquet"]     # never started on it
    assert [c[1] for c in st.calls if c[0] == "delete"] == objects_of(SIX[:3], PREFIX)
    assert pub.published == [] and "upload" not in report(run)


@pytest.mark.parametrize("table", CHUNKED)
def test_parallel_failure_aborts_and_cleans_up(mpu, tmp_path, fixed_prefix, table):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    name = f"{PREFIX}/{table}.parquet"
    m.fail[name] = ServiceUnavailable("a part failed")
    with pytest.raises(U.SSOUploadError, match="in chunks") as ei:
        U.run("dev", run, parallel=True)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    # the incomplete multipart upload aborted, every object of this run gone
    assert m.uploads == {} and [a[0] for a in m.aborted] == [name]
    assert st.objects == {} and pub.published == []
    k = SIX.index(table)
    assert [c[1] for c in st.calls if c[0] == "delete"] == objects_of(SIX[:k], PREFIX)
    assert "upload" not in report(run)


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_parallel_interrupt_aborts_and_cleans_up(mpu, tmp_path, fixed_prefix, exc):
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    m.fail[f"{PREFIX}/mpc_orbits.parquet"] = exc()
    with pytest.raises(exc):
        U.run("dev", run, parallel=True)
    assert m.uploads == {} and len(m.aborted) == 1
    assert st.objects == {} and pub.published == []


@pytest.mark.parametrize("how", ["corrupt", "short"])
def test_parallel_crc32c_or_size_mismatch_deletes_it(mpu, tmp_path, fixed_prefix, how):
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    name = f"{PREFIX}/SSObservation.parquet"
    m.fail[name] = how
    with pytest.raises(U.SSOUploadError, match="CRC32C .* the upload is corrupt"):
        U.run("dev", run, parallel=True)
    assert [c[1] for c in st.calls if c[0] == "delete"] == [name]
    assert st.objects == {} and pub.published == []


def test_parallel_cleanup_failures_keep_the_original_error(mpu, tmp_path, fixed_prefix, caplog):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    m.fail[f"{PREFIX}/mpc_orbits.parquet"] = ServiceUnavailable("a part failed")
    m.list_exc = RuntimeError("can't list")
    with pytest.raises(U.SSOUploadError, match="in chunks"):
        U.run("dev", run, parallel=True)
    assert "failed to list the multipart uploads" in caplog.text
    assert st.objects == {}          # the earlier objects are still deleted
    # an abort that fails is logged, too
    caplog.clear()
    m.list_exc, m.abort_exc = None, RuntimeError("can't abort")
    m.uploads.clear()
    with pytest.raises(U.SSOUploadError, match="in chunks"):
        U.run("dev", run, parallel=True)
    assert "abort it by hand" in caplog.text


def test_parallel_needs_the_crc32c_c_extension(mpu, tmp_path, fixed_prefix, monkeypatch):
    import google_crc32c
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    monkeypatch.setattr(google_crc32c, "implementation", "python")
    with pytest.raises(U.SSOUploadError, match="C extension"):
        U.run("dev", run, parallel=True)
    assert st.calls == [] and m.calls == []
    # not needed when nothing is big enough to go in chunks
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", 1 << 40)
    U.run("dev", run, parallel=True)
    assert m.calls == []


@pytest.mark.parametrize("chunk_mib, workers, match", [
    (4, 8, "--chunk-mib"), (5 * 1024 + 1, 8, "--chunk-mib"), ("x", 8, "--chunk-mib"),
    (64, 0, "--parallel-workers"), (64, U.PARALLEL_MAX_WORKERS + 1, "--parallel-workers")])
def test_parallel_options_refused(no_remote, tmp_path, chunk_mib, workers, match):
    with pytest.raises(U.SSOUploadError, match=match):
        U.parallel_options(chunk_mib, workers)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match=match):
        U.run("dev", run, dry_run=True, chunk_mib=chunk_mib, workers=workers)       # the default
    with pytest.raises(U.SSOUploadError, match=match):
        U.run("dev", run, dry_run=True, parallel=True, chunk_mib=chunk_mib, workers=workers)
    # not checked with --no-parallel
    U.run("dev", run, dry_run=True, parallel=False, chunk_mib=chunk_mib, workers=workers)


def test_parallel_too_many_parts(mpu, tmp_path, fixed_prefix, monkeypatch):
    st, pub, m = mpu
    run = make_big_run(tmp_path)
    monkeypatch.setattr(U, "MPU_MAX_PARTS", 0)
    with pytest.raises(U.SSOUploadError, match="at most 0"):
        U.run("dev", run, parallel=True)
    assert m.calls == [] and st.objects == {}


def test_parallel_dry_run(no_remote, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", BIG)
    run = make_big_run(tmp_path)
    rec = U.run("dev", run, dry_run=True, chunk_mib=32, workers=4)     # parallel by default
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    assert f"parallel: files of at least {BIG} bytes in chunks of 32 MiB, 4 threads: {list(CHUNKED)}" in out
    for t in SIX:
        line, = [x for x in out.splitlines() if f"/{prefix}/{t}.parquet " in x]
        if t in CHUNKED:
            assert "in chunks of 32 MiB, 4 threads" in line
        else:
            assert line.endswith("(if_generation_match=0)")
    assert rec["parallel"]["tables"] == list(CHUNKED)
    rec = U.run("dev", run, dry_run=True, parallel=False)
    out = capsys.readouterr().out
    assert "parallel: off" in out and "in chunks" not in out
    assert out.count("(if_generation_match=0)") == len(SIX)
    assert rec["parallel"] is None


def test_main_parallel_flags(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(U, "run", lambda *a, **kw: seen.update(kw))
    assert U.main(["dev", str(tmp_path)]) == 0
    assert seen["parallel"] is None
    assert seen["chunk_mib"] == U.PARALLEL_CHUNK_MIB and seen["workers"] == U.PARALLEL_WORKERS
    assert U.main(["dev", str(tmp_path), "--parallel", "--chunk-mib", "64", "--parallel-workers", "8"]) == 0
    assert (seen["parallel"], seen["chunk_mib"], seen["workers"]) == (True, 64, 8)
    assert U.main(["dev", str(tmp_path), "--no-parallel"]) == 0
    assert seen["parallel"] is False


def test_size_pool():
    """The https pool is grown to the thread count; retries are kept; a
    bigger pool or a non-plain adapter is left alone."""
    requests = pytest.importorskip("requests")
    s = requests.Session()
    s.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))
    U._size_pool(s, 16)
    a = s.adapters["https://"]
    assert type(a) is requests.adapters.HTTPAdapter
    assert a._pool_maxsize == 16 and a.max_retries.total == 3
    U._size_pool(s, 8)
    assert s.adapters["https://"] is a

    class Custom(requests.adapters.HTTPAdapter):
        pass
    s.mount("https://", c := Custom())
    U._size_pool(s, 16)
    assert s.adapters["https://"] is c


def test_crc32c_file(tmp_path):
    pytest.importorskip("google_crc32c")
    p = tmp_path / "f"
    p.write_bytes(b"123456789")
    assert U.crc32c_file(p) == "4waSgw=="          # the CRC32C check value, 0xE3069283
    data = os.urandom(3 * 1000 + 17)
    p.write_bytes(data)
    assert U.crc32c_file(p, bufsize=1000) == _crc32c(data)


class _Resp:
    def __init__(self, status, body):
        self.status_code, self.content, self.text = status, body.encode(), body


def _uploads_xml(items, next_key=None, next_uid=None):
    ups = "".join(f"<Upload><Key>{k}</Key><UploadId>{u}</UploadId></Upload>" for k, u in items)
    more = (f"<NextKeyMarker>{next_key}</NextKeyMarker><NextUploadIdMarker>{next_uid}</NextUploadIdMarker>"
            if next_key else "")
    return ('<?xml version="1.0" encoding="UTF-8"?><ListMultipartUploadsResult '
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Bucket>b</Bucket>'
            f"<IsTruncated>{'true' if next_key else 'false'}</IsTruncated>{more}{ups}"
            "</ListMultipartUploadsResult>")


def test_list_and_abort_multipart_uploads(monkeypatch):
    """The XML API calls themselves, on a fake session."""
    class Http:
        def __init__(self):
            self.gets, self.deletes = [], []

        def get(self, url, params):
            self.gets.append((url, dict(params)))
            if "key-marker" not in params:
                return _Resp(200, _uploads_xml([("p/A.parquet", "u1")], "p/A.parquet", "u1"))
            return _Resp(200, _uploads_xml([("p/B.parquet", "u2")]))

        def delete(self, url, params):
            self.deletes.append((url, params))
            return _Resp(204, "")

    http = Http()
    monkeypatch.setattr(U, "_xml_api", lambda bucket: ("https://storage.example/b", http))
    assert U._list_multipart_uploads(None, "p/") == [("p/A.parquet", "u1"), ("p/B.parquet", "u2")]
    assert http.gets == [("https://storage.example/b", {"uploads": "", "prefix": "p/"}),
                         ("https://storage.example/b", {"uploads": "", "prefix": "p/",
                                                        "key-marker": "p/A.parquet",
                                                        "upload-id-marker": "u1"})]
    U._abort_multipart_upload(None, "p/A b.parquet", "u1")
    assert http.deletes == [("https://storage.example/b/p/A%20b.parquet", {"uploadId": "u1"})]
    http.get = lambda url, params: _Resp(403, "denied")
    with pytest.raises(U.SSOUploadError, match="HTTP 403"):
        U._list_multipart_uploads(None, "p/")
    http.delete = lambda url, params: _Resp(500, "oops")
    with pytest.raises(U.SSOUploadError, match="HTTP 500"):
        U._abort_multipart_upload(None, "p/A.parquet", "u1")
