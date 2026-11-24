"""ssp-upload-sso (stage 3) with fake GCS and Pub/Sub clients; no network.

The contract is the PPDB team's manifest upload (dax_ppdb DM-56960;
docs/design/sso-upload-manifest.md)."""
import concurrent.futures
import hashlib
import itertools
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ssp import delivery_contract as C
from ssp import sso_upload as U
from ssp.ssobservation_contract import PART_FILE_FORMAT, SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE

PROJECT = "ppdb-dev-5c07"
BUCKET = "ppdb-dev-sso-ingest"
TOPIC_PATH = f"projects/{PROJECT}/topics/load-sso-topic"
SUB_PATH = f"projects/{PROJECT}/subscriptions/load-sso-result-sub"
#: All six tables, in the upload order (dax_ppdb SSO_TABLES).
SIX = ("SSObservation", "NearbySSO", "SSObject", "mpc_orbits", "current_identifications",
       "numbered_identifications")


# --------------------------------------------------------------------------
# A local copy of dax_ppdb's manifest model
# --------------------------------------------------------------------------

def dax_ppdb_manifest_model():
    """dax_ppdb's sso.Manifest, as at lsst/dax_ppdb ec576a6:
    python/lsst/dax/ppdb/bigquery/sso/manifest.py (Manifest, FileEntry) and
    python/lsst/dax/ppdb/bigquery/manifest.py (ParquetFileEntry: row_count
    and size_bytes ge=1). Only the fields and constraints; FileEntry is
    given extra="forbid" too (dax_ppdb's inherits pydantic's "ignore"), so
    that our entries carry no key the PPDB doesn't know."""
    pytest.importorskip("pydantic")
    from pydantic import BaseModel, ConfigDict, Field

    class FileEntry(BaseModel):
        model_config = ConfigDict(extra="forbid")
        row_count: int = Field(ge=1)
        checksum: str
        size_bytes: int = Field(ge=1)
        source_path: str

    class Manifest(BaseModel):
        model_config = ConfigDict(extra="forbid")
        uploaded_at: datetime
        tables: dict[str, dict[str, FileEntry]]

    return Manifest


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

class FakeBlob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def _write(self, if_generation_match, data):
        from google.api_core.exceptions import PreconditionFailed
        self.store.calls.append(("upload", self.name, if_generation_match))
        how = self.store.fail_on.get(self.name)
        if isinstance(how, tuple) and how[0] == "after-write":       # written, then the call fails
            self.store.objects[self.name] = data
            raise how[1]
        if how is not None:
            raise how
        if if_generation_match == 0 and self.name in self.store.objects:
            raise PreconditionFailed("exists")
        self.store.objects[self.name] = data

    def upload_from_filename(self, filename, if_generation_match=None):
        with open(filename, "rb") as f:
            self._write(if_generation_match, f.read())

    def upload_from_string(self, data, content_type=None, if_generation_match=None):
        self.store.content_types[self.name] = content_type
        self._write(if_generation_match, data.encode() if isinstance(data, str) else data)

    def delete(self, if_generation_match=None):
        from google.api_core.exceptions import NotFound, PreconditionFailed
        self.store.calls.append(("delete", self.name))
        self.store.delete_generations.append(if_generation_match)
        if self.name in self.store.fail_delete:
            raise RuntimeError("injected delete failure")
        if self.name not in self.store.objects:
            raise NotFound("no such object")
        if if_generation_match is not None and self.store.generation(self.name) != if_generation_match:
            raise PreconditionFailed("generation changed")
        del self.store.objects[self.name]

    # what the chunked (parallel) path uses
    def exists(self):
        self.store.calls.append(("exists", self.name))
        if self.store.exists_exc:
            raise self.store.exists_exc
        return self.name in self.store.objects

    def reload(self):
        self.store.calls.append(("reload", self.name))
        if self.store.reload_exc:
            raise self.store.reload_exc
        data = self.store.objects[self.name]
        self.size, self.crc32c = len(data), _crc32c(data)
        self.generation = self.store.generation(self.name)
        if self.store.after_reload:
            self.store.after_reload(self)


def _crc32c(data):
    import base64

    import google_crc32c
    return base64.b64encode(google_crc32c.Checksum(data).digest()).decode()


class FakeBucket:
    def __init__(self, store):
        self.store = store

    def blob(self, name):
        return FakeBlob(self.store, name)

    def list_blobs(self, prefix=None, max_results=None):
        self.store.lists.append((prefix, max_results))
        if self.store.list_exc:
            raise self.store.list_exc
        names = sorted(n for n in self.store.objects if n.startswith(prefix or ""))
        return iter([SimpleNamespace(name=n) for n in names][:max_results])


class FakeStorage:
    def __init__(self):
        self.objects, self.calls, self.buckets, self.lists = _Objects(), [], [], []
        self.fail_on, self.fail_delete, self.content_types = {}, set(), {}
        self.exists_exc = self.reload_exc = self.after_reload = self.list_exc = None
        self.delete_generations = []

    def generation(self, name):
        return self.objects.generations.get(name) if name in self.objects else None

    def bucket(self, name):
        self.buckets.append(name)
        return FakeBucket(self)

    def uploads(self):
        return [c[1] for c in self.calls if c[0] == "upload"]

    def deletes(self):
        return [c[1] for c in self.calls if c[0] == "delete"]


class _Objects(dict):
    """The fake bucket's {name: bytes}; every write gets a new generation."""

    def __init__(self):
        super().__init__()
        self.generations, self._next = {}, 1

    def __setitem__(self, name, data):
        super().__setitem__(name, data)
        self.generations[name], self._next = self._next, self._next + 1


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


class Clock:
    """U._monotonic and U._sleep, on a fake clock."""

    def __init__(self):
        self.t, self.slept = 1000.0, []

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


def result(prefix, status="succeeded", **kw):
    """A load result, as load_sso publishes it."""
    if not kw:
        kw = {"job_id": "job-1"} if status == "succeeded" else {"error": "the load failed"}
    return json.dumps({"object_prefix": prefix, "status": status, **kw}).encode()


def ours(status="succeeded", **kw):
    """A script step: the result for the published prefix."""
    return lambda prefix: [result(prefix, status, **kw)]


class FakeSubscriber:
    """A Pub/Sub subscriber on a Clock. Each pull takes the next step of
    ``script``: an exception to raise, or fn(prefix) -> [message bytes]
    (prefix: the last published message's), optionally (fn, seconds the
    pull takes). With no steps left, a pull waits out its timeout and
    raises DeadlineExceeded."""

    def __init__(self, clock, pub, script=None):
        self.clock, self.pub = clock, pub
        self.script = [ours()] if script is None else list(script)
        self.pulls, self.acks, self.entered, self.exited = [], [], 0, 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *exc):
        self.exited += 1
        return False

    def pull(self, subscription, max_messages, timeout):
        from google.api_core.exceptions import DeadlineExceeded
        self.pulls.append((subscription, max_messages, timeout))
        prefix = json.loads(self.pub.published[-1][1])["object_prefix"] if self.pub.published else None
        if not self.script:
            self.clock.t += timeout
            raise DeadlineExceeded("no message")
        step = self.script.pop(0)
        if isinstance(step, BaseException):
            self.clock.t += 1
            raise step
        fn, dt = step if isinstance(step, tuple) else (step, 2.0)
        self.clock.t += dt
        n = len(self.pulls)
        return SimpleNamespace(received_messages=[
            SimpleNamespace(ack_id=f"a{n}.{i}", message=SimpleNamespace(data=d))
            for i, d in enumerate(fn(prefix))])

    def acknowledge(self, subscription, ack_ids):
        assert subscription == SUB_PATH
        self.acks += list(ack_ids)


@pytest.fixture
def fakes(monkeypatch):
    """Fake GCS, publisher and subscriber (the load succeeds), on a fake
    clock."""
    pytest.importorskip("google.api_core.exceptions")
    st, pub, clock = FakeStorage(), FakePublisher(), Clock()
    sub = FakeSubscriber(clock, pub)
    monkeypatch.setattr(U, "_storage_client", lambda: st)
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    monkeypatch.setattr(U, "_subscriber_client", lambda: sub)
    monkeypatch.setattr(U, "_monotonic", clock.monotonic)
    monkeypatch.setattr(U, "_sleep", clock.sleep)
    # a new prefix per upload, even within one millisecond (fixed_prefix,
    # after this fixture, replaces it)
    n = itertools.count(1)
    monkeypatch.setattr(U, "generate_prefix", lambda now=None: f"20260914T2004{next(n):05d}")
    return SimpleNamespace(st=st, pub=pub, sub=sub, clock=clock)


def _boom(what):
    def boom():
        raise AssertionError(f"a {what} client was created")
    return boom


@pytest.fixture
def no_remote(monkeypatch):
    """Any attempt to make a client fails the test."""
    monkeypatch.setattr(U, "_storage_client", _boom("storage"))
    monkeypatch.setattr(U, "_publisher_client", _boom("publisher"))
    monkeypatch.setattr(U, "_subscriber_client", _boom("subscriber"))


# --------------------------------------------------------------------------
# A stand-in delivery
# --------------------------------------------------------------------------

#: SSObservation's parts in make_run: rows of each, and the row group size
#: they are written with.
PART_ROWS = (7, 5)
PART_RG = 3
#: Rows of each other table in make_run.
TABLE_ROWS = 2

#: The stand-in SSObservation schema.
SSOBS_SCHEMA = pa.schema([
    pa.field("obsid", pa.string(), nullable=False),
    pa.field("ssObjectId", pa.int64()),
    pa.field("band", pa.dictionary(pa.int32(), pa.string()), nullable=False),
    pa.field("midpointMjdTai", pa.float64(), nullable=False),
])


def part_table(k, n, start=0):
    base = 100 * k + start
    return pa.table({"obsid": [f"o{base + i:05d}" for i in range(n)],
                     "ssObjectId": [None if i == 2 else base + i // 2 for i in range(n)],
                     "band": pa.array(["gri"[(base + i) % 3] for i in range(n)]).dictionary_encode(),
                     "midpointMjdTai": [60000.0 + base + i / 10 for i in range(n)]}, schema=SSOBS_SCHEMA)


def write_part(path, k, n, start=0):
    pq.write_table(part_table(k, n, start), path, compression="zstd", row_group_size=PART_RG)


def write_table(path, table, n, extra=0):
    """A stand-in single-file table of ``n`` rows (``extra`` random
    float64 columns' worth of bytes, to make it big)."""
    cols = {"table": [table] * n, "i": list(range(n))}
    if extra:
        cols["x"] = list(os.urandom(8 * extra))[:n] if n else []
    pq.write_table(pa.table(cols), path, compression="none")


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


def table_record(p, rows):
    return {"file": f"{C.DELIVERY_DIR}/{p.name}", "rows": rows, "md5": U.md5_file(p),
            "bytes": p.stat().st_size}


def make_run(tmp_path, deliverable=True, tables=C.DELIVERY_TABLES):
    run = tmp_path / "run"
    (run / C.DELIVERY_DIR).mkdir(parents=True)
    rec = {}
    for t in tables:
        if t == "SSObservation":
            rec[t] = make_partitioned(run)
            continue
        p = run / C.DELIVERY_DIR / f"{t}.parquet"
        write_table(p, t, TABLE_ROWS)
        rec[t] = table_record(p, TABLE_ROWS)
    report = {"inputs": {}, "ssp_tools_commit": "abc", "steps": {s: {"status": "ok"} for s in C.BUILD_STEPS},
              "tables": rec, "checks": {"schema": {"status": "PASS"}}, "deliverable": deliverable}
    (run / C.REPORT_FILE).write_text(json.dumps(report))
    return run


def set_table(run, table, rows, extra=0, report_rows=None):
    """Rewrite single-file ``table`` with ``rows`` rows, and its report
    entry to match (rows: ``report_rows`` if given)."""
    p = run / C.DELIVERY_DIR / f"{table}.parquet"
    write_table(p, table, rows, extra)
    r = report(run)
    r["tables"][table] = table_record(p, rows if report_rows is None else report_rows)
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    return p


def rewrite_ssobservation(run, write, manifest_rows=None):
    """Rewrite the delivery's SSObservation parts with ``write(k, path)``,
    and its manifest and report entry to match (a rebuilt delivery);
    ``manifest_rows(k, rows)`` may change the rows the manifest gives."""
    d = run / C.DELIVERY_DIR
    mpath = d / SSOBSERVATION_MANIFEST_FILE
    m = json.loads(mpath.read_text())
    for k, e in enumerate(m["parts"]):
        p = d / e["file"]
        write(k, p)
        n = pq.ParquetFile(p).metadata.num_rows
        e.update(_part_entry(p, manifest_rows(k, n) if manifest_rows else n))
    m["rows"] = sum(e["rows"] for e in m["parts"])
    mpath.write_text(json.dumps(m))
    r = report(run)
    r["tables"]["SSObservation"].update(manifest_md5=U.md5_file(mpath), rows=m["rows"],
                                        bytes=sum(e["bytes"] for e in m["parts"]))
    (run / C.REPORT_FILE).write_text(json.dumps(r))


def report(run):
    return json.loads((run / C.REPORT_FILE).read_text())


def parts_of(run):
    return sorted((run / C.DELIVERY_DIR).glob("SSObservation.part*.parquet"))


def sources(run, tables=SIX):
    """The local files of ``tables``' parts, in upload order."""
    out = []
    for t in (t for t in C.UPLOAD_TABLE_ORDER if t in tables):
        out += parts_of(run) if t == "SSObservation" else [run / C.DELIVERY_DIR / f"{t}.parquet"]
    return out


def part_objects(prefix, tables=SIX, n_ssobs=len(PART_ROWS)):
    """The part objects of ``tables``, in upload order."""
    out = []
    for t in (t for t in C.UPLOAD_TABLE_ORDER if t in tables):
        n = n_ssobs if t == "SSObservation" else 1
        out += [f"{prefix}/{t}/part-{i:05d}.parquet" for i in range(n)]
    return out


def objects_of(prefix, tables=SIX):
    """Every object of an upload, in the order written: the parts, then the
    manifest."""
    return part_objects(prefix, tables) + [f"{prefix}/manifest.json"]


def message(prefix, bucket=BUCKET):
    return json.dumps({"bucket": bucket, "object_prefix": prefix})


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
                                                  ("int", "ppdb-int-sso-ingest", "ppdb-int-6c62"),
                                                  ("prod", "ppdb-prod-sso-ingest", "ppdb-prod-84a4")])
def test_configs(env, bucket, project):
    """The three configs, prod included; no topic key (a fixed name)."""
    _, cfg = U.load_config(env)
    assert cfg == {"bucket_name": bucket, "project": project, "tables": list(SIX)}


def test_unknown_config_name():
    with pytest.raises(U.SSOUploadError, match="no config named"):
        U.load_config("nosuchenv")


def test_old_topic_key(tmp_path, caplog):
    """An old config's topic is accepted only if it is load-sso-topic."""
    p = tmp_path / "c.yaml"
    p.write_text("bucket_name: b\ntopic: load-sso-topic\nproject: p\n")
    with caplog.at_level("INFO"):
        assert U.load_config(str(p))[1]["bucket_name"] == "b"
    assert "no longer needed" in caplog.text
    p.write_text("bucket_name: b\ntopic: my-topic\nproject: p\n")
    with pytest.raises(U.SSOUploadError, match="topic is 'my-topic'.*'load-sso-topic'"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\ntopic: null\nproject: p\n")
    with pytest.raises(U.SSOUploadError, match="topic is None"):
        U.load_config(str(p))


def test_old_topic_key_never_reaches_the_topic(fakes, tmp_path, fixed_prefix):
    """The topic published to is always load-sso-topic."""
    run = make_run(tmp_path)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(f"bucket_name: {BUCKET}\ntopic: load-sso-topic\nproject: {PROJECT}\n")
    U.run(str(cfg), run)
    assert [t for t, _ in fakes.pub.published] == [TOPIC_PATH]


def test_config_name_vs_path(tmp_path, monkeypatch):
    # a bare name is looked up in config/sso-upload/, never in the cwd
    monkeypatch.chdir(tmp_path)
    (tmp_path / "dev").write_text("bucket_name: evil\nproject: p\n")
    assert U.load_config("dev")[1]["bucket_name"] == "ppdb-dev-sso-ingest"
    # a path: *.yaml, *.yml, or with a /
    for name in ("c.yaml", "c.yml"):
        (tmp_path / name).write_text("bucket_name: b\nproject: p\n")
        assert U.load_config(name)[1]["bucket_name"] == "b"
    assert U.load_config("./dev")[1]["bucket_name"] == "evil"
    with pytest.raises(U.SSOUploadError, match="no config file"):
        U.load_config("missing.yaml")


def test_config_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("bucket_name: b\n")
    with pytest.raises(U.SSOUploadError, match="missing"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\nproject: p\nbukcet: x\n")
    with pytest.raises(U.SSOUploadError, match="unknown"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\nproject: p\ntables: [SSObservation]\n")
    assert U.load_config(str(p))[1]["tables"] == ["SSObservation"]
    p.write_text("# nothing\n")
    with pytest.raises(U.SSOUploadError, match="no settings"):
        U.load_config(str(p))


def test_nearbysso_in_the_default_set(no_remote, tmp_path, capsys):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    assert rec["tables"] == list(SIX)
    out = capsys.readouterr().out
    assert f"gs://ppdb-dev-sso-ingest/{rec['object_prefix']}/NearbySSO/part-00000.parquet" in out
    # and with a config without tables
    cfg = tmp_path / "c.yaml"
    cfg.write_text("bucket_name: b\nproject: p\n")
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
    with pytest.raises(U.SSOUploadError, match="mixes snapshots.*--allow-partial"):
        U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"])
    rec = U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"], allow_partial=True)
    assert rec["tables"] == ["SSObservation", "SSObject"]       # the upload order
    assert list(rec["manifest"]["tables"]) == ["SSObservation", "SSObject"]
    # the config's six in any order count as complete
    rec = U.run("dev", run, dry_run=True, tables=list(reversed(SIX)))
    assert rec["tables"] == list(SIX)
    with pytest.raises(U.SSOUploadError, match="not delivery tables"):
        U.run("dev", run, tables=["DiaSource"])


# --------------------------------------------------------------------------
# The manifest
# --------------------------------------------------------------------------

def test_manifest_contents(no_remote, tmp_path):
    """Exactly dax_ppdb's form: SHA-256, rows from the Parquet metadata,
    sizes, absolute source paths; the tables in SSO_TABLES order, the parts
    numbered per table in manifest order."""
    run = make_run(tmp_path)
    t0 = datetime.now(timezone.utc)
    m = U.run("dev", run, dry_run=True)["manifest"]
    assert list(m) == ["uploaded_at", "tables"]
    at = datetime.fromisoformat(m["uploaded_at"].replace("Z", "+00:00"))
    assert m["uploaded_at"].endswith("Z") and at.utcoffset() == timedelta(0)
    assert t0 - timedelta(seconds=1) <= at <= datetime.now(timezone.utc)
    assert list(m["tables"]) == list(SIX)
    assert list(m["tables"]["SSObservation"]) == ["part-00000.parquet", "part-00001.parquet"]
    for t in SIX[1:]:
        assert list(m["tables"][t]) == ["part-00000.parquet"]
    files = sources(run)
    entries = [e for t in SIX for e in m["tables"][t].values()]
    assert len(entries) == len(files)
    for e, p in zip(entries, files, strict=True):
        assert list(e) == ["row_count", "checksum", "size_bytes", "source_path"]
        assert e["source_path"] == os.path.abspath(p) and os.path.isabs(e["source_path"])
        assert e["checksum"] == hashlib.sha256(p.read_bytes()).hexdigest()
        assert e["size_bytes"] == p.stat().st_size
        assert e["row_count"] == pq.read_metadata(p).num_rows
        assert type(e["row_count"]) is int and type(e["size_bytes"]) is int
    assert [e["row_count"] for e in m["tables"]["SSObservation"].values()] == list(PART_ROWS)
    assert all(e["row_count"] == TABLE_ROWS for t in SIX[1:] for e in m["tables"][t].values())


def test_manifest_source_path_absolute_from_a_relative_run_dir(no_remote, tmp_path, monkeypatch):
    run = make_run(tmp_path)
    monkeypatch.chdir(tmp_path)
    m = U.run("dev", "run", dry_run=True)["manifest"]
    assert m["tables"]["SSObject"]["part-00000.parquet"]["source_path"] == str(
        run / C.DELIVERY_DIR / "SSObject.parquet")


def test_manifest_validates_with_the_dax_ppdb_model(fakes, tmp_path, fixed_prefix):
    """The manifest uploaded (and the one recorded) validates under a copy
    of dax_ppdb's model (extra="forbid", row_count and size_bytes >= 1)."""
    Manifest = dax_ppdb_manifest_model()
    run = make_run(tmp_path)
    rec = U.run("dev", run)
    raw = fakes.st.objects[f"{PREFIX}/manifest.json"]
    m = Manifest.model_validate_json(raw.decode("utf-8"))
    assert m.uploaded_at.utcoffset() == timedelta(0)
    assert {t: list(v) for t, v in m.tables.items()} == {
        t: list(v) for t, v in rec["manifest"]["tables"].items()}
    assert json.loads(raw) == rec["manifest"]
    # an extra key, or an empty file, is what the PPDB refuses
    from pydantic import ValidationError
    bad = json.loads(raw)
    bad["uploaded_tables"] = list(SIX)
    with pytest.raises(ValidationError):
        Manifest.model_validate(bad)
    bad = json.loads(raw)
    bad["tables"]["SSObject"]["part-00000.parquet"]["row_count"] = 0
    with pytest.raises(ValidationError):
        Manifest.model_validate(bad)
    bad = json.loads(raw)
    bad["tables"]["SSObject"]["part-00000.parquet"]["md5"] = "x"
    with pytest.raises(ValidationError):
        Manifest.model_validate(bad)


def test_manifest_serialization():
    """JSON indented by 2, as dax_ppdb's model_dump_json(indent=2)."""
    m = {"uploaded_at": "2026-11-23T12:00:00.123456Z", "tables": {"SSObject": {"part-00000.parquet": {
        "row_count": 1, "checksum": "a" * 64, "size_bytes": 2, "source_path": "/x/SSObject.parquet"}}}}
    s = U.manifest_json(m)
    assert json.loads(s) == m
    assert s.startswith('{\n  "uploaded_at": "2026-11-23T12:00:00.123456Z",\n  "tables": {\n')
    assert U._iso_utc(datetime(2026, 11, 23, 4, 0, 0, 123456, tzinfo=timezone(timedelta(hours=-8)))) == \
        "2026-11-23T12:00:00.123456Z"


def test_one_read_per_file(no_remote, tmp_path, monkeypatch):
    """Each delivered file is read once, for its md5 and SHA-256 together;
    md5_file is used only for SSObservation's manifest."""
    run = make_run(tmp_path)
    hashed, md5ed = [], []
    orig_hash, orig_md5 = U.hash_file, U.md5_file
    monkeypatch.setattr(U, "hash_file", lambda p, *a: hashed.append(p.name) or orig_hash(p, *a))
    monkeypatch.setattr(U, "md5_file", lambda p, *a: md5ed.append(p.name) or orig_md5(p, *a))
    U.run("dev", run, dry_run=True)
    assert sorted(hashed) == sorted(p.name for p in sources(run))
    assert md5ed == [SSOBSERVATION_MANIFEST_FILE]
    assert U.hash_file(sources(run)[0]) == (orig_md5(sources(run)[0]),
                                            hashlib.sha256(sources(run)[0].read_bytes()).hexdigest())


def test_build_manifest_refusals(tmp_path):
    run = make_run(tmp_path)
    files = U.verify_delivery(run, report(run), list(SIX))
    assert U.build_manifest(files)["tables"]["SSObject"]
    f = files["SSObject"][0]
    for bad, match in [
        ({"SSObject": [U.LocalFile(run / C.DELIVERY_DIR / SIDECAR_FILE, 1, 1, "m", "s")]}, "never uploaded"),
        ({"SSObject": [U.LocalFile(run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE, 1, 1, "m", "s")]},
         "never uploaded"),
        ({"SSObject": [f.__class__(f.path, 0, f.size, f.md5, f.sha256)]}, "no empty file"),
        ({"SSObject": [f.__class__(f.path, 1, 0, f.md5, f.sha256)]}, "no empty file"),
        ({"SSObject": []}, "no empty table"),
        ({"SSObject": [f], "mpc_orbits": [f]}, "more than once"),
        ({"SSObject": [f.__class__(U.Path("rel.parquet"), 1, 1, "m", "s")]}, "not an absolute path"),
        ({"DiaSource": [f]}, "not upload tables"),
    ]:
        with pytest.raises(U.SSOUploadError, match=match):
            U.build_manifest(bad)


def test_upload_refuses_a_bad_manifest(fakes, tmp_path):
    """upload() checks the manifest it is given: a sidecar or SSObservation's
    manifest as a part, misnamed parts, extra keys, unknown tables."""
    run = make_run(tmp_path)
    good = U.build_manifest(U.verify_delivery(run, report(run), list(SIX)))
    cfg = U.load_config("dev")[1]

    def edited(fn):
        m = json.loads(json.dumps(good))
        fn(m)
        return m
    side = str(run / C.DELIVERY_DIR / SIDECAR_FILE)
    mfile = str(run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE)
    for m, match in [
        (edited(lambda m: m["tables"]["SSObject"]["part-00000.parquet"].update(source_path=side)),
         "never uploaded"),
        (edited(lambda m: m["tables"]["SSObservation"]["part-00001.parquet"].update(source_path=mfile)),
         "never uploaded"),
        (edited(lambda m: m["tables"].update(SSObject={"SSObject.parquet":
                                                        m["tables"]["SSObject"]["part-00000.parquet"]})),
         "named 'SSObject.parquet'"),
        (edited(lambda m: m["tables"]["SSObject"]["part-00000.parquet"].update(md5="x")),
         "exactly the fields"),
        (edited(lambda m: m.update(uploaded_tables=list(SIX))), "exactly the fields"),
        (edited(lambda m: m["tables"].update(DiaSource={})), "not upload tables"),
        (edited(lambda m: m["tables"].update(SSObject={})), "no parts"),
        (edited(lambda m: m.update(tables={})), "no tables"),
    ]:
        with pytest.raises(U.SSOUploadError, match=match):
            U.upload(cfg, m)
    assert fakes.st.calls == [] and fakes.st.lists == [] and fakes.pub.published == []


# --------------------------------------------------------------------------
# The upload
# --------------------------------------------------------------------------

def test_upload_success_and_message(fakes, tmp_path, capsys):
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    rec = U.run("dev", run)
    prefix = rec["object_prefix"]
    assert re.fullmatch(r"\d{8}T\d{9}", prefix)
    assert st.buckets == ["ppdb-dev-sso-ingest"]
    # the prefix checked empty first, with one object at most
    assert st.lists == [(f"{prefix}/", 1)]
    # the parts in table and part order, then the manifest, last; nothing else
    assert st.uploads() == objects_of(prefix)
    assert sorted(st.objects) == sorted(objects_of(prefix))
    assert all(c[2] == 0 for c in st.calls if c[0] == "upload")    # if_generation_match=0
    for name, p in zip(part_objects(prefix), sources(run), strict=True):
        assert st.objects[name] == p.read_bytes()
    assert st.content_types == {f"{prefix}/manifest.json": "application/json"}
    assert json.loads(st.objects[f"{prefix}/manifest.json"]) == rec["manifest"]
    assert st.objects[f"{prefix}/manifest.json"].decode() == U.manifest_json(rec["manifest"])
    # the message: exactly {"bucket", "object_prefix"}, as json.dumps
    assert len(pub.published) == 1
    topic, data = pub.published[0]
    assert topic == TOPIC_PATH
    assert data == b'{"bucket": "ppdb-dev-sso-ingest", "object_prefix": "' + prefix.encode() + b'"}'
    assert data == message(prefix).encode("utf-8")
    assert tuple(json.loads(data)) == C.UPLOAD_MESSAGE_FIELDS
    assert pub.futures[0].timeouts == [U.PUBLISH_TIMEOUT_S]
    # the wait
    assert rec["load"] == {"status": "succeeded", "job_id": "job-1", "waited_s": 2.0}
    assert "loaded: job job-1" in capsys.readouterr().out
    r = report(run)
    assert r["upload"] == rec and r["uploads"] == [rec]
    assert list(rec) == ["config", "bucket", "object_prefix", "tables", "message_id", "dry_run",
                         "published", "utc", "parallel", "manifest", "load"]
    assert "ssobservation" not in rec
    # parallel by default, but nothing here is big enough to go in chunks
    assert rec["parallel"] == {"chunk_bytes": 64 << 20, "workers": 16, "min_bytes": 128 << 20,
                               "tables": [], "parts": []}
    assert rec["message_id"] == "msg-123" and rec["dry_run"] is False and rec["published"] is True
    # nothing written in RUN_DIR but report.json
    assert sorted(p.name for p in run.iterdir()) == [C.DELIVERY_DIR, C.REPORT_FILE]


def test_manifest_written_last(fakes, tmp_path, fixed_prefix):
    """The manifest is the last object written, with if_generation_match=0,
    and only after every part is in the bucket."""
    st = fakes.st
    run = make_run(tmp_path)
    seen = {}
    orig = FakeBlob.upload_from_string

    def spy(self, data, content_type=None, if_generation_match=None):
        seen["objects then"] = sorted(st.objects)
        seen["igm"] = if_generation_match
        return orig(self, data, content_type=content_type, if_generation_match=if_generation_match)
    FakeBlob.upload_from_string = spy
    try:
        U.run("dev", run)
    finally:
        FakeBlob.upload_from_string = orig
    assert seen == {"objects then": sorted(part_objects(PREFIX)), "igm": 0}
    assert st.calls[-1] == ("upload", f"{PREFIX}/manifest.json", 0)


def test_upload_order_whatever_the_config_order(fakes, tmp_path, fixed_prefix):
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    cfg = tmp_path / "c.yaml"
    cfg.write_text("bucket_name: b\nproject: p\ntables: [" + ", ".join(reversed(SIX)) + "]\n")
    fakes.sub.script = []           # not waited for below
    rec = U.run(str(cfg), run, wait=False)
    assert rec["tables"] == list(SIX) and list(rec["manifest"]["tables"]) == list(SIX)
    assert st.uploads() == objects_of(PREFIX)
    assert pub.published == [("projects/p/topics/load-sso-topic", message(PREFIX, bucket="b").encode())]


def test_refuses_an_occupied_prefix(fakes, tmp_path, fixed_prefix):
    """Any object under the prefix refuses the upload before anything is
    written; the object is left alone."""
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    st.objects[f"{PREFIX}/something/else"] = b"someone else's"
    with pytest.raises(U.SSOUploadError, match="already holds objects"):
        U.run("dev", run)
    assert st.lists == [(f"{PREFIX}/", 1)]
    assert st.calls == [] and pub.published == []
    assert st.objects == {f"{PREFIX}/something/else": b"someone else's"}
    assert "upload" not in report(run)
    # a neighbouring prefix that only starts the same is not ours to refuse
    st.objects.clear()
    st.objects[f"{PREFIX}0/x"] = b"other"
    U.run("dev", run)
    assert f"{PREFIX}/manifest.json" in st.objects


def test_prefix_check_failure_is_wrapped(fakes, tmp_path, fixed_prefix):
    from google.api_core.exceptions import Forbidden
    run = make_run(tmp_path)
    fakes.st.list_exc = Forbidden("no storage.objects.list")
    with pytest.raises(U.SSOUploadError, match="can't check whether .* is empty"):
        U.run("dev", run)
    assert fakes.st.calls == [] and U.main(["dev", str(run)]) == 1


def test_precondition_failure_aborts_and_cleans_up(fakes, tmp_path, fixed_prefix, monkeypatch):
    """An object appears at a part's name after the prefix check (a
    colliding upload): it is untouched, ours are deleted."""
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    names = part_objects(PREFIX)
    theirs = names[3]
    orig = FakeBucket.list_blobs

    def then_theirs(self, prefix=None, max_results=None):
        out = orig(self, prefix, max_results)
        st.objects[theirs] = b"someone else's"
        return out
    monkeypatch.setattr(FakeBucket, "list_blobs", then_theirs)
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run)
    assert st.objects == {theirs: b"someone else's"}
    assert st.deletes() == names[:3]
    assert pub.published == [] and "upload" not in report(run)


@pytest.mark.parametrize("k", range(len(objects_of(PREFIX))))
@pytest.mark.parametrize("how", ["before-write", "after-write"])
def test_failure_on_any_object_cleans_up(fakes, tmp_path, fixed_prefix, k, how):
    """A failure on any part, or on the manifest, deletes everything this
    upload wrote, the manifest first, the object of the failed call too (it
    may have been written); nothing is published."""
    from google.api_core.exceptions import ServiceUnavailable
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    names = objects_of(PREFIX)
    exc = ServiceUnavailable("injected")
    st.fail_on[names[k]] = ("after-write", exc) if how == "after-write" else exc
    with pytest.raises(U.SSOUploadError, match="failed to upload") as ei:
        U.run("dev", run)
    assert ei.value.__cause__ is exc
    assert st.objects == {} and pub.published == []
    deleted = names[:k + 1]
    if names[k].endswith("manifest.json"):
        deleted = [names[k]] + names[:k]        # the manifest first
    assert st.deletes() == deleted
    assert "upload" not in report(run) and fakes.sub.pulls == []


@pytest.mark.parametrize("exc", ["auth", "other"])
def test_any_upload_error_is_wrapped(fakes, tmp_path, fixed_prefix, exc):
    from google.auth.exceptions import RefreshError
    st = fakes.st
    run = make_run(tmp_path)
    st.fail_on[part_objects(PREFIX)[1]] = (
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
    st = fakes.st
    run = make_run(tmp_path)
    names = part_objects(PREFIX)
    st.fail_on[names[4]] = ServiceUnavailable("injected")
    st.fail_delete.add(names[0])
    with pytest.raises(U.SSOUploadError, match="failed to upload") as ei:
        U.run("dev", run)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    assert st.deletes() == names[:5]
    assert set(st.objects) == {names[0]}     # the one that wouldn't delete


def test_a_part_changed_since_the_manifest(fakes, tmp_path, fixed_prefix):
    """A file whose size changed between the checks and its upload is
    refused, and what was written is deleted."""
    st = fakes.st
    run = make_run(tmp_path)
    orig = FakeBlob.upload_from_filename
    victim = run / C.DELIVERY_DIR / "SSObject.parquet"

    def grow(self, filename, if_generation_match=None):
        orig(self, filename, if_generation_match)
        if self.name.endswith("NearbySSO/part-00000.parquet"):
            with open(victim, "ab") as f:
                f.write(b"more")
    FakeBlob.upload_from_filename = grow
    try:
        with pytest.raises(U.SSOUploadError, match="SSObject.parquet is .* bytes now.*it changed"):
            U.run("dev", run)
    finally:
        FakeBlob.upload_from_filename = orig
    assert st.objects == {} and fakes.pub.published == []


def test_publish_failure_cleans_up(fakes, tmp_path, monkeypatch, fixed_prefix):
    """publish() itself fails: the message never left, so the objects are
    deleted, the manifest first."""
    from google.api_core.exceptions import ServiceUnavailable
    st = fakes.st
    pub = FakePublisher(publish_exc=ServiceUnavailable("injected"))
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="failed to publish"):
        U.run("dev", run)
    assert pub.published == []
    assert st.objects == {}
    assert st.deletes() == [f"{PREFIX}/manifest.json"] + part_objects(PREFIX)
    assert "upload" not in report(run)


def test_publisher_creation_failure_cleans_up(fakes, tmp_path, monkeypatch):
    monkeypatch.setattr(U, "_publisher_client", _boom("publisher"))
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="failed to publish"):
        U.run("dev", run)
    assert fakes.st.objects == {} and "upload" not in report(run)


def test_publish_error_after_handover_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix):
    """The message was handed to Pub/Sub, then the wait failed: it may have
    gone through, so every object, the manifest included, is kept and the
    upload recorded; nothing is waited for."""
    from google.api_core.exceptions import ServiceUnavailable
    st = fakes.st
    pub = FakePublisher(exc=ServiceUnavailable("injected"))
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.PublishUnconfirmedError, match="interrupted or failed.*KEPT") as ei:
        U.run("dev", run)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    assert sorted(st.objects) == sorted(objects_of(PREFIX))
    assert st.deletes() == [] and fakes.sub.pulls == []
    up = report(run)["upload"]
    assert up["published"] is None and up["object_prefix"] == PREFIX and "ServiceUnavailable" in up["error"]
    assert up["load"] == {"status": "not_waited", "waited_s": 0}
    assert up["manifest"] == json.loads(st.objects[f"{PREFIX}/manifest.json"])
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)
    assert U.main(["dev", str(run), "--force"]) == 1


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_publish_interrupted_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix, exc):
    """Ctrl-C (or SystemExit) while waiting for the confirmation: the
    objects are kept, the upload recorded with published null, and the
    interrupt propagates."""
    st = fakes.st
    pub = FakePublisher(exc=exc())
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(exc):
        U.run("dev", run)
    assert sorted(st.objects) == sorted(objects_of(PREFIX))
    assert st.deletes() == []
    r = report(run)
    assert r["upload"]["published"] is None and r["upload"]["object_prefix"] == PREFIX
    err = r["upload"]["error"]
    assert "interrupted" in err and "may or may not have gone through" in err
    assert r["uploads"] == [r["upload"]]
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


def test_interrupt_during_upload_still_cleans_up(fakes, tmp_path, fixed_prefix):
    """Before the message is handed over, even a KeyboardInterrupt cleans
    up, including the object being written."""
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    names = part_objects(PREFIX)
    st.fail_on[names[2]] = ("after-write", KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        U.run("dev", run)
    assert st.objects == {} and pub.published == []
    assert st.deletes() == names[:3]
    assert "upload" not in report(run)


def test_publish_timeout_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix):
    st = fakes.st
    pub = FakePublisher(exc=concurrent.futures.TimeoutError())
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.PublishUnconfirmedError, match="KEPT"):
        U.run("dev", run)
    assert sorted(st.objects) == sorted(objects_of(PREFIX))
    assert st.deletes() == []
    r = report(run)
    assert r["upload"]["object_prefix"] == PREFIX and r["upload"]["message_id"] is None
    assert r["upload"]["published"] is None
    assert "wasn't confirmed" in r["upload"]["error"]
    # and a plain re-upload is refused: the message may have gone through
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


# --------------------------------------------------------------------------
# Waiting for the load
# --------------------------------------------------------------------------

def test_wait_success_exit_0(fakes, tmp_path, fixed_prefix):
    run = make_run(tmp_path)
    assert U.main(["dev", str(run)]) == 0
    up = report(run)["upload"]
    assert up["load"] == {"status": "succeeded", "job_id": "job-1", "waited_s": 2.0}
    sub = fakes.sub
    assert sub.pulls == [(SUB_PATH, U.PULL_MAX_MESSAGES, U.PULL_TIMEOUT_S)]
    assert sub.acks == ["a1.0"] and sub.entered == sub.exited == 1


def test_wait_failure_exit_3(fakes, tmp_path, fixed_prefix, capsys, caplog):
    run = make_run(tmp_path)
    fakes.sub.script = [ours("failed", error="row count mismatch for SSObject")]
    assert U.main(["dev", str(run)]) == C.UPLOAD_EXIT_LOAD_FAILED == 3
    up = report(run)["upload"]
    assert up["load"] == {"status": "failed", "error": "row count mismatch for SSObject", "waited_s": 2.0}
    assert up["published"] is True
    assert "FAILED: row count mismatch for SSObject" in capsys.readouterr().out
    assert "row count mismatch for SSObject" in caplog.text
    # the objects stay: the PPDB loaded nothing, and the upload counts
    assert f"{PREFIX}/manifest.json" in fakes.st.objects
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


def test_wait_timeout_exit_4(fakes, tmp_path, fixed_prefix, capsys):
    """No result: pulls of at most 60 s until the wait (5 min) is over."""
    run = make_run(tmp_path)
    fakes.sub.script = []
    assert U.main(["dev", str(run)]) == C.UPLOAD_EXIT_LOAD_TIMEOUT == 4
    up = report(run)["upload"]
    assert up["load"] == {"status": "timeout", "waited_s": 300.0}
    timeouts = [p[2] for p in fakes.sub.pulls]
    assert timeouts == [60] * 5 and sum(timeouts) == U.WAIT_TIMEOUT_S == 300
    assert "may still load" in capsys.readouterr().out
    assert fakes.sub.exited == 1


def test_wait_timeout_option(fakes, tmp_path, fixed_prefix):
    """--wait-timeout 90: a 60 s pull, then the 30 s left."""
    run = make_run(tmp_path)
    fakes.sub.script = []
    assert U.main(["dev", str(run), "--wait-timeout", "90"]) == 4
    assert [p[2] for p in fakes.sub.pulls] == [60, 30]
    assert report(run)["upload"]["load"]["waited_s"] == 90.0


@pytest.mark.parametrize("bad", ["0", "-5", "nan", "inf"])
def test_wait_timeout_refused(no_remote, tmp_path, bad):
    run = make_run(tmp_path)
    assert U.main(["dev", str(run), "--dry-run", "--wait-timeout", bad]) == 1
    assert "upload" not in report(run)
    # not checked with --no-wait
    assert U.main(["dev", str(run), "--dry-run", "--no-wait", "--wait-timeout", bad]) == 0


def test_wait_skips_other_prefixes_and_malformed(fakes, tmp_path, fixed_prefix, caplog):
    """Every message is acked; other prefixes' results, and malformed ones,
    are logged and skipped; ours ends the wait."""
    run = make_run(tmp_path)
    fakes.sub.script = [
        lambda p: [result("20261114T115654059"), b"not json", b"[1, 2]",
                   json.dumps({"object_prefix": p, "status": "running"}).encode(),
                   json.dumps({"status": "succeeded"}).encode()],
        lambda p: [],
        lambda p: [result("20261120T000000000", "failed"), result(p, job_id="job-ours"),
                   result(p, "failed")],
    ]
    with caplog.at_level("INFO"):
        assert U.main(["dev", str(run)]) == 0
    up = report(run)["upload"]
    assert up["load"]["status"] == "succeeded" and up["load"]["job_id"] == "job-ours"
    assert fakes.sub.acks == ["a1.0", "a1.1", "a1.2", "a1.3", "a1.4", "a3.0", "a3.1", "a3.2"]
    assert "another upload, 20261114T115654059" in caplog.text
    assert "another upload, 20261120T000000000" in caplog.text
    assert caplog.text.count("malformed load result") == 4


def test_wait_ours_failed_after_others(fakes, tmp_path, fixed_prefix):
    run = make_run(tmp_path)
    fakes.sub.script = [lambda p: [result("x"), result(p, "failed", error="boom")]]
    assert U.main(["dev", str(run)]) == 3
    assert report(run)["upload"]["load"]["error"] == "boom"


def test_wait_deadline_exceeded_then_result(fakes, tmp_path, fixed_prefix):
    """A pull that raises DeadlineExceeded (nothing yet) is not an error."""
    from google.api_core.exceptions import DeadlineExceeded
    run = make_run(tmp_path)
    fakes.sub.script = [DeadlineExceeded("nothing"), ours()]
    assert U.main(["dev", str(run)]) == 0
    assert len(fakes.sub.pulls) == 2


def test_wait_never_spins(fakes, tmp_path, fixed_prefix):
    """Empty pulls that return at once are spaced out by a sleep."""
    run = make_run(tmp_path)
    fakes.sub.script = [(lambda p: [], 0.0)] * 3 + [ours()]
    assert U.main(["dev", str(run)]) == 0
    assert fakes.clock.slept == [1.0, 1.0, 1.0]


@pytest.mark.parametrize("where", ["pull", "client"])
def test_wait_read_error_exit_4(fakes, tmp_path, fixed_prefix, monkeypatch, where):
    """The results can't be read (e.g. no subscriber permission): the
    upload is published, the load unknown: exit 4, the error recorded."""
    from google.api_core.exceptions import PermissionDenied
    run = make_run(tmp_path)
    if where == "pull":
        fakes.sub.script = [PermissionDenied("no pubsub.subscriptions.consume")]
    else:
        def no_sub():
            raise PermissionDenied("no subscriber")
        monkeypatch.setattr(U, "_subscriber_client", no_sub)
    assert U.main(["dev", str(run)]) == 4
    load = report(run)["upload"]["load"]
    assert load["status"] == "timeout" and "can't read the load results" in load["error"]
    assert report(run)["upload"]["published"] is True


def test_wait_ack_failure_still_reads_the_result(fakes, tmp_path, fixed_prefix, caplog):
    run = make_run(tmp_path)

    def bad_ack(subscription, ack_ids):
        raise RuntimeError("ack failed")
    fakes.sub.acknowledge = bad_ack
    assert U.main(["dev", str(run)]) == 0
    assert "failed to acknowledge" in caplog.text


def test_recorded_before_the_wait(fakes, tmp_path, fixed_prefix):
    """The published upload is in report.json, load null, while it waits."""
    run = make_run(tmp_path)
    seen = []

    def look(prefix):
        seen.append(report(run)["upload"])
        return [result(prefix)]
    fakes.sub.script = [look]
    rec = U.run("dev", run)
    (during,) = seen
    assert during["published"] is True and during["load"] is None and during["object_prefix"] == PREFIX
    assert report(run)["upload"] == rec and report(run)["uploads"] == [rec]


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_wait_interrupted(fakes, tmp_path, fixed_prefix, exc):
    """Ctrl-C during the wait: the upload stays recorded (it is published),
    with the interruption, and the interrupt propagates."""
    run = make_run(tmp_path)
    fakes.sub.script = [exc()]
    with pytest.raises(exc):
        U.run("dev", run)
    up = report(run)["upload"]
    assert up["published"] is True and up["load"]["status"] == "timeout"
    assert "interrupted" in up["load"]["error"]
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


def test_no_wait_does_not_touch_the_subscription(fakes, tmp_path, fixed_prefix, monkeypatch, capsys):
    monkeypatch.setattr(U, "_subscriber_client", _boom("subscriber"))
    run = make_run(tmp_path)
    assert U.main(["dev", str(run), "--no-wait"]) == 0
    up = report(run)["upload"]
    assert up["published"] is True and up["load"] == {"status": "not_waited", "waited_s": 0}
    assert "--no-wait" in capsys.readouterr().out


def test_exit_status():
    assert U.exit_status(None) == 0
    assert U.exit_status({"load": None}) == 0
    assert [U.exit_status({"load": {"status": s}}) for s in C.UPLOAD_LOAD_STATUSES] == [0, 3, 4, 0]


def test_main_wait_flags(monkeypatch, tmp_path):
    seen = {}
    monkeypatch.setattr(U, "run", lambda *a, **kw: seen.update(kw))
    assert U.main(["dev", str(tmp_path)]) == 0
    assert seen["wait"] is True and seen["wait_timeout"] == 300
    assert U.main(["dev", str(tmp_path), "--no-wait", "--wait-timeout", "12.5"]) == 0
    assert seen["wait"] is False and seen["wait_timeout"] == 12.5


# --------------------------------------------------------------------------
# --no-publish
# --------------------------------------------------------------------------

def test_no_publish(tmp_path, monkeypatch, fixed_prefix, capsys):
    pytest.importorskip("google.api_core.exceptions")
    st = FakeStorage()
    monkeypatch.setattr(U, "_storage_client", lambda: st)
    monkeypatch.setattr(U, "_publisher_client", _boom("publisher"))
    monkeypatch.setattr(U, "_subscriber_client", _boom("subscriber"))
    run = make_run(tmp_path)
    rec = U.run("dev", run, no_publish=True)
    # the parts and the manifest uploaded and kept, nothing published
    assert st.uploads() == objects_of(PREFIX)
    assert sorted(st.objects) == sorted(objects_of(PREFIX))
    assert st.deletes() == []
    out = capsys.readouterr().out
    assert f"gs://ppdb-dev-sso-ingest/{PREFIX}/" in out and "KEPT" in out
    assert message(PREFIX) in out
    assert rec["published"] is False and rec["dry_run"] is False and rec["message_id"] is None
    assert rec["object_prefix"] == PREFIX and rec["load"] == {"status": "not_waited", "waited_s": 0}
    assert rec["manifest"] == json.loads(st.objects[f"{PREFIX}/manifest.json"])
    assert report(run)["upload"] == rec
    assert U.main(["dev", str(run), "--no-publish"]) == 1      # the prefix is taken (fixed here)


def test_no_publish_does_not_block_a_real_upload(fakes, tmp_path):
    pub = fakes.pub
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
    st, pub = fakes.st, fakes.pub
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/manifest.json"] = ServiceUnavailable("injected")
    with pytest.raises(U.SSOUploadError, match="failed to upload the manifest"):
        U.run("dev", run, no_publish=True)
    assert st.objects == {} and pub.published == []


# --------------------------------------------------------------------------
# Dry run, history, refusals
# --------------------------------------------------------------------------

def test_dry_run_touches_nothing_remote(no_remote, tmp_path, capsys, monkeypatch):
    """No client, and no Google library needed; the planned objects, the
    manifest and the message are printed."""
    for mod in ("google", "google.cloud", "google.api_core", "google.api_core.exceptions", "google_crc32c"):
        monkeypatch.setitem(sys.modules, mod, None)       # an import of it fails
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    for p, name in zip(sources(run), part_objects(prefix), strict=True):
        assert f"{p} -> gs://ppdb-dev-sso-ingest/{name}  (" in out
    assert f"then the manifest -> gs://ppdb-dev-sso-ingest/{prefix}/manifest.json" in out
    assert SIDECAR_FILE not in out and SSOBSERVATION_MANIFEST_FILE not in out
    printed = out.split("  the manifest:\n", 1)[1].split("  publish to", 1)[0]
    assert json.loads("\n".join(line[4:] for line in printed.splitlines())) == rec["manifest"]
    assert TOPIC_PATH in out and SUB_PATH in out
    assert message(prefix) in out
    up = report(run)["upload"]
    assert up["dry_run"] is True and up["message_id"] is None and up["published"] is False
    assert up["load"] == {"status": "not_waited", "waited_s": 0}
    assert up["manifest"] == rec["manifest"]
    assert not (run / "upload").exists()
    assert U.main(["dev", str(run), "--dry-run"]) == 0


def test_upload_history(fakes, tmp_path):
    run = make_run(tmp_path)
    dry = U.run("dev", run, dry_run=True)
    real = U.run("dev", run)
    dry2 = U.run("dev", run, dry_run=True)       # doesn't replace the real one as 'upload'
    assert report(run)["upload"] == real
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)
    fakes.sub.script = [ours()]
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


def _flip(p):
    b = bytearray(p.read_bytes())
    b[len(b) // 2] ^= 0xFF          # same size, other content
    p.write_bytes(bytes(b))
    return p


def test_refuses_md5_mismatch(no_remote, tmp_path):
    run = make_run(tmp_path)
    _flip(run / C.DELIVERY_DIR / "SSObject.parquet")
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


@pytest.mark.parametrize("table", ["SSObject", "SSObservation"])
def test_refuses_a_zero_row_file(fakes, tmp_path, table):
    """A file with 0 rows, consistent with its report, refuses the whole
    upload before anything remote is touched."""
    run = make_run(tmp_path)
    if table == "SSObject":
        set_table(run, "SSObject", 0)
        match = r"SSObject: .*SSObject\.parquet has 0 rows.*refusing the whole upload"
    else:
        rewrite_ssobservation(run, lambda k, p: write_part(p, k, 0 if k == 1 else PART_ROWS[k]))
        match = r"SSObservation: .*part0001\.parquet has 0 rows.*refusing the whole upload"
    for kw in ({"dry_run": True}, {}):
        with pytest.raises(U.SSOUploadError, match=match):
            U.run("dev", run, **kw)
    assert fakes.st.calls == [] and fakes.st.lists == [] and fakes.pub.published == []
    assert "upload" not in report(run)
    assert U.main(["dev", str(run)]) == 1


def test_refuses_a_row_count_mismatch(no_remote, tmp_path):
    """The Parquet metadata's rows must equal the report's (per
    SSObservation part, its manifest's)."""
    run = make_run(tmp_path)
    set_table(run, "SSObject", 2, report_rows=3)
    with pytest.raises(U.SSOUploadError, match=r"SSObject\.parquet has 2 rows, the report says 3"):
        U.run("dev", run, dry_run=True)
    set_table(run, "SSObject", 2)
    rewrite_ssobservation(run, lambda k, p: write_part(p, k, PART_ROWS[k]),
                          manifest_rows=lambda k, n: n + 1 if k == 0 else n)
    with pytest.raises(U.SSOUploadError, match=r"part0000\.parquet has 7 rows, the manifest says 8"):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)


def test_refuses_missing_rows_in_the_report(no_remote, tmp_path):
    run = make_run(tmp_path)
    r = report(run)
    del r["tables"]["SSObject"]["rows"]
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="the report gives no rows for SSObject.parquet"):
        U.run("dev", run, dry_run=True)


def test_refuses_a_file_that_is_not_parquet(no_remote, tmp_path):
    run = make_run(tmp_path)
    p = run / C.DELIVERY_DIR / "SSObject.parquet"
    p.write_bytes(b"PAR1 not really PAR1")
    r = report(run)
    r["tables"]["SSObject"] = table_record(p, 1)
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="cannot read the Parquet metadata"):
        U.run("dev", run, dry_run=True)


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
    return _flip(run / C.DELIVERY_DIR / PART_FILE_FORMAT.format(k))


def _unreadable_manifest(run):
    (run / C.DELIVERY_DIR / SSOBSERVATION_MANIFEST_FILE).write_text("{")
    _set_report_manifest_md5(run)


def _manifest_lists(run, name):
    """The manifest (and the report) list ``name`` as SSObservation's last
    part, with its true size and md5."""
    d = run / C.DELIVERY_DIR
    p = d / name

    def edit(m):
        m["parts"][-1].update(file=name, bytes=p.stat().st_size, md5=U.md5_file(p))
    _edit_manifest(run, edit)
    r = report(run)
    r["tables"]["SSObservation"]["parts"][-1] = f"{C.DELIVERY_DIR}/{name}"
    (run / C.REPORT_FILE).write_text(json.dumps(r))


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
    (lambda run: _edit_manifest(run, lambda m: m["parts"][0].pop("rows")), "cannot read"),
    # a sidecar or the manifest offered as a part, consistently everywhere
    (lambda run: _manifest_lists(run, SIDECAR_FILE), f"lists '{SIDECAR_FILE}', not a part"),
    (lambda run: _manifest_lists(run, "NearbySSO_internal.parquet"),
     "lists 'NearbySSO_internal.parquet', not a part"),
    (lambda run: _manifest_lists(run, SSOBSERVATION_MANIFEST_FILE),
     f"lists '{SSOBSERVATION_MANIFEST_FILE}', not a part"),
])
def test_refuses_bad_parts(no_remote, tmp_path, damage, match):
    """Each part is checked against the manifest, and the manifest against
    the report, before anything is uploaded."""
    run = make_run(tmp_path)
    (run / C.DELIVERY_DIR / "NearbySSO_internal.parquet").write_bytes(b"sidecar")
    damage(run)
    with pytest.raises(U.SSOUploadError, match=match):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)


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


def test_refuses_part_rows_that_dont_add_up(no_remote, tmp_path):
    run = make_run(tmp_path)

    def edit(m):
        m["parts"][0]["rows"] += 1      # the total and the report unchanged
    _edit_manifest(run, edit)
    with pytest.raises(U.SSOUploadError, match="the manifest's parts have 13 rows, its total says 12"):
        U.run("dev", run, dry_run=True)


def test_sidecars_and_manifest_never_uploaded(fakes, tmp_path):
    """Neither sidecar nor SSObservation's manifest is ever picked as a
    part, or uploaded."""
    from ssp.nearbysso._contract import NEARBYSSO_SIDECAR_FILE
    st = fakes.st
    run = make_run(tmp_path)
    (run / C.DELIVERY_DIR / NEARBYSSO_SIDECAR_FILE).write_bytes(b"PAR1 sidecar PAR1")
    files = U.verify_delivery(run, report(run), list(C.DELIVERY_TABLES))
    assert [f.path for f in files["SSObservation"]] == parts_of(run)
    assert [f.path for f in files["NearbySSO"]] == [run / C.DELIVERY_DIR / "NearbySSO.parquet"]
    rec = U.run("dev", run)
    assert sorted(st.objects) == sorted(objects_of(rec["object_prefix"]))
    uploaded = set(st.objects.values())
    for name in (SIDECAR_FILE, NEARBYSSO_SIDECAR_FILE, SSOBSERVATION_MANIFEST_FILE):
        assert (run / C.DELIVERY_DIR / name).read_bytes() not in uploaded
    paths = [e["source_path"] for t in rec["manifest"]["tables"].values() for e in t.values()]
    assert not any(os.path.basename(p) in U.NEVER_PARTS for p in paths)


def test_refuses_no_report(no_remote, tmp_path):
    with pytest.raises(U.SSOUploadError, match="report.json"):
        U.run("dev", tmp_path, dry_run=True)


def test_accepts_the_delivery_dir(no_remote, tmp_path):
    run = make_run(tmp_path)
    U.run("dev", run / C.DELIVERY_DIR, dry_run=True)
    assert report(run)["upload"]["dry_run"] is True


def test_run_dir_read_only_delivery(fakes, tmp_path):
    """The delivery is only read: nothing in it changes (it may be hard
    links to another run's files), nothing is added to RUN_DIR."""
    run = make_run(tmp_path)
    d = run / C.DELIVERY_DIR
    before = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, U.md5_file(p)) for p in d.iterdir()}
    for p in d.iterdir():
        os.chmod(p, 0o444)
    U.run("dev", run, dry_run=True)
    U.run("dev", run)
    after = {p.name: (p.stat().st_ino, p.stat().st_mtime_ns, U.md5_file(p)) for p in d.iterdir()}
    assert after == before
    assert sorted(p.name for p in run.iterdir()) == [C.DELIVERY_DIR, C.REPORT_FILE]


# --------------------------------------------------------------------------
# --parallel: chunked uploads of the big files
# (docs/design/sso-upload-parallel.md)
# --------------------------------------------------------------------------

#: The objects that go in chunks in make_big_run: SSObservation's parts and
#: mpc_orbits, in upload order.
CHUNKED_TABLES = ("SSObservation", "mpc_orbits")


def chunked_objects(prefix):
    return [n for n in part_objects(prefix) if n.split("/")[1] in CHUNKED_TABLES]


def make_big_run(tmp_path, monkeypatch):
    """make_run, with mpc_orbits made big, and PARALLEL_MIN_BYTES set
    between the small files and the big ones (SSObservation's parts and
    mpc_orbits). Returns (run, the threshold)."""
    run = make_run(tmp_path)
    set_table(run, "mpc_orbits", 400, extra=400)
    files = dict(zip(part_objects("x"), sources(run), strict=True))
    big = [p.stat().st_size for n, p in files.items() if n.split("/")[1] in CHUNKED_TABLES]
    small = [p.stat().st_size for n, p in files.items() if n.split("/")[1] not in CHUNKED_TABLES]
    assert max(small) < min(big), (small, big)
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", min(big))
    return run, min(big)


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
    m = FakeMPU(fakes.st)
    monkeypatch.setattr(U, "_chunks_concurrently", m.chunks)
    monkeypatch.setattr(U, "_list_multipart_uploads", m.list)
    monkeypatch.setattr(U, "_abort_multipart_upload", m.abort)
    return fakes.st, fakes.pub, m


DEV_CFG = f"bucket_name: {BUCKET}\nproject: {PROJECT}\n"


def test_parallel_defaults():
    """The owner's choices: parallel by default, 64 MiB x 16 (2026-11-13),
    files of 128 MiB or more (2026-11-23)."""
    assert (U.PARALLEL_CHUNK_MIB, U.PARALLEL_WORKERS, U.PARALLEL_MIN_BYTES) == (64, 16, 128 << 20)
    assert U.parallel_options() == {"chunk_bytes": 64 << 20, "workers": 16}


def test_threshold():
    """Files of 128 MiB or more go in chunks; smaller ones, or any with
    --no-parallel, in one stream."""
    par = U.parallel_options()
    assert U.is_chunked(128 << 20, par) and U.is_chunked(866_110_174, par)
    assert not U.is_chunked((128 << 20) - 1, par) and not U.is_chunked(88_959_875, par)
    assert not U.is_chunked(1 << 40, None)


def test_threshold_in_a_run(mpu, tmp_path, monkeypatch):
    """A file exactly at the threshold goes in chunks; one byte less
    doesn't."""
    st, pub, m = mpu
    run = make_run(tmp_path)
    size = (run / C.DELIVERY_DIR / "SSObject.parquet").stat().st_size
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", size)
    rec = U.run("dev", run)
    assert f"{rec['object_prefix']}/SSObject/part-00000.parquet" in [c[0] for c in m.calls]
    assert "SSObject/part-00000.parquet" in rec["parallel"]["parts"]
    monkeypatch.setattr(U, "PARALLEL_MIN_BYTES", size + 1)
    rec = U.run("dev", run, force=True)
    assert "SSObject/part-00000.parquet" not in rec["parallel"]["parts"]


def test_parallel_by_default(mpu, tmp_path, fixed_prefix, monkeypatch):
    """No flag, no config key: the big files go in chunks of 64 MiB, 16
    threads."""
    st, pub, m = mpu
    run, big = make_big_run(tmp_path, monkeypatch)
    rec = U.run("dev", run)
    assert [c[0] for c in m.calls] == chunked_objects(PREFIX)
    assert {c[2:] for c in m.calls} == {(64 << 20, 16)}
    assert rec["parallel"] == {"chunk_bytes": 64 << 20, "workers": 16, "min_bytes": big,
                               "tables": list(CHUNKED_TABLES),
                               "parts": [n.split("/", 1)[1] for n in chunked_objects(PREFIX)]}
    assert pub.published == [(TOPIC_PATH, message(PREFIX).encode())]
    assert sorted(st.objects) == sorted(objects_of(PREFIX))


def test_parallel_chunks_only_the_big_files(mpu, tmp_path, fixed_prefix, monkeypatch):
    st, pub, m = mpu
    run, big = make_big_run(tmp_path, monkeypatch)
    rec = U.run("dev", run, parallel=True, chunk_mib=64, workers=8)
    files = dict(zip(part_objects(PREFIX), sources(run), strict=True))
    # the big ones in chunks, the rest (and the manifest) as before
    assert m.calls == [(n, str(files[n]), 64 << 20, 8) for n in chunked_objects(PREFIX)]
    plain = [n for n in objects_of(PREFIX) if n not in chunked_objects(PREFIX)]
    assert [c[1:] for c in st.calls if c[0] == "upload"] == [(o, 0) for o in plain]
    # each chunked object checked not to exist first, then read back
    for name in chunked_objects(PREFIX):
        assert [c[0] for c in st.calls if c[1] == name] == ["exists", "reload"]
    # the same objects and the same message as without --parallel
    assert sorted(st.objects) == sorted(objects_of(PREFIX))
    for name, p in files.items():
        assert st.objects[name] == p.read_bytes()
    assert pub.published == [(TOPIC_PATH, message(PREFIX).encode())]
    assert rec["parallel"]["workers"] == 8 and rec["parallel"]["tables"] == list(CHUNKED_TABLES)
    assert report(run)["upload"] == rec
    assert m.uploads == {} and m.aborted == []


@pytest.mark.parametrize("how", ["cli-off", "config-off", "cli-off-over-config-on"])
def test_no_parallel_gives_the_single_stream_path(mpu, tmp_path, fixed_prefix, monkeypatch, how):
    """--no-parallel (also over the config's parallel: true), or the
    config's parallel: false: even the big files go in one
    if_generation_match=0 upload, and the record's parallel is null."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    cfg = "dev"
    if how != "cli-off":
        cfg = tmp_path / "c.yaml"
        cfg.write_text(DEV_CFG + f"parallel: {'false' if how == 'config-off' else 'true'}\n")
    rec = U.run(str(cfg), run, parallel=None if how == "config-off" else False)
    assert m.calls == []
    assert [c[1:] for c in st.calls] == [(o, 0) for o in objects_of(PREFIX)]
    assert all(c[0] == "upload" for c in st.calls)
    assert "parallel" in rec and rec["parallel"] is None
    assert report(run)["upload"]["parallel"] is None
    assert pub.published == [(TOPIC_PATH, message(PREFIX).encode())]


def test_cli_parallel_over_config_off(mpu, tmp_path, fixed_prefix, monkeypatch):
    """--parallel wins over the config's parallel: false."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(DEV_CFG + "parallel: false\n")
    rec = U.run(str(cfg), run, parallel=True)
    assert [c[0] for c in m.calls] == chunked_objects(PREFIX)
    assert rec["parallel"]["tables"] == list(CHUNKED_TABLES)


def test_parallel_from_the_config(mpu, tmp_path, fixed_prefix, monkeypatch):
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    cfg = tmp_path / "c.yaml"
    cfg.write_text(DEV_CFG + "parallel: true\n")
    rec = U.run(str(cfg), run)
    assert [c[0] for c in m.calls] == chunked_objects(PREFIX)
    assert rec["parallel"]["chunk_bytes"] == U.PARALLEL_CHUNK_MIB << 20
    assert rec["parallel"]["workers"] == U.PARALLEL_WORKERS
    cfg.write_text("bucket_name: b\nproject: p\nparallel: yes please\n")
    with pytest.raises(U.SSOUploadError, match="parallel must be true or false"):
        U.load_config(str(cfg))


def test_parallel_refuses_an_existing_object(mpu, tmp_path, fixed_prefix, monkeypatch):
    """No precondition on a multipart upload: an object that appears at the
    name after the prefix check is found by the existence check, left
    untouched, and the upload fails cleanly."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    theirs = f"{PREFIX}/mpc_orbits/part-00000.parquet"
    orig = FakeBucket.list_blobs

    def then_theirs(self, prefix=None, max_results=None):
        out = orig(self, prefix, max_results)
        st.objects[theirs] = b"someone else's"
        return out
    monkeypatch.setattr(FakeBucket, "list_blobs", then_theirs)
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run, parallel=True)
    assert st.objects == {theirs: b"someone else's"}
    names = part_objects(PREFIX)
    k = names.index(theirs)
    assert [c[0] for c in m.calls] == [n for n in chunked_objects(PREFIX) if n != theirs]   # never on it
    assert st.deletes() == names[:k]
    assert pub.published == [] and "upload" not in report(run)


@pytest.mark.parametrize("which", [0, 1, 2])
def test_parallel_failure_aborts_and_cleans_up(mpu, tmp_path, fixed_prefix, monkeypatch, which):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[which]
    m.fail[name] = ServiceUnavailable("a part failed")
    with pytest.raises(U.SSOUploadError, match="in chunks") as ei:
        U.run("dev", run, parallel=True)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    # the incomplete multipart upload aborted, every object of this run gone
    assert m.uploads == {} and [a[0] for a in m.aborted] == [name]
    assert st.objects == {} and pub.published == []
    names = part_objects(PREFIX)
    assert st.deletes() == names[:names.index(name)]
    assert "upload" not in report(run)


@pytest.mark.parametrize("exc", [KeyboardInterrupt, SystemExit])
def test_parallel_interrupt_aborts_and_cleans_up(mpu, tmp_path, fixed_prefix, monkeypatch, exc):
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    m.fail[f"{PREFIX}/mpc_orbits/part-00000.parquet"] = exc()
    with pytest.raises(exc):
        U.run("dev", run, parallel=True)
    assert m.uploads == {} and len(m.aborted) == 1
    assert st.objects == {} and pub.published == []


@pytest.mark.parametrize("how", ["corrupt", "short"])
def test_parallel_crc32c_or_size_mismatch_deletes_it(mpu, tmp_path, fixed_prefix, monkeypatch, how):
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[0]
    m.fail[name] = how
    with pytest.raises(U.SSOUploadError, match="CRC32C .* the upload is corrupt"):
        U.run("dev", run, parallel=True)
    assert st.deletes() == [name]
    assert st.objects == {} and pub.published == []


def test_parallel_cleanup_failures_keep_the_original_error(mpu, tmp_path, monkeypatch, caplog):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    prefixes = iter(["20260914T200401001", "20260914T200401002"])
    monkeypatch.setattr(U, "generate_prefix", lambda now=None: next(prefixes))
    m.fail["20260914T200401001/mpc_orbits/part-00000.parquet"] = ServiceUnavailable("a part failed")
    m.list_exc = RuntimeError("can't list")
    with pytest.raises(U.SSOUploadError, match="in chunks"):
        U.run("dev", run, parallel=True)
    assert "failed to list the multipart uploads" in caplog.text
    assert st.objects == {}          # the earlier objects are still deleted
    # an abort that fails is logged, too
    caplog.clear()
    m.fail["20260914T200401002/mpc_orbits/part-00000.parquet"] = ServiceUnavailable("a part failed")
    m.list_exc, m.abort_exc = None, RuntimeError("can't abort")
    m.uploads.clear()
    with pytest.raises(U.SSOUploadError, match="in chunks"):
        U.run("dev", run, parallel=True)
    assert "abort it by hand" in caplog.text


def test_parallel_needs_the_crc32c_c_extension(mpu, tmp_path, fixed_prefix, monkeypatch):
    import google_crc32c
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
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
    (float("nan"), 8, "--chunk-mib"), (float("inf"), 8, "--chunk-mib"), (float("-inf"), 8, "--chunk-mib"),
    (True, 8, "--chunk-mib"),
    (64, 0, "--parallel-workers"), (64, U.PARALLEL_MAX_WORKERS + 1, "--parallel-workers"),
    (64, True, "--parallel-workers"), (64, 8.0, "--parallel-workers")])
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
    run, _ = make_big_run(tmp_path, monkeypatch)
    monkeypatch.setattr(U, "MPU_MAX_PARTS", 0)
    with pytest.raises(U.SSOUploadError, match="at most 0"):
        U.run("dev", run, parallel=True)
    assert m.calls == [] and st.objects == {}


def test_parallel_dry_run(no_remote, tmp_path, monkeypatch, capsys):
    run, big = make_big_run(tmp_path, monkeypatch)
    rec = U.run("dev", run, dry_run=True, chunk_mib=32, workers=4)     # parallel by default
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    parts = [n.split("/", 1)[1] for n in chunked_objects(prefix)]
    assert f"parallel: files of at least {big} bytes in chunks of 32 MiB, 4 threads: {parts}" in out
    for name in part_objects(prefix):
        line, = [x for x in out.splitlines() if f"/{name}  (" in x]
        if name in chunked_objects(prefix):
            assert "in chunks of 32 MiB, 4 threads" in line
        else:
            assert line.endswith("; if_generation_match=0)")
    assert rec["parallel"]["tables"] == list(CHUNKED_TABLES) and rec["parallel"]["parts"] == parts
    rec = U.run("dev", run, dry_run=True, parallel=False)
    out = capsys.readouterr().out
    assert "parallel: off" in out and "in chunks" not in out
    assert out.count("if_generation_match=0)") == len(objects_of(prefix))
    assert rec["parallel"] is None


def test_main_non_finite_chunk(no_remote, tmp_path):
    """--chunk-mib nan / inf from the command line: a clean error, not a
    ValueError or OverflowError."""
    run = make_run(tmp_path)
    for v in ("nan", "inf"):
        assert U.main(["dev", str(run), "--dry-run", "--chunk-mib", v]) == 1


def test_parallel_existence_check_403(mpu, tmp_path, fixed_prefix, monkeypatch):
    from google.api_core.exceptions import Forbidden
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    st.exists_exc = Forbidden("no storage.objects.get")
    with pytest.raises(U.SSOUploadError, match=r"lacks storage\.objects\.get.*--no-parallel avoids it"):
        U.run("dev", run)
    assert m.calls == [] and st.objects == {}


def test_parallel_mismatch_deletes_only_our_generation(mpu, tmp_path, fixed_prefix, monkeypatch):
    """On a CRC32C mismatch the object is deleted with if_generation_match
    on the generation read back."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[0]
    m.fail[name] = "corrupt"
    gens = []
    st.after_reload = lambda blob: gens.append(blob.generation)
    with pytest.raises(U.SSOUploadError, match="the upload is corrupt"):
        U.run("dev", run)
    assert gens and st.delete_generations == [gens[0]]
    assert st.objects == {}


def test_parallel_mismatch_never_deletes_a_newer_object(mpu, tmp_path, fixed_prefix, monkeypatch, caplog):
    """If the object changed after the read-back (someone else's write),
    the generation-matched delete fails and it is kept."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[0]
    m.fail[name] = "corrupt"

    def overwrite(blob):
        st.objects[name] = b"someone else's"
    st.after_reload = overwrite
    with pytest.raises(U.SSOUploadError, match="the upload is corrupt"):
        U.run("dev", run)
    assert st.objects == {name: b"someone else's"}
    assert "remove it by hand" in caplog.text


def test_parallel_failure_before_complete_never_deletes(mpu, tmp_path, fixed_prefix, monkeypatch):
    """A failure before our multipart upload completed: an object that
    appeared at the name meanwhile isn't ours, and is left alone."""
    from google.api_core.exceptions import ServiceUnavailable
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[0]

    def theirs_then_fail(path, blob, chunk_bytes, workers):
        st.objects[name] = b"someone else's"
        m.fail[name] = ServiceUnavailable("a part failed")
        m.chunks(path, blob, chunk_bytes, workers)
    monkeypatch.setattr(U, "_chunks_concurrently", theirs_then_fail)
    with pytest.raises(U.SSOUploadError, match="in chunks"):
        U.run("dev", run)
    assert st.objects == {name: b"someone else's"}
    assert st.deletes() == []
    assert m.uploads == {} and len(m.aborted) == 1


def test_parallel_unread_generation_is_not_deleted(mpu, tmp_path, fixed_prefix, monkeypatch, caplog):
    """Completed, but the read-back failed: without the generation, the
    object is not deleted (logged for removal by hand)."""
    st, pub, m = mpu
    run, _ = make_big_run(tmp_path, monkeypatch)
    name = chunked_objects(PREFIX)[0]
    st.reload_exc = RuntimeError("read-back failed")
    with pytest.raises(U.SSOUploadError, match="can't read back"):
        U.run("dev", run)
    assert name in st.objects
    assert st.deletes() == []
    assert "remove it by hand" in caplog.text


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


def test_list_multipart_uploads_any_namespace(monkeypatch):
    """Elements are matched in any namespace, or none."""
    body = ("<ListMultipartUploadsResult><IsTruncated>false</IsTruncated>"
            "<Upload><Key>p/A.parquet</Key><UploadId>u1</UploadId></Upload></ListMultipartUploadsResult>")
    monkeypatch.setattr(U, "_xml_api", lambda bucket: ("https://h/b", type("H", (), {
        "get": staticmethod(lambda url, params: _Resp(200, body))})))
    assert U._list_multipart_uploads(None, "p/") == [("p/A.parquet", "u1")]
    other = body.replace("<ListMultipartUploadsResult>", '<ListMultipartUploadsResult xmlns="urn:x">')
    monkeypatch.setattr(U, "_xml_api", lambda bucket: ("https://h/b", type("H", (), {
        "get": staticmethod(lambda url, params: _Resp(200, other))})))
    assert U._list_multipart_uploads(None, "p/") == [("p/A.parquet", "u1")]


@pytest.mark.parametrize("pages", [
    # truncated, but no markers
    [_uploads_xml([("p/A.parquet", "u1")]).replace("<IsTruncated>false", "<IsTruncated>true")],
    # truncated, one marker missing
    [_uploads_xml([("p/A.parquet", "u1")], "p/A.parquet", "u1").replace(
        "<NextUploadIdMarker>u1</NextUploadIdMarker>", "")],
    # truncated, the same markers again
    [_uploads_xml([("p/A.parquet", "u1")], "p/A.parquet", "u1")] * 3,
])
def test_list_multipart_uploads_stops_on_bad_markers(monkeypatch, pages):
    calls = []

    class Http:
        @staticmethod
        def get(url, params):
            calls.append(dict(params))
            assert len(calls) <= 3, "looping"
            return _Resp(200, pages[min(len(calls), len(pages)) - 1])
    monkeypatch.setattr(U, "_xml_api", lambda bucket: ("https://h/b", Http))
    with pytest.raises(U.SSOUploadError, match="missing or unchanged"):
        U._list_multipart_uploads(None, "p/")
