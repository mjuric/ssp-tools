"""ssp-upload-sso (stage 3) with fake GCS and Pub/Sub clients; no network."""
import json
import re
from datetime import UTC, datetime

import pytest

from ssp import delivery_contract as C
from ssp import sso_upload as U

TOPIC_PATH = "projects/ppdb-dev-5c07/topics/load-sso-topic"


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------

class FakeBlob:
    def __init__(self, store, name, fail_on):
        self.store, self.name, self.fail_on = store, name, fail_on

    def upload_from_filename(self, filename, if_generation_match=None):
        from google.api_core.exceptions import PreconditionFailed, ServiceUnavailable
        self.store.calls.append(("upload", self.name, if_generation_match))
        if self.name in self.fail_on:
            raise ServiceUnavailable("injected")
        if if_generation_match == 0 and self.name in self.store.objects:
            raise PreconditionFailed("exists")
        with open(filename, "rb") as f:
            self.store.objects[self.name] = f.read()

    def delete(self):
        self.store.calls.append(("delete", self.name))
        del self.store.objects[self.name]


class FakeBucket:
    def __init__(self, store, fail_on):
        self.store, self.fail_on = store, fail_on

    def blob(self, name):
        return FakeBlob(self.store, name, self.fail_on)


class FakeStorage:
    def __init__(self, fail_on=()):
        self.objects, self.calls, self.buckets, self.fail_on = {}, [], [], set(fail_on)

    def bucket(self, name):
        self.buckets.append(name)
        return FakeBucket(self, self.fail_on)


class FakeFuture:
    def __init__(self, exc=None):
        self.exc = exc

    def result(self, timeout=None):
        if self.exc:
            raise self.exc
        return "msg-123"


class FakePublisher:
    def __init__(self, exc=None):
        self.published, self.exc = [], exc

    @staticmethod
    def topic_path(project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data):
        self.published.append((topic, data))
        return FakeFuture(self.exc)


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


def make_run(tmp_path, deliverable=True, tables=C.DELIVERY_TABLES):
    run = tmp_path / "run"
    (run / C.DELIVERY_DIR).mkdir(parents=True)
    rec = {}
    for t in tables:
        p = run / C.DELIVERY_DIR / f"{t}.parquet"
        p.write_bytes(f"PAR1 {t} PAR1".encode())
        rec[t] = {"file": f"{C.DELIVERY_DIR}/{t}.parquet", "rows": 1, "md5": U.md5_file(p),
                  "bytes": p.stat().st_size}
    report = {"inputs": {}, "ssp_tools_commit": "abc", "steps": {s: {"status": "ok"} for s in C.BUILD_STEPS},
              "tables": rec, "checks": {"schema": {"status": "PASS"}}, "deliverable": deliverable}
    (run / C.REPORT_FILE).write_text(json.dumps(report))
    return run


def report(run):
    return json.loads((run / C.REPORT_FILE).read_text())


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------

def test_prefix_format():
    now = datetime(2026, 9, 14, 20, 4, 1, 1234, tzinfo=UTC)
    assert U.generate_prefix(now) == "20260914T200401001"
    assert re.fullmatch(r"\d{8}T\d{9}", U.generate_prefix())
    # exactly the reference expression
    assert U.generate_prefix(now) == now.strftime("%Y%m%dT%H%M%S%f")[:-3]


def test_dev_config_and_templates():
    _, cfg = U.load_config("dev")
    assert cfg == {"bucket_name": "ppdb-dev-sso-ingest", "topic": "load-sso-topic",
                   "project": "ppdb-dev-5c07"}
    for env in ("int", "prod"):
        with pytest.raises(U.SSOUploadError, match="template"):
            U.load_config(env)
    with pytest.raises(U.SSOUploadError, match="no config"):
        U.load_config("nosuchenv")


def test_upload_success_and_message(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    rec = U.run("dev", run)
    prefix = rec["object_prefix"]
    assert re.fullmatch(r"\d{8}T\d{9}", prefix)
    assert st.buckets == ["ppdb-dev-sso-ingest"]
    assert sorted(st.objects) == sorted(f"{prefix}/{t}.parquet" for t in C.DELIVERY_TABLES)
    assert all(c[2] == 0 for c in st.calls if c[0] == "upload")    # if_generation_match=0
    assert len(pub.published) == 1
    topic, data = pub.published[0]
    assert topic == TOPIC_PATH
    assert data == json.dumps({"bucket": "ppdb-dev-sso-ingest", "object_prefix": prefix,
                               "uploaded_tables": list(C.DELIVERY_TABLES)}).encode("utf-8")
    up = report(run)["upload"]
    assert up == rec
    assert set(up) == {"config", "bucket", "object_prefix", "tables", "message_id", "dry_run", "utc"}
    assert up["message_id"] == "msg-123" and up["dry_run"] is False
    assert up["tables"] == list(C.DELIVERY_TABLES)


def test_tables_subset_in_contract_order(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    U.run("dev", run, tables=["SSObject", "SSSource"])
    body = json.loads(pub.published[0][1])
    assert body["uploaded_tables"] == ["SSSource", "SSObject"]
    assert len(st.objects) == 2
    with pytest.raises(U.SSOUploadError, match="not delivery tables"):
        U.run("dev", run, tables=["DiaSource"], force=True)


def test_nearbysso_warned_once(fakes, tmp_path, caplog):
    run = make_run(tmp_path)
    with caplog.at_level("WARNING"):
        U.run("dev", run)
    assert sum("NearbySSO" in r.getMessage() for r in caplog.records) == 1


def test_precondition_failure_aborts_and_cleans_up(fakes, tmp_path, monkeypatch):
    st, pub = fakes
    run = make_run(tmp_path)
    prefix = "20260914T200401001"
    # an object already at the third table's name, as from a colliding upload
    st.objects[f"{prefix}/{C.DELIVERY_TABLES[2]}.parquet"] = b"someone else's"
    monkeypatch.setattr(U, "generate_prefix", lambda now=None: prefix)
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run)
    # the pre-existing object is untouched, ours are deleted, nothing published
    assert st.objects == {f"{prefix}/{C.DELIVERY_TABLES[2]}.parquet": b"someone else's"}
    assert [c[1] for c in st.calls if c[0] == "delete"] == [f"{prefix}/{t}.parquet"
                                                           for t in C.DELIVERY_TABLES[:2]]
    assert pub.published == []
    assert "upload" not in report(run)


def test_mid_upload_failure_cleans_up(fakes, tmp_path, monkeypatch):
    st, pub = fakes
    run = make_run(tmp_path)
    prefix = "20260914T200401001"
    monkeypatch.setattr(U, "generate_prefix", lambda now=None: prefix)
    st.fail_on.add(f"{prefix}/{C.DELIVERY_TABLES[3]}.parquet")
    with pytest.raises(U.SSOUploadError, match="failed to upload"):
        U.run("dev", run)
    assert st.objects == {}
    assert len([c for c in st.calls if c[0] == "delete"]) == 3
    assert pub.published == []
    assert "upload" not in report(run)


def test_publish_failure_cleans_up(fakes, tmp_path, monkeypatch):
    from google.api_core.exceptions import ServiceUnavailable
    st, _ = fakes
    pub = FakePublisher(exc=ServiceUnavailable("injected"))
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="failed to publish"):
        U.run("dev", run)
    assert len(pub.published) == 1
    assert st.objects == {}
    assert len([c for c in st.calls if c[0] == "delete"]) == len(C.DELIVERY_TABLES)


def test_dry_run_touches_nothing(no_remote, tmp_path, capsys):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    for t in C.DELIVERY_TABLES:
        assert f"gs://ppdb-dev-sso-ingest/{prefix}/{t}.parquet" in out
    assert TOPIC_PATH in out
    assert json.dumps({"bucket": "ppdb-dev-sso-ingest", "object_prefix": prefix,
                       "uploaded_tables": list(C.DELIVERY_TABLES)}) in out
    up = report(run)["upload"]
    assert up["dry_run"] is True and up["message_id"] is None
    assert U.main(["dev", str(run), "--dry-run"]) == 0


def test_dry_run_does_not_clobber_a_real_upload(fakes, tmp_path):
    run = make_run(tmp_path)
    real = U.run("dev", run)
    U.run("dev", run, dry_run=True)
    assert report(run)["upload"] == real


def test_refuses_a_second_real_upload_without_force(fakes, tmp_path):
    st, pub = fakes
    run = make_run(tmp_path)
    U.run("dev", run)
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)
    assert len(pub.published) == 1
    U.run("dev", run, force=True)
    assert len(pub.published) == 2


def test_refuses_not_deliverable(no_remote, tmp_path):
    run = make_run(tmp_path, deliverable=False)
    with pytest.raises(U.SSOUploadError, match="deliverable"):
        U.run("dev", run, dry_run=True)
    assert U.main(["dev", str(run)]) == 1
    assert "upload" not in report(run)


def test_refuses_md5_mismatch(no_remote, tmp_path):
    run = make_run(tmp_path)
    p = run / C.DELIVERY_DIR / "SSObject.parquet"
    p.write_bytes(p.read_bytes().replace(b"PAR1", b"PARX"))     # same size, other content
    with pytest.raises(U.SSOUploadError, match="SSObject.*md5"):
        U.run("dev", run, dry_run=True)
    assert "upload" not in report(run)


def test_refuses_missing_file_or_record(no_remote, tmp_path):
    run = make_run(tmp_path)
    (run / C.DELIVERY_DIR / "NearbySSO.parquet").unlink()
    with pytest.raises(U.SSOUploadError, match="NearbySSO.*missing"):
        U.run("dev", run, dry_run=True)
    # but uploading the others is fine
    U.run("dev", run, dry_run=True, tables=["SSSource"])
    r = report(run)
    del r["tables"]["SSSource"]
    (run / C.REPORT_FILE).write_text(json.dumps(r))
    with pytest.raises(U.SSOUploadError, match="SSSource: no md5"):
        U.run("dev", run, dry_run=True, tables=["SSSource"])


def test_refuses_no_report(no_remote, tmp_path):
    with pytest.raises(U.SSOUploadError, match="report.json"):
        U.run("dev", tmp_path, dry_run=True)


def test_accepts_the_delivery_dir(no_remote, tmp_path):
    run = make_run(tmp_path)
    U.run("dev", run / C.DELIVERY_DIR, dry_run=True)
    assert report(run)["upload"]["dry_run"] is True


def test_config_validation(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("bucket_name: b\ntopic: t\n")
    with pytest.raises(U.SSOUploadError, match="missing"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\ntopic: t\nproject: p\nbukcet: x\n")
    with pytest.raises(U.SSOUploadError, match="unknown"):
        U.load_config(str(p))
    p.write_text("bucket_name: b\ntopic: t\nproject: p\ntables: [SSSource]\n")
    assert U.load_config(str(p))[1]["tables"] == ["SSSource"]
