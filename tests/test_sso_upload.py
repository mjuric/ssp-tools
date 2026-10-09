"""ssp-upload-sso (stage 3) with fake GCS and Pub/Sub clients; no network."""
import concurrent.futures
import json
import re
from datetime import datetime, timedelta, timezone

import pytest

from ssp import delivery_contract as C
from ssp import sso_upload as U

TOPIC_PATH = "projects/ppdb-dev-5c07/topics/load-sso-topic"
FIVE = ("SSObservation", "SSObject", "mpc_orbits", "current_identifications", "numbered_identifications")


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
    def __init__(self, exc=None):
        self.published, self.exc, self.futures = [], exc, []

    @staticmethod
    def topic_path(project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, topic, data):
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


def ordered(tables):
    return [t for t in C.DELIVERY_TABLES if t in tables]


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
    assert U.ACCEPTED_TABLES == tuple(ordered(FIVE))
    assert "NearbySSO" in C.DELIVERY_TABLES and "NearbySSO" not in U.ACCEPTED_TABLES


def test_dev_config_and_templates():
    _, cfg = U.load_config("dev")
    assert cfg == {"bucket_name": "ppdb-dev-sso-ingest", "topic": "load-sso-topic",
                   "project": "ppdb-dev-5c07",
                   "tables": ["SSObservation", "SSObject", "numbered_identifications",
                              "current_identifications", "mpc_orbits"]}
    assert set(cfg["tables"]) == set(FIVE)
    for env in ("int", "prod"):
        with pytest.raises(U.SSOUploadError, match="template"):
            U.load_config(env)
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


def test_nearbysso_refused_without_include_unsupported(no_remote, tmp_path):
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="fails the entire load.*--include-unsupported"):
        U.run("dev", run, dry_run=True, tables=list(FIVE) + ["NearbySSO"])
    # a config listing it is refused too
    cfg = tmp_path / "c.yaml"
    cfg.write_text("bucket_name: b\ntopic: t\nproject: p\ntables: [SSObservation, NearbySSO]\n")
    with pytest.raises(U.SSOUploadError, match="--include-unsupported"):
        U.run(str(cfg), run, dry_run=True)
    assert "upload" not in report(run)


def test_nearbysso_with_include_unsupported_warns_once(no_remote, tmp_path, caplog):
    run = make_run(tmp_path)
    with caplog.at_level("WARNING"):
        rec = U.run("dev", run, dry_run=True, tables=list(FIVE) + ["NearbySSO"],
                    include_unsupported=True)
    assert rec["tables"] == list(C.DELIVERY_TABLES)
    warned = [r for r in caplog.records if "NearbySSO" in r.getMessage()]
    assert len(warned) == 1 and "fails the entire load" in warned[0].getMessage()


def test_default_holds_nearbysso_back(no_remote, tmp_path, caplog):
    run = make_run(tmp_path)
    with caplog.at_level("INFO"):
        rec = U.run("dev", run, dry_run=True)
    assert rec["tables"] == ordered(FIVE)
    assert any("NearbySSO" in r.getMessage() and "not uploaded until" in r.getMessage()
               for r in caplog.records)


def test_partial_upload_refused_without_allow_partial(no_remote, tmp_path):
    run = make_run(tmp_path)
    with pytest.raises(U.SSOUploadError, match="WRITE_TRUNCATE.*--allow-partial"):
        U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"])
    rec = U.run("dev", run, dry_run=True, tables=["SSObject", "SSObservation"], allow_partial=True)
    assert rec["tables"] == ["SSObservation", "SSObject"]       # the contract's order
    # the config's five in any order count as complete
    rec = U.run("dev", run, dry_run=True, tables=list(reversed(FIVE)))
    assert rec["tables"] == ordered(FIVE)
    with pytest.raises(U.SSOUploadError, match="not delivery tables"):
        U.run("dev", run, tables=["DiaSource"])


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
    assert sorted(st.objects) == sorted(f"{prefix}/{t}.parquet" for t in FIVE)
    assert all(c[2] == 0 for c in st.calls if c[0] == "upload")    # if_generation_match=0
    assert len(pub.published) == 1
    topic, data = pub.published[0]
    assert topic == TOPIC_PATH
    assert data == json.dumps({"bucket": "ppdb-dev-sso-ingest", "object_prefix": prefix,
                               "uploaded_tables": ordered(FIVE)}).encode("utf-8")
    assert pub.futures[0].timeouts == [U.PUBLISH_TIMEOUT_S]
    r = report(run)
    assert r["upload"] == rec and r["uploads"] == [rec]
    assert set(rec) == {"config", "bucket", "object_prefix", "tables", "message_id", "dry_run", "utc"}
    assert rec["message_id"] == "msg-123" and rec["dry_run"] is False


def test_precondition_failure_aborts_and_cleans_up(fakes, tmp_path, fixed_prefix):
    st, pub = fakes
    run = make_run(tmp_path)
    tabs = ordered(FIVE)
    # an object already at the third table's name, as from a colliding upload
    st.objects[f"{PREFIX}/{tabs[2]}.parquet"] = b"someone else's"
    with pytest.raises(U.SSOUploadError, match="already exists"):
        U.run("dev", run)
    # the pre-existing object is untouched, ours are deleted, nothing published
    assert st.objects == {f"{PREFIX}/{tabs[2]}.parquet": b"someone else's"}
    assert [c[1] for c in st.calls if c[0] == "delete"] == [f"{PREFIX}/{t}.parquet" for t in tabs[:2]]
    assert pub.published == []
    assert "upload" not in report(run)


def test_mid_upload_failure_cleans_up(fakes, tmp_path, fixed_prefix):
    from google.api_core.exceptions import ServiceUnavailable
    st, pub = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{ordered(FIVE)[3]}.parquet"] = ServiceUnavailable("injected")
    with pytest.raises(U.SSOUploadError, match="failed to upload"):
        U.run("dev", run)
    assert st.objects == {}
    assert len([c for c in st.calls if c[0] == "delete"]) == 3
    assert pub.published == []
    assert "upload" not in report(run)


@pytest.mark.parametrize("exc", ["auth", "other"])
def test_any_upload_error_is_wrapped(fakes, tmp_path, fixed_prefix, exc):
    from google.auth.exceptions import RefreshError
    st, _ = fakes
    run = make_run(tmp_path)
    st.fail_on[f"{PREFIX}/{ordered(FIVE)[1]}.parquet"] = (
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
    tabs = ordered(FIVE)
    st.fail_on[f"{PREFIX}/{tabs[3]}.parquet"] = ServiceUnavailable("injected")
    st.fail_delete.add(f"{PREFIX}/{tabs[0]}.parquet")
    with pytest.raises(U.SSOUploadError, match="failed to upload") as ei:
        U.run("dev", run)
    assert isinstance(ei.value.__cause__, ServiceUnavailable)
    assert [c[1] for c in st.calls if c[0] == "delete"] == [f"{PREFIX}/{t}.parquet" for t in tabs[:3]]
    assert set(st.objects) == {f"{PREFIX}/{tabs[0]}.parquet"}     # the one that wouldn't delete


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
    assert len([c for c in st.calls if c[0] == "delete"]) == len(FIVE)


def test_publish_timeout_keeps_objects(fakes, tmp_path, monkeypatch, fixed_prefix):
    st, _ = fakes
    pub = FakePublisher(exc=concurrent.futures.TimeoutError())
    monkeypatch.setattr(U, "_publisher_client", lambda: pub)
    run = make_run(tmp_path)
    with pytest.raises(U.PublishUnconfirmedError, match="KEPT"):
        U.run("dev", run)
    assert len(st.objects) == len(FIVE)
    assert not [c for c in st.calls if c[0] == "delete"]
    r = report(run)
    assert r["upload"]["object_prefix"] == PREFIX and r["upload"]["message_id"] is None
    assert "wasn't confirmed" in r["upload"]["error"]
    # and a plain re-upload is refused: the message may have gone through
    with pytest.raises(U.SSOUploadError, match="--force"):
        U.run("dev", run)


# --------------------------------------------------------------------------
# Dry run, history, refusals
# --------------------------------------------------------------------------

def test_dry_run_touches_nothing(no_remote, tmp_path, capsys):
    run = make_run(tmp_path)
    rec = U.run("dev", run, dry_run=True)
    out = capsys.readouterr().out
    prefix = rec["object_prefix"]
    for t in FIVE:
        assert f"gs://ppdb-dev-sso-ingest/{prefix}/{t}.parquet" in out
    assert "NearbySSO" not in out
    assert TOPIC_PATH in out
    assert json.dumps({"bucket": "ppdb-dev-sso-ingest", "object_prefix": prefix,
                       "uploaded_tables": ordered(FIVE)}) in out
    up = report(run)["upload"]
    assert up["dry_run"] is True and up["message_id"] is None
    assert U.main(["dev", str(run), "--dry-run"]) == 0


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
    with pytest.raises(U.SSOUploadError, match="SSObservation: no md5"):
        U.run("dev", run, dry_run=True, tables=["SSObservation"], allow_partial=True)


def test_nearbysso_not_needed_by_default(no_remote, tmp_path):
    run = make_run(tmp_path, tables=FIVE)       # no NearbySSO in the delivery
    assert U.run("dev", run, dry_run=True)["tables"] == ordered(FIVE)


def test_refuses_no_report(no_remote, tmp_path):
    with pytest.raises(U.SSOUploadError, match="report.json"):
        U.run("dev", tmp_path, dry_run=True)


def test_accepts_the_delivery_dir(no_remote, tmp_path):
    run = make_run(tmp_path)
    U.run("dev", run / C.DELIVERY_DIR, dry_run=True)
    assert report(run)["upload"]["dry_run"] is True
