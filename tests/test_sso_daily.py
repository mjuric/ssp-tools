"""ssp-sso-daily's sequencing, with stub stage commands on PATH."""
import os
import sys
from datetime import datetime, timezone

import pytest

from ssp import sso_daily as D

CT = D.DEFAULT_CORRECTION_TABLE

STUB = """#!{python}
import os, sys
print("stub " + os.path.basename(sys.argv[0]) + " output")
with open(os.environ["STUB_LOG"], "a") as f:
    f.write(" ".join([os.path.basename(sys.argv[0])] + sys.argv[1:]) + "\\n")
sys.exit(int(os.environ.get("STUB_FAIL_" + os.path.basename(sys.argv[0]).replace("-", "_"), "0")))
"""


@pytest.fixture
def stubs(tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name in (D.EXTRACT, D.BUILD, D.UPLOAD):
        p = bindir / name
        p.write_text(STUB.format(python=sys.executable))
        p.chmod(0o755)
    log = tmp_path / "calls.log"
    monkeypatch.setattr(D, "_local_bin", lambda: tmp_path / "no-local-bin")
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("STUB_LOG", str(log))
    return lambda: log.read_text().splitlines() if log.exists() else []


def fail(monkeypatch, cmd, rc=3):
    monkeypatch.setenv("STUB_FAIL_" + cmd.replace("-", "_"), str(rc))


def test_all_three_in_order(stubs, tmp_path):
    w = tmp_path / "work"
    assert D.main([str(w), "--upload", "dev", "--dry-run", "--stamp", "2026-10-01"]) == 0
    day = w / "2026-10-01"
    assert stubs() == [
        f"ssp-extract-sso-inputs {day}/inputs --correction-table {CT}",
        f"ssp-build-sso {day}/inputs {day}/run",
        f"ssp-upload-sso dev {day}/run --dry-run",
    ]
    assert "done" in (day / "daily.log").read_text()


def test_default_stamp_is_utc_date(stubs, tmp_path):
    w = tmp_path / "work"
    assert D.main([str(w)]) == 0
    day = w / datetime.now(timezone.utc).strftime("%Y-%m-%d")
    assert day.is_dir()
    assert stubs() == [f"ssp-extract-sso-inputs {day}/inputs --correction-table {CT}",
                       f"ssp-build-sso {day}/inputs {day}/run"]


def test_no_upload_without_config(stubs, tmp_path):
    assert D.main([str(tmp_path / "w"), "--stamp", "x"]) == 0
    assert not any(c.startswith("ssp-upload-sso") for c in stubs())
    with pytest.raises(SystemExit):
        D.main([str(tmp_path / "w"), "--stamp", "y", "--dry-run"])


def test_stops_at_first_failure(stubs, tmp_path, monkeypatch):
    fail(monkeypatch, D.EXTRACT, 5)
    assert D.main([str(tmp_path / "w"), "--upload", "dev", "--stamp", "a"]) == 5
    assert [c.split()[0] for c in stubs()] == [D.EXTRACT]
    assert "stopping: extract failed" in (tmp_path / "w" / "a" / "daily.log").read_text()


def test_build_failure_skips_upload(stubs, tmp_path, monkeypatch):
    fail(monkeypatch, D.BUILD)
    assert D.main([str(tmp_path / "w"), "--upload", "dev", "--stamp", "a"]) == 3
    assert [c.split()[0] for c in stubs()] == [D.EXTRACT, D.BUILD]


def test_upload_failure_is_the_exit_status(stubs, tmp_path, monkeypatch):
    fail(monkeypatch, D.UPLOAD, 1)
    assert D.main([str(tmp_path / "w"), "--upload", "dev", "--stamp", "a"]) == 1
    assert [c.split()[0] for c in stubs()] == [D.EXTRACT, D.BUILD, D.UPLOAD]


def test_ssobservation_options_passed_to_build(stubs, tmp_path):
    """--part-rows and --internal-columns go to ssp-build-sso as given
    (an empty --internal-columns too); without them, nothing is added, so
    ssp-build-sso's defaults apply."""
    inputs = tmp_path / "in"
    inputs.mkdir()
    w = tmp_path / "w"
    assert D.main([str(w), "--reuse-inputs", str(inputs), "--stamp", "a", "--part-rows", "500000",
                   "--internal-columns", "matchMethod"]) == 0
    assert D.main([str(w), "--reuse-inputs", str(inputs), "--stamp", "b", "--internal-columns", ""]) == 0
    assert stubs() == [f"ssp-build-sso {inputs} {w}/a/run --part-rows 500000 --internal-columns matchMethod",
                       f"ssp-build-sso {inputs} {w}/b/run --internal-columns "]
    assert "--part-rows 500000 --internal-columns matchMethod" in (w / "a" / "daily.log").read_text()
    assert D.plan(w / "c")[-1] == ("build", [D.BUILD, str(w / "c" / "inputs"), str(w / "c" / "run")])
    with pytest.raises(SystemExit, match="--part-rows 0"):
        D.main([str(w), "--reuse-inputs", str(inputs), "--stamp", "d", "--part-rows", "0"])


def test_reuse_inputs(stubs, tmp_path):
    inputs = tmp_path / "old" / "inputs"
    inputs.mkdir(parents=True)
    w = tmp_path / "w"
    assert D.main([str(w), "--reuse-inputs", str(inputs), "--upload", "dev", "--stamp", "a"]) == 0
    assert stubs() == [f"ssp-build-sso {inputs} {w}/a/run", f"ssp-upload-sso dev {w}/a/run"]
    with pytest.raises(SystemExit):
        D.main([str(w), "--reuse-inputs", str(tmp_path / "nope"), "--stamp", "b"])


def test_refuses_an_existing_day(stubs, tmp_path):
    w = tmp_path / "w"
    assert D.main([str(w), "--stamp", "a"]) == 0
    with pytest.raises(SystemExit, match="exists"):
        D.main([str(w), "--stamp", "a"])


def test_missing_command(tmp_path, monkeypatch):
    monkeypatch.setattr(D, "_local_bin", lambda: tmp_path)
    monkeypatch.setenv("PATH", str(tmp_path))     # nothing on PATH
    assert D.main([str(tmp_path / "w"), "--stamp", "a"]) == 127


class _FakeDatetime(datetime):
    """now() at 2026-10-02T03:00Z; local (naive) time is the day before."""
    @classmethod
    def now(cls, tz=None):
        instant = datetime(2026, 10, 2, 3, 0, tzinfo=timezone.utc)
        return instant.astimezone(tz) if tz else datetime(2026, 10, 1, 20, 0)


def test_stamp_pinned_to_utc(monkeypatch):
    monkeypatch.setattr(D, "datetime", _FakeDatetime)
    assert D.utc_stamp() == "2026-10-02"


def test_local_bin_searched_before_path(stubs, tmp_path, monkeypatch):
    local = tmp_path / "venvbin"
    local.mkdir()
    for name in (D.EXTRACT, D.BUILD):
        p = local / name
        p.write_text(STUB.format(python=sys.executable).replace("os.path.basename(sys.argv[0])]",
                                                               "'local:' + os.path.basename(sys.argv[0])]"))
        p.chmod(0o755)
    monkeypatch.setattr(D, "_local_bin", lambda: local)
    assert D.main([str(tmp_path / "w"), "--stamp", "a"]) == 0
    assert [c.split()[0] for c in stubs()] == ["local:" + D.EXTRACT, "local:" + D.BUILD]


def test_signal_maps_to_128_plus(stubs, tmp_path, monkeypatch):
    bindir = tmp_path / "bin"
    (bindir / D.EXTRACT).write_text("#!/bin/sh\nkill -TERM $$\n")
    assert D.main([str(tmp_path / "w"), "--stamp", "a"]) == 128 + 15
    assert "killed by signal 15" in (tmp_path / "w" / "a" / "daily.log").read_text()


@pytest.mark.parametrize("stamp", ["a/b", "..", "../x", "x..y"])
def test_bad_stamp(stubs, tmp_path, stamp):
    with pytest.raises(SystemExit, match="plain name"):
        D.main([str(tmp_path / "w"), "--stamp", stamp])
    assert stubs() == []


# --------------------------------------------------------------------------
# The correction table: read as it is (ssp-daily keeps it up to date)
# --------------------------------------------------------------------------

def test_correction_table_option(stubs, tmp_path):
    ct = tmp_path / "ct"
    assert D.main([str(tmp_path / "w"), "--stamp", "a", "--correction-table", str(ct)]) == 0
    assert stubs() == [f"ssp-extract-sso-inputs {tmp_path}/w/a/inputs --correction-table {ct}",
                       f"ssp-build-sso {tmp_path}/w/a/inputs {tmp_path}/w/a/run"]


@pytest.mark.parametrize("opt", [["--skip-stage0"], ["--stage0-workers", "8"]])
def test_stage0_options_gone(stubs, tmp_path, opt):
    with pytest.raises(SystemExit):
        D.main([str(tmp_path / "w"), "--stamp", "a"] + opt)
    assert stubs() == []
