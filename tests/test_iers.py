"""ssp.iers: the IERS-A file as one of the day's inputs
(docs/design/iers-a.md).

Network-free: the download (ssp.iers.fetch) is replaced, and the files are
cut from astropy's bundled table (tests/iers_fixture.py).
"""

import datetime
import json
import os
import sys
from pathlib import Path

import pytest

from ssp import iers as I
from ssp import sso_build as B
from ssp.delivery_contract import IERS_A_ENV, IERS_A_FILE, IERS_A_MANIFEST_FIELDS

sys.path.insert(0, str(Path(__file__).parent))
from iers_fixture import write_finals

UTC = datetime.timezone.utc
URL, MIRROR = "https://primary.invalid/finals2000A.all", "https://mirror.invalid/finals2000A.all"


@pytest.fixture
def net(monkeypatch, tmp_path):
    """The two URLs and a fake fetch: ``net["files"][url]`` is the file it
    "downloads" (a path), or an exception it raises; calls are recorded."""
    state = {"files": {}, "calls": []}

    def fetch(url, dest, timeout=None, deadline=None):
        state["calls"].append(url)
        src = state["files"].get(url, OSError("connection refused"))
        if isinstance(src, Exception):
            Path(dest).write_text("partial")     # a failed download leaves a partial file
            raise src
        Path(dest).write_bytes(Path(src).read_bytes())

    monkeypatch.setattr(I, "urls", lambda: [URL, MIRROR])
    monkeypatch.setattr(I, "fetch", fetch)
    return state


@pytest.fixture(autouse=True)
def clock(monkeypatch, tmp_path):
    """The extract's clock, fixed 5 days after the test files' predictions
    start (they are cut from astropy's bundled table), so no test depends on
    the date it runs."""
    t = at(write_finals(tmp_path / "clock.all"), 5)()
    monkeypatch.setattr(I, "utcnow", lambda: t)
    return t


def start_of(path):
    return I.predictions_start(I.read(path))


def at(path, days):
    """A ``now`` ``days`` days after ``path``'s predictions start."""
    t = datetime.datetime.fromisoformat(start_of(path)).replace(tzinfo=UTC) + datetime.timedelta(days=days)
    return lambda: t


@pytest.fixture
def dirs(tmp_path):
    store, staging = tmp_path / "store", tmp_path / "inputs" / ".partial"
    staging.mkdir(parents=True)
    return store, staging


def check_types(e):
    """The types ssp-daily checks strictly."""
    assert set(e) == set(IERS_A_MANIFEST_FIELDS)
    assert isinstance(e["reused"], bool)
    assert isinstance(e["age_days"], (int, float)) and not isinstance(e["age_days"], bool)
    assert isinstance(e["source"], str)
    assert isinstance(e["rows"], int) and isinstance(e["md5"], str)
    assert e["file"] == IERS_A_FILE
    datetime.datetime.strptime(e["extracted_utc"], "%Y-%m-%dT%H:%M:%SZ")
    datetime.date.fromisoformat(e["predictions_start"])


# --------------------------------------------------------------------------
# acquire
# --------------------------------------------------------------------------

def test_download_success(tmp_path, net, dirs, capsys):
    store, staging = dirs
    new = write_finals(tmp_path / "new.all")
    net["files"][URL] = new
    e = I.acquire(store, staging, now=at(new, 2.5))
    check_types(e)
    assert e["reused"] is False and e["source"] == URL
    assert e["age_days"] == 2.5
    assert e["predictions_start"] == start_of(new)
    assert e["rows"] == 240
    assert (staging / IERS_A_FILE).read_bytes() == new.read_bytes()
    assert e["md5"] == I.md5sum(new)
    assert (store / IERS_A_FILE).read_bytes() == new.read_bytes()     # the store filled
    assert net["calls"] == [URL]
    assert sorted(p.name for p in staging.iterdir()) == [IERS_A_FILE]
    assert sorted(p.name for p in store.iterdir()) == [IERS_A_FILE]
    assert "WARNING" not in capsys.readouterr().err
    json.dumps(e)


def test_mirror_fallback(tmp_path, net, dirs):
    store, staging = dirs
    new = write_finals(tmp_path / "new.all")
    net["files"][MIRROR] = new
    e = I.acquire(store, staging, now=at(new, 1))
    assert e["source"] == MIRROR and e["reused"] is False
    assert net["calls"] == [URL, MIRROR]
    assert (store / IERS_A_FILE).read_bytes() == new.read_bytes()


@pytest.mark.parametrize("bad, match", [
    ("corrupt", "not a readable IERS-A"),
    ("older", "before the store copy's"),
    ("no_predictions", "IERS-A"),
    ("truncated", "predictions span only 99 days"),
])
def test_bad_download_is_a_failure(tmp_path, net, dirs, capsys, bad, match):
    store, staging = dirs
    store.mkdir()
    stored = write_finals(store / IERS_A_FILE, promote=3)          # predictions 3 days later
    before = stored.read_bytes()
    if bad == "corrupt":
        (tmp_path / "bad.all").write_text("<html>Service unavailable</html>\n")
    elif bad == "older":
        write_finals(tmp_path / "bad.all")
    elif bad == "truncated":
        write_finals(tmp_path / "bad.all", promote=10, after=110)     # newer than the store, but cut
    else:
        write_finals(tmp_path / "bad.all", predicted=False)
    net["files"][URL] = net["files"][MIRROR] = tmp_path / "bad.all"
    e = I.acquire(store, staging, now=at(stored, 5))
    check_types(e)
    assert e["reused"] is True
    assert e["source"].startswith(f"reused {store / IERS_A_FILE} (download failed: {URL}: ")
    assert match in e["source"] and MIRROR in e["source"]
    assert (staging / IERS_A_FILE).read_bytes() == before
    assert stored.read_bytes() == before                             # the store untouched
    assert sorted(p.name for p in staging.iterdir()) == [IERS_A_FILE]
    err = capsys.readouterr().err
    assert "WARNING: IERS-A download failed" in err and "5.0 days old" in err


def test_failure_reuses_store(tmp_path, net, dirs, capsys):
    store, staging = dirs
    store.mkdir()
    stored = write_finals(store / IERS_A_FILE)
    e = I.acquire(store, staging, now=at(stored, 12.25))
    check_types(e)
    assert e["reused"] is True and e["age_days"] == 12.25
    assert "connection refused" in e["source"]
    assert e["md5"] == I.md5sum(stored)
    err = capsys.readouterr().err
    assert "12.25 days old" in err and "connection refused" in err and "STALE" not in err


@pytest.mark.parametrize("age, stale, fails", [
    (30, False, False), (30.01, True, False), (89.5, True, False), (90, True, False), (90.01, True, True),
])
def test_age_thresholds(tmp_path, net, dirs, capsys, age, stale, fails):
    store, staging = dirs
    store.mkdir()
    stored = write_finals(store / IERS_A_FILE)
    if fails:
        with pytest.raises(I.IERSError) as ex:
            I.acquire(store, staging, now=at(stored, age))
        msg = str(ex.value)
        assert str(store / IERS_A_FILE) in msg and f"{age} days old" in msg
        assert "connection refused" in msg and "90-day limit" in msg
        assert not (staging / IERS_A_FILE).exists()
        return
    e = I.acquire(store, staging, now=at(stored, age))
    assert e["age_days"] == age
    err = capsys.readouterr().err
    assert ("STALE" in err) == stale
    if stale:
        assert f"runs will fail in {round(90 - float(age), 2)} days" in err


def test_old_download_is_a_failure(tmp_path, net, dirs, capsys):
    """A download over 90 days old fails validation: the store is not
    updated, the mirror is tried, then the store."""
    store, staging = dirs
    old = write_finals(tmp_path / "old.all")
    net["files"][URL] = old
    with pytest.raises(I.IERSError, match="has no copy") as ex:
        I.acquire(store, staging, now=at(old, 90.5))
    assert "past the 90-day limit" in str(ex.value) and URL in str(ex.value)
    assert not (store / IERS_A_FILE).exists()
    # the mirror's, newer, is taken (stale: warned)
    net["files"][MIRROR] = newer = write_finals(tmp_path / "newer.all", promote=10)
    e = I.acquire(store, staging, now=at(old, 90.5))
    assert e["source"] == MIRROR and e["reused"] is False and e["age_days"] == 80.5
    assert (store / IERS_A_FILE).read_bytes() == newer.read_bytes()
    err = capsys.readouterr().err
    assert "STALE: the downloaded file" in err and "runs will fail in 9.5 days" in err
    # with a store copy: an old download is refused, the store's (newer)
    # copy reused
    e = I.acquire(store, staging, now=at(old, 95))
    assert e["source"] == MIRROR and e["age_days"] == 85
    net["files"][MIRROR] = old
    e = I.acquire(store, staging, now=at(old, 95))
    assert e["reused"] is True and "before the store copy" in e["source"]


def test_fresh_download_stale_warning(tmp_path, net, dirs, capsys):
    store, staging = dirs
    net["files"][URL] = new = write_finals(tmp_path / "new.all")
    assert I.acquire(store, staging, now=at(new, 30))["reused"] is False
    assert "STALE" not in capsys.readouterr().err
    assert I.acquire(store, staging, now=at(new, 31))["age_days"] == 31
    assert "STALE: the downloaded file" in capsys.readouterr().err


def test_truncated_file_refused():
    """A file whose predictions span under IERS_A_MIN_PREDICTION_DAYS (a cut
    download) does not read; one at 180 days does."""
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        I.read(write_finals(Path(d) / "a.all", after=181))
        with pytest.raises(I.IERSError, match="span only 179 days"):
            I.read(write_finals(Path(d) / "b.all", after=180))


def test_unwritable_store_is_a_warning(tmp_path, net, dirs, capsys):
    """A good download whose store cannot be created is used (reused false),
    with a warning naming the store and the error."""
    _, staging = dirs
    (tmp_path / "file").write_text("x")
    store = tmp_path / "file" / "store"            # its parent is a file
    net["files"][URL] = new = write_finals(tmp_path / "new.all")
    e = I.acquire(store, staging)
    assert e["reused"] is False and e["md5"] == I.md5sum(new)
    assert (staging / IERS_A_FILE).read_bytes() == new.read_bytes()
    err = capsys.readouterr().err
    assert f"cannot update the store {store / IERS_A_FILE}" in err and "NotADirectoryError" in err


def test_no_store_copy_fails(tmp_path, net, dirs):
    store, staging = dirs
    with pytest.raises(I.IERSError, match=f"the store {store} has no copy") as ex:
        I.acquire(store, staging)
    assert "connection refused" in str(ex.value)
    assert not (staging / IERS_A_FILE).exists()


def test_unreadable_store_copy(tmp_path, net, dirs):
    store, staging = dirs
    store.mkdir()
    (store / IERS_A_FILE).write_text("garbage\n")
    with pytest.raises(I.IERSError, match="unusable"):
        I.acquire(store, staging)
    # a good download replaces it
    net["files"][URL] = write_finals(tmp_path / "new.all")
    assert I.acquire(store, staging)["reused"] is False
    assert start_of(store / IERS_A_FILE) == start_of(tmp_path / "new.all")


def test_store_replacement_is_atomic(tmp_path, net, dirs, monkeypatch, capsys):
    """The store is written through a temporary file and renamed: a copy that
    fails half way leaves the old copy whole and no temporary file."""
    store, staging = dirs
    store.mkdir()
    old = write_finals(store / IERS_A_FILE)
    before = old.read_bytes()
    net["files"][URL] = write_finals(tmp_path / "new.all", promote=2)
    real = I.shutil.copyfileobj

    def broken(fi, fo, n=0):
        fo.write(fi.read(100))
        raise OSError("disk full")

    monkeypatch.setattr(I.shutil, "copyfileobj", broken)
    # (owner decision 2026-11-17: the run goes on with the download, warned)
    e = I.acquire(store, staging)
    assert e["reused"] is False and e["source"] == URL
    assert (staging / IERS_A_FILE).read_bytes() == (tmp_path / "new.all").read_bytes()
    assert old.read_bytes() == before
    assert sorted(p.name for p in store.iterdir()) == [IERS_A_FILE]
    err = capsys.readouterr().err
    assert f"WARNING: IERS-A: downloaded {URL}, but cannot update the store {store / IERS_A_FILE}" in err
    assert "disk full" in err

    monkeypatch.setattr(I.shutil, "copyfileobj", real)
    replaced = []
    real_replace = os.replace
    monkeypatch.setattr(I.os, "replace",
                        lambda a, b: (replaced.append((Path(a), Path(b))), real_replace(a, b)))
    I.acquire(store, staging)
    assert (store / IERS_A_FILE).read_bytes() == (tmp_path / "new.all").read_bytes()
    (src, dst), = [r for r in replaced if r[1] == store / IERS_A_FILE]
    assert src.parent == store and src.name.startswith(".") and not src.exists()


def test_fetch_uses_urllib_and_proxy_env(tmp_path, monkeypatch):
    """The real fetch honours the environment's proxy: a dead proxy fails
    fast instead of reaching the network."""
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.delenv("no_proxy", raising=False)
    monkeypatch.delenv("NO_PROXY", raising=False)
    with pytest.raises(OSError):
        I.fetch("https://datacenter.iers.org/data/9/finals2000A.all", tmp_path / "x", timeout=5)


# --------------------------------------------------------------------------
# install: the build's steps and their forked workers
# --------------------------------------------------------------------------

PROBE = '''
import json, multiprocessing, sys
import astropy.units as u
from astropy.coordinates import EarthLocation
from astropy.time import Time
from astropy.utils import iers

MJD = float(sys.argv[1])
# Rubin (X05), as ssp.util.observatory_barycentric_posvel places it, but
# without the MPC observatory-code lookup (which needs a cache or network)
SITE = EarthLocation.from_geodetic(-70.7494 * u.deg, -30.2444 * u.deg, 2650 * u.m)


def probe(_):
    t = iers.earth_orientation_table.get()
    out = dict(cls=type(t).__name__, data_path=str(t.meta.get("data_path")),
               predictive_mjd=float(t.meta["predictive_mjd"]),
               dut1=float(Time(MJD, format="mjd", scale="utc").delta_ut1_utc),
               auto_download=bool(iers.conf.auto_download))
    r, _ = SITE.get_gcrs_posvel(Time(MJD, format="mjd", scale="tai"))
    out["r"] = [float(x) for x in r.xyz.to_value("m").ravel()]
    try:
        Time(MJD + 2000, format="mjd", scale="utc").delta_ut1_utc
        out["beyond"] = "no error"
    except Exception as e:
        out["beyond"] = type(e).__name__
    return out


def main():
    with multiprocessing.get_context("fork").Pool(2) as pool:
        print("PROBE " + json.dumps(dict(parent=probe(0), workers=pool.map(probe, [0, 1]))))
    return 0
'''


def _probe_env(tmp_path, path):
    env = {k: v for k, v in os.environ.items() if k != IERS_A_ENV}
    env.update(PYTHONPATH=str(tmp_path), HOME=str(tmp_path / "home"), XDG_CACHE_HOME=str(tmp_path / "cache"),
               http_proxy="http://127.0.0.1:9", https_proxy="http://127.0.0.1:9",
               HTTP_PROXY="http://127.0.0.1:9", HTTPS_PROXY="http://127.0.0.1:9")
    env.pop("no_proxy", None), env.pop("NO_PROXY", None)
    if path is not None:
        env[IERS_A_ENV] = str(path)
    return env


def test_step_and_forked_workers_use_the_file(tmp_path):
    """A step started as the build starts it (_entry_point, run_logged, the
    path in IERS_A_ENV), with the network blocked, and its forked pool
    workers use exactly the given table, with downloads off; a time beyond
    the table raises rather than downloading."""
    (tmp_path / "home").mkdir()
    (tmp_path / "iersprobe.py").write_text(PROBE)
    f = write_finals(tmp_path / IERS_A_FILE, ut1_offset=0.25)   # unlike any real table
    table = I.read(f)
    mjd = float(table.meta["predictive_mjd"]) + 5
    want = float(table.ut1_utc(mjd + 2400000.5, 0.0).to_value("s"))
    base = float(I.read(write_finals(tmp_path / "base.all")).ut1_utc(mjd + 2400000.5, 0.0).to_value("s"))
    assert abs(want - base - 0.25) < 1e-9

    log = tmp_path / "probe.log"
    code, _, _ = B.run_logged([*B._entry_point("iersprobe"), str(mjd)], log, env=_probe_env(tmp_path, f))
    text = log.read_text()
    assert code == 0, text
    out = json.loads(text.split("PROBE ", 1)[1].splitlines()[0])
    for p in [out["parent"], *out["workers"]]:
        assert p["cls"] == "IERS_A" and p["data_path"] == str(f)
        assert p["predictive_mjd"] == float(table.meta["predictive_mjd"])
        assert abs(p["dut1"] - want) < 1e-9
        assert p["auto_download"] is False
        assert p["beyond"] == "IERSRangeError"
        assert p["r"] == out["parent"]["r"]
    assert not any((tmp_path / "home").iterdir())        # nothing written under HOME
    # the position depends on the table: with the unshifted one it differs
    import astropy.units as u
    from astropy.coordinates import EarthLocation
    from astropy.time import Time
    from astropy.utils import iers
    site = EarthLocation.from_geodetic(-70.7494 * u.deg, -30.2444 * u.deg, 2650 * u.m)
    with iers.earth_orientation_table.set(table):      # (a Time caches its UT1-UTC: one each)
        same = site.get_gcrs_posvel(Time(mjd, format="mjd", scale="tai"))[0].xyz.to_value("m")
    with iers.earth_orientation_table.set(I.read(tmp_path / "base.all")):
        other = site.get_gcrs_posvel(Time(mjd, format="mjd", scale="tai"))[0].xyz.to_value("m")
    assert max(abs(a - b) for a, b in zip(same, out["parent"]["r"])) < 1e-6
    assert max(abs(a - b) for a, b in zip(other, out["parent"]["r"])) > 10      # 0.25 s of UT1: ~100 m


def test_step_without_the_file_fails(tmp_path):
    (tmp_path / "home").mkdir()
    (tmp_path / "iersprobe.py").write_text(PROBE)
    log = tmp_path / "probe.log"
    code, _, _ = B.run_logged([*B._entry_point("iersprobe"), "61000"], log, env=_probe_env(tmp_path, None))
    assert code != 0 and f"{IERS_A_ENV} is not set" in log.read_text()
    code, _, _ = B.run_logged([*B._entry_point("iersprobe"), "61000"], log,
                              env=_probe_env(tmp_path, tmp_path / "missing.all"))
    assert code != 0 and "no IERS-A file" in log.read_text()


def test_install_from_env_optional(monkeypatch):
    monkeypatch.delenv(IERS_A_ENV, raising=False)
    assert I.install_from_env() is None
    with pytest.raises(I.IERSError, match="not set"):
        I.install_from_env(required=True)
