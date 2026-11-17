"""The IERS-A Earth-orientation file as one of the day's inputs.

See docs/design/iers-a.md and ssp.delivery_contract (IERS_A_*).

The extract (``ssp-extract-sso-inputs --iers-a-store DIR``) calls
``acquire``: download ``finals2000A.all`` from astropy's configured
``iers_auto_url``, then ``iers_auto_url_mirror``, straight into a file in
the extract's staging directory (never through astropy's cache, never under
$HOME); validate it; on success replace the store's copy atomically; on
failure fall back to the store's copy, with a warning, failing past
IERS_A_MAX_AGE_DAYS.

The build uses exactly the file the manifest records: it passes its path to
every step subprocess in IERS_A_ENV, and ``install_from_env`` (called where
the step's ``main`` is started, ssp.sso_build._entry_point) installs it with
``install``: downloads off, the table read from that file set as astropy's
``earth_orientation_table``. The steps' process pools fork, so their workers
inherit the installed table. A pool that does not fork would have to call
``install_from_env`` in its initializer.
"""

from __future__ import annotations

import datetime
import hashlib
import os
import shutil
import sys
import time
import urllib.request
from pathlib import Path

from ssp.delivery_contract import (
    IERS_A_ENV,
    IERS_A_FILE,
    IERS_A_MAX_AGE_DAYS,
    IERS_A_STALE_DAYS,
)

#: Socket timeout of a download [s], and the longest a whole download may
#: take [s].
DOWNLOAD_TIMEOUT = 60
DOWNLOAD_DEADLINE = 300

_MJD0 = datetime.date(1858, 11, 17)


class IERSError(RuntimeError):
    """No usable IERS-A file."""


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def iso(t):
    """ISO 8601 UTC, to the second, with a Z (as the other manifest
    entries)."""
    return t.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def md5sum(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 22):
            h.update(chunk)
    return h.hexdigest()


def count_lines(path):
    with open(path, "rb") as f:
        return sum(1 for _ in f)


# --------------------------------------------------------------------------
# Reading and installing
# --------------------------------------------------------------------------

def read(path):
    """The IERS_A table of ``path`` (astropy ``IERS_A.read``). Raises
    IERSError if it is missing, does not parse, or has no predicted rows."""
    from astropy.utils import iers

    path = Path(path)
    if not path.is_file():
        raise IERSError(f"no IERS-A file {path}")
    try:
        table = iers.IERS_A.read(path)
    except Exception as e:
        raise IERSError(f"{path} is not a readable IERS-A (finals2000A) file: {type(e).__name__}: {e}") \
            from None
    if len(table) == 0:
        raise IERSError(f"{path} has no IERS-A rows")
    if table.meta.get("predictive_index", len(table)) >= len(table):
        raise IERSError(f"{path} has no predicted rows")
    return table


def predictions_start(table):
    """The ISO date (YYYY-MM-DD) of ``table``'s first predicted row."""
    mjd = float(table.meta["predictive_mjd"])
    return (_MJD0 + datetime.timedelta(days=int(mjd))).isoformat()


def age_days(predictions_start, when):
    """``when`` (an aware datetime) minus 00:00 UTC of ``predictions_start``
    (an ISO date), in days, rounded to 0.01."""
    start = datetime.datetime.combine(datetime.date.fromisoformat(predictions_start), datetime.time(),
                                      datetime.timezone.utc)
    return round((when - start).total_seconds() / 86400, 2)


def configure():
    """astropy never downloads Earth-orientation data or leap seconds in this
    process. (``auto_max_age`` applies only to IERS_Auto, which is no longer
    used once ``install`` sets an IERS_A table: our own code enforces the
    IERS_A_MAX_AGE_DAYS limit, in the extract and in the build's manifest
    check.)"""
    from astropy.utils import iers

    iers.conf.auto_download = False
    iers.conf.auto_max_age = IERS_A_MAX_AGE_DAYS


def install(path):
    """Use exactly the IERS-A file ``path`` for UT1-UTC and polar motion in
    this process (and in processes forked from it): downloads off, its table
    set as astropy's ``earth_orientation_table``. Returns the table. Raises
    IERSError if it is missing or unreadable."""
    from astropy.utils import iers

    configure()
    table = read(path)
    iers.earth_orientation_table.set(table)
    return table


def install_from_env(required=False):
    """``install`` the file named by IERS_A_ENV. Without it: an IERSError
    if ``required``, else nothing (astropy's defaults). Returns the table
    or None."""
    path = os.environ.get(IERS_A_ENV)
    if not path:
        if required:
            raise IERSError(f"{IERS_A_ENV} is not set: the build passes every step the IERS-A file to use")
        return None
    return install(path)


# --------------------------------------------------------------------------
# Downloading (the extract)
# --------------------------------------------------------------------------

def urls():
    """astropy's configured URLs (astropy.cfg may change them):
    ``iers_auto_url``, then ``iers_auto_url_mirror``."""
    from astropy.utils import iers

    return [iers.conf.iers_auto_url, iers.conf.iers_auto_url_mirror]


def fetch(url, dest, timeout=DOWNLOAD_TIMEOUT, deadline=DOWNLOAD_DEADLINE):
    """Download ``url`` into ``dest`` (urllib: the environment's proxy
    settings apply). Tests replace this function."""
    t0 = time.monotonic()
    req = urllib.request.Request(url, headers={"User-Agent": "ssp-tools (ssp-extract-sso-inputs)"})
    with urllib.request.urlopen(req, timeout=timeout) as r, open(dest, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
            if time.monotonic() - t0 > deadline:
                raise TimeoutError(f"download took over {deadline} s")


def _fsync_dir(d):
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def copy_atomic(src, dst):
    """Copy ``src`` to ``dst`` through a temporary file next to ``dst``:
    write, fsync, rename, fsync the directory."""
    dst = Path(dst)
    tmp = dst.parent / f".{dst.name}.{os.getpid()}.tmp"
    try:
        with open(src, "rb") as fi, open(tmp, "wb") as fo:
            shutil.copyfileobj(fi, fo, 1 << 22)
            fo.flush()
            os.fsync(fo.fileno())
        os.replace(tmp, dst)
    finally:
        if tmp.exists():
            tmp.unlink()
    _fsync_dir(dst.parent)


def describe(path, source, extracted, start, reused):
    """The manifest entry (IERS_A_MANIFEST_FIELDS) of ``path``."""
    return {
        "file": IERS_A_FILE,
        "md5": md5sum(path),
        "rows": count_lines(path),
        "source": str(source),
        "extracted_utc": iso(extracted),
        "predictions_start": start,
        "age_days": float(age_days(start, extracted)),
        "reused": bool(reused),
    }


def _warn(msg):
    print(f"WARNING: {msg}", file=sys.stderr, flush=True)


def _print(msg):
    print(msg, flush=True)


def acquire(store_dir, staging_dir, now=utcnow, log=_print):
    """Put the day's IERS-A file in ``staging_dir/IERS_A_FILE`` and return
    its manifest entry (see the module docstring and docs/design/iers-a.md).
    Raises IERSError, with the store, the copy's age and the download error,
    when there is neither a good download nor a store copy within
    IERS_A_MAX_AGE_DAYS."""
    store_dir, staging_dir = Path(store_dir), Path(staging_dir)
    store = store_dir / IERS_A_FILE
    dest = staging_dir / IERS_A_FILE
    part = staging_dir / f".{IERS_A_FILE}.download"

    store_start, store_error = None, None
    if store.exists():
        try:
            store_start = predictions_start(read(store))
        except IERSError as e:
            store_error = str(e)
            _warn(f"IERS-A: the store's copy is unusable: {e}")

    errors = []
    for url in urls():
        try:
            fetch(url, part)
            start = predictions_start(read(part))
            if store_start is not None and start < store_start:
                raise IERSError(f"its predictions start {start}, before the store copy's ({store_start})")
        except Exception as e:      # any failure, a validation failure included
            errors.append(f"{url}: {type(e).__name__}: {e}")
            log(f"IERS-A: download from {url} failed: {type(e).__name__}: {e}")
            if part.exists():
                part.unlink()
            continue
        extracted = now()
        os.replace(part, dest)
        try:
            store_dir.mkdir(parents=True, exist_ok=True)
            copy_atomic(dest, store)
        except OSError as e:
            raise IERSError(f"IERS-A: downloaded {url}, but cannot update the store {store}: "
                            f"{type(e).__name__}: {e}") from None
        entry = describe(dest, url, extracted, start, reused=False)
        log(f"IERS-A: downloaded {url}: predictions from {start}, {entry['age_days']} days old; "
            f"store {store} updated")
        return entry

    error = "; ".join(errors)
    if not store.exists():
        raise IERSError(f"IERS-A download failed ({error}), and the store {store_dir} has no copy "
                        f"of {IERS_A_FILE}")
    if store_start is None:
        raise IERSError(f"IERS-A download failed ({error}), and the store's copy {store} is unusable "
                        f"({store_error})")
    extracted = now()
    copy_atomic(store, dest)
    entry = describe(dest, f"reused {store} (download failed: {error})", extracted, store_start,
                     reused=True)
    age = entry["age_days"]
    if age > IERS_A_MAX_AGE_DAYS:
        dest.unlink()
        raise IERSError(f"IERS-A download failed ({error}), and the store's copy {store} is {age} days "
                        f"old (predictions from {store_start}), past the {IERS_A_MAX_AGE_DAYS}-day limit")
    _warn(f"IERS-A download failed ({error}); reusing the store's copy {store}, {age} days old "
          f"(predictions from {store_start})")
    if age > IERS_A_STALE_DAYS:
        _warn(f"IERS-A is STALE: the reused copy is {age} days old (over {IERS_A_STALE_DAYS}); runs will "
              f"fail in {round(IERS_A_MAX_AGE_DAYS - age, 2)} days unless a download succeeds")
    return entry
