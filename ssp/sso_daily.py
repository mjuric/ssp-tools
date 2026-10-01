"""The daily SSO build and delivery: the three stages in sequence.

    ssp-sso-daily WORK_DIR [--upload CONFIG] [--dry-run] [--reuse-inputs DIR]
                  [--stamp STAMP]

runs, in a fresh dated directory DAY = WORK_DIR/<stamp> (default: today's
UTC date, YYYY-MM-DD):

    ssp-extract-sso-inputs DAY/inputs        (skipped with --reuse-inputs DIR)
    ssp-build-sso DAY/inputs DAY/run         (or DIR in place of DAY/inputs)
    ssp-upload-sso CONFIG DAY/run [--dry-run]   (only with --upload)

Each stage is its own console script, run as a subprocess and looked up
first next to the running Python (Path(sys.executable).parent, the venv's
bin/), then on PATH. The first that fails stops the run, and its exit status
is the wrapper's (128+N for a stage killed by signal N). Every command and
its outcome is appended to DAY/daily.log.

Run it at most once per load window when uploading: the loader silently
drops an upload made while its previous load is still running (see
ssp.sso_upload).
"""

import argparse
import logging
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

_LOG = logging.getLogger("ssp.sso_daily")

EXTRACT = "ssp-extract-sso-inputs"
BUILD = "ssp-build-sso"
UPLOAD = "ssp-upload-sso"


def _local_bin():
    """The running Python's bin/ (the venv's), searched before PATH."""
    return Path(sys.executable).parent


def resolve_command(name):
    """``name`` in the running Python's bin/ if it is there, else on PATH."""
    local = _local_bin() / name
    if local.is_file() and os.access(local, os.X_OK):
        return str(local)
    return shutil.which(name) or name


def utc_stamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def plan(day, upload=None, dry_run=False, reuse_inputs=None):
    """The commands to run, as [(stage, argv)]."""
    inputs = Path(reuse_inputs) if reuse_inputs else day / "inputs"
    run_dir = day / "run"
    cmds = []
    if not reuse_inputs:
        cmds.append(("extract", [EXTRACT, str(inputs)]))
    cmds.append(("build", [BUILD, str(inputs), str(run_dir)]))
    if upload:
        cmds.append(("upload", [UPLOAD, str(upload), str(run_dir)] + (["--dry-run"] if dry_run else [])))
    return cmds


def _log(day, msg):
    line = f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}"
    _LOG.info("%s", msg)
    with open(day / "daily.log", "a") as f:
        f.write(line + "\n")


def run(work_dir, upload=None, dry_run=False, reuse_inputs=None, stamp=None):
    """Run the stages. Returns the exit status (0, or the first failure's)."""
    if dry_run and not upload:
        raise SystemExit("ssp-sso-daily: --dry-run applies to the upload; give --upload CONFIG")
    if reuse_inputs and not Path(reuse_inputs).is_dir():
        raise SystemExit(f"ssp-sso-daily: --reuse-inputs {reuse_inputs}: not a directory")
    stamp = stamp or utc_stamp()
    if "/" in stamp or os.sep in stamp or stamp in (".", "..") or ".." in stamp:
        raise SystemExit(f"ssp-sso-daily: --stamp {stamp!r}: must be a plain name (no '/' or '..')")
    day = Path(work_dir) / stamp
    if day.exists() and any(day.iterdir()):
        raise SystemExit(f"ssp-sso-daily: {day} exists already; remove it, or pick another --stamp")
    day.mkdir(parents=True, exist_ok=True)

    _log(day, f"ssp-sso-daily in {day}" + (f", reusing inputs {reuse_inputs}" if reuse_inputs else ""))
    for stage, argv in plan(day, upload, dry_run, reuse_inputs):
        argv = [resolve_command(argv[0])] + argv[1:]
        _log(day, f"{stage}: {shlex.join(argv)}")
        t0 = time.monotonic()
        try:
            rc = subprocess.run(argv).returncode
        except FileNotFoundError:
            _log(day, f"{stage}: {argv[0]} not found in {_local_bin()} or on PATH")
            return 127
        if rc < 0:                      # killed by signal -rc: report it as a shell would
            _log(day, f"{stage}: killed by signal {-rc}")
            rc = 128 - rc
        _log(day, f"{stage}: exit {rc} after {time.monotonic() - t0:.1f} s")
        if rc != 0:
            _log(day, f"stopping: {stage} failed")
            return rc
    _log(day, "done")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="ssp-sso-daily",
        description="The daily SSO build and delivery: extract the inputs, build and check the "
                    "delivery, and optionally upload it, in WORK_DIR/<UTC date>/.",
    )
    parser.add_argument("work_dir", metavar="WORK_DIR",
                        help="parent of the dated run directories")
    parser.add_argument("--upload", metavar="CONFIG",
                        help="upload with this ssp-upload-sso config (a path, or dev/int/prod); "
                             "without it nothing is uploaded")
    parser.add_argument("--dry-run", action="store_true", help="pass --dry-run to ssp-upload-sso")
    parser.add_argument("--reuse-inputs", metavar="DIR",
                        help="skip the extraction and build from this existing INPUTS_DIR")
    parser.add_argument("--stamp", help="name of the run directory under WORK_DIR "
                                        "(default: today's UTC date, YYYY-MM-DD)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return run(args.work_dir, upload=args.upload, dry_run=args.dry_run,
               reuse_inputs=args.reuse_inputs, stamp=args.stamp)


if __name__ == "__main__":
    sys.exit(main())
