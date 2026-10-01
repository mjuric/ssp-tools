"""The daily SSO build and delivery: the three stages in sequence.

    ssp-sso-daily WORK_DIR [--upload CONFIG] [--dry-run] [--reuse-inputs DIR]
                  [--stamp STAMP]

runs, in a fresh dated directory DAY = WORK_DIR/<stamp> (default: today's
UTC date, YYYY-MM-DD):

    ssp-extract-sso-inputs DAY/inputs        (skipped with --reuse-inputs DIR)
    ssp-build-sso DAY/inputs DAY/run         (or DIR in place of DAY/inputs)
    ssp-upload-sso CONFIG DAY/run [--dry-run]   (only with --upload)

Each stage is its own console script, run as a subprocess; the first that
fails stops the run, and its exit status is the wrapper's. Every command
and its outcome is appended to DAY/daily.log.
"""

import argparse
import logging
import shlex
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

_LOG = logging.getLogger("ssp.sso_daily")

EXTRACT = "ssp-extract-sso-inputs"
BUILD = "ssp-build-sso"
UPLOAD = "ssp-upload-sso"


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
    line = f"{datetime.now(UTC).isoformat(timespec='seconds')} {msg}"
    _LOG.info("%s", msg)
    with open(day / "daily.log", "a") as f:
        f.write(line + "\n")


def run(work_dir, upload=None, dry_run=False, reuse_inputs=None, stamp=None):
    """Run the stages. Returns the exit status (0, or the first failure's)."""
    if dry_run and not upload:
        raise SystemExit("ssp-sso-daily: --dry-run applies to the upload; give --upload CONFIG")
    if reuse_inputs and not Path(reuse_inputs).is_dir():
        raise SystemExit(f"ssp-sso-daily: --reuse-inputs {reuse_inputs}: not a directory")
    stamp = stamp or datetime.now(UTC).strftime("%Y-%m-%d")
    day = Path(work_dir) / stamp
    if day.exists() and any(day.iterdir()):
        raise SystemExit(f"ssp-sso-daily: {day} exists already; remove it, or pick another --stamp")
    day.mkdir(parents=True, exist_ok=True)

    _log(day, f"ssp-sso-daily in {day}" + (f", reusing inputs {reuse_inputs}" if reuse_inputs else ""))
    for stage, argv in plan(day, upload, dry_run, reuse_inputs):
        _log(day, f"{stage}: {shlex.join(argv)}")
        t0 = time.monotonic()
        try:
            rc = subprocess.run(argv).returncode
        except FileNotFoundError:
            _log(day, f"{stage}: {argv[0]} not found on PATH")
            return 127
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
