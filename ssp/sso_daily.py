"""The daily SSO build and delivery: the stages in sequence.

    ssp-sso-daily WORK_DIR [--upload CONFIG] [--dry-run] [--reuse-inputs DIR]
                  [--stamp STAMP] [--correction-table DIR] [--skip-stage0]
                  [--stage0-workers N] [--part-rows N]
                  [--internal-columns A,B,...]

runs, in a fresh dated directory DAY = WORK_DIR/<stamp> (default: today's
UTC date, YYYY-MM-DD):

    shutter-timing-table --out CT --refresh-recent 3 --workers N
                                             (stage 0; skipped with
                                              --skip-stage0 or --reuse-inputs)
    ssp-extract-sso-inputs DAY/inputs --correction-table CT
                                             (skipped with --reuse-inputs DIR)
    ssp-build-sso DAY/inputs DAY/run [--part-rows N]
                  [--internal-columns A,B,...]
                                             (or DIR in place of DAY/inputs)
    ssp-upload-sso CONFIG DAY/run [--dry-run]   (only with --upload)

--part-rows and --internal-columns go to ssp-build-sso as given (and so
to the SSObservation builder); without them, its defaults
(ssp.ssobservation_contract PART_ROWS_DEFAULT and
SSOBSERVATION_INTERNAL_DEFAULT) apply.

CT, the shutter-timing correction table, defaults to
DEFAULT_CORRECTION_TABLE. Stage 0 brings it up to date
(docs/design/shutter-timing.md): it resumes, skips nights already built,
re-checks the last 3 nights for raws that arrived late, and holds
CT/.lock while it runs. Its output goes to DAY/stage0.log. Its exit 2
(refused: a calibration mismatch, the lock held, an unknown table format,
an integrity failure) and 3 (it left the table mixed) are explained in
daily.log and on stderr; see docs/runbooks/sso-daily.md, "Stage 0".

Each stage is its own console script, run as a subprocess and looked up
first next to the running Python (Path(sys.executable).parent, the venv's
bin/, so stage 0 runs the shutter-timing pinned in ssp-tools' venv), then
on PATH. The first that fails stops the run, and its exit status is the
wrapper's (128+N for a stage killed by signal N). Every command, its
duration and its exit status are appended to DAY/daily.log.

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

from .ssobservation_contract import PART_ROWS_DEFAULT, SSOBSERVATION_INTERNAL_DEFAULT

_LOG = logging.getLogger("ssp.sso_daily")

STAGE0 = "shutter-timing-table"
EXTRACT = "ssp-extract-sso-inputs"
BUILD = "ssp-build-sso"
UPLOAD = "ssp-upload-sso"

#: The production correction table (docs/design/shutter-timing.md).
DEFAULT_CORRECTION_TABLE = "/sdf/data/rubin/user/mjuric/shutter-timing/corrections"
#: Stage 0 re-checks this many of the last nights written for late raws.
REFRESH_RECENT = 3
#: Stage 0's worker processes: the default and the shared-node limit.
STAGE0_WORKERS = 32
MAX_STAGE0_WORKERS = 32
STAGE0_LOG = "stage0.log"
RUNBOOK = "docs/runbooks/sso-daily.md"
#: shutter-timing-table's exit codes that stop the run with an explanation.
STAGE0_EXIT = {
    2: "the builder refused (a calibration mismatch, its lock held by another build, an unknown "
       "table format, or an integrity failure)",
    3: "the builder left the correction table mixed (nights on more than one calibration)",
}


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


def plan(day, upload=None, dry_run=False, reuse_inputs=None, correction_table=DEFAULT_CORRECTION_TABLE,
         skip_stage0=False, stage0_workers=STAGE0_WORKERS, part_rows=None, internal_columns=None):
    """The commands to run, as [(stage, argv)]. ``part_rows`` and
    ``internal_columns`` (a comma-separated string; '' for none) go to
    ssp-build-sso when given (not None)."""
    inputs = Path(reuse_inputs) if reuse_inputs else day / "inputs"
    run_dir = day / "run"
    cmds = []
    if not reuse_inputs:
        if not skip_stage0:
            cmds.append(("stage0", [STAGE0, "--out", str(correction_table),
                                    "--refresh-recent", str(REFRESH_RECENT),
                                    "--workers", str(stage0_workers)]))
        cmds.append(("extract", [EXTRACT, str(inputs), "--correction-table", str(correction_table)]))
    build = [BUILD, str(inputs), str(run_dir)]
    if part_rows is not None:
        build += ["--part-rows", str(part_rows)]
    if internal_columns is not None:
        build += ["--internal-columns", internal_columns]
    cmds.append(("build", build))
    if upload:
        cmds.append(("upload", [UPLOAD, str(upload), str(run_dir)] + (["--dry-run"] if dry_run else [])))
    return cmds


def _log(day, msg):
    line = f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}"
    _LOG.info("%s", msg)
    with open(day / "daily.log", "a") as f:
        f.write(line + "\n")


def _stage0_failure(day, rc, correction_table):
    """Explain a stage-0 failure in daily.log and on stderr."""
    what = STAGE0_EXIT.get(rc, "the builder failed")
    msg = (f"stage0: {STAGE0} exit {rc}: {what}. The correction table {correction_table} was not "
           f"brought up to date, so the run stops before the extract. See {day / STAGE0_LOG} (and "
           f"{Path(correction_table) / 'build.log'}) for the builder's message, and {RUNBOOK}, "
           "\"Stage 0\", for what to do.")
    _log(day, msg)
    try:
        tail = [ln for ln in (day / STAGE0_LOG).read_text(errors="replace").splitlines() if ln.strip()][-5:]
    except OSError:
        tail = []
    for ln in tail:
        _log(day, f"stage0: | {ln}")
    print(f"ssp-sso-daily: {msg}", file=sys.stderr)


def run(work_dir, upload=None, dry_run=False, reuse_inputs=None, stamp=None,
        correction_table=DEFAULT_CORRECTION_TABLE, skip_stage0=False, stage0_workers=STAGE0_WORKERS,
        part_rows=None, internal_columns=None):
    """Run the stages. Returns the exit status (0, or the first failure's)."""
    if part_rows is not None and int(part_rows) < 1:
        raise SystemExit(f"ssp-sso-daily: --part-rows {part_rows}: must be at least 1")
    if not 1 <= int(stage0_workers) <= MAX_STAGE0_WORKERS:
        raise SystemExit(f"ssp-sso-daily: --stage0-workers {stage0_workers}: must be in "
                         f"1..{MAX_STAGE0_WORKERS} (shared-node limit)")
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
    if reuse_inputs:
        _log(day, "stage0: skipped (--reuse-inputs: no extract)")
    elif skip_stage0:
        _log(day, f"stage0: skipped (--skip-stage0); the extract reads {correction_table} as it is")
    stages = plan(day, upload, dry_run, reuse_inputs, correction_table, skip_stage0, stage0_workers,
                  part_rows, internal_columns)
    for stage, argv in stages:
        argv = [resolve_command(argv[0])] + argv[1:]
        redirect = f" > {day / STAGE0_LOG} 2>&1" if stage == "stage0" else ""
        _log(day, f"{stage}: {shlex.join(argv)}{redirect}")
        t0 = time.monotonic()
        try:
            if stage == "stage0":
                with open(day / STAGE0_LOG, "ab") as out:
                    rc = subprocess.run(argv, stdout=out, stderr=subprocess.STDOUT).returncode
            else:
                rc = subprocess.run(argv).returncode
        except FileNotFoundError:
            _log(day, f"{stage}: {argv[0]} not found in {_local_bin()} or on PATH")
            return 127
        if rc < 0:                      # killed by signal -rc: report it as a shell would
            _log(day, f"{stage}: killed by signal {-rc}")
            rc = 128 - rc
        _log(day, f"{stage}: exit {rc} after {time.monotonic() - t0:.1f} s")
        if rc != 0:
            if stage == "stage0" and rc > 0 and rc < 128:
                _stage0_failure(day, rc, correction_table)
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
    parser.add_argument("--correction-table", metavar="DIR", default=DEFAULT_CORRECTION_TABLE,
                        help="the shutter-timing correction table: stage 0 updates it, the extract "
                             f"reads it (default: {DEFAULT_CORRECTION_TABLE})")
    parser.add_argument("--skip-stage0", action="store_true",
                        help=f"don't run stage 0 ({STAGE0}); the extract reads the table as it is "
                             "(testing)")
    parser.add_argument("--stage0-workers", type=int, default=STAGE0_WORKERS, metavar="N",
                        help=f"stage 0's worker processes (default and maximum {MAX_STAGE0_WORKERS})")
    parser.add_argument("--part-rows", type=int, default=None, metavar="N",
                        help="passed to ssp-build-sso: SSObservation parts close at the first object "
                             f"boundary at or after N rows (default: {PART_ROWS_DEFAULT:,})")
    parser.add_argument("--internal-columns", default=None, metavar="A,B,...",
                        help="passed to ssp-build-sso: the SSObservation columns written to the sidecar "
                             "(not uploaded) instead of the delivered table; '' for none (default: "
                             f"{','.join(SSOBSERVATION_INTERNAL_DEFAULT)})")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return run(args.work_dir, upload=args.upload, dry_run=args.dry_run,
               reuse_inputs=args.reuse_inputs, stamp=args.stamp, correction_table=args.correction_table,
               skip_stage0=args.skip_stage0, stage0_workers=args.stage0_workers,
               part_rows=args.part_rows, internal_columns=args.internal_columns)


if __name__ == "__main__":
    sys.exit(main())
