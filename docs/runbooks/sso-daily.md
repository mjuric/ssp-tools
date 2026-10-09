# Runbook: the daily PPDB Solar System tables

This builds and delivers the six PPDB Solar System tables (RFC-1188), in three stages that talk only through files, after a stage 0 that updates the shutter-timing correction table:

| stage | command | touches |
|---|---|---|
| 0. corrections | `shutter-timing-table --out CT --refresh-recent 3 --workers 32` | the LSSTCam raw zips (read), the correction table CT |
| 1. extract | `ssp-extract-sso-inputs INPUTS_DIR` | the MPC replica at USDF, ClickHouse |
| 2. build | `ssp-build-sso INPUTS_DIR RUN_DIR` | files only |
| 3. deliver | `ssp-upload-sso CONFIG RUN_DIR [--dry-run]` | GCS, Pub/Sub |
| all of them | `ssp-sso-daily WORK_DIR [--upload CONFIG] [--dry-run]` | runs 0–3 in `WORK_DIR/<UTC date>/` |

The tables: SSObservation, SSObject, NearbySSO, `mpc_orbits`, `current_identifications` and `numbered_identifications`, per `sdm_schemas` `ppdb.yaml`/`sso_base.yaml` (tickets/DM-55375, lsst/sdm_schemas#549; copies in `tests/data/sdm_schemas/`).

Design: `docs/design/sso-delivery.md` (the stages), `docs/design/sssource-widened.md` (SSObservation, SSObject), `docs/design/nearbysso.md` (NearbySSO) and `docs/design/nongrav.md` (non-gravitational forces, and comets in NearbySSO). The contract between the stages is `ssp/delivery_contract.py`.

## Setup (once)

- **Code:** an ssp-tools **source checkout** with its venv.
  - `uv sync --extra all`; building ASSIST needs a C compiler.
  - The build stage's SSObservation checks need `bench/` from the checkout, and the upload configs live in `config/sso-upload/`. So run from the checkout (an editable install).
  - If a console script is missing after a pull, refresh the entry points: `VIRTUAL_ENV=$PWD/.venv uv pip install --no-deps -e .`
- **ASSIST data:** `data/assist/linux_p1550p2650.440` and `data/assist/sb441-n16.bsp`. Export `SSP_ASSIST_PLANETS` and `SSP_ASSIST_ASTEROIDS`, and set `OMP_NUM_THREADS=1`.
- **The correction table** (CT): `/sdf/data/rubin/user/mjuric/shutter-timing/corrections`, built by `shutter-timing-table` (the `shutter-timing` package, pinned in ssp-tools' venv; `docs/design/shutter-timing.md`). `--correction-table DIR` points stage 0 and the extract at another one. Stage 0 reads the raw zips under `/sdf/data/rubin/lsstdata/offline/instrument/LSSTCam`.
- **The MPC replica:** `mpcorb-db.slac.stanford.edu:5432`, database `mpc_sbn`, user `rubin`, password in `~/.pgpass`. It is exported read-only, in one REPEATABLE READ transaction.
- **ClickHouse:** the host is read from `~/.clickhouse.host` (a line `river:<host>`, kept current by the server's operators; `sdfiana032.sdf.slac.stanford.edu:8123` since 2026-10-05), port 8123, HTTP only, user `ssp_xmatch`, read-only. `--ch-host` overrides it. The tools keep it out of SDF's HTTP proxy, which refuses it. When the host moves, add a `~/.chpass` line for the new one. It needs `SELECT` on `ssp.*` and `ppdb.DiaSource`. Credentials go in `~/.chpass` (mode 600, `host:port:database:user:password`). The server is shared, so use **at most 8 concurrent queries**; that is the default.
- **Uploads:** Google application-default credentials for the `sso-uploader` service account, which has object-user access on the bucket and publish access on the topic (`lsst/idf_deploy` `services/sso-uploader.tf`). **Not yet issued to SSP**; ask the DM-55678 owners.
- **Machine:** about 30 GB of RAM (the extract peaks at 28 GB), and 32 or more cores for the build.
  - Disk: about 5.5 GB for the inputs and about 5.5 GB for the delivery, per day.
  - `fast-export`'s temporary CSVs (about 15 GB) go inside `INPUTS_DIR/.partial`, not `/tmp`.

## The daily run

```bash
cd $SSP_TOOLS
export SSP_ASSIST_PLANETS=$PWD/data/assist/linux_p1550p2650.440 SSP_ASSIST_ASTEROIDS=$PWD/data/assist/sb441-n16.bsp OMP_NUM_THREADS=1
.venv/bin/ssp-sso-daily /sdf/data/rubin/user/mjuric/sso-delivery/daily --upload dev            # real upload
.venv/bin/ssp-sso-daily /sdf/data/rubin/user/mjuric/sso-delivery/daily --upload dev --dry-run  # no remote effect
```

The run happens in `WORK_DIR/<UTC date>/`. `--stamp NAME` uses a different name, and an existing non-empty day directory is refused.

| path | contents |
|---|---|
| `stage0.log` | stage 0: the builder's output (it also appends to `CT/build.log`) |
| `inputs/` | stage 1: `obs_sbn`, `mpc_orbits`, `current_identifications`, `numbered_identifications` (raw MPC, one snapshot), `dia_sources`, `ppdb_dia_sources` and `manifest.json` |
| `run/delivery/` | stage 2: `<Table>.parquet` for five of the six tables; SSObservation as parts, a manifest and a sidecar (below) |
| `run/report.json` | the steps (status, time, peak memory of the largest process, commit; for `ssobservation` also `part_rows` and `internal_columns`), the tables (rows, md5; for SSObservation the manifest and its md5, the parts, the sidecar and the total rows and bytes), the checks, `deliverable`, and the uploads |
| `run/checks/`, `run/logs/` | the check reports and each step's log |
| `daily.log` | the commands run, their durations and exit codes |

`ssp-sso-daily` stops at the first failing stage and exits with its status.

- `--skip-stage0` runs the extract on the correction table as it is (testing).
- `--reuse-inputs DIR` skips stage 0 too: there is no extract to feed.
- `--stage0-workers N` (default and maximum 32) sets the builder's worker processes.
- `--part-rows N` and `--internal-columns A,B,...` go to `ssp-build-sso` (below). Without them, its defaults apply.

## The partitioned SSObservation

SSObservation is delivered as a series of Parquet parts, with a manifest describing them, and a sidecar of internal columns (`docs/design/ssobservation-delivery.md`; the rules are in `ssp/ssobservation_contract.py`, "The partitioned delivery"):

```
run/delivery/SSObservation.part0000.parquet   ssObjectId range 1
run/delivery/SSObservation.part0001.parquet   ...
run/delivery/SSObservation.partNNNN.parquet   the rows with a NULL ssObjectId, last
run/delivery/SSObservation.manifest.json      the parts: rows, ssObjectId range, bytes, md5; the totals; the sidecar
run/delivery/SSObservation_internal.parquet   the sidecar: obsid + the internal columns (NOT uploaded)
```

- **Parts.** Rows are in (`ssObjectId`, `midpointMjdTai`, `obsid`) order across the parts taken in order. A part holds whole objects: it closes at the first object boundary at or after `--part-rows` rows (default 2,000,000), so its `ssObjectId` range is disjoint from the others'. The rows the MPC hasn't linked to an object (NULL `ssObjectId`) come last, in their own part(s). Every part has exactly the Felis schema of SSObservation. BigQuery loads them with `uris = ['gs://.../SSObservation.part*.parquet']`.
- **The manifest** is written last; the delivery check (`ssp.delivery_check`) checks every part, and the sidecar, against it.
- **The sidecar** holds the internal columns: computed by the build, kept for our own use, but not in the Felis schema or the delivered table. One row per SSObservation row, in the same order, keyed by `obsid`. It stays in `run/delivery/` but is **never uploaded**. The default internal columns are `matchMethod` and `midpointMjdTai_flag_degraded`.
- **Options** (`ssp-build-sso` and `ssp-sso-daily`, passed to the builder and recorded in `report.json`'s `steps.ssobservation`):
  - `--part-rows N`: the part size (default 2,000,000).
  - `--internal-columns A,B,...`: the internal columns (default `matchMethod,midpointMjdTai_flag_degraded`; `''` for none, which leaves the sidecar with `obsid` only). Only columns the build computes and that aren't in the Felis schema can be internal: a column returning to the delivered table goes back into `sso_base.yaml` first.
  - With `--from` a later step, these must be those the kept `ssobservation` step was built with (or not given); rerun `--from ssobservation` to change them.
- **Reading it.** `ssp.ssobservation_parts.read_ssobservation(RUN_DIR/delivery)` gives the parts as one table (`internal=True` joins the sidecar). `ssp-build-ssobject` and the bench tools take the delivery directory or the manifest wherever they took `SSObservation.parquet`; a single SSObservation Parquet file from before the partitioned delivery still works.

## Stage 0: the shutter-timing correction table

`ssp-sso-daily` first runs, from ssp-tools' venv:

```bash
shutter-timing-table --out /sdf/data/rubin/user/mjuric/shutter-timing/corrections --refresh-recent 3 --workers 32
```

- It resumes: nights already written are skipped, and only new nights are built.
- `--refresh-recent 3` re-checks the last 3 nights written, and rebuilds (the whole night) any whose raw directory holds exposures missing from its exposure log: raws that arrived after it was built.
- It holds `CT/.lock` for the whole run.
- Its output goes to `DAY/stage0.log`; its duration and exit code to `daily.log`.
- On a failure, `daily.log` and stderr name the exit code, what it means, and the builder's last lines. The run stops before the extract.

| exit | meaning | what to do |
|---|---|---|
| 0 | the table is up to date | nothing |
| 2 | **refused**, before writing: a calibration mismatch, the lock held, an unknown table format, or an integrity failure. The builder's `ERROR` line names which. | see below |
| 3 | it finished but **left the table mixed** (nights on more than one calibration); the extract would refuse it | finish the recalibration (below) |
| other | the builder failed (e.g. a crash, a bad option) | read `stage0.log`; fix; rerun |

**A held lock** (`TableLockedError`): another build is writing the table. The message gives the host, pid and start time recorded in `CT/.lock`; these come from the last build that took the lock, so check that the process is still running (`ssh <host> ps -p <pid>`). If it is, wait for it and rerun the day (`--stamp` another name, or remove the day directory). If it isn't, the lock was released when it exited (POSIX `fcntl` locks die with their process), so a rerun goes through.

**A calibration change** (`CalibrationMismatchError`): the pinned `shutter-timing` computes a different `calibration_id` from the table's (a new package version, or a changed calibration constant), so it refuses to add new nights. Every night must be rebuilt on the new calibration, with:

```bash
.venv/bin/shutter-timing-table --out /sdf/data/rubin/user/mjuric/shutter-timing/corrections --rebuild-stale --workers 32
```

- `--rebuild-stale` rebuilds only the nights whose calibration isn't the current one (or that aren't done), so it is resumable: if it is interrupted, run it again.
- It rebuilds the whole history from the raws (the default `--start` is 20250401): hours. Plan it outside the daily window; the daily run fails until it finishes.
- Agree the change with the table's owner first: a changed calibration changes every corrected time.

**An interrupted recalibration** (exit 3, `MixedTableError`, or exit 2 on a mixed table): run the same `--rebuild-stale` command to finish it.

**An unknown format or an integrity failure** (`TableFormatError`, `TableIntegrityError`): the table is from another `shutter-timing` version, or a night's files disagree. Don't edit the files. Ask the shutter-timing owners; a night can be rebuilt with `--overwrite --days YYYYMMDD`.

**Visits not built.** A DiaSource whose visit the table doesn't cover yet (its night has no table, or the visit is missing from the night's exposure log, typically a late raw) is NOT_BUILT. The extract gives it the visit time with `midpointMjdTai_flag` = True, logs a warning naming the visits, and goes on; stage 0's `--refresh-recent` fixes it on a later day, once the raw has arrived. **More than 20** distinct NOT_BUILT visits (`MAX_NOT_BUILT_VISITS`, the extract's `--max-not-built-visits`) fail the extract, with a message pointing at stage 0: that many means stage 0 was skipped or is stale, not a few late raws. Run stage 0 (or check why it is behind) and rerun. The input manifest counts the omitted and the not-built visits, and lists the not-built ones.

## Running the stages separately

```bash
ssp-extract-sso-inputs INPUTS_DIR              # --force to replace, --only/--skip STEP (with --force), --reuse NAME=PATH
ssp-build-sso INPUTS_DIR RUN_DIR --workers 32  # --from STEP to resume
python -m ssp.delivery_check RUN_DIR/delivery  # the schema check alone
ssp-upload-sso dev RUN_DIR --dry-run           # prints the planned objects and the Pub/Sub message
ssp-build-ssobject RUN_DIR/delivery RUN_DIR/delivery/mpc_orbits.parquet -o ssobject.parquet  # SSObject alone
```

- **Stage 1 is atomic.**
  - Every step writes into `INPUTS_DIR/.partial`, and only a fully successful run moves the files into place and writes `manifest.json`, last. A failed or refused run leaves `INPUTS_DIR` untouched.
  - Resume with `--force --only STEP`. The previous manifest is kept as `.manifest.previous.json`.
  - Rerunning `mpc` also reruns `dia_sources`, since `dia_sources` is built from `obs_sbn`.
- **Stage 2 refuses inputs** whose manifest doesn't match the files (md5, rows, columns), where `dia_sources` wasn't built from this `obs_sbn`, or where the four MPC files aren't from one snapshot.
  - It re-hashes the inputs before checking.
  - `deliverable` is true only if every step and every expected check passed.
  - It refuses to rebuild a run that has been uploaded, unless `--force-rebuild`.
- **Stage 3 refuses** unless `report.json` is deliverable and every file matches its md5: for SSObservation, the manifest must have the md5 the report recorded (`manifest_md5`) and list the parts, rows and bytes the report recorded, and every part must have the manifest's size and md5.
  - It never overwrites an object, and removes its own uploads on any failure.
  - It refuses a second real upload of a run unless `--force`, and a partial table set unless `--allow-partial`.
  - Inputs from another source (not ClickHouse or the MPC replica) can replace stage 1, provided they honour `ssp/delivery_contract.py`'s `INPUT_FILES` and `MANIFEST_FIELDS`.

## Upload configs and the DM-55678 loader

- **Configs** are in `config/sso-upload/`; pass the name or the path.
  - **`dev`:** bucket `ppdb-dev-sso-ingest`, topic `load-sso-topic`, project `ppdb-dev-5c07`.
  - **`int`, `prod`:** commented templates from `lsst/idf_deploy`. The int project `ppdb-int-6c62` and all the prod names need confirming.
- **Layout:** `gs://<bucket>/<YYYYMMDDTHHMMSSmmm>/<Table>.parquet`; SSObservation as its parts, in order, then `SSObservation.manifest.json`, each under its own name (never the sidecar). Then a Pub/Sub message `{"bucket", "object_prefix", "uploaded_tables", "files"}`, `files` being `{Table: [object names relative to the prefix]}`. The rest is the contract of `lsst/dax_ppdb` `bigquery/sso_uploader.py`; the parts and `files` are new with the partitioned SSObservation and are with the PPDB loader owners for review.
- **NearbySSO is built and staged but not uploaded.** The loader (`lsst-dm/ppdb-cloud-functions` `load_sso`) fails the *whole* load when an unknown table is listed. NearbySSO joins the configs' `tables` once `dax_ppdb`'s `SSO_TABLES` and `load_sso` add it.
- **Upload at most once per load window.** `load_sso` starts Dataflow under a fixed job name, `load-sso`, so a message that arrives while a load is running is acknowledged and dropped. Check that the previous load has finished before `--force`.
- **The PPDB dataset must match the schema.** The loader writes into BigQuery tables built from the deployed `ppdb.yaml`, so SSObservation and the new columns load only once DM-55375 (lsst/sdm_schemas#549) is merged and the dataset rebuilt.

## Open items with the DM-55678 owners

1. The `sso-uploader` service account credentials for SSP.
2. NearbySSO in `dax_ppdb` `SSO_TABLES` and `load_sso`.
3. Rebuilding the PPDB dataset from DM-55375's `ppdb.yaml`.
4. Confirming the int project id (`ppdb-int-6c62`) and the prod names.
5. Whether naive (UTC wall-time) Parquet timestamps, which match the MPC's `timestamp without time zone`, load correctly into the PPDB's TIMESTAMP columns.
6. The partitioned SSObservation: the manifest's format and the message's `files` field (review).

## A reference run

See the end of this file, "Reference run".

## Reference run (2026-10-01)

`ssp-sso-daily /sdf/data/rubin/user/mjuric/sso-delivery/daily --upload dev --dry-run`: ssp-tools `sso-delivery` 394d8bd, sdfiana031, 32 workers. The output is in `/sdf/data/rubin/user/mjuric/sso-delivery/daily/2026-10-01/`.

| stage / step | wall | largest process |
|---|---|---|
| extract (MPC snapshot 2026-10-01T19:47:15Z) | 20:01 | — (the whole run's peak RSS was 25.9 GB) |
| build: mpc | 40 s | 6.9 GB |
| build: ssobservation | 3:03 | 13.7 GB |
| build: ssobject | 1:14 | 2.9 GB |
| build: nearbysso | 5:26 | 12.9 GB |
| build: check | 48 s | 5.8 GB |
| upload (dry run, dev) | 9 s | |
| **total** | **31:46** | |

| table | rows |
|---|---|
| SSObservation | 8,070,610 |
| SSObject | 297,762 |
| NearbySSO | 1,290,832 |
| mpc_orbits | 1,575,025 |
| current_identifications | 2,099,684 |
| numbered_identifications | 896,611 |

- All 8 checks PASS: the six `delivery:<Table>` checks plus `ssobservation:conformance` and `ssobservation:offsets`. `deliverable` is true.
- The dry-run upload planned `gs://ppdb-dev-sso-ingest/20261001T201900014/<Table>.parquet` for the five accepted tables, with NearbySSO held back.

## Rerun with non-gravitational forces (2026-10-01)

Since `docs/design/nongrav.md`, the ephemerides apply the MPC's non-gravitational fits: 184 comets and 454 Yarkovsky asteroids. NearbySSO also includes comets.
- The run is the same: no new inputs, options or steps.
- Expect about 3 GB more peak memory in the nearbysso step (16.0 GB).
- Warnings in the ssobservation and nearbysso logs name any orbit whose non-grav fit can't be parsed (it is integrated gravity-only), or whose `non_gravs` flag and CAR coefficients disagree. There were none on 2026-10-01.

`ssp-build-sso` on the 2026-10-01 inputs above, `nongrav` 6bdfe06, 32 workers: deliverable, all 8 checks PASS, in `/sdf/data/rubin/user/mjuric/nongrav/rerun/2026-10-01/run/`. SSObservation 8,070,610 rows (only the 495 rows of non-grav objects changed); NearbySSO 1,290,839 rows (7 comet rows added). Timings and the comparison are in the design doc, "The full daily rerun".

## Tail position angles (2026-10-03)

SSObservation and NearbySSO have two new columns, `ephAntiSunPA` and `ephAntiMotionPA` (`docs/design/tail-angles.md`), from sdm_schemas `tickets/DM-55375` f2541a5. Nothing changes in the run itself. To check a delivery's angles:

```bash
python -m bench.tail_angles_validate consistency RUN_DIR/delivery RUN_DIR/delivery/NearbySSO.parquet \
    --dia-sources INPUTS_DIR/ppdb_dia_sources.parquet
```

`--dia-sources` is needed since the shutter-timing correction (below); without it every pair is held to the strict rule.

**2026-10-01 rerun:** deliverable, all checks PASS. The output is in `/sdf/data/rubin/user/mjuric/tail-angles/rerun/2026-10-01/run/`.

## Comet match radius (2026-10-03)

NearbySSO matches comets and ISOs (designations C/, P/, D/, I/) within 15″, and every other object within 5″ (`docs/design/comet-radius.md`). Nothing changes in the run itself. The nearbysso log's "comets and ISOs" line counts the rows the larger radius adds.

**2026-10-01 rerun:** deliverable, all checks PASS; 104 comet rows added, 99 of them P/2002 T6. The output is in `/sdf/data/rubin/user/mjuric/comet-radius/rerun/2026-10-01/run/`.

## Shutter-motion-corrected times (2026-10)

SSObservation's `midpointMjdTai` is the exposure midpoint corrected for the shutter's motion, with two flags, `midpointMjdTai_flag` and `midpointMjdTai_flag_degraded` (`docs/design/shutter-timing.md`). The run gains stage 0 (above); the extract reads the correction table.

**SSObservation vs. NearbySSO.** NearbySSO predicts at each DiaSource's own `midpointMjdTai`, still the visit time until AP corrects DiaSource. So at the same DiaSource the two tables differ by about the rate × Δt, Δt = SSObservation's time − the DiaSource's (≤ 0.24 s for most visits, up to ~2 s for header-timed, degraded ones): up to a few tens of mas for fast objects, more on the degraded visits. The checks that compare the two (`bench/tail_angles_validate.py consistency`, `bench/nearbysso_validate.py same-orbits`, `bench/nongrav_validate.py nearbysso`) take Δt from SSObservation and the DiaSource input, and allow for it (`bench/time_shift.py`):

- where Δt = 0, the old tolerances, unchanged;
- positions: the motion over Δt is taken out first, and what's left must be within the old tolerance plus 0.2% of |rate| |Δt|, a bound on the track's curvature over Δt, and 0.05 mas (2% of |rate| |Δt| where SSObservation has no ranges);
- `ephOffset`, and "beyond the match radius" or "a nearer object": |rate| |Δt| × 1.02 + 0.05 mas on top;
- rates, V and the tail angles: twice a geometric bound on their change over Δt;
- each row's own Δt is used; only |Δt| > 10 s (not the same exposure) is a failure.
