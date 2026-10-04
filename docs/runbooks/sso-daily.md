# Runbook: the daily PPDB Solar System tables

This builds and delivers the six PPDB Solar System tables (RFC-1188), in three stages that talk only through files:

| stage | command | touches |
|---|---|---|
| 1. extract | `ssp-extract-sso-inputs INPUTS_DIR` | the MPC replica at USDF, ClickHouse |
| 2. build | `ssp-build-sso INPUTS_DIR RUN_DIR` | files only |
| 3. deliver | `ssp-upload-sso CONFIG RUN_DIR [--dry-run]` | GCS, Pub/Sub |
| all three | `ssp-sso-daily WORK_DIR [--upload CONFIG] [--dry-run]` | runs 1–3 in `WORK_DIR/<UTC date>/` |

The tables: SSSource, SSObject, NearbySSO, `mpc_orbits`, `current_identifications` and `numbered_identifications`, per `sdm_schemas` `ppdb.yaml`/`sso_base.yaml` (tickets/DM-55375, lsst/sdm_schemas#549; copies in `tests/data/sdm_schemas/`).

Design: `docs/design/sso-delivery.md` (the stages), `docs/design/sssource-widened.md` (SSSource, SSObject), `docs/design/nearbysso.md` (NearbySSO) and `docs/design/nongrav.md` (non-gravitational forces, and comets in NearbySSO). The contract between the stages is `ssp/delivery_contract.py`.

## Setup (once)

- **Code:** an ssp-tools **source checkout** with its venv.
  - `uv sync --extra all`; building ASSIST needs a C compiler.
  - The build stage's SSSource checks need `bench/` from the checkout, and the upload configs live in `config/sso-upload/`. So run from the checkout (an editable install).
  - If a console script is missing after a pull, refresh the entry points: `VIRTUAL_ENV=$PWD/.venv uv pip install --no-deps -e .`
- **ASSIST data:** `data/assist/linux_p1550p2650.440` and `data/assist/sb441-n16.bsp`. Export `SSP_ASSIST_PLANETS` and `SSP_ASSIST_ASTEROIDS`, and set `OMP_NUM_THREADS=1`.
- **The MPC replica:** `mpcorb-db.slac.stanford.edu:5432`, database `mpc_sbn`, user `rubin`, password in `~/.pgpass`. It is exported read-only, in one REPEATABLE READ transaction.
- **ClickHouse:** `172.24.10.116:8123` (HTTP only; on Kubernetes since 2026-10-04, replacing `sdfiana035.sdf.slac.stanford.edu`), user `ssp_xmatch`, read-only. The tools keep it out of SDF's HTTP proxy, which refuses it. It needs `SELECT` on `ssp.*` and `ppdb.DiaSource`. Credentials go in `~/.chpass` (mode 600, `host:port:database:user:password`). The server is shared, so use **at most 8 concurrent queries**; that is the default.
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
| `inputs/` | stage 1: `obs_sbn`, `mpc_orbits`, `current_identifications`, `numbered_identifications` (raw MPC, one snapshot), `dia_sources`, `ppdb_dia_sources` and `manifest.json` |
| `run/delivery/` | stage 2: `<Table>.parquet` for the six tables |
| `run/report.json` | the steps (status, time, peak memory of the largest process, commit), the tables (rows, md5), the checks, `deliverable`, and the uploads |
| `run/checks/`, `run/logs/` | the check reports and each step's log |
| `daily.log` | the commands run and their exit codes |

`ssp-sso-daily` stops at the first failing stage and exits with its status.

## Running the stages separately

```bash
ssp-extract-sso-inputs INPUTS_DIR              # --force to replace, --only/--skip STEP (with --force), --reuse NAME=PATH
ssp-build-sso INPUTS_DIR RUN_DIR --workers 32  # --from STEP to resume
python -m ssp.delivery_check RUN_DIR/delivery  # the schema check alone
ssp-upload-sso dev RUN_DIR --dry-run           # prints the planned objects and the Pub/Sub message
```

- **Stage 1 is atomic.**
  - Every step writes into `INPUTS_DIR/.partial`, and only a fully successful run moves the files into place and writes `manifest.json`, last. A failed or refused run leaves `INPUTS_DIR` untouched.
  - Resume with `--force --only STEP`. The previous manifest is kept as `.manifest.previous.json`.
  - Rerunning `mpc` also reruns `dia_sources`, since `dia_sources` is built from `obs_sbn`.
- **Stage 2 refuses inputs** whose manifest doesn't match the files (md5, rows, columns), where `dia_sources` wasn't built from this `obs_sbn`, or where the four MPC files aren't from one snapshot.
  - It re-hashes the inputs before checking.
  - `deliverable` is true only if every step and every expected check passed.
  - It refuses to rebuild a run that has been uploaded, unless `--force-rebuild`.
- **Stage 3 refuses** unless `report.json` is deliverable and every file matches its md5.
  - It never overwrites an object, and removes its own uploads on any failure.
  - It refuses a second real upload of a run unless `--force`, and a partial table set unless `--allow-partial`.
  - Inputs from another source (not ClickHouse or the MPC replica) can replace stage 1, provided they honour `ssp/delivery_contract.py`'s `INPUT_FILES` and `MANIFEST_FIELDS`.

## Upload configs and the DM-55678 loader

- **Configs** are in `config/sso-upload/`; pass the name or the path.
  - **`dev`:** bucket `ppdb-dev-sso-ingest`, topic `load-sso-topic`, project `ppdb-dev-5c07`.
  - **`int`, `prod`:** commented templates from `lsst/idf_deploy`. The int project `ppdb-int-6c62` and all the prod names need confirming.
- **Layout:** `gs://<bucket>/<YYYYMMDDTHHMMSSmmm>/<Table>.parquet`, then a Pub/Sub message `{"bucket", "object_prefix", "uploaded_tables"}`. This is the contract of `lsst/dax_ppdb` `bigquery/sso_uploader.py`.
- **NearbySSO is built and staged but not uploaded.** The loader (`lsst-dm/ppdb-cloud-functions` `load_sso`) fails the *whole* load when an unknown table is listed. NearbySSO joins the configs' `tables` once `dax_ppdb`'s `SSO_TABLES` and `load_sso` add it.
- **Upload at most once per load window.** `load_sso` starts Dataflow under a fixed job name, `load-sso`, so a message that arrives while a load is running is acknowledged and dropped. Check that the previous load has finished before `--force`.
- **The PPDB dataset must match the schema.** The loader writes into BigQuery tables built from the deployed `ppdb.yaml`, so the widened SSSource and the new columns load only once DM-55375 (lsst/sdm_schemas#549) is merged and the dataset rebuilt.

## Open items with the DM-55678 owners

1. The `sso-uploader` service account credentials for SSP.
2. NearbySSO in `dax_ppdb` `SSO_TABLES` and `load_sso`.
3. Rebuilding the PPDB dataset from DM-55375's `ppdb.yaml`.
4. Confirming the int project id (`ppdb-int-6c62`) and the prod names.
5. Whether naive (UTC wall-time) Parquet timestamps, which match the MPC's `timestamp without time zone`, load correctly into the PPDB's TIMESTAMP columns.

## A reference run

See the end of this file, "Reference run".

## Reference run (2026-10-01)

`ssp-sso-daily /sdf/data/rubin/user/mjuric/sso-delivery/daily --upload dev --dry-run`: ssp-tools `sso-delivery` 394d8bd, sdfiana031, 32 workers. The output is in `/sdf/data/rubin/user/mjuric/sso-delivery/daily/2026-10-01/`.

| stage / step | wall | largest process |
|---|---|---|
| extract (MPC snapshot 2026-10-01T19:47:15Z) | 20:01 | — (the whole run's peak RSS was 25.9 GB) |
| build: mpc | 40 s | 6.9 GB |
| build: sssource | 3:03 | 13.7 GB |
| build: ssobject | 1:14 | 2.9 GB |
| build: nearbysso | 5:26 | 12.9 GB |
| build: check | 48 s | 5.8 GB |
| upload (dry run, dev) | 9 s | |
| **total** | **31:46** | |

| table | rows |
|---|---|
| SSSource | 8,070,610 |
| SSObject | 297,762 |
| NearbySSO | 1,290,832 |
| mpc_orbits | 1,575,025 |
| current_identifications | 2,099,684 |
| numbered_identifications | 896,611 |

- All 8 checks PASS: the six `delivery:<Table>` checks plus `sssource:conformance` and `sssource:offsets`. `deliverable` is true.
- The dry-run upload planned `gs://ppdb-dev-sso-ingest/20261001T201900014/<Table>.parquet` for the five accepted tables, with NearbySSO held back.

## Rerun with non-gravitational forces (2026-10-01)

Since `docs/design/nongrav.md`, the ephemerides apply the MPC's non-gravitational fits: 184 comets and 454 Yarkovsky asteroids. NearbySSO also includes comets.
- The run is the same: no new inputs, options or steps.
- Expect about 3 GB more peak memory in the nearbysso step (16.0 GB).
- Warnings in the sssource and nearbysso logs name any orbit whose non-grav fit can't be parsed (it is integrated gravity-only), or whose `non_gravs` flag and CAR coefficients disagree. There were none on 2026-10-01.

`ssp-build-sso` on the 2026-10-01 inputs above, `nongrav` 6bdfe06, 32 workers: deliverable, all 8 checks PASS, in `/sdf/data/rubin/user/mjuric/nongrav/rerun/2026-10-01/run/`. SSSource 8,070,610 rows (only the 495 rows of non-grav objects changed); NearbySSO 1,290,839 rows (7 comet rows added). Timings and the comparison are in the design doc, "The full daily rerun".

## Tail position angles (2026-10-03)

SSSource and NearbySSO have two new columns, `ephAntiSunPA` and `ephAntiMotionPA` (`docs/design/tail-angles.md`), from sdm_schemas `tickets/DM-55375` f2541a5. Nothing changes in the run itself. To check a delivery's angles:

```bash
python -m bench.tail_angles_validate consistency RUN_DIR/delivery/SSSource.parquet RUN_DIR/delivery/NearbySSO.parquet
```

**2026-10-01 rerun:** deliverable, all checks PASS. The output is in `/sdf/data/rubin/user/mjuric/tail-angles/rerun/2026-10-01/run/`.

## Comet match radius (2026-10-03)

NearbySSO matches comets and ISOs (designations C/, P/, D/, I/) within 15″, and every other object within 5″ (`docs/design/comet-radius.md`). Nothing changes in the run itself. The nearbysso log's "comets and ISOs" line counts the rows the larger radius adds.

**2026-10-01 rerun:** deliverable, all checks PASS; 104 comet rows added, 99 of them P/2002 T6. The output is in `/sdf/data/rubin/user/mjuric/comet-radius/rerun/2026-10-01/run/`.
