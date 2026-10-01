# Design: building and delivering the PPDB Solar System tables daily

Status: **proposed**, for owner approval (2026-10-01).

## Context

The RFC-1188 Solar System tables are already built by separate tools:
- SSSource (`ssp-build-sssource`);
- SSObject (`ssp-build-ssobject`);
- NearbySSO (`ssp-build-nearbysso`);
- the three MPC tables (`fast-export`).

What's missing, and what this design adds:
1. MPC exports that match the schema;
2. one command for the daily build;
3. delivery to the PPDB's SSO ingestion (DM-55678).

The schema is `sdm_schemas` `tickets/DM-55375`, a copy of which is in `tests/data/sdm_schemas/`.

## The DM-55678 delivery contract (dev)

From DM-55678 and `lsst/dax_ppdb` `bigquery/sso_uploader.py`:
- **Upload** each table as `gs://<bucket>/<prefix>/<Table>.parquet`.
  - The prefix is the upload time in UTC, `%Y%m%dT%H%M%S` plus milliseconds (e.g. `20260914T200401001`). It's fresh per upload, so earlier uploads are never overwritten.
  - Uploads use `if_generation_match=0`, so they never overwrite.
  - On any failure, the objects already uploaded are deleted.
- **Then publish** a Pub/Sub message to `load-sso-topic`: `{"bucket": ..., "object_prefix": ..., "uploaded_tables": [...]}`.
- **Dev:** bucket `ppdb-dev-sso-ingest`, topic `load-sso-topic`, project `ppdb-dev-5c07`. Int and prod names are in `lsst/idf_deploy`.
- **Loading:** a Cloud Run service (`lsst-dm/ppdb-cloud-functions`, `load_sso/`) starts a Dataflow job that loads each Parquet into BigQuery staging tables and then the internal ones. Both are built from the deployed PPDB schema.

**Known gaps on the DM-55678 side (not ours to change):**
- `dax_ppdb`'s `SSO_TABLES`, and so its uploader and the loader, list SSSource, SSObject, `numbered_identifications`, `current_identifications` and `mpc_orbits`, **but not NearbySSO**.
- The loader's BigQuery tables follow the deployed `ppdb.yaml` (main's 39-column SSSource), so the widened tables load only once DM-55375 is merged and the PPDB dataset is rebuilt from it.
- **No service account has been issued to SSP yet.** The intended one has upload-only access to the bucket and publish access to the topic.

## Owner decisions (2026-10-01)

| item | decision |
|---|---|
| `current_identifications.identifier_ids` | Dropped from the export, to match the schema. |
| `current_identifications.published` | The **schema** changes to `int`, matching the MPC. |
| `numbered_identifications`: `numbered_publication_references`, `named_publication_references` | Dropped from the export, to match the schema. |
| `mpc_orbits.designation` | Added by the export: a Rubin-provided copy of `unpacked_primary_provisional_designation`, per the schema. |
| AP-side DiaSource columns (`ssObjectId`, `ssObjectReassocTimeMjdTai`) | Not touched now. |
| Pushing `tickets/DM-55375` | Once all schema changes are in, i.e. after Phase 0 below. |
| NearbySSO | **Built and staged, but not uploaded** (revised 2026-10-01). The loader (`load_sso`) fails the *entire* load if NearbySSO is in `uploaded_tables`: no BigQuery table exists for it, and promotion waits on every table. It is uploaded once `dax_ppdb`'s `SSO_TABLES` and `load_sso` add it. The upload configs list the five accepted tables. |
| Uploader | Our own, in ssp-tools, following the `dax_ppdb` contract exactly, without the LSST-stack dependency. |
| Environments | Config-driven. Ship the dev config, with int and prod as commented templates from `idf_deploy`. Nothing uploads until the service account exists. |
| Driver | One run builds every table from one MPC snapshot. **Extraction is a separate stage** (`ssp-extract-sso-inputs`), decoupled from the build by a file contract, because the upstream data may later come from somewhere other than ClickHouse. |
| Runbook | In the repo, `docs/runbooks/sso-daily.md`; each run's results stay in its run directory. |

## Three stages, decoupled by files

The upstream data may not always come from ClickHouse and the MPC replica. So **extraction is its own stage**: it writes a set of input files with a fixed contract, and the build stage reads only those files, never a database. Another source can supply the same files instead.

### Stage 1, extract: `ssp-extract-sso-inputs INPUTS_DIR`

The only stage with network or database access. Every output is a raw dump of its source, not yet shaped to the schema.

| input file (`INPUTS_DIR/`) | from (today) | contract |
|---|---|---|
| `obs_sbn.parquet` | MPC replica: `SELECT * FROM obs_sbn WHERE stn='X05'` | the `obs_sbn` columns `extract-submitted-sources` and SSSource read |
| `mpc_orbits.parquet`, `current_identifications.parquet`, `numbered_identifications.parquet` | MPC replica: `SELECT *` of each, **in the same transaction as `obs_sbn`** | the MPC tables as the MPC defines them; extra columns allowed, since the build stage selects |
| `dia_sources.parquet` | `extract-submitted-sources` against ClickHouse `ssp.SubmittableSources` (≤ 8 queries) | the measurement record for each submitted `obs_sbn` row (today's `dia_sources.parquet`) |
| `ppdb_dia_sources.parquet` | ClickHouse `ppdb.DiaSource`, the five columns NearbySSO reads | `diaSourceId`, `visit`, `midpointMjdTai`, `ra`, `dec` |
| `manifest.json` | written last | each file's source, snapshot time, row count and md5, and the ssp-tools commit; the build stage requires it |

The `obs_sbn` → `dia_sources` step is an extraction, because it reads ClickHouse. A different source would supply a `dia_sources.parquet` honouring the same contract.

### Stage 2, build: `ssp-build-sso INPUTS_DIR RUN_DIR [--from STEP]`

Files in, files out; no database access. It refuses inputs without a valid manifest.

| step | does | output (`RUN_DIR/delivery/`) |
|---|---|---|
| 1. `mpc` | shape the three MPC tables to `sso_base.yaml`: schema columns and order only; add `mpc_orbits.designation`; drop `identifier_ids` and the two publication-reference columns; check types | `mpc_orbits.parquet`, `current_identifications.parquet`, `numbered_identifications.parquet` |
| 2. `sssource` | `ssp-build-sssource` | `SSSource.parquet` |
| 3. `ssobject` | `ssp-build-ssobject` | `SSObject.parquet` |
| 4. `nearbysso` | `ssp-build-nearbysso` on `ppdb_dia_sources.parquet` | `NearbySSO.parquet` |
| 5. `check` | schema conformance of all six delivered tables (from the YAML), plus SSSource `conformance` and `offsets`; all must pass | `RUN_DIR/checks/` |

It writes `RUN_DIR/report.json` with per-step timings, row counts, md5s, the commit, the check results, and the input manifest copied in.

### Stage 3, deliver: `ssp-upload-sso CONFIG RUN_DIR/delivery [--dry-run]`

It uploads only a delivery whose `report.json` shows all checks passed, then publishes the Pub/Sub message, per the contract above.

A convenience wrapper, `ssp-sso-daily`, runs the three stages in sequence. Each stage also runs on its own.

## Plan

**Phase 0 (integrator):**
- `tickets/DM-55375`: `current_identifications.published` becomes `int`. Then `felis validate`, the repo's tests, a news fragment, and **push the branch and open the `sdm_schemas` PR** (the version question goes in its description).
- Re-vendor `sso_base.yaml`.
- `ssp/delivery_contract.py`: the input-file contract and manifest, the delivered tables' names and files, the report fields, the uploader's interface.

**Phase 1 (parallel):**

| WP | builds | independent review |
|---|---|---|
| **E extract stage** | `ssp-extract-sso-inputs`: the MPC dumps in one transaction (through the existing `fast-export`), `extract-submitted-sources`, the `ppdb.DiaSource` columns, and the manifest. Tests offline with stubbed sources; one real run into scratch. | light |
| **F build stage** | `ssp-build-sso`: the MPC shaping (column lists generated from `sso_base.yaml`), the builders, the checks, `report.json`, `--from`; manifest validation. Tests: an offline end-to-end run on the subset fixture, including a set of inputs from a non-ClickHouse source to exercise the contract. | **yes** |
| **G deliver stage** | `ssp-upload-sso`, as above: refuses unchecked deliveries; configs for dev (int and prod as commented templates); tests with fakes for the prefix, no-overwrite, cleanup, message and dry run. Plus the `ssp-sso-daily` wrapper. | light |
| **H validation** | `bench/delivery_validate.py`: every delivered table against `sso_base.yaml` (names, order, types, nullability), plus row-count and key sanity, and a check of the input manifest; used by step 5. Tests with failing cases. | no (it is the check) |

`ssp/delivery_contract.py` (Phase 0) fixes the input file names and required columns, the manifest fields, the delivery file names, the report fields and the uploader interface, so that E, F and G can be built in parallel.

**Phase 2 (integrator):**
- Run the three stages end to end on fresh inputs, with the upload in dry-run mode.
- Write `docs/runbooks/sso-daily.md` and record the results here.
- A real upload to dev comes once the service account is issued. Ask the DM-55678 owners for it, and for NearbySSO support in `dax_ppdb` and `load_sso`.
