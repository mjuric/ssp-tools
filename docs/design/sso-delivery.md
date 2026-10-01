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
| NearbySSO | **Built, staged and uploaded** with the others, though the DM-55678 side doesn't accept it yet. The run report and the runbook note this. |
| Uploader | Our own, in ssp-tools, following the `dax_ppdb` contract exactly, without the LSST-stack dependency. |
| Environments | Config-driven. Ship the dev config, with int and prod as commented templates from `idf_deploy`. Nothing uploads until the service account exists. |
| Driver | One run builds every table from one MPC snapshot. |
| Runbook | In the repo, `docs/runbooks/sso-daily.md`; each run's results stay in its run directory. |

## The daily run: `ssp-build-sso RUN_DIR [--upload CONFIG] [--from STEP] [--reuse ...]`

| step | does | output (`RUN_DIR/`) |
|---|---|---|
| 1. `mpc` | `ssp-export-mpc`: one transaction from the USDF replica: `obs_sbn` (X05), and the three MPC tables with explicit, schema-ordered column lists | `inputs/obs_sbn.parquet`, `delivery/{mpc_orbits,current_identifications,numbered_identifications}.parquet` |
| 2. `extract` | `extract-submitted-sources` (ClickHouse, ≤ 8 queries) | `inputs/dia_sources.parquet` |
| 3. `ppdb` | the five `ppdb.DiaSource` columns NearbySSO reads (ClickHouse, `ssp_xmatch`) | `inputs/ppdb_dia_sources.parquet` |
| 4. `sssource` | `ssp-build-sssource` | `delivery/SSSource.parquet` |
| 5. `ssobject` | `ssp-build-ssobject` | `delivery/SSObject.parquet` |
| 6. `nearbysso` | `ssp-build-nearbysso` | `delivery/NearbySSO.parquet` |
| 7. `check` | fast schema conformance of all six delivered tables, read from the YAML, plus SSSource `conformance` and `offsets`. Every check must pass before the upload | `checks/` |
| 8. `upload` | optional: `ssp-upload-sso` with the config | the upload prefix, in the report |

The run writes `report.json`: per-step timings, row counts, inputs' snapshot times, md5s, the ssp-tools commit, check results and the upload prefix. Each step can be rerun from the start (`--from`), or an input reused (`--reuse dia_sources=...`).

## Plan

**Phase 0 (integrator):**
- `tickets/DM-55375`: `current_identifications.published` becomes `int`. Then `felis validate`, the repo's tests, a news fragment, and **push the branch and open the `sdm_schemas` PR** (the version question goes in its description).
- Re-vendor `sso_base.yaml`.
- `ssp/delivery_contract.py`: the delivered tables' names, files and steps; the uploader's interface; the report fields.

**Phase 1 (parallel):**

| WP | builds | independent review |
|---|---|---|
| **E MPC export** | `ssp-export-mpc`: column lists generated from `sso_base.yaml`, one transaction through the existing `fast-export` machinery, and a type check against the schema. Tests offline (SQL generation, type mapping); a real export into scratch. | light |
| **F daily driver** | `ssp-build-sso` per the table above: `report.json`, `--from` and `--reuse`, failing loudly. Tests: an offline end-to-end run on small synthetic inputs, with the network steps stubbed. | **yes** |
| **G uploader** | `ssp-upload-sso CONFIG DELIVERY_DIR [--dry-run] [--tables ...]`, using google-cloud-storage and google-cloud-pubsub, following the contract above. Configs in `config/sso-upload/{dev,int,prod}.yaml` (int and prod commented). Tests with fakes: the prefix format, no-overwrite, cleanup on failure, the message body, and the dry run. | light |
| **H validation** | `bench/delivery_validate.py`: every delivered table against `sso_base.yaml` (names, order, types, nullability), plus row-count and key sanity, used by step 7. Tests with failing cases. | no (it is the check) |

**Phase 2 (integrator):**
- Run `ssp-build-sso` end to end on fresh inputs, with `--upload` in dry-run mode.
- Write `docs/runbooks/sso-daily.md` and record the results here.
- A real upload to dev comes once the service account is issued. Ask the DM-55678 owners for it, and for NearbySSO support in `dax_ppdb` and `load_sso`.
