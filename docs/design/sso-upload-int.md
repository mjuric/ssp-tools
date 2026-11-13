# ssp-upload-sso: the PPDB loader's layout, all six tables, and int

## Context

`ssp-upload-sso` (`ssp/sso_upload.py`, `docs/design/sso-delivery.md`) uploads a checked delivery to the PPDB's SSO ingestion (DM-55678). It was written before the PPDB side accepted NearbySSO and before anything was loaded in int. The owner is about to test a real upload to int, so it must match what the PPDB side does today.

**The PPDB side** (read 2026-11-12):
- **dax_ppdb `SSOUploader`** (lsst/dax_ppdb PR #63, branch `tickets/DM-55375`; `sso_uploader.py` is unchanged from main):
  - one object per table, `gs://<bucket>/<prefix>/<Table>.parquet`;
  - then one message, `{"bucket", "object_prefix", "uploaded_tables"}`, to `load-sso-topic`;
  - `SSO_TABLES` = SSObservation, NearbySSO, SSObject, mpc_orbits, current_identifications, numbered_identifications. All six are required unless `allow_partial_upload` is set.
- **load_sso** (lsst-dm/ppdb-cloud-functions `load_sso`):
  - reads exactly `gs://<bucket>/<prefix>/<table>.parquet`, one file per table, with no parts, globs or manifest;
  - reads only `bucket`, `object_prefix` and `uploaded_tables` from the message.
- **int:** the PPDB loader developer reported the new tables live in data-int on 2026-10-13. They loaded all six with `ppdb-cli upload-sso-data` from the PR #63 branch (`bucket_name: ppdb-int-sso-ingest`, `allow_partial_upload: false`), after concatenating our SSObservation parts by hand.
- **Open:** load_sso main pins dax_ppdb `cloudrun-v3.0.0`, whose `SSO_TABLES` still names SSSource. What the int (and dev) deployment runs is to be confirmed with the PPDB team.

**What we do today, and why it fails:**
- SSObservation is uploaded as its parts plus a manifest, so load_sso finds no `SSObservation.parquet`.
- NearbySSO is held back as unsupported.
- The message carries an extra `files` field.
- int's config is a commented-out template.

## Owner decisions (2026-11-12)

| Decision | Choice |
|---|---|
| Where the single SSObservation file is made | At upload. `ssp-upload-sso` combines the manifest's parts after their md5 checks. The delivery, its checks and `ssp-build-sso` are unchanged. |
| The combined file | Kept, as `RUN_DIR/upload/SSObservation.parquet`. |
| The message's `files` field | Dropped. The message is exactly dax_ppdb's: `{"bucket", "object_prefix", "uploaded_tables"}`. |
| Validation against the stack | A full upload to the int bucket without the Pub/Sub message is allowed. It must be shown identical to an upload by dax_ppdb's `SSOUploader` (the latest daily stack, with dax_ppdb from `tickets/DM-55375`). |

## Outputs

- **Objects:** `gs://<bucket>/<prefix>/<Table>.parquet` for each of the six tables, one object each. No manifest, parts or sidecars.
- **The message:** `{"bucket", "object_prefix", "uploaded_tables"}`.
- **The upload record** in `report.json` gains the combined file's path, rows, bytes and md5.
- **`config/sso-upload/int.yaml`** is active: `ppdb-int-sso-ingest`, `load-sso-topic`, `ppdb-int-6c62`, all six tables.

## Approach

1. **The combined SSObservation** (`ssp/sso_upload.py`).
   - After `verify_delivery` has checked the manifest and every part's md5, stream the parts, in manifest order, one row group at a time, into `RUN_DIR/upload/SSObservation.parquet.tmp`.
     - It has the parts' schema; the parts must all have the same schema, or the upload fails.
     - It is zstd, like the parts. Each part's row group becomes one row group (about 1 M rows today), so memory stays at one row group.
   - Check that the file has the manifest's total rows and the parts' schema. Then rename it into place and write `RUN_DIR/upload/SSObservation.json`: rows, bytes, md5, and the `manifest_md5` it was built from.
   - **Reuse:** if `SSObservation.json` names the current `manifest_md5`, and the file's size and md5 match, reuse the file. Otherwise rebuild it.
   - The sidecars are never read or uploaded.
   - A dry run builds (or reuses) the file too, so it verifies the real thing. Nothing remote is touched.
2. **The upload.** Each table is uploaded as one object under `<Table>.parquet`: SSObservation from the combined file, the others from `delivery/`. Everything else is unchanged:
   - `if_generation_match=0`;
   - cleanup on any failure;
   - the publish timeout keeps the objects;
   - one upload per load window.
   - **Table order:** tables are uploaded, and listed in `uploaded_tables`, in dax_ppdb's `SSO_TABLES` order (SSObservation, NearbySSO, SSObject, mpc_orbits, current_identifications, numbered_identifications), as `SSOUploader.from_directory` does. load_sso doesn't depend on the order, but this makes the message byte-identical to the stack's.
   - **`--no-publish`** (new): upload the objects and stop. Nothing is published, so no load starts. The objects are kept and the run prints their prefix. The record gets `"published": false`, and like a dry run it doesn't count as an earlier upload for `--force`. For tests such as the validation below; the objects are deleted by hand afterwards.
3. **NearbySSO is accepted.** `UNSUPPORTED_TABLES` becomes empty, so the default complete set is all six `DELIVERY_TABLES`. `--include-unsupported` stays as a no-op hook, ready for the next table the loader lags on.
4. **The contract** (`ssp/delivery_contract.py`):
   - `UPLOAD_MESSAGE_FIELDS` = `("bucket", "object_prefix", "uploaded_tables")`;
   - the stage-3 comment block describes the one-file layout and the combined SSObservation;
   - `REPORT_FIELDS["upload"]` gains the combined file's fields.
5. **Configs.**
   - `int.yaml` is activated, with all six tables.
   - `dev.yaml` and the `prod.yaml` template list all six, with the NearbySSO notes removed.
   - Credentials stay application-default. For int, that is the `sso-uploader@ppdb-int-6c62` service-account key, given through `GOOGLE_APPLICATION_CREDENTIALS` (`~/.ppdb.google.token`). It is never printed or copied into the repository.
6. **Docs.** The module docstring, the CLI help and `docs/runbooks/sso-daily.md`.

## Validation

- **Tests** (network-free; GCS and Pub/Sub faked as today):
  - the combined file has the parts' rows in order, their schema, no sidecar columns, and row groups no bigger than the parts';
  - reuse and rebuild: the manifest changes, or the file is damaged;
  - a schema mismatch between parts is refused;
  - one object per table, in `SSO_TABLES` order, and the exact message;
  - `--no-publish` uploads, publishes nothing, keeps the objects, and doesn't block a later real upload;
  - NearbySSO is in the default set;
  - the int config loads.
- **The `upload` extra:** in a fresh venv without the stack, `pip install '.[upload]'` brings `google-cloud-storage` and `google-cloud-pubsub`, and both import.
- **A real-data dry run:** `ssp-upload-sso int /sdf/home/m/mjuric/ssp_daily_data/ssp-tools/2026-10-12/run --dry-run`.
  - The planned objects and the message.
  - The combined file's rows (8,078,546), bytes, md5, row groups and build time.
  - A full read-back against the parts: every row equal, in order.
- **A full upload without the message, compared with the stack's** (owner, 2026-11-12), on a hard-linked copy of the run at `/sdf/data/rubin/user/mjuric/sso-upload-int/run`, so ssp-daily's run directory is untouched:
  1. **Ours:** `ssp-upload-sso int RUN --no-publish` uploads to the int bucket under prefix A, with the int service-account key.
  2. **The stack's:** on the latest daily stack (`d_2026_11_11` today), with dax_ppdb's `tickets/DM-55375` set up over it:
     - `SSOUploader.from_directory` runs on a directory holding the same six files (hard links to the delivery and to our combined SSObservation), with `allow_partial_upload: false`;
     - its `_publish` is replaced by a stub that records the message instead of sending it;
     - it uploads under prefix B, with the same key.
  3. **Compare** A with B:
     - the object names relative to the prefix;
     - size, md5, crc32c, content type and encoding, custom metadata and storage class;
     - the message ours would publish and the one the stub recorded, byte for byte apart from the prefix.
  4. **Delete** both prefixes and check that they're empty.
     - The bucket's versioning is a Terraform variable. If it is on, the deleted objects stay as noncurrent versions, which the uploader account can't list or purge.
     - That's about 12 GB for both uploads. It is noted for the PPDB team.
  - Checked beforehand: `load_sso` runs only from a push subscription on `load-sso-topic`, and the PPDB Terraform (lsst/idf_deploy `environment/deployments/ppdb/services/`) has no bucket notification. So objects without a message start no load.
- **No real upload** (with the message). The owner approves that separately.

## Implementation plan

- **One work package (WP-U), in a worktree on `sso-upload-int`:** steps 1–6 and their tests.
- **Then an independent review:** read the combined file back against the parts on the real delivery, and probe reuse, rebuild and cleanup.
- **Then the integrator's work:**
  - the dry run on the 2026-10-12 delivery;
  - the full upload without the message and its comparison with the stack's, then deleting both;
  - the `upload` extra check;
  - the results here;
  - the draft question to the PPDB team about the deployed loader's table set, for the owner to send.
- **Merge to master** with the owner's approval. Then tell ssp-daily.
