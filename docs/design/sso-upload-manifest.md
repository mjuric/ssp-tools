# ssp-upload-sso: the PPDB's manifest upload contract (DM-56960)

## Context

The PPDB team rewrote the SSO uploader and loader (dax_ppdb DM-56960):
- **dax_ppdb:** merged as eea0379 on 2026-11-20; read at main ec576a6. The code is `python/lsst/dax/ppdb/bigquery/sso/{uploader,loader,manifest}.py`.
- **ppdb-cloud-functions:** `load_sso` at a4ac9f0. The Dataflow job is gone.
- **lsst/idf_deploy:** at 55a3dec, `environment/deployments/ppdb/services/`.
- **Docs:** https://dax-ppdb.lsst.io/user/sso-data.html.

`ssp-upload-sso` still uploads the old layout, one `<prefix>/<Table>.parquet` per table and a message with `uploaded_tables`. The new `load_sso` first reads `<prefix>/manifest.json`. With our layout it finds none, publishes a `failed` result, and loads nothing. Nothing is damaged, since its table swap is one transaction, but nothing loads either. The last int upload with the old contract was on 2026-11-14 (prefix `20261114T115654059`).

**The new contract**, as dax_ppdb's `Uploader` implements it:

1. **Parts.** Each table's files go to `gs://<bucket>/<prefix>/<Table>/part-NNNNN.parquet`.
   - Parts are numbered from 00000, per table, in sorted order of the local paths.
   - The prefix is the UTC time to the millisecond, e.g. `20261118T120000123`. The uploader refuses a prefix that already holds any object.
   - Each part is written with `if_generation_match=0`.
2. **Manifest.** `<prefix>/manifest.json` is written last, with `if_generation_match=0`, so its presence means the upload is complete.
   - Its pydantic model has `extra="forbid"`: `{"uploaded_at": ISO 8601 UTC, "tables": {Table: {"part-00000.parquet": {"row_count", "checksum", "size_bytes", "source_path"}}}}`.
   - `checksum` is the SHA-256 hex digest of the file. `row_count` and `size_bytes` must be at least 1, so **an empty file or table can't be uploaded.**
3. **Message.** `{"bucket", "object_prefix"}` is published to `projects/<project>/topics/load-sso-topic`. There is no `uploaded_tables`: the tables come from the manifest.
4. **Result.** The uploader waits for the load's result on the subscription `load-sso-result-sub` (topic `load-sso-result`).
   - Each result is JSON `{object_prefix, status: succeeded|failed, job_id | error}`.
   - dax_ppdb acks every message, skips other prefixes' results, and waits 5 minutes by default.
   - The subscription keeps messages for 1 hour.
5. **The loader** (`sso/loader.py`):
   - It loads each table's parts into a landing table, with the internal table's schema: a wrong type fails the load, and extra columns are ignored.
   - It then runs one transaction: TRUNCATE and INSERT per table, then ASSERT that the count equals the manifest's `row_count`.
   - Only the tables in the manifest are replaced.
   - It runs on one Cloud Run instance, one message at a time.
   - The docs still say not to upload while another load runs.

**Checked 2026-11-23** (read-only `testIamPermissions`; nothing was read or written). The `sso-uploader` account of int (`ppdb-int-6c62`) and of prod (`ppdb-prod-84a4`; the key's project confirms the ID) has:
- on `ppdb-<env>-sso-ingest`: `storage.objects.{create,delete,get,list}` and `storage.multipartUploads.{create,list,abort}`;
- `pubsub.topics.publish` on `load-sso-topic`;
- `pubsub.subscriptions.consume` on `load-sso-result-sub`.

Both buckets exist. `storage.buckets.get` isn't granted, and isn't needed.

**Also found:** the bucket has a 30-day Delete lifecycle rule and **no** AbortIncompleteMultipartUpload rule. Our docstring says otherwise, so it needs correcting: an upload killed outright can leave an incomplete multipart upload, which must be aborted by hand.

## Owner decisions (2026-11-23)

| Decision | Choice |
|---|---|
| SSObservation | **Upload the delivery's parts as they are**, each part one manifest part, after the existing md5 checks. The combine step, `RUN_DIR/upload/SSObservation.parquet` and its record, is removed. |
| Parallel threshold | **`PARALLEL_MIN_BYTES` from 1 GiB to 128 MiB.** The four big SSObservation parts (0.87 GB each), `mpc_orbits` and NearbySSO go as parallel chunks (64 MiB × 16 threads, as measured in `sso-upload-parallel.md`). |
| Waiting for the load | **Wait by default, for 5 minutes** (dax_ppdb's default), with `--wait-timeout S` and `--no-wait`. A failed load or a timeout exits non-zero, with distinct codes. |
| Prod key | Read-only checks with `~/.ppdb-prod.google.token` (through `GOOGLE_APPLICATION_CREDENTIALS`, never printed) are allowed. A real prod upload needs a separate OK after int works. |
| The design | *Pending approval.* |

## Outputs

**Objects.**
- `gs://<bucket>/<prefix>/<Table>/part-NNNNN.parquet` for each table: one part for each single-file table, and one per delivery part for SSObservation, in manifest order (the manifest's order is the sorted file order).
- Then `<prefix>/manifest.json`.
- Still never uploaded: the SSObservation manifest, the `_internal` sidecars, or any other file.

**`manifest.json`.**
- dax_ppdb's model exactly; no extra keys.
- `source_path` is the absolute local path.
- `uploaded_at` is the time the manifest is built, in UTC.
- Tables and parts are in `UPLOAD_TABLE_ORDER` and part order.

**The message:** `{"bucket", "object_prefix"}`, the exact bytes dax_ppdb publishes (`json.dumps`, key order as there).

**The upload record** in `report.json` gains:
- `manifest`: the manifest written, with every part's SHA-256, rows and bytes, kept for audit;
- `load`: `{status: succeeded|failed|timeout|not_waited, job_id | error, waited_s}`.

The `ssobservation` field, which described the combined file, is dropped.

**Exit codes:**

| code | meaning |
|---|---|
| 0 | loaded, or no wait was asked for |
| 1 | refused or failed before or during the upload, as now |
| 3 | the load failed: the `error` is printed and recorded |
| 4 | no result within the wait. The upload is published and may still load; check before uploading again. |

`ssp-sso-daily --upload` passes the code through. It doesn't upload in production today (ssp-daily `bin/deliver.sh`, owner decision of 2026-10-11).

**Configs.**
- The `topic` key goes: the topic and subscription are fixed names in dax_ppdb. An old config that still has `topic` is accepted only if it says `load-sso-topic`.
- `prod.yaml` is activated: `ppdb-prod-sso-ingest`, project `ppdb-prod-84a4`, all six tables.

## Approach

1. **Verify, as now.** `report.json` must say `deliverable: true`, and every table file must have the md5 and size the report records. For SSObservation, the manifest's md5, and each part's md5 and size.
   - **New:** each file's SHA-256 is computed in the same read as its md5, so there is no second pass over 6 GB.
   - **New:** each file's row count comes from its Parquet metadata and must equal the report's (per part: the SSObservation manifest's).
   - **New:** a file with 0 rows refuses the whole upload. The PPDB can't take it (`row_count >= 1`). Uploading the other tables without it would leave that table's old rows in the PPDB beside new ones in the others, and we never fill in for missing data. Today this happens only if a whole table is empty, which none has ever been.
2. **Build the manifest** before uploading anything, so a bad file fails first.
3. **Prefix.** Make the prefix and refuse it if `list_blobs(prefix=…, max_results=1)` finds anything.
4. **Upload the parts** in table and part order: chunked at 128 MiB or more, otherwise in one stream with `if_generation_match=0`. The chunked path keeps everything it has now: the existence check, the CRC32C check, aborting incomplete multipart uploads and the cleanup.
5. **Upload `manifest.json` last**, with `if_generation_match=0`. Any failure up to here deletes everything this upload wrote, including the manifest.
6. **Publish** `{"bucket", "object_prefix"}`. The current handling of an unconfirmed publish stays: keep the objects, record `published: null`.
7. **Wait** (unless `--no-wait`). Pull from `load-sso-result-sub` in pulls of up to 60 s, until a result for our prefix arrives or the time runs out.
   - Like dax_ppdb, ack every message, and log and skip other prefixes' results.
   - Record the result, and exit 0, 3 or 4.
   - Acking everything means a second waiter on the same subscription would lose its result. Uploading once per load window, which the docs already require, avoids that.
8. **Record** the upload in `report.json`, as now, with `manifest` and `load` added.

**Unchanged:**
- `--dry-run`, which now also prints the manifest it would write;
- `--no-publish`, which uploads the parts and the manifest and publishes nothing; the objects are kept;
- `--force`;
- `--allow-partial`;
- `--tables`;
- `--include-unsupported` (`UNSUPPORTED_TABLES` stays empty);
- `--no-parallel`, `--chunk-mib`, `--parallel-workers`.

**One upload per load window.** The rule still holds, with the Dataflow explanation replaced: the loader runs one load at a time, and the docs say not to upload during a load. With the wait, a run normally ends after its load, so the refusal of a second upload without `--force` matters only after a timeout or `--no-wait`.

**Contract and docs.** `ssp/delivery_contract.py`'s stage-3 block and `UPLOAD_*` constants change:
- `UPLOAD_MESSAGE_FIELDS` becomes `("bucket", "object_prefix")`;
- `UPLOAD_MANIFEST_FILE`, `UPLOAD_PART_FORMAT` and the result subscription name are added;
- the combined-file constants are removed;
- `REPORT_FIELDS["upload"]` changes as above.

Also updated:
- the module docstring;
- `docs/runbooks/sso-daily.md`: the layout, `run/upload/` no longer written, the exit codes, the bucket lifecycle note;
- `sso-upload-int.md`, a pointer saying it is superseded;
- `config/sso-upload/{dev,int,prod}.yaml`.

## Validation

**Tests** (network-free; GCS and Pub/Sub faked as today):
- **Layout and manifest:**
  - the part names and order;
  - the manifest's fields, validated against a local copy of dax_ppdb's model (`extra="forbid"`, `ge=1`);
  - SHA-256, row counts, sizes and `source_path`;
  - the manifest written last, with `if_generation_match=0`.
- **Refusals:**
  - a 0-row file;
  - a row count that disagrees with the report;
  - a prefix that already holds an object;
  - a sidecar or the SSObservation manifest offered as a part.
- **Cleanup:** after a failure at each step, including the manifest upload, nothing is left; after an unconfirmed publish, everything is kept.
- **The message** is exactly `{"bucket", "object_prefix"}`.
- **The wait:**
  - success → 0;
  - failure → 3, with the error recorded;
  - timeout → 4;
  - other prefixes' results are acked and skipped;
  - `--no-wait` doesn't touch the subscription.
- **The threshold:** files of 128 MiB or more go chunked.
- **Configs:** `prod.yaml` loads; an old `topic` key is handled as specified.

**With the stack:** our `manifest.json` from the 2026-11-23 dry run, validated with dax_ppdb main's own `Manifest.model_validate_json` (the latest daily stack with dax_ppdb main, or the model loaded standalone).

**Real data, in order, outside 10:00–10:50 PT:**
1. **A dry run** on `~/ssp_daily_data/ssp-tools/2026-11-23/run` (deliverable, no uploads yet, SSObservation in 6 parts): the planned objects, the manifest and the message.
2. **A real upload to int**, *with the owner's OK*. Record the parts, the manifest, the transfer times, the result and the `job_id`.
3. **Check int.** Through data-int TAP (one async job at a time; every job deleted), each table's row count must equal the delivery's: SSObservation 8,223,440, NearbySSO 4,248,257, SSObject 307,231, `mpc_orbits` 1,593,987, `current_identifications` 2,118,484, `numbered_identifications` 896,611.
4. **A real upload to prod**, only *with the owner's separate OK* once int works, then the same checks where a prod TAP exists.

Avoid the daily build window. Never upload while another load runs.

## Implementation plan

- **WP-M:** one work package, by a subagent in a worktree on `sso-manifest-upload`: everything under Approach, with its tests.
- **Review:** an independent review that tries to break it (cleanup paths, the wait loop, manifest-model conformance, the 0-row and mismatch refusals) and runs the real-data dry run. Fixes go back to WP-M until the review is clean.
- **The integrator:**
  - the stack validation of the manifest;
  - the dry run;
  - then ask the owner for the int upload, and after that for prod;
  - record the results here, and update the runbook.
- **Merge** the branch to master with the owner's approval, then tell ssp-daily the commit to pin. The daily run's upload stays off (2026-10-11) until the owner decides otherwise.

**Open, outside this work:** the 2026-11-14 int objects under the old layout expire under the bucket's 30-day rule, with nothing to do.
