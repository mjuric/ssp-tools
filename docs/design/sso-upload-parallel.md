# ssp-upload-sso: parallel uploads of the two big files (an experiment)

## Context

The int test of `ssp-upload-sso` (`docs/design/sso-upload-int.md`, Results) moved about 20 MB/s per stream: SSObservation (3.3 GB) took 167 s and mpc_orbits (2.3 GB) 119 s, of 317 s in all. That looks like a per-connection limit on the path from SLAC, not a bandwidth limit.

Splitting a file into pieces sent in parallel streams should be faster. Two ways:
- **GCS parallel composite upload:** the pieces become temporary objects, then a server-side compose;
- **The XML API's multipart upload:** `google.cloud.storage.transfer_manager.upload_chunks_concurrently`.

**The cost:** such an object has a CRC32C but no MD5, unlike what dax_ppdb's uploader writes today.

## Owner decisions

| Decision (date) | Choice |
|---|---|
| Scope (2026-11-12) | Chunked parallel uploads for the two big files only (SSObservation, mpc_orbits), accepting CRC32C instead of MD5 for them. |
| Process (2026-11-12) | Built by a subagent. Measure the upload speed and report to the owner whether it is worth it, before anything is merged. |
| Live tests (2026-11-12) | `--no-publish` uploads to the int bucket, deleted afterwards. Nothing is published. |
| After the measurement (2026-11-13) | Worth it: the default, at the measured optimum (64 MiB chunks × 16 streams), with an opt-out (`--no-parallel`, config `parallel: false`). Incomplete multipart uploads after a hard kill are left to a bucket lifecycle rule (the PPDB side). |

## Approach

- **Opt-in:** `ssp-upload-sso --parallel` (default off), and a config key `parallel` for later.
  - Files of at least `PARALLEL_MIN_BYTES` (1 GiB, which today means the two big ones) go in chunks: `transfer_manager.upload_chunks_concurrently`, threads, a chunk size and worker count to be measured.
  - Smaller files go as today.
- **Kept from today:**
  - Never overwrite: `if_generation_match=0` on the final object, or, if the multipart path can't take it, a check that the object doesn't exist plus the fresh prefix. The WP reports which.
  - On failure, abort the multipart upload, delete what was written, and leave no orphaned pieces.
  - The message is unchanged.
- **Verification:** after a chunked upload, the object's CRC32C must equal the local file's (computed with `google-crc32c`), or the upload fails and is cleaned up.

## Measurement (the deliverable)

The 2026-10-12 test copy (`/sdf/data/rubin/repo/dp2_prep/u/mjuric/sso-upload-int/run`), from the same node, alternating:
- plain;
- parallel, at 2–3 settings (chunk size, workers ≤ 16).

Each is a `--no-publish` upload of the two big files (`--tables SSObservation mpc_orbits --allow-partial`), 2 runs per setting, deleted after each. Report:
- the per-file and total times;
- MB/s;
- CPU and RSS;
- the object metadata of a chunked object (no MD5, a component count or none, CRC32C equal to the local file's);
- what's left in the bucket after the deletes.

The owner then decides whether to merge.

## Results (2026-11-12)

The WP is on branch `wp-parallel` (d2bfe00 and the commit after it), not merged.

### What was built

- **`--parallel`** (`--no-parallel`; config key `parallel`, a boolean; default off), `--chunk-mib` (default 64, 5 to 5120) and `--parallel-workers` (default 16, 1 to 16).
  - Files of at least `PARALLEL_MIN_BYTES` (1 GiB) go through `transfer_manager.upload_chunks_concurrently`: threads, each part checked by CRC32C.
  - Other files, and every file without `--parallel`, go exactly as before.
  - The message is unchanged. The upload record gets `parallel`: `{chunk_bytes, workers, min_bytes, tables}`.
- **Overwrite protection: the fallback.** GCS refuses preconditions on XML multipart uploads. Checked on the int bucket, under a probe prefix that was then deleted:
  - `x-goog-if-generation-match: 0` on the initiate request gets HTTP 400 `NotImplemented`, "Object preconditions are not supported for multipart uploads";
  - the same header on the complete request alone is ignored: the upload replaced an existing object.

  So `if_generation_match=0` can't be given. Instead, the object is checked not to exist just before its upload, under the fresh per-upload prefix. A writer that slips in between the check and the complete would be overwritten. Only another upload under the same millisecond prefix could do that.
- **Cleanup.**
  - The library cancels the multipart upload when a part fails.
  - On top of that, on any failure or interrupt (Ctrl-C included), the run lists the object's incomplete multipart uploads (XML `GET ?uploads&prefix=`) and aborts them (`DELETE ?uploadId=`), then deletes the object if it was written. The earlier objects of the run are deleted as before.
  - A run killed outright (SIGKILL, node loss) can still leave an incomplete multipart upload. The library's docs suggest a bucket lifecycle rule, `AbortIncompleteMultipartUpload`; that is the PPDB team's to set.
  - Checked live with an injected failure after the initiate: the incomplete upload was listed, aborted, and nothing was left.
- **Verification.** The local CRC32C (`google-crc32c` 1.9.0, C extension; required) is computed in a thread while the upload runs. The object's CRC32C and size must equal the local file's, or the object is deleted and the run fails.
- **Tests:** 24 new network-free tests with fakes:
  - only the big files go in chunks, and the default path is unchanged;
  - an existing object is refused and left untouched;
  - part failure and interrupt: the multipart upload is aborted and the objects deleted;
  - a CRC32C or size mismatch deletes the object;
  - cleanup failures are logged and the original error kept;
  - option limits, the dry run, CLI flags, the CRC32C known value, and the XML list (with pagination) and abort.

### Measurements

**The setup:**
- sdfiana031, load 10–30 (shared), one upload at a time, 2026-11-13 03:08–03:48 UTC.
- ssp-tools `wp-parallel`, google-cloud-storage 3.15.1.
- `ssp-upload-sso int …/sso-upload-int/run --no-publish --tables SSObservation mpc_orbits --allow-partial [--parallel --chunk-mib C --parallel-workers W]`.
- The files: SSObservation (combined, reused) 3,302,438,055 bytes; `mpc_orbits` 2,272,499,732 bytes.
- Each prefix was deleted after its run.

The upload times are from the run's per-file log lines; wall time, CPU and RSS are from `/usr/bin/time -v`, so they include the roughly 15–23 s of md5 checks before the upload.

| run (in order) | SSObservation s (MB/s) | mpc_orbits s (MB/s) | both s (MB/s) | wall | CPU s (user+sys) | peak RSS GB |
|---|---|---|---|---|---|---|
| 1 plain | 131.8 (25) | 87.5 (26) | 219.3 (25) | 4:02 | 32 | 0.28 |
| 1 64 MiB × 8 | 25.0 (132) | 25.2 (90) | 50.2 (111) | 1:05 | 33 | 0.62 |
| 1 128 MiB × 16 | 21.0 (157) | 20.3 (112) | 41.3 (135) | 0:57 | 35 | 2.19 |
| 1 256 MiB × 16 | 8.5 (389) | 19.6 (116) | 28.1 (198) | 0:43 | 31 | 3.31 |
| 2 plain | 119.8 (28) | 105.8 (21) | 225.6 (25) | 4:02 | 31 | 0.28 |
| 2 64 MiB × 8 | 34.8 (95) | 26.5 (86) | 61.3 (91) | 1:17 | 31 | 0.62 |
| 2 128 MiB × 16 | 28.3 (117) | 13.6 (167) | 41.9 (133) | 0:58 | 32 | 2.20 |
| 2 256 MiB × 16 | 38.6 (86) | 32.2 (71) | 70.8 (79) | 1:28 | 31 | 3.31 |
| 3 64 MiB × 16 | 14.1 (234) | 8.7 (261) | 22.8 (245) | 0:38 | 34 | 1.15 |
| 3 128 MiB × 16 | 18.6 (178) | 13.0 (175) | 31.6 (176) | 0:47 | 33 | 2.20 |
| 3 plain | 583.6 (6) | 272.6 (8) | 856.2 (7) | 14:31 | 31 | 0.28 |
| 4 64 MiB × 16 | 9.3 (355) | 8.6 (264) | 17.9 (311) | 0:34 | 35 | 1.15 |
| 4 128 MiB × 16 | 11.7 (282) | 10.4 (219) | 22.1 (252) | 0:38 | 37 | 2.19 |
| 4 plain | 213.6 (15) | 186.6 (12) | 400.2 (14) | 6:56 | 31 | 0.28 |

**By setting, for both files:**

| setting | runs | upload s | MB/s | peak RSS GB |
|---|---|---|---|---|
| plain (one stream) | 4 | 219–856 | 7–25 | 0.28 |
| 64 MiB × 8 | 2 | 50–61 | 91–111 | 0.62 |
| 64 MiB × 16 | 2 | 18–23 | 245–311 | 1.15 |
| 128 MiB × 16 | 4 | 22–42 | 133–252 | 2.2 |
| 256 MiB × 16 | 2 | 28–71 | 79–198 | 3.3 |

- **The speed-up is large.**
  - Every parallel run took 18–71 s for both files; every plain run 219–856 s.
  - The single-stream rate fell from 25 MB/s to 7 MB/s over the evening. Parallel runs either side of the slowest plain run took 32 s and 18 s, so the path had capacity: the limit is per stream.
- **Peak memory is about chunk size × threads.**
- **The CPU cost is negligible:** 31–37 s in every run, about 20 s of it the md5 checks before the upload.
- **256 MiB chunks are worse,** not better: SSObservation is only 13 parts and `mpc_orbits` 9, so the threads aren't all busy at the end, and one slow part holds up the whole file.
- **urllib3 warns** "Connection pool is full, discarding connection" (pool size 10, 16 threads), 3–12 times a run. It only costs a TLS reconnect for some parts; harmless.

**The object metadata:** SSObservation and `mpc_orbits` from every run, before the delete.

| | plain | chunked (all settings) |
|---|---|---|
| size | equal to the local file's | equal to the local file's |
| md5 | present (SSObservation `JztpsXt7+V92quh6YRBGOw==`, i.e. `273b69b1…463b`) | **none** |
| crc32c | SSObservation `7Lzq9g==`, `mpc_orbits` `87CJkw==`, equal to the local files' | the same values, equal to the local files' |
| component_count | none | none (an XML multipart object is not a composite) |
| content type, storage class, metadata | `application/octet-stream`, STANDARD, none | the same |

The local CRC32Cs were computed separately (`google-crc32c`), and the md5s match `docs/design/sso-upload-int.md`.

**Cleanup:**
- After each of the 14 deletes, the prefix was empty: no objects and no incomplete multipart uploads under it.
- Listing the whole bucket's incomplete multipart uploads (read-only; the service account may list them) found 0 every time.
- The three small probe prefixes (`wp-parallel-probe-*`) were deleted and checked empty too.
- The 14 runs appended `--no-publish` records to the test copy's `report.json`. Nothing was published.

### Recommendation

**Worth it.** At 64 MiB × 16 (now the default), the two big files go 10–40 times faster than one stream: 18–23 s against 219–856 s. That brings the whole upload from about 4–15 minutes to under a minute of transfer, at about 1.2 GB of memory and no extra CPU worth counting.

**The costs, for the owner to weigh:**
1. **No MD5 on the two big objects.** The CRC32C is verified end to end instead. Whether anything downstream (the loader, the PPDB team's checks) expects an MD5 needs confirming with the DM-55678 owners.
2. **Weaker overwrite protection:** a check plus the fresh prefix, instead of a server-side precondition. Within one millisecond prefix, a collision is very unlikely.
3. **Incomplete multipart uploads** are possible after a hard kill. They are invisible to `gsutil ls` and are billed. Ask the PPDB team for an `AbortIncompleteMultipartUpload` lifecycle rule on the bucket.

**If merged:**
- `ssp/delivery_contract.py` needs the integrator's update. It says each object is uploaded with `if_generation_match=0`, and it lists neither the `parallel` config key nor the `parallel` field of the upload record.
- Keep `--parallel` opt-in until point 1 is settled, then consider `parallel: true` in the configs.
