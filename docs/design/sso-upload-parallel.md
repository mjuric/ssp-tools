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
