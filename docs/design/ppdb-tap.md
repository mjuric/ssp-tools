# ppdb_dia_sources from the PPDB TAP service

## Context

`ppdb_dia_sources.parquet` is NearbySSO's only view of the PPDB: five columns of `DiaSource` (`diaSourceId`, `visit`, `midpointMjdTai`, `ra`, `dec`), written by the `ppdb_dia_sources` step of `ssp-extract-sso-inputs` (`ssp/sso_inputs.py`, `export_ppdb`). Today the step runs one ClickHouse query:

```sql
SELECT diaSourceId, visit, midpointMjdTai, ra, dec FROM ppdb.DiaSource ORDER BY visit, diaSourceId
```

ClickHouse `ppdb.DiaSource` is a copy of the PPDB, and an incomplete one. Compared with the PPDB itself, read over its TAP service on 2026-10-09:

| | ClickHouse `ppdb.DiaSource` | PPDB TAP (data-int) |
|---|---|---|
| rows | 13,086,925 | 17,863,456 |
| visits | 4,422 | 5,919 |
| last visit | 2026062600599 | 2026071300816 |

- ClickHouse lacks **1,497 visits**: parts of the nights 2026-05-21 to 06-26, and all of 06-27 to 07-13.
- On four nights (2026-02-18, 02-19, 02-22 and 02-23), ClickHouse has 5,876 rows, over 3,160 (visit, detector) pairs, that the PPDB doesn't.
- Elsewhere the two agree: 615,130 of the 618,290 (visit, detector) pairs in ClickHouse have the same row count.

So the daily NearbySSO is built against about three quarters of the PPDB's visits. The PPDB's own TAP service is the authoritative source.

**The TAP service.** `https://data-int.lsst.cloud/api/ppdbtap`: OpenCADC/cadc-rest, UWS async jobs, bearer token, `RESPONSEFORMAT=parquet` (`application/vnd.apache.parquet`). The `ppdb` schema's `DiaSource` is readable. The other SSO tables give BigQuery "Access Denied", but this design doesn't need them.

Two of its behaviours matter here, both seen in the full export of 2026-10-09 (`/sdf/data/rubin/user/mjuric/ppdb-export/`):
- Two async jobs of about 2M rows of `SELECT *` hung in EXECUTING, with "Bytes processed: 0", for over 55 minutes, until they were aborted. The same visits went through as four chunks of about 1M rows each. The hang has been written up for the service's operators.
- Sync requests answer with a 303 redirect to the result.

## Owner decisions

| Date | Topic | Decision |
|---|---|---|
| 2026-10-10 | Source | A new option runs the `ppdb_dia_sources` query on a TAP endpoint. **The data-int PPDB TAP service is the default.** |
| 2026-10-10 | Option | `--tap-url URL`, default `https://data-int.lsst.cloud/api/ppdbtap`. `--tap-url none` keeps today's ClickHouse query, following `--correction-table none`. |
| *proposed* | Token | `--tap-token FILE`, default `~/.data-int.token`, mode 600 required (like `~/.chpass`). There is no flag that takes the token itself, and the token is never logged or written to the manifest. The `--tap-` prefix follows the per-source groups `--ch-*` and `--mpc-*`. |
| *proposed* | Tuning | No more options: the chunk size, concurrent jobs and hang timeout are constants (below). |
| 2026-10-10 | Testing | Keep it simple: no fake TAP server or network-free unit tests. One real run against data-int, with the checks below. |
| *proposed* | Fallback | None. If TAP fails, the step fails and names the cause. It never falls back to ClickHouse silently: that would change NearbySSO's input without anyone noticing. |
| *proposed* | Consistency | The extract fixes the set of visits first. A chunk whose row count changes between that count and its fetch (late-arriving DiaSources) is fetched again once, then fails. Visits that arrive after the count are left for the next day's run. |

## Output

Unchanged: `ppdb_dia_sources.parquet`, the same five columns and types (int64, int64, double, double, double), no NULLs, sorted by (`visit`, `diaSourceId`), zstd. NearbySSO and the build stage don't change.

The manifest entry gains the details of where the file came from:

```json
"ppdb_dia_sources": {
  "file": "ppdb_dia_sources.parquet", "rows": ..., "md5": ..., "extracted_utc": ...,
  "source": "PPDB TAP https://data-int.lsst.cloud/api/ppdbtap: SELECT diaSourceId, visit, midpointMjdTai, ra, dec FROM ppdb.DiaSource",
  "ppdb_tap": {"url": ..., "visits": 5919, "visit_min": ..., "visit_max": ..., "chunks": 18,
               "count_star": 17863456, "refetched_chunks": 0}
}
```

`source` stays a string. A ClickHouse-sourced entry keeps exactly today's form.

## Approach

All of this lives in a new module, `ssp/export/ppdb_tap.py`. `export_ppdb` dispatches on `--tap-url`.

1. **The visit counts.** A sync query, `SELECT visit, COUNT(*) AS n FROM ppdb.DiaSource GROUP BY visit` (Parquet), gives the visits and their counts. A NULL visit fails the step: NearbySSO can't use a row without one.
2. **The chunks.** Contiguous visit ranges of about `CHUNK_ROWS` = 1,000,000 rows each (the size that went through on 2026-10-09).
   - Each chunk is one async job: `SELECT diaSourceId, visit, midpointMjdTai, ra, dec FROM ppdb.DiaSource WHERE visit BETWEEN lo AND hi`.
   - At most `MAX_JOBS` = 4 jobs run at once.
   - Each result streams to `INPUTS_DIR/.partial/ppdb_tap/part-NNNN.parquet`.
3. **The job lifecycle.** POST `/async` (`MAXREC` above the chunk), then PHASE=RUN, then poll every 5 s, then GET `results/result`, then DELETE the job.
   - Every job is deleted, also on failure or interruption (`finally`), so none are left on the server.
4. **Hangs.** A job still QUEUED or EXECUTING after `JOB_TIMEOUT_S` = 900 s is aborted (PHASE=ABORT) and deleted.
   - It is retried once.
   - If that fails too, the step fails. The message gives the job URLs and phases, for a report to the operators, and the step doesn't retry in a loop.
   - Errors (ERROR, HTTP 5xx) get the same single retry.
   - HTTP 401/403 fails at once: "the token in FILE was refused (expired?)".
5. **Checks per chunk.** The rows equal the sum of the step-1 counts for its visits. Every visit is inside its range. The schema is the five columns with the types above, cast exactly (an overflowing or lossy cast fails). There are no NULLs.
   - If the count differs, the chunk is fetched again once (data that arrived in between). If it still differs, the step fails.
6. **Assembly.** Concatenate the parts, then check:
   - `diaSourceId` is unique;
   - the total equals the sum of the step-1 counts;
   - every counted visit is present.

   Then sort by (`visit`, `diaSourceId`) and write zstd to `.partial/ppdb_dia_sources.parquet`. The existing step machinery moves it into place with the manifest.
7. **HTTP.** `requests`, now a direct dependency (it is already installed, through astroquery). SDF's proxy settings come from the environment as usual. `bypass_proxy` is for ClickHouse only.

Expected cost: 5 columns rather than `SELECT *`, so far less to move than the 2026-10-09 export. That export's DiaSource took about 10–20 min across 4 jobs. The real-data check (below) measures it.

## Shared-resource etiquette (added to CLAUDE.md)

PPDB TAP (data-int):
- at most 4 concurrent jobs;
- delete every job;
- abort hung jobs, and report them to the operators rather than retry in a loop;
- the token comes only from the file the owner designates.

## Validation

No fake TAP server or network-free unit tests (owner, 2026-10-10). The checks of steps 5 and 6 run on every extract. They are validated once, on real data:
- **The extract:**
  - Run `ssp-extract-sso-inputs DIR --only ppdb_dia_sources` against data-int, on a copy of the 2026-10-09 inputs.
  - Compare with the 2026-10-09 TAP export (`DiaSource.parquet`, same columns). They should be equal, apart from visits added since.
  - Compare with the ClickHouse file, and report the difference by night.
  - Record wall time, peak RSS and job count, and that no jobs were left on the server.
- **`--tap-url none`:** gives today's ClickHouse file. Same md5, if ClickHouse hasn't changed in between.
- **NearbySSO on the new input:**
  - rebuild NearbySSO from the 2026-10-09 inputs with the TAP `ppdb_dia_sources`, and run every check;
  - record how the row count changes (more visits, so more NearbySSO rows);
  - the other tables must be byte-identical.
- **The existing suite** still passes. The only new test checks that the new options are parsed (`--tap-url none` and the defaults).

## Implementation plan

| Step | Work | Who |
|---|---|---|
| 0 | This design; owner approval | integrator |
| 1 | `ssp/export/ppdb_tap.py` and its wiring into `ssp-extract-sso-inputs`; `requests` in the dependencies; the CLAUDE.md etiquette | integrator |
| 2 | The real-data checks and the NearbySSO rebuild; the runbook (`docs/runbooks/sso-daily.md`: the token, the option, failures); results recorded here | integrator |
| 3 | Tell ssp-daily about the new default (the token on its node, network access to data-int) | integrator |
| 4 | Branch → master | owner approval |

A single module, so no work packages and no separate review agent.

## Out of scope

- The other PPDB tables over TAP.
- Using the TAP data for the (visit, detector) work of #86. This design leaves that open; the visit counts from step 1 could feed it later.
