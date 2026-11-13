# ClickHouse queries: client timeouts, keep-alive, and cancel on close

## Context

`ssp-extract-sso-inputs` failed on 2026-11-13 with `Read timed out. (read timeout=300)` (Slurm job 44597225, ssp-tools 7d3281b).
- **The query:** one id-pass query (`processing = 'AP-S'`) ran 2,218 s on the server.
  - It scanned 29.5 GiB of `source_daytime` on a cold Weka cache, reading at 0.7–17 MB/s.
  - With `FORMAT Parquet` nothing is sent until the result is complete, so clickhouse-connect's default `send_receive_timeout` of 300 s fired.
- **Cancel on close didn't help:** the server kept running the query for the full 37 minutes after the client had gone, although `cancel_http_readonly_queries_on_client_close=1` was set.
- **The clients:** every ClickHouse client in the code has the same settings and the same default timeout:
  - `ssp/export/submittable.py` `run_queries`, the extract's queries;
  - `ssp/sso_inputs.py`, the `--tap-url none` ppdb path;
  - `bench/ssobservation_validate.py`.

## Owner decisions

| Decision (date) | Choice |
|---|---|
| Fix (2026-11-13, via ssp-daily's brief, priority above the ASCII fix) | Client timeout at least the server's `max_execution_time`, from one constant; keep long queries observably alive with progress headers; make cancel-on-close work; the same for every ClickHouse client. A WP by a subagent, as usual. |

## Approach

- **One place for the settings:** a helper in `ssp/export/submittable.py`, used by all three clients, e.g. `clickhouse_client(host, port, database, user, max_execution_time=QUERY_TIMEOUT_S)`.
  - **Server:** `max_execution_time` = `QUERY_TIMEOUT_S` (3600 for the extract; the bench keeps its 600).
  - **Client:** `send_receive_timeout` = `QUERY_TIMEOUT_S` + a margin (120 s), so the server's own limit always fires first, with a clear error.
  - **Keep-alive:** `send_progress_in_http_headers=1` and `http_headers_progress_interval_ms` = 10000. The server then writes an `X-ClickHouse-Progress` header every 10 s while it works. That keeps the connection observably alive (proxies, load balancers) and gives the server a write on which to notice a closed client.
  - `cancel_http_readonly_queries_on_client_close=1` stays, and `readonly=1` is set if the account allows it (that setting only applies to read-only queries).
  - The `credentials` and `bypass_proxy` handling is unchanged.
- **The trap to rule out:** with progress headers, the HTTP status (200) is sent before the query ends. An error in the middle of a query then arrives in the body, not as an error status.
  - The WP must show that `raw_query` and `raw_stream`, with Parquet output, still raise on a query that fails midway, e.g. one that exceeds a small `max_execution_time`.
  - A truncated or garbled Parquet must never be returned as a result. If clickhouse-connect doesn't detect it, we detect it: the `X-ClickHouse-Exception-Code` trailer or text, and Parquet's footer check.
- **Cancel on close:** establish empirically whether, with progress headers, the server cancels a query when the client goes away. Look it up in system.processes (and system.query_log if readable) by `query_id`.

## Validation

- **Tests (network-free):** a stub HTTP server that sends progress headers for longer than a short test timeout and then a valid Parquet body. Show that:
  - the client waits for it;
  - a stub that goes silent past the timeout fails;
  - a stub that sends an error after the headers raises.

  Test the helper's settings through their effect, not as a constant test.
- **Live, on the shared ClickHouse, small and polite:**
  - `squeue` first. Nothing while ssp-daily's `ssp-daily-extract-test` job runs; at most one query at a time.
  - Use cheap, deliberately slow queries, e.g. `SELECT sum(sleepEachRow(0.5)) FROM numbers(240) SETTINGS max_block_size = 1`: about 2 min of sleeping and no scan. With them, show:
    1. a query silent for longer than 300 s doesn't time out on the client, with the progress headers seen;
    2. a mid-query error, from a small `max_execution_time`, raises through `raw_query` and `raw_stream` with Parquet;
    3. killing the client mid-query (closing the connection) ends the query on the server within about one progress interval, seen in system.processes or system.query_log.
- No full extract against ClickHouse: ssp-daily is testing that.

## Implementation plan

- **One WP:** the helper, the three clients, the tests and the live checks.
- **Then the integrator's review.**
- **Merge to master** with the owner's approval, and tell ssp-daily the commit to pin.

## Results

### What was built

- `ssp/export/submittable.py`:
  - `clickhouse_client(host, port, database, user, max_execution_time=QUERY_TIMEOUT_S)`: `QUERY_TIMEOUT_S` = 3600, `send_receive_timeout` = that + `TIMEOUT_MARGIN_S` (120), `send_progress_in_http_headers=1`, `http_headers_progress_interval_ms` = `PROGRESS_INTERVAL_MS` (10000), `readonly=1`, `cancel_http_readonly_queries_on_client_close=1`. Credentials and the proxy bypass as before.
  - `query_parquet` (`raw_query` + `FORMAT Parquet`) and `stream_parquet` (`raw_stream` to a file): raise `ClickHouseResultError` on a body that isn't a complete Parquet file (magic at both ends, a footer that fits, then pyarrow's own parse), with the server's message when the body carries one (the 26.x `__exception__` block, or a `Code: N. DB::Exception` text); `stream_parquet` also turns a broken-off chunked response into `ClickHouseResultError`.
  - `run_queries` (the extract), the `--tap-url none` ppdb path of `ssp/sso_inputs.py` (an error there is an `ExtractError`) and `bench/ssobservation_validate.py` (`max_execution_time` 600) use them.
- **clickhouse-connect detail:** 1.9.0 already sends `send_progress_in_http_headers=1` itself when the setting is writable, with an interval derived from the timeout, capped at 120 s, and its value wins over a client setting. The helper sets the transport's interval to 10 s. The old clients were therefore already getting a progress header every 120 s.
- **readonly=1 forbids `SETTINGS` clauses in the SQL** (`Cannot modify 'max_block_size' setting in readonly mode`): settings must be passed as `settings=` (request parameters), as all our queries already do. External data and bound parameters work under `readonly=1` (checked live).
- **Tests:** `tests/test_clickhouse_client.py`, a threaded stub ClickHouse HTTP server on localhost (it honours the requested progress interval): a query kept alive by progress headers for 2.5 s outlives a 1 s client timeout (raw_query and raw_stream); a silent one times out; the client timeout follows `max_execution_time`; an error after the headers (plain text, the 26.x tagged block, after a whole Parquet file, an empty or truncated body, a broken chunked body, the `X-ClickHouse-Exception-Code` header with status 200) raises through both paths, and through `run_queries` and `export_ppdb`.

### Live checks (2026-11-12, ClickHouse 26.8.16.41 at sdfiana032, account `ssp_xmatch`)

Run after ssp-daily's `ssp-daily-extract-test` job had finished, one query at a time, sleep/`numbers()` queries only. The account can't read `system.processes` or `system.query_log` (code 497, no grant). The probe for "is the query still running" was therefore a `SELECT 1` with the same `query_id`, which fails with `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` (216) exactly while the original runs. Every check confirmed that the query was running before the kill.

| Check | query_id | Result |
|---|---|---|
| 1. Silent 360 s (`sum(sleepEachRow(0.5))` over 720 rows), `query_parquet` | ad359d7c-7459-4754-bc67-524639b08c35 | Completed in 360.5 s, no client timeout; 36 `X-ClickHouse-Progress` headers (the first at 10.0 s) |
| 2a. `max_execution_time` 15 s on a 30 s sleep, `query_parquet` | 3c04d7f0-1f45-4539-a7c7-a2aee34990b2 | Raised `DatabaseError` 159 TIMEOUT_EXCEEDED at 15.0 s |
| 2b. The same, `stream_parquet` | ca253d62-01ce-4cdc-a112-ce97e43b1686 | Raised `DatabaseError` 159 at 15.0 s |
| 2c. Error after a progress header *and* Parquet body (`throwIf` at row 5500 of 6000, 2000-row row groups, ~33 s), `query_parquet` | e47e3155-eeca-4c3a-999d-d796ceebb06d | Raised `DatabaseError` 395 at 33.6 s (with `wait_end_of_query=1` the server buffers the body and sends `X-ClickHouse-Exception-Code` after the progress header) |
| 2d. The same, the old unchecked `raw_stream` copy | 9153c508-a76e-45e6-b73b-13b39da0b104 | 200, 1 progress header, `X-ClickHouse-Exception-Tag`; the server ended the chunked body early: urllib3 `ProtocolError` (IncompleteRead) at 33.6 s. With `stream_parquet` that is a `ClickHouseResultError`. |
| 2e. The same over raw HTTP/1.0 (no chunking: the only framing is the content) | 2df2477f-1417-4f22-8480-2004727e16f0 | 200, body = 20,102 bytes starting `PAR1` and ending in the tagged `__exception__` block; `check_parquet` raised with the server's message (code 395) |
| 3a. Kill (SIGKILL) the client 25 s into a 120 s sleep, the helper's settings | e86a4484-9a90-48ac-8795-80231ba2af7d | Query ended within 2.0 s of the kill (the probe's interval) |
| 3b. The same, with progress headers off (`readonly=1` kept) | 43081dd2-309d-4639-8e1d-edd7969d37a0 | Ended within 2.0 s |
| 3c. The same, with the **old** settings (no `readonly`; clickhouse-connect's own 120 s progress headers) | 632a41aa-7600-47db-a611-3f7b935c97a7 | **Not cancelled:** still running 96 s after the kill, i.e. ran to its natural end |

**Why cancel-on-close didn't work before:** `cancel_http_readonly_queries_on_client_close` applies only to queries run with `readonly` > 0. The server sets `readonly=2` itself for HTTP GET, but clickhouse-connect POSTs. The extract's clients set no `readonly`, so the setting did nothing. That is check 3c, and so the 37-minute orphaned query of 2026-11-13. With `readonly=1`, the server notices the closed connection within about a second, with or without progress headers (3a, 3b), so the progress headers are not what makes the cancel work. (The ppdb path already had `readonly=1`, passed per query; the bench had it per client.)
