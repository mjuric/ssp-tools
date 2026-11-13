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
