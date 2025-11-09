# AI Coding Agent Instructions for `ssp-ddpp`

Guidance for exporting arbitrary large Postgres tables to Parquet via streaming Arrow. Package `ddpp.export` contains the core export logic.

## Big picture
- Flow: Postgres `COPY` → temp CSV → PyArrow streaming parse → Parquet (zstd) row groups.
- Goals: bounded memory, faithful types (PG OID → Arrow), high throughput.

## Key files
- `ddpp/export/postgres.py`: Core extractor (DSN builder, `SQL`, OID map, CSV reader, Parquet writer).
- `ddpp/export/__init__.py`: Public API exports.
- `README.md`: Usage documentation and configuration guide.

## Core mechanics
- Introspection: `SELECT * FROM (<SQL>) t LIMIT 0` to get names + OIDs; map via `PGOID_TO_ARROW` / `arrow_type_for_oid`.
- CSV reader: `pacsv.open_csv` uses explicit `column_types`, large `block_size` (`1<<26`), null handling (`"", "NULL"`).
- Batching: accumulate `RecordBatch` objects; flush when `rows_accum >= ROW_GROUP_SIZE`.
- Parquet writer: initialize once from first non-empty batch schema; `compression="zstd"`, `use_dictionary=False`.
- Float fidelity: DSN includes `extra_float_digits=3` for IEEE-754 round-trip.

## Config knobs (per export)
- Connection: Use env vars (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) or CLI flags (`--host`, `--dbname`, etc.) or full `--dsn` string.
- `--sql`: choose columns + filters (e.g. `SELECT col1, col2 FROM schema.table WHERE ts >= '2025-01-01'`).
- `--out`: output file (one file, multiple row groups).
- `--row-group-size`: tune vs memory (reduce for wide tables, increase for narrow).
- `--block-size`: adjust for I/O/memory trade-offs.
- `PGOID_TO_ARROW`: extend for new PG types; unknowns default to `pa.string()`.

## Generalizing to other tables
- Set `--sql` to target table; script auto-infers schema.
- For derived columns: transform each batch pre-flush; ensure first batch defines full schema.
- Maintain uniform timezone policy for timestamptz (`UTC` in current map).

## Testing
- Unit: assert OID mappings return expected Arrow dtypes.
- Synthetic integration: small CSV through the same convert options → check Parquet schema + row-group count.

## Operational details
- Nulls: empty and `NULL` strings → null.
- Temp CSV cleaned up by default; use `--keep-temp` for debugging.
- Credentials: Use environment variables or CLI flags; never commit hardcoded credentials.

## CLI usage
```bash
export PGHOST=your.postgres.host PGDATABASE=your_db PGUSER=your_user PGPASSWORD=your_pass
fast-export --sql "SELECT * FROM schema.table WHERE updated > '2025-01-01'" --out table.parquet --row-group-size 500000
```

Keep this file updated when adjusting batching, compression, or type maps.
