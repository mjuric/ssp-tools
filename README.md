# LSST Daily Data Products Pipeline

Efficient streaming export of large Postgres tables to Parquet format using PyArrow.

## Overview

`ddpp` provides tools for exporting arbitrary Postgres tables to columnar Parquet files with:
- **Memory efficiency**: Streaming batches, bounded memory footprint
- **Type fidelity**: Automatic OID→Arrow type mapping
- **High throughput**: Postgres COPY → temp CSV → Arrow streaming → zstd-compressed Parquet

## Installation

```bash
pip install -e .
```

Or with dev dependencies:
```bash
pip install -e ".[dev]"
```

## Configuration

### Database Connection

Set connection parameters via environment variables:

```bash
export PGHOST=your.postgres.host
export PGPORT=5432
export PGDATABASE=your_database
export PGUSER=your_user
export PGPASSWORD=your_password  # or use ~/.pgpass
```

Alternatively, use CLI flags (`--host`, `--port`, `--dbname`, `--user`, `--password`) or provide a full DSN string with `--dsn`.

### Basic Usage

Export a full table:
```bash
fast-export --sql "SELECT * FROM schema.table" --out output.parquet
```

Export with filtering and projection:
```bash
fast-export \
  --sql "SELECT col1, col2, col3 FROM schema.table WHERE updated_at >= '2025-01-01'" \
  --out filtered_export.parquet \
  --row-group-size 500000
```

### Performance Tuning

- `--row-group-size`: Rows per Parquet row group (default: 1,000,000)
  - Reduce for very wide tables to control memory
  - Increase for narrow tables to improve scan performance
- `--block-size`: Arrow CSV block size in bytes (default: 67MB)
  - Adjust based on available memory and I/O patterns

### Debugging

Keep the intermediate CSV file for inspection:
```bash
fast-export --sql "SELECT * FROM table" --out output.parquet --keep-temp
```

## Type Mapping

Postgres types are automatically mapped to Arrow types:
- `bool` → `bool()`
- `int2/int4/int8` → `int16/int32/int64()`
- `float4/float8` → `float32/float64()`
- `text/varchar/char` → `string()`
- `date` → `date32()`
- `timestamp` → `timestamp('us')`
- `timestamptz` → `timestamp('us', tz='UTC')`
- Unknown types → `string()` (fallback)

Extend `PGOID_TO_ARROW` in `ddpp/export/postgres.py` for additional types.

## Development

Run tests:
```bash
pytest
```

## Security Note

**Never commit database credentials to version control.** Always use environment variables or external configuration files (e.g., `.env`, `.pgpass`).

## License

MIT
