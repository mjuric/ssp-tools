# LSST Daily Data Products Pipeline (ddpp) - AI Coding Assistant Guide

## Project Overview

This is a specialized data export library for LSST (Rubin Observatory) that provides two complementary CLI tools for streaming large datasets into Parquet format:

1. **`fast-export`**: Streams Postgres tables via PyArrow with memory-efficient batching
2. **`extract-catalog`**: Extracts LSST Butler catalog datasets (requires Science Pipelines environment)

**Core philosophy**: Memory-bounded streaming architecture using temporary CSV as an intermediary between Postgres COPY and PyArrow readers. All exports can run within a single transaction for consistency.

## Architecture

### Two-Stage Export Pipeline (Postgres)

The `fast-export` tool uses a deliberate two-stage approach:
1. **Stage 1**: Postgres `COPY TO` → temporary CSV file (fast, native serialization)
2. **Stage 2**: PyArrow CSV reader → streaming batches → Parquet with zstd compression

This design avoids loading entire tables into memory and leverages Postgres's optimized COPY protocol.

### Key Implementation Details

- **Type mapping**: `ddpp/export/postgres.py::PGOID_TO_ARROW` maps Postgres OIDs to Arrow types. Extend this dict for custom types; unknown OIDs default to `pa.string()`.
- **Transaction isolation**: Batch exports use `ISOLATION_LEVEL_REPEATABLE_READ` to ensure consistent snapshots across multiple tables.
- **Row groups**: Default 1M rows per group. Reduce for very wide tables (memory pressure), increase for narrow tables (scan performance).
- **Float precision**: DSN automatically includes `options='-c extra_float_digits=3'` to preserve numeric precision.

### Butler Integration (extract-catalog)

The Butler extraction tool (`ddpp/export/butler.py`) streams LSST Science Pipelines datasets:
- **Dependency**: Requires `lsst.daf.butler` (only available in Science Pipelines environment)
- **Pattern**: One Parquet row-group per Butler dataset (e.g., per visit)
- **Filtering**: Supports ID-based filtering via `--filter-ids` (must be int64-convertible)
- **Conversion**: Uses `lsst.daf.butler.formatters.parquet.astropy_to_arrow` for Astropy→Arrow table conversion

## Development Workflows

### Setup

```bash
# Standard install
pip install -e .

# With dev dependencies (pytest)
pip install -e ".[dev]"
```

### Testing

Run tests with pytest:
```bash
pytest
```

Key test: `tests/test_types_and_rowgroup.py` validates OID→Arrow mapping and row-group sizing.

### Database Configuration

**Preferred approach**: PostgreSQL service files (`~/.pg_service.conf`):
- Centralizes credentials outside codebase
- Works with all Postgres tools
- Use `--service <name>` or `PGSERVICE=<name>` environment variable

See `examples/pg_service.conf` for template. Never commit credentials.

## Critical Patterns & Conventions

### Adding New Postgres Types

Extend `PGOID_TO_ARROW` in `ddpp/export/postgres.py`:

```python
PGOID_TO_ARROW = {
    # ... existing mappings
    1082: pa.date32(),      # date
    # Add your OID here:
    # 123: pa.your_type(),  # your_typename
}
```

Find OIDs via `SELECT oid, typname FROM pg_type WHERE typname = 'yourtype';`

### Batch Configuration Files

Both YAML and JSON formats supported. Each export spec must have `sql` and `out` keys:

```yaml
- sql: "SELECT * FROM table1"
  out: "table1.parquet"
  row_group_size: 500000  # Optional per-export override
```

All exports in a config file run in **a single transaction** for consistency.

### CLI Entry Points

Defined in `pyproject.toml`:
- `fast-export` → `ddpp.export:main` (Postgres exports)
- `extract-catalog` → `ddpp.export.butler:main` (Butler datasets)

### Error Handling for Butler Filtering

`extract-catalog` strictly validates filter IDs must be int64-convertible. If conversion fails, the tool exits immediately with a descriptive error. This prevents silent data loss from ID mismatches.

## File Organization

- `ddpp/export/postgres.py`: Core Postgres→Parquet logic, type mapping, CLI for fast-export
- `ddpp/export/butler.py`: Butler dataset extraction, requires Science Pipelines environment
- `ddpp/export/__init__.py`: Public API exports for postgres module
- `examples/`: Sample configs (exports.yaml, pg_service.conf) and validation scripts
- `tests/`: Unit tests for type mapping and Parquet structure validation

## Common Operations

### Export with custom row groups
```bash
fast-export --sql "SELECT * FROM large_table" --out output.parquet --row-group-size 2000000
```

### Batch export with service file
```bash
fast-export --config exports.yaml --service mpc_sbn
```

### Extract Butler datasets with filtering
```bash
extract-catalog output.parquet /repo/main COLLECTION_NAME \
  --filter-ids=filter.parquet \
  --filter-column=id_column \
  --target-column=diaSourceId
```

### Debug mode (keep CSV)
```bash
fast-export --sql "SELECT * FROM table" --out output.parquet --keep-temp
```

## Environment Considerations

- **Butler commands**: Only work in LSST Science Pipelines environment (not standard Python)
- **Database access**: Examples reference `mpc-usdf.sp.mjuric.org` (internal LSST service)
- **Temp files**: CSV intermediaries written to system temp dir, cleaned up unless `--keep-temp` specified

## When Making Changes

1. **Adding Postgres types**: Update `PGOID_TO_ARROW` and add test case to `test_types_and_rowgroup.py`
2. **Modifying export logic**: Ensure memory-bounded streaming is preserved (no full-table loads)
3. **Butler changes**: Remember the Science Pipelines dependency constraint
4. **CLI flags**: Both postgres and butler modules have independent argparse configurations
5. **Transaction behavior**: Batch mode semantics (single transaction) are critical for consistency guarantees
6. **Dependencies**: When updating `requirements.txt` or `pyproject.toml` dependencies, also update `environment.yml` to keep conda environment in sync
