# LSST Daily Data Products Pipeline

Efficient streaming export of large Postgres tables to Parquet format using PyArrow.

## Overview

`ssp` provides tools for exporting arbitrary Postgres tables to columnar Parquet files with:
- **Memory efficiency**: Streaming batches, bounded memory footprint
- **Type fidelity**: Automatic OID→Arrow type mapping
- **High throughput**: Postgres COPY → temp CSV → Arrow streaming → zstd-compressed Parquet

## Installation

### Option 1: Conda Environment (Recommended)

Create and activate the development environment:

```bash
conda env create -f environment.yml
conda activate ssp-dev
```

This installs all dependencies including dev tools (pytest, ipython).

### Option 2: Pip Install

```bash
pip install -e .
```

Or with dev dependencies:
```bash
pip install -e ".[dev]"
```

Or with all optional dependencies (includes jorbit, requires JAX):
```bash
pip install -e ".[all]"
```

## Configuration

### Database Connection

There are multiple ways to configure database connections:

#### Option 1: PostgreSQL Service File (Recommended)

Use a `pg_service.conf` file to define named connection profiles:

1. Copy the example service file:
   ```bash
   cp examples/pg_service.conf ~/.pg_service.conf
   chmod 600 ~/.pg_service.conf
   ```

2. Edit `~/.pg_service.conf` to add your database credentials:
   ```ini
   [mpc_sbn]
   host=mpc-usdf.sp.mjuric.org
   port=5432
   dbname=mpc_sbn
   user=rubin
   ```

3. Store your password in `~/.pgpass`:
   ```bash
   echo "mpc-usdf.sp.mjuric.org:5432:mpc_sbn:rubin:your_password" >> ~/.pgpass
   chmod 600 ~/.pgpass
   ```

4. Use the service name with fast-export:
   ```bash
   fast-export --service mpc_sbn --sql "SELECT * FROM table" --out output.parquet
   ```

   Or set the `PGSERVICE` environment variable:
   ```bash
   export PGSERVICE=mpc_sbn
   fast-export --sql "SELECT * FROM table" --out output.parquet
   ```

**Benefits**: Centralized configuration, no credentials in scripts, works with all PostgreSQL tools.

#### Option 2: Environment Variables

Set connection parameters via environment variables:

```bash
export PGHOST=your.postgres.host
export PGPORT=5432
export PGDATABASE=your_database
export PGUSER=your_user
export PGPASSWORD=your_password  # or use ~/.pgpass
```

#### Option 3: CLI Flags

Use CLI flags (`--host`, `--port`, `--dbname`, `--user`, `--password`) or provide a full DSN string with `--dsn`.

### Basic Usage

#### Single Table Export

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

#### Batch Export (Multiple Tables in Single Transaction)

For exporting multiple tables consistently, use a YAML or JSON config file:

**examples/exports.yaml:**
```yaml
- sql: "SELECT * FROM current_identifications"
  out: "current_identifications.parquet"

- sql: "SELECT * FROM mpc_orbits"
  out: "mpc_orbits.parquet"
  row_group_size: 500000  # Optional: override default per export

- sql: "SELECT * FROM obs_sbn WHERE stn='X05'"
  out: "obs_sbn.parquet"
```

Then run:
```bash
fast-export --config examples/exports.yaml --host your.host --dbname your_db --user your_user
```

**Key benefits of batch mode:**
- All exports execute within a **single database transaction** (REPEATABLE READ isolation)
- Ensures consistent snapshot across all tables
- Reduces database connection overhead
- Simplifies operational workflows

**JSON format is also supported:**
```json
[
  {"sql": "SELECT * FROM table1", "out": "table1.parquet"},
  {"sql": "SELECT * FROM table2", "out": "table2.parquet"}
]
```

### Butler Catalog Extraction

`extract-catalog` streams LSST Butler dataset tables into a single Parquet file (one row group per dataset, e.g. per visit). This complements `fast-export` for Postgres sources by enabling efficient extraction of Science Pipelines data products.

Basic invocation (shows a progress bar by default):
```bash
extract-catalog output.parquet /repo/main SOME/COLLECTION/NAME
```

Positional arguments:
- `output.parquet` – destination Parquet file (created/overwritten)
- `/repo/main` – Butler repository root
- `SOME/COLLECTION/NAME` – collection (e.g. `LSSTCam/runs/DRP/FL/w_2025_19/DM-50795`)

Key options:
- `--dataset-type dia_source_visit` (default) dataset type to stream
- `--filter-ids ids.parquet` Parquet file whose first (or specified) column contains int64‑convertible IDs used to filter rows
- `--filter-column obssubid` Column name inside the filter Parquet (if omitted, first column is used)
- `--target-column diaSourceId` Column in each Butler table matched against the filter IDs (default `diaSourceId`)
- `--compression zstd` Parquet compression codec (default `zstd`)
- `--silent` Disable the progress bar

Filter file requirement: all IDs must be convertible to int64 or the tool exits with an error.

Example: extract DIA sources limited to IDs listed in `obs_sbn.parquet`:
```bash
extract-catalog dia_sources.parquet /repo/main \
  LSSTCam/runs/DRP/FL/w_2025_19/DM-50795 \
  --filter-ids=obs_sbn.parquet \
  --filter-column=obssubid
```

Silent (no progress bar):
```bash
extract-catalog dia_sources.parquet /repo/main LSSTCam/runs/DRP/FL/w_2025_19/DM-50795 \
  --filter-ids=obs_sbn.parquet --filter-column=obssubid --silent
```

The resulting Parquet file is optimized for downstream columnar analytics (Arrow / DuckDB / Spark) and predicate pushdown.

### SSObject Table Construction

`ssp-build-ssobject` constructs SSObject tables from SSSource, DiaSource, and MPC orbit data. This tool processes photometric and orbital data to create comprehensive solar system object catalogs with fitted parameters.

Basic usage:
```bash
ssp-build-ssobject sssource.parquet dia_sources.parquet mpc_orbits.parquet --output ssobject.parquet
```

Arguments:
- `sssource.parquet` – SSSource Parquet file containing solar system source detections
- `dia_sources.parquet` – DiaSource Parquet file with photometric measurements
- `mpc_orbits.parquet` – MPC orbit Parquet file with orbital elements
- `--output ssobject.parquet` – Output SSObject Parquet file

The tool performs:
- Photometric fitting (H/G12 parameters) for each band (ugrizy)
- Orbital analysis including Tisserand parameter and MOID calculations
- Quality metrics and observation statistics per object

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

Extend `PGOID_TO_ARROW` in `ssp/export/postgres.py` for additional types.

## Development

Run tests:
```bash
pytest
```

## Security Note

**Never commit database credentials to version control.** Always use environment variables or external configuration files (e.g., `.env`, `.pgpass`).

## License

MIT
