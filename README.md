# LSST Solar System Pipeline Support Tools

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

Or with all optional dependencies (includes ASSIST/REBOUND for ephemerides):
```bash
pip install -e ".[all]"
```

With [uv](https://docs.astral.sh/uv/), using the committed lockfile:
```bash
uv sync --extra dev --extra all
uv run pytest
```

### Ephemeris data files

SSSource ephemerides are computed with [ASSIST](https://assist.readthedocs.io),
which needs the JPL DE440 planet file (`linux_p1550p2650.440`) and the ASSIST
asteroid perturber file (`sb441-n16.bsp`). Point these environment variables
at them:

```bash
export SSP_ASSIST_PLANETS=/path/to/linux_p1550p2650.440
export SSP_ASSIST_ASTEROIDS=/path/to/sb441-n16.bsp
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

### Submitted-source Extraction (ClickHouse)

`extract-submitted-sources` is an alternative to `extract-catalog`: it builds
`dia_sources.parquet` for the X05 rows of an MPC `obs_sbn` dump from the
ClickHouse view `ssp.SubmittableSources`, which serves the source catalogs of
every processing Rubin has submitted from (each under a collection label such
as `DP2-DS` or `AP-DS`).

```bash
extract-submitted-sources obs_sbn.parquet dia_sources.parquet
```

Options: `--host`, `--port` (HTTP, default 8123), `--database` (default
`ssp`), `--user`, `--workers N` (concurrent queries, at most 8: the server is
shared) and `--chunk-size N` (ids per query).

Credentials are taken from `SSP_CH_USER`/`SSP_CH_PASSWORD` if set, else from
`~/.chpass`, a pgpass-format file (`host:port:database:user:password`, mode
0600). There is no password flag.

Matching: each `obsSubID` (`LSST-<collection>-<id>`, or a bare `<id>` from
before labels existed) is looked up by id, in its collection or, for bare
ids, in all of them. A candidate is accepted if its PSF or trail centroid is
within 3 mas of the submitted position and its time within 10 ms (band and
magnitude are recorded but never reject); trailed sources submitted as two
endpoints (`...-A`/`...-B`) are matched on the endpoints' midpoint. Among
accepted candidates the winner prefers a non-superseded collection, then a
matching band, then the smallest separation. Rows that cannot be resolved by
id are searched for by position and time, with the same acceptance rule.

Outputs:
- `dia_sources.parquet` – one row per resolved observation (an A/B pair is
  one row): all of the view's columns (`id` renamed to `diaSourceId`,
  `mjd_tai` to `midpointMjdTai`; DiaSource columns the view lacks, such as
  `extendedness`, are null), plus `obsid` (and `obsid_b` for A/B pairs),
  `obssubid`, `match` (`id` or `position`) and the match diagnostics `sep_mas`,
  `dt_ms`, `dmag`, `band_ok`, `n_pass`, `ambiguous`. `(collection, diaSourceId)`
  is unique: when one detection was submitted more than once, the row from
  the earliest submission (by `submission_id`) is kept.
- `dia_sources.duplicates.parquet` – the obs_sbn rows dropped that way, with
  the `kept_obsid` and the source they claimed. They get no SSSource row.
- `dia_sources.unresolved.parquet` – the obs_sbn rows that did not resolve,
  with a `reason` and the closest failing candidate's separation and time
  offset.

`python -m ssp.sssource` links these DiaSources to obs_sbn by `obsid` (instead
of `diaSourceId == obssubid`) and carries `collection` into SSSource. Detections
of undesignated objects (unidentified tracklets) are kept with `ssObjectId` 0,
an empty designation and NaN orbit-derived columns, as are designated objects
with no `mpc_orbits` orbit (but with their `ssObjectId`); neither gets an
SSObject row;
`ssp-build-ssobject` then joins SSSource to DiaSource on
`(collection, diaSourceId)`.

### SSSource Table Construction

`python -m ssp.sssource` builds the SSSource table (one row per DiaSource
associated with a known solar system object). It links DiaSources to MPC
designations through the MPC observations table, then computes per-source
ephemerides with ASSIST from the MPC orbits: predicted positions, on-sky rates
and offsets from the measured positions; heliocentric and topocentric
positions, velocities and ranges at light-emission time; phase angle and
predicted V magnitude. The geometry follows JPL Horizons conventions (see
`ssp/ephem_assist.py`), and `bench/ephem_bench.py` checks it against Horizons.

Inputs, read from `--input-dir` (default `./analysis/inputs`):
- `dia_sources.parquet` – DiaSources (from `extract-catalog` or `extract-submitted-sources`)
- `obs_sbn.parquet` – MPC observations from Rubin (`stn='X05'`)
- `numbered_identifications.parquet`, `current_identifications.parquet` – MPC designation tables
- `mpc_orbits.parquet` – MPC orbits

The MPC tables can be exported with `fast-export --config examples/exports.yaml`.
The ASSIST data files must be configured as described under
[Ephemeris data files](#ephemeris-data-files).

```bash
python -m ssp.sssource                               # all objects -> ./analysis/outputs/sssource.parquet
python -m ssp.sssource --max-objects 10              # quick test on 10 random objects
python -m ssp.sssource --input-dir in/ --output-dir out/
```

Options: `--max-objects N` and `--dia-sample-frac F` subsample the inputs for
testing (`--seed` sets the random seed); `--reraise` re-raises exceptions for
debugging.

An end-to-end run is SSSource followed by SSObject:

```bash
python -m ssp.sssource
ssp-build-ssobject analysis/outputs/sssource.parquet analysis/inputs/dia_sources.parquet \
  analysis/inputs/mpc_orbits.parquet --output analysis/outputs/ssobject.parquet
```

or, with the DiaSources taken from ClickHouse instead of the Butler:

```bash
extract-submitted-sources analysis/inputs/obs_sbn.parquet analysis/inputs/dia_sources.parquet
python -m ssp.sssource
ssp-build-ssobject analysis/outputs/sssource.parquet analysis/inputs/dia_sources.parquet \
  analysis/inputs/mpc_orbits.parquet --output analysis/outputs/ssobject.parquet
```

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

### NumPy Dtype Generation from Felis Schema

`ssp-generate-dtypes` generates pretty-printed NumPy dtype definitions from Felis YAML table schemas. This utility converts table specifications into Python code suitable for defining structured arrays, with automatic type mapping and metadata preservation. The typical use is to regenerate the `ssp/schema.py` file.

Basic usage:
```bash
ssp-generate-dtypes schema.yaml > ssp/schema.py
```

Generate dtypes for specific tables:
```bash
ssp-generate-dtypes schema.yaml SSObject SSSource > some-table-dtypes.py
```

Arguments:
- `schema.yaml` – Felis YAML schema file containing table definitions
- `table_names` – Optional list of table names to process (default: SSObject, SSSource, mpc_orbits, current_identifications, numbered_identifications)

The output includes:
- Generated file header with command provenance
- Import statements
- Pretty-formatted dtype assignments with comments
- Table descriptions and column metadata

Example output:
```python
# ***** GENERATED FILE, DO NOT EDIT BY HAND *****
# generated with ssp-generate-dtypes schema.yaml SSObject
import numpy as np

## table: SSObject
# SSObject table description...
SSObjectDtype = np.dtype([
    ('id', '<i8'),                    # Unique object identifier
    ('ra', '<f8'),                    # Right ascension [deg]
    # ... more fields
])
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
