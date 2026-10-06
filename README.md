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

`extract-catalog` streams LSST Butler dataset tables into a single Parquet file (one row group per dataset, e.g. per visit). This complements `fast-export` for Postgres sources by enabling efficient extraction of Science Pipelines data products. Its output can no longer be the input of `ssp-build-sssource`, which needs the `obs_sbn` linkage `extract-submitted-sources` adds (see below).

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

`extract-submitted-sources` builds the `dia_sources.parquet` that
`ssp-build-sssource` needs (an `extract-catalog` DiaSource file can't feed it
any more: SSSource is built per `obs_sbn` row, from the columns this tool
adds). It makes it for the X05 rows of an MPC `obs_sbn` dump from the
ClickHouse view `ssp.SubmittableSources`, which serves the source catalogs of
every processing Rubin has submitted from (each under a `processing` label such
as `DP2-DS` or `AP-DS`; the view's `processingTable` names the `ssp` table a
row comes from).

```bash
extract-submitted-sources obs_sbn.parquet dia_sources.parquet
```

Options: `--host`, `--port` (HTTP, default 8123), `--database` (default
`ssp`), `--user`, `--workers N` (concurrent queries, at most 8: the server is
shared) and `--chunk-size N` (ids per query).

Credentials are taken from `SSP_CH_USER`/`SSP_CH_PASSWORD` if set, else from
`~/.chpass`, a pgpass-format file (`host:port:database:user:password`, mode
0600). There is no password flag.

Matching: each `obsSubID` (`LSST-<processing>-<id>`, or a bare `<id>` from
before labels existed) is looked up by id, in its processing or, for bare
ids, in all of them. A candidate is accepted if its PSF or trail centroid is
within 3 mas of the submitted position and its time within 10 ms (band and
magnitude are recorded but never reject); trailed sources submitted as two
endpoints (`...-A`/`...-B`) are matched on the endpoints' midpoint. Among
accepted candidates the winner prefers a non-superseded processing, then a
matching band, then the smallest separation. A bare `obsSubID` accepted by
both DP2 processings goes by submission date: `pDP2-DS` (the DP2 prerelease
run the April 2026 submissions were made from) if submitted before
2026-06-04, else `DP2-DS`. Rows that cannot be resolved by
id are searched for by position and time, with the same acceptance rule.

Shutter-motion correction (`docs/design/shutter-timing.md`): each candidate's
corrected exposure midpoint is looked up in the correction table
(`--correction-table DIR`, default
`/sdf/data/rubin/user/mjuric/shutter-timing/corrections`; `none` turns it
off) before matching, and the time test passes within 10 ms of either the
visit time or the corrected time. The output's `midpointMjdTai` is the
corrected time (the visit's is kept as `midpointMjdTaiVisit`), with
`midpointMjdTai_flag` (not corrected: the table omits the visit, or hasn't
built it yet), `midpointMjdTai_flag_degraded`, `obstime_basis` (`visit`,
`corrected`, `both`) and `dt_corrected_ms`. Visits the table hasn't built
are warned about; more than `--max-not-built-visits` (default 20) fail the
extract.

Outputs:
- `dia_sources.parquet` – one row per resolved obs_sbn row (`obsid` is the
  key): all of the view's columns (`id` renamed to `diaSourceId`; DiaSource
  columns SSSource/SSObject need but the view lacks are null), plus the obs_sbn row's `obsid`, `obssubid` and submitted
  tracklet (`submission_id`, `trksub`, and MPC's `trkid`), `primary`, `match`
  (`id` or `position`) and the match diagnostics `sep_mas`, `dt_ms`, `dmag`,
  `band_ok`, `n_pass`, `ambiguous`. A source can be claimed by several rows:
  both endpoints of an A/B pair (matched once, at their midpoint), or
  repeated submissions of one detection. `primary` is true on exactly one row
  per `(processing, diaSourceId)`: the -A endpoint, else the row from the
  earliest submission.
- `dia_sources.unresolved.parquet` – the obs_sbn rows that did not resolve,
  with a `reason` and the closest failing candidate's separation and time
  offset.

`ssp-build-sssource` turns this file into SSSource (below); `ssp-build-ssobject`
then joins SSSource to it on `obsid` and computes every per-object quantity
from the `primary` rows of objects with an orbit only, so no detection is
counted twice.

### SSSource Table Construction

`ssp-build-sssource` (or `python -m ssp.sssource`) builds the SSSource table
of the PPDB (RFC-1188; see `docs/design/sssource-widened.md`): **one row per
row of `dia_sources.parquet`**, i.e. per Rubin observation submitted to and
accepted by the MPC (`obs_sbn`, keyed by `obsid`), measurement-complete, so
that users need no join to DiaSource or Source. Its columns are exactly those
of `ssp.schema_ppdb.SSSourceDtype` (generated from `sdm_schemas`'
`sso_base.yaml`), in six blocks:

1. the `obs_sbn` link: `obsid`, `trksub`, `trkid`, `submission_id`, `status`
   (from `obs_sbn`), `primary` and `matchMethod` (how the measurement was
   found: `obssubid`, `obssubid_trail` or `position`);
2. the identification: `ssObjectId` and `designation`. `ssObjectId` is NULL
   when the observation has no SSObject: unidentified tracklets (status `I`)
   and designated objects missing from `mpc_orbits`. `designation` is set
   whenever the MPC gives one;
3. the measurement's metadata: `measuredOn` (`difference` for DiaSources,
   `science` for Sources), `processing`, `processingTable`, and the view's
   `id`/`parentId` split into `diaSourceId`/`parentDiaSourceId` or
   `sourceId`/`parentSourceId` by `measuredOn` (the other pair is NULL);
4. the measurement: every `ssp.SubmittableSources` column, copied and cast to
   the DiaSource schema's (narrower) types;
5. (none: the view's query helpers `hpix29`, `cx`, `cy`, `cz` are dropped, as
   are the extractor's match diagnostics);
6. the ephemeris and geometry, computed per object with ASSIST from the MPC
   orbits: predicted positions and their error ellipse (`ephRaErr`,
   `ephDecErr`, `ephRa_ephDec_Cov`), on-sky rates and offsets from the
   measured positions; heliocentric and topocentric positions, velocities
   and ranges at light-emission time; phase angle and predicted V magnitude.
   The geometry follows JPL Horizons conventions (see `ssp/ephem_assist.py`),
   and `bench/ephem_bench.py` checks it against Horizons. They are NULL for
   rows without an orbit, except the measured `elongation`, `eclLambda`,
   `eclBeta`, `galLon` and `galLat`. (`diaDistanceRank`,
   `ephOffsetAlongTrack` and `ephOffsetCrossTrack` aren't computed yet: they
   are 0 as before, the last two NULL without an orbit.)

The file is zstd-compressed, with the low-cardinality string columns
dictionary-encoded, sorted by (`ssObjectId`, `midpointMjdTai`, `obsid`), the
NULL-`ssObjectId` rows last. The build fails, rather than writing a
non-conforming file, if a non-null column has a NULL, a narrowing integer
cast overflows, or a string is longer than its column (float64 to float32
rounding is expected). A `dia_sources.parquet` without `matchMethod` (made
before the extractor wrote it) gets it derived from `match` and `obssubid`.

Inputs, read from `--input-dir` (default `./analysis/inputs`):
- `dia_sources.parquet` – the submitted sources, from `extract-submitted-sources`
- `obs_sbn.parquet` – MPC observations from Rubin (`stn='X05'`), the same dump
- `numbered_identifications.parquet`, `current_identifications.parquet` – MPC designation tables
- `mpc_orbits.parquet` – MPC orbits

The MPC tables can be exported with `fast-export --config examples/exports.yaml`.
The ASSIST data files must be configured as described under
[Ephemeris data files](#ephemeris-data-files).

```bash
ssp-build-sssource                                   # all objects -> ./analysis/outputs/sssource.parquet
ssp-build-sssource --max-objects 10                  # quick test on 10 random objects
ssp-build-sssource --input-dir in/ --output-dir out/
```

Options: `--workers N` sets the number of worker processes for the
per-object ephemerides (default: `min(64, CPUs)`; each opens its own ASSIST
ephemeris, ~100 MB). `--workers 1` runs serially, with no process pool. The
output is identical for any `N`. `--max-objects N` and `--dia-sample-frac F`
subsample the inputs for testing (`--seed` sets the random seed); `--reraise`
re-raises exceptions for debugging.

An end-to-end run is extraction, SSSource, then SSObject:

```bash
extract-submitted-sources analysis/inputs/obs_sbn.parquet analysis/inputs/dia_sources.parquet
ssp-build-sssource
ssp-build-ssobject analysis/outputs/sssource.parquet analysis/inputs/mpc_orbits.parquet \
  --output analysis/outputs/ssobject.parquet
```

### SSObject Table Construction

`ssp-build-ssobject` constructs SSObject tables from SSSource and MPC orbit data. This tool processes photometric and orbital data to create comprehensive solar system object catalogs with fitted parameters.

Basic usage:
```bash
ssp-build-ssobject sssource.parquet mpc_orbits.parquet --output ssobject.parquet
```

(The older form, `sssource.parquet dia_sources.parquet mpc_orbits.parquet`, is
still accepted; the DiaSource file is not read.)

Arguments:
- `sssource.parquet` – SSSource Parquet file (from `ssp-build-sssource`; only the columns used are read).
  The photometry is SSSource's own: `band`, and the float32 `psfFlux` and `psfFluxErr`, so SSObject
  can be reproduced from the published SSSource.
- `mpc_orbits.parquet` – MPC orbit Parquet file with orbital elements
- `--output ssobject.parquet` – Output SSObject Parquet file
- `--workers N` – Number of worker processes for the per-object fits and the MOIDs (default: `min(64, CPUs)`).
  `--workers 1` runs serially, with no process pool. The output is identical for any `N`.
- `--hg12MagSigmaFloor` – error floor (mag) added in quadrature to the magnitude errors before the H/G12 fits (default 0.05; 0 for none)
- `--hg12NSigmaClip` – reject points beyond this many sigma of an initial robust fit (default 10; `inf` to keep every point)
- `--hg12FixedG12` – fix G12 to this value and fit only H (default: unset, G12 is fit)
- `--hg12FiducialG12` – the G12 of the fixed-G12 refit where a band's slope fit fails (default: `--hg12FixedG12` if set, else 0.5, as in DP2)
- `--hg12MinPhaseSpan` – a slope fit whose points span less than this phase angle (deg) fails (default 2)

The tool performs:
- Photometric fitting (H/G12 parameters) for each band (ugrizy). By default
  each band's fit follows DP2's robust recipe: a 0.05 mag error floor, a
  robust (`soft_l1`) fit, rejection of points beyond 10σ, and a final
  least-squares fit of the rest. Unlike DP2, which fixed G12 at 0.5 (use
  `--hg12FixedG12 0.5` for that), G12 is fit, bounded to [0, 1].
- **Failed slope fits** (`{band}_slope_fit_failed`: "G12 fit failed in {band}
  band. G12 contains a fiducial value used to fit H."). A band's G12 fit fails
  when any of these holds:
  1. the free G12 ends at a bound, within 1e-5 of 0 or 1;
  2. the fit isn't invertible (JᵀJ singular): no finite result, or no
     finite `G12Err` with G12 inside (0, 1);
  3. fewer than 3 points are usable, or used after clipping (a band with
     fewer than 3 observations gets no free fit);
  4. the points it uses span less than `--hg12MinPhaseSpan` (2°) in phase
     angle.

  H is then refit with G12 fixed at the fiducial value (`--hg12FiducialG12`,
  0.5), with the same error floor and clipping, which, as for the free fit,
  applies only with more than 3 usable points. G12 holds that value,
  `G12Err` and the H–G12 covariance are NULL, and `HErr`, `nObsUsed` and
  `Chi2` are the fixed-G12 fit's. If clipping leaves a single point, H and
  `HErr` come from that point (`nObsUsed` 1, `Chi2` NULL). Only if no point is
  usable (every flux ≤ 0) or none survives clipping are H, `HErr` and G12
  NULL (`nObsUsed` 0); the flag is set in every case. (As elsewhere in
  SSObject, NULL is stored as NaN in the Parquet file.) With
  `--hg12FixedG12`, G12 isn't fit, and the flag means the fixed fit failed.
  Every band with at least one observation is fit (one point gives H at the
  fiducial G12). The fits don't depend on the order of
  the rows: each takes its observations in a canonical order (by phase angle,
  magnitude, error, distances), so any permutation gives bitwise identical
  results.
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
