# IERS-A: the Earth-orientation file as one of the day's inputs (ssp-tools)

Status: implemented on `iers-a-input` (2026-11-17).

## Context

The approved design is ssp-daily's `docs/design/iers-a.md` (approved
2026-11-17): its context, owner decisions, store, checks and the ssp-daily
side are there. This note records the ssp-tools half, its sections 1 and 2
("`ssp-extract-sso-inputs --iers-a-store DIR`" and "`ssp-build-sso` uses
exactly that file"), and how they were done.

In short: on 2026-11-16 the build failed because astropy's `IERS_Auto`
needed a fresh `finals2000A.all` and could not download it. astropy never
reuses an earlier download, and once its bundled table is over 30 days old
every run depends on a live download. Now the extract downloads the file as
one of the day's inputs, keeps the last good one in a store, and the build
uses exactly the file in the inputs, never the bundled table or a download.

## Owner decisions (from ssp-daily's design)

| Date | Decision |
|---|---|
| 2026-11-17 | Downloading `finals2000A.all` is a step of the extract; the file is one of the day's inputs, recorded in `manifest.json`. |
| 2026-11-17 | The build always uses exactly that file. A manifest without `iers_a` fails the build. |
| 2026-11-17 | A failed download reuses the store's copy, with a warning every time, stronger past 30 days; past 90 days the run fails. The build accepts files up to 90 days old. |

## Outputs and inputs

The contract is `ssp/delivery_contract.py` (`IERS_A_*`); ssp-daily reads
these fields with strict type checks.

`INPUTS_DIR/finals2000A.all`, and `manifest.json` → `files.iers_a`:

| Field | Type | Meaning |
|---|---|---|
| `file` | string | `finals2000A.all` |
| `md5` | string | its md5 |
| `rows` | int | lines of the file |
| `source` | string | the URL, or `reused <store path> (download failed: <url>: <error>; <url>: <error>)` |
| `extracted_utc` | string | when downloaded, or copied from the store (`YYYY-MM-DDTHH:MM:SSZ`, as the other entries) |
| `predictions_start` | string | ISO date of the first predicted row (astropy's `meta["predictive_mjd"]`) |
| `age_days` | number | `extracted_utc` minus 00:00 UTC of `predictions_start`, in days: a **float rounded to 0.01** |
| `reused` | bool | `false` for a fresh download, `true` for the store's copy |

`iers_a` is a key of `files` but not one of `INPUT_FILES` (which are
Parquet). The store is `DIR/finals2000A.all`.

`RUN_DIR/report.json` → `iers_a`: `{file (the absolute path used), md5,
predictions_start, age_days, reused}` (`IERS_A_REPORT_FIELDS`), next to
`inputs` (the whole manifest) and `input_paths.iers_a`.

## Approach

### The extract (`ssp.iers.acquire`, called from `ssp.sso_inputs`)

- **When:** with `--iers-a-store DIR`, it is the first thing run, after the
  cheap offline checks of the arguments and before the MPC export. It
  **always runs when the flag is given**, whatever `--only`, `--skip` and
  `--reuse` say: it is cheap, and a rerun should take the newest file.
  Without the flag, a `--force` rerun carries a previous manifest's
  `iers_a` entry over (its md5 checked, like a step not run); inputs without
  one stay without one, as before.
- **Staging:** the download goes into `INPUTS_DIR/.partial/` (the extract's
  staging directory, which is in `inputs/`), and is moved into place with
  the other files at the commit, before the manifest, which is still written
  last. So a later step's failure leaves INPUTS_DIR as it was, the
  extract's existing rule. The store, though, is updated as soon as a
  download is validated: it is the last good download, not tied to a day.
- **Download:** urllib (the environment's proxy settings apply; SDF uses an
  HTTP proxy), socket timeout 60 s and at most 300 s per download, from
  `iers.conf.iers_auto_url`, then `iers.conf.iers_auto_url_mirror` (so
  `astropy.cfg` can change them), straight into a file: never astropy's
  cache, nothing under `$HOME`.
- **Validation:** parses with `IERS_A.read`, has predicted rows, and its
  `predictions_start` is not before the store copy's. A failure counts as a
  failed download; its error goes into `source`.
- **Success:** the store is replaced atomically (a temporary file in DIR,
  fsync, rename, fsync of DIR).
- **Failure:** the store's copy is copied into staging (same way),
  `reused: true`, and a warning on stderr with the errors and `age_days`;
  past 30 days a second, `IERS-A is STALE`, warning with the days left
  before runs fail. No store copy, an unreadable one, or one over 90 days
  old: the extract fails (exit 1) with the store, the copy's age and the
  download errors, before the MPC export. A store that cannot be written
  after a good download also fails the extract.

### The build (`ssp.sso_build`, `ssp.iers.install`)

- **Manifest check:** `files.iers_a` must be there, with the fields and
  types above, the file present with its md5 and line count, readable by
  `IERS_A.read`, its `predictions_start` as recorded, and `age_days` at
  most 90. With an `IERS_A` table installed, astropy's own `auto_max_age`
  check does not apply, so this is where the build enforces the limit. The
  age is the manifest's (at extraction), so rerunning a day reproduces it.
  The md5 is checked again after the builders (the `check` step's
  "inputs changed" check).
- **Workers (the mechanism):** the build steps run as separate subprocesses
  (`python -c "from module import main; ..."`), so a table installed in the
  build process would not reach them. The build passes the file's absolute
  path to every step subprocess in the environment variable
  **`SSP_IERS_A_FILE`** (`IERS_A_ENV`). The step command
  (`sso_build._entry_point`) calls `ssp.iers.install_from_env(required=True)`
  before importing the step's `main`: it sets `iers.conf.auto_download =
  False` and `auto_max_age = 90`, reads the file with `IERS_A.read`, and
  sets it with `iers.earth_orientation_table.set(...)`. A missing variable,
  a missing file or an unreadable one fails the step at once. Every process
  pool in the steps forks (`ssp.util.run_chunks`, NearbySSO's pools), so the
  workers inherit the installed table; a non-fork pool would need
  `install_from_env` in its initializer (there is none today; the `mpc`
  step's forkserver pool uses no Earth orientation). The build process
  itself installs nothing: it uses no Earth orientation, and installing a
  global table in-process would leak into the callers of `build()` (tests).
- Earth orientation enters only through `EarthLocation.get_gcrs_posvel` in
  `ssp.util.observatory_barycentric_posvel` (SSObservation, NearbySSO).
- A time beyond the table's predictions (about a year ahead) raises
  `IERSRangeError`, the outcome the design asks for.

### `ssp-sso-daily`

The by-hand wrapper passes `--iers-a-store` to the extract; its default is
`WORK_DIR/iers-a`, so a run by hand keeps its own store and never updates
ssp-daily's. `--iers-a-store DIR` changes it.

## Validation

Tests (network-free; the download is replaced; small files are cut from
astropy's bundled table by `tests/iers_fixture.py`):

- `tests/test_iers.py`: download success; mirror fallback; a corrupt, an
  older-than-store and a no-predictions download treated as failures; store
  reuse; the 30- and 90-day thresholds (30, 30.01, 89.5, 90 pass; 90.01
  fails); no store copy, and an unreadable one; atomic store replacement (a
  copy failing half way leaves the old copy and no temporary file); the real
  `fetch` going through the environment's proxy; and a step started exactly
  as the build starts it (`_entry_point`, `run_logged`, `SSP_IERS_A_FILE`),
  with the network blocked by a dead proxy and HOME empty, whose parent and
  forked pool workers all use the given table (a copy with UT1-UTC shifted
  0.25 s, so it differs from any real one): `IERS_A`, its `data_path` and
  `predictive_mjd`, `delta_ut1_utc` equal to the file's, downloads off, the
  site's GCRS position equal to the in-process one with that table (and
  ~100 m from the unshifted one), a time past the table raising
  `IERSRangeError`, and nothing written under HOME; without the variable or
  with a missing file the step fails.
- `tests/test_sso_inputs.py`: the step runs first, before the MPC export,
  and the manifest has `files.iers_a`; the types ssp-daily checks (`reused`
  a JSON boolean, `age_days` a number, `source` a string), fresh and reused;
  a failure with no store stops the extract before the MPC export, with no
  manifest; a later step's failure leaves INPUTS_DIR as it was; without
  the flag nothing changes; with `--only` the step still runs, and without
  the flag a `--force` rerun carries the entry over (md5 checked).
- `tests/test_sso_build.py`: the manifest checks (missing entry, md5, rows,
  `age_days` over 90 and exactly 90, types, `predictions_start`, missing or
  unreadable file); the build fails without `iers_a`; every step gets
  `SSP_IERS_A_FILE` and installs it; `report.json`'s `iers_a`. The
  end-to-end builds use the whole bundled file.

## Results (2026-11-17)

Real-data checks, the IERS step alone (`ssp.iers.acquire`, as the extract
calls it) into `/sdf/data/rubin/user/mjuric/scratch/iers-wp/`, with `HOME`
set to an empty directory. The full extract was not run (it reads the MPC
replica and ClickHouse).

1. **Download** (store empty): 9.9 s, from datacenter.iers.org; the store
   filled; `HOME` still empty afterwards. The manifest entry:

   ```json
   {
     "file": "finals2000A.all",
     "md5": "e3422a8e980e661b1c76b32a3fd81395",
     "rows": 20095,
     "source": "https://datacenter.iers.org/data/9/finals2000A.all",
     "extracted_utc": "2026-11-17T12:13:24Z",
     "predictions_start": "2026-11-13",
     "age_days": 4.51,
     "reused": false
   }
   ```

2. **Forced failure** (both URLs set to `.invalid` hosts through
   `XDG_CONFIG_HOME/astropy/astropy.cfg`): 0.3 s; both downloads failed
   (the proxy answered 500), the store's copy was reused with the warning,
   `HOME` still empty:

   ```text
   WARNING: IERS-A download failed (https://nonexistent.invalid/finals2000A.all: URLError: <urlopen error Tunnel connection failed: 500 Internal Server Error>; https://nonexistent-mirror.invalid/finals2000A.all: URLError: ...); reusing the store's copy .../store/finals2000A.all, 4.51 days old (predictions from 2026-11-13)
   ```

   with `"reused": true`, the same md5, and `source` = `reused
   .../store/finals2000A.all (download failed: ...)`.

3. **Observer positions:** `ssp.util.observatory_barycentric_posvel("X05",
   t)` (NearbySSO's and SSObservation's) for five times from 2026-10-01 to
   2026-12-20, with astropy's own current data (`IERS_Auto`, predictions
   from 2026-11-13) and with the downloaded file installed: identical
   (|Δr| = 0 m, |Δv| = 0 m/s at every time). With the bundled table
   (predictions from 2026-09-11) instead, the positions differ by 0.35 to
   1.7 m, so the comparison does see the table.
