# Design: the widened SSSource table (RFC-1188)

Status: **implemented and validated** (2026-10-01) on the full 2026-09-30 fixture; see "Results". The schema change is committed locally on `sdm_schemas` `tickets/DM-55375` (a8615ae), not yet pushed.

## Context

[RFC-1188](https://rubinobs.atlassian.net/browse/RFC-1188) decouples the PPDB Solar System tables from PPDB `DiaSource`:

- `SSSource` and `SSObject` are built from **the persistent record of the measurements submitted to the MPC**, not from `DiaSource`.
- `SSSource` becomes **measurement-complete**: it carries the astrometric and photometric columns that users would otherwise get by joining to `DiaSource`.
- It is **bulk-replaced daily**, with no joins or queries against the APDB or PPDB.
- The schema ticket is DM-55375 ("Apply RFC-1188 changes to `sso_base.yaml`"). Ingestion is DM-55678 (Parquet to GCS).

In this repo, that record is already what `extract-submitted-sources` reads:
- `ssp.SubmittableSources`, a ClickHouse view over every DiaSource **and** Source table we submit from: DP2-DS, pDP2-DS, AP-DS, prompt, DP1, NV, daytime, and so on;
- joined to the X05 rows of the MPC's `obs_sbn`.

`extract-submitted-sources` writes `dia_sources.parquet`, one row per resolved `obs_sbn` row. `python -m ssp.sssource` then adds the ephemeris and geometry columns. It has no console script yet; this work adds `ssp-build-sssource`.

**The widened SSSource is those two put together:** one table, one row per `obs_sbn` row, with the measurement columns of `SubmittableSources` and the ephemeris columns of today's SSSource.

The 2026-09-25 X05 `obs_sbn` dump has 8,069,956 rows: status `p` 7,734,209, `P` 271,888 and `I` 63,859. They include rows submitted from Source tables (e.g. NV), not only DiaSources.

## Owner decisions (2026-09-30)

| question | decision |
|---|---|
| Row granularity | **One row per `obs_sbn` row**, as today. A source submitted more than once has several rows, one flagged `primary`. |
| Primary key | **`obsid`** (from `obs_sbn`). `diaSourceId` is no longer unique, because the measurement part now mixes DiaSources and Sources. |
| Measurement columns | **Every `SubmittableSources` column**, which supersedes the DiaSource part of the RFC's draft schema. The exceptions are the four query helpers, `hpix29`, `cx`, `cy` and `cz`, which are dropped. |
| Identifier columns | The view's `id` and `parentId` are **split into `diaSourceId`/`sourceId` and `parentDiaSourceId`/`parentSourceId`**. The pair matching `measuredOn` is filled, and the other pair is NULL. |
| What kind of measurement a row is | `measuredOn` (`difference` or `science`), with `processing` and `processingTable` for provenance. |
| `timeProcessedMjdTai` | Dropped from the schema. The view doesn't have it, and most of the tables behind the view don't either. |
| Types | DiaSource's (narrower) Felis types where a column exists in DiaSource, not the view's widened ones. The `ap03`/`ap06`/`ap25` fluxes and errors are **float**, following `apFlux`. Every `*Mag`/`*MagErr` is float. |
| Nullability | Non-null where every branch of the view has a value: `obsid`, `visit`, `midpointMjdTai`, `ra`, `dec` and the others confirmed from the data. |
| `obs_sbn` columns kept | `obsid`, `trksub`, `trkid`, `submission_id`, `status`, and our `primary` flag. |
| Match diagnostic | One categorical column, `matchMethod`. The numeric diagnostics stay in `dia_sources.parquet`. |
| Unidentified rows (Isolated Tracklet File, status `I`) | Kept, as today. |
| `ssObjectId` | **NULL whenever there is no match:** status `I` rows, and designated objects missing from `mpc_orbits` (#7). Then `ssObjectId` is non-NULL exactly when an SSObject row exists. `designation` is filled whenever the MPC gives one. |
| Ephemeris uncertainty | Add `ephRaErr`, `ephDecErr` and `ephRa_ephDec_Cov`, the same as in NearbySSO, placed next to `ephRa`/`ephDec`. |
| Schema branch | **`tickets/DM-55375`, from current `sdm_schemas` main.** The December draft on `u/mjuric/ppdb-sso-ng` (PR #441, `tickets/DM-53677`) predates main's `columnRefs` layout, so it isn't used. |

## Output: `sssource.parquet`

**181 columns in six blocks**, ordered so that related columns sit together:

| block | n | columns |
|---|---|---|
| 1. `obs_sbn` | 7 | `obsid`, `trksub`, `trkid`, `submission_id`, `status`, `primary`, `matchMethod` |
| 2. identification | 2 | `ssObjectId`, `designation` |
| 3. measurement metadata | 7 | `measuredOn`, `processing`, `processingTable`, `diaSourceId`, `sourceId`, `parentDiaSourceId`, `parentSourceId` |
| 4. measurement (DiaSource schema order) | 126 | `visit`, `detector`, `midpointMjdTai`, `exposureTime`, `ra`, `raErr`, `dec`, `decErr`, `ra_dec_Cov`, `x`, `xErr`, `y`, `yErr`, `centroid_flag`, `apFlux`, `apFluxErr`, `apMag`, `apMagErr`, `apFlux_flag`, `apFlux_flag_apertureTruncated`, `ap03Flux`, `ap03FluxErr`, `ap03Mag`, `ap03MagErr`, `ap03Flux_flag`, `ap03Flux_flag_apertureTruncated`, the same six for `ap06` and `ap25`, `isNegative`, `snr`, `psfFlux`, `psfFluxErr`, `psfMag`, `psfMagErr`, `psfLnL`, `psfChi2`, `psfNdata`, `psfFlux_flag`, `psfFlux_flag_edge`, `psfFlux_flag_noGoodPixels`, `trailFlux`, `trailFluxErr`, `trailMag`, `trailMagErr`, `trailRa`, `trailRaErr`, `trailDec`, `trailDecErr`, `trailLength`, `trailLengthErr`, `trailAngle`, `trailAngleErr`, `trailChi2`, `trailNdata`, `trailAlgorithm`, `trail_flag_edge`, `trail_flag`, `dipoleMeanFlux`, `dipoleMeanFluxErr`, `dipoleMeanMag`, `dipoleMeanMagErr`, `dipoleFluxDiff`, `dipoleFluxDiffErr`, `dipoleLength`, `dipoleAngle`, `dipoleChi2`, `dipoleNdata`, `scienceFlux`, `scienceFluxErr`, `scienceMag`, `scienceMagErr`, `forced_PsfFlux_flag`, `forced_PsfFlux_flag_edge`, `forced_PsfFlux_flag_noGoodPixels`, `templateFlux`, `templateFluxErr`, `templateMag`, `templateMagErr`, `ixx`, `iyy`, `ixy`, `ixxPSF`, `iyyPSF`, `ixyPSF`, `shape_flag`, `shape_flag_no_pixels`, `shape_flag_not_contained`, `shape_flag_parent_source`, `extendedness`, `reliability`, `reliabilityVersion`, `band`, `isDipole`, `dipoleFitAttempted`, `bboxSize`, the 21 `pixelFlags*`, `glint_trail` |
| 5. Source-only | 0 | none: the Source-only aperture columns sit in block 4, and the four query helpers are dropped |
| 6. ephemeris and geometry | 39 | `eclLambda`, `eclBeta`, `galLon`, `galLat`, `elongation`, `phaseAngle`, `topoRange`, `topoRangeRate`, `helioRange`, `helioRangeRate`, `ephRa`, **`ephRaErr`**, `ephDec`, **`ephDecErr`**, **`ephRa_ephDec_Cov`**, `ephVmag`, `ephRate`, `ephRateRa`, `ephRateDec`, `ephOffset`, `ephOffsetRa`, `ephOffsetDec`, `ephOffsetAlongTrack`, `ephOffsetCrossTrack`, the 7 `helio_*`, the 7 `topo_*`, `diaDistanceRank` |

Each `*Mag`/`*MagErr` follows its own `*FluxErr`. The Source-only `ap03`/`ap06`/`ap25` blocks follow DiaSource's 12-px `apFlux` block.

**Types, block by block:**
- **Block 4:**
  - DiaSource's type for every column `apdb.yaml`'s DiaSource has (on `sdm_schemas` main: it includes `exposureTime`, `trailAlgorithm`, `trail_flag` and `reliabilityVersion`);
  - float for the fluxes, errors and magnitudes DiaSource lacks;
  - boolean for their flags.
- **Blocks 1–3:**
  - `obsid`, `trksub`, `trkid`, `submission_id` and `status`: `char`, as in `obs_sbn`;
  - `primary`: boolean;
  - `matchMethod`: `char`, with a small fixed set of values;
  - the ids: long;
  - `measuredOn`, `processing` and `processingTable`: `char`.
- **Block 6:** as in today's SSSource; the three ellipse columns are float, as in NearbySSO.

**Writing the file:**
- One Parquet file, zstd. The low-cardinality strings (`status`, `matchMethod`, `measuredOn`, `processing`, `processingTable`, `band`, `reliabilityVersion`) are dictionary-encoded.
- Rows are sorted by (`ssObjectId`, `midpointMjdTai`), with the unidentified rows last.
- The NumPy dtype comes from the Felis schema (`ssp/schema_ppdb.py`, generated). The writer **fails** if a non-null column has a null, or if a narrowing cast overflows. Narrowing float64 to float32 is expected and not an error.

**`matchMethod` values:** how `extract-submitted-sources` found the measurement each `obs_sbn` row was submitted from.
- `obssubid`: the `obsSubID` (`LSST-<processing>-<id>`, or a bare id) parsed, looked up in the view, and verified by position and time.
- `obssubid_trail`: a trailed source submitted as two `obs_sbn` rows, `…-A` and `…-B`, one per trail end. They are paired, and their averaged position and time are verified against the single measurement. Both rows point at it; the `-A` row is `primary`.
- `position`: the position + time search, for rows whose `obsSubID` doesn't parse or verify, including an `-A`/`-B` row without its partner.

Today the extractor's `match` column has only `id`/`position`.

The extractor's other diagnostics (`sep_mas`, `dt_ms`, `dmag`, `band_ok`, `n_pass`, `ambiguous`) stay in `dia_sources.parquet` and the extractor's report.

## Inputs

All regenerated daily:
- `dia_sources.parquet` from `extract-submitted-sources`, with `ssp.SubmittableSources` from ClickHouse on sdfiana035 as `ssp_xmatch`;
- `obs_sbn` (X05), `mpc_orbits`, `current_identifications` and `numbered_identifications` from the MPC replica at USDF (`mpcorb-db.slac.stanford.edu`);
- the ASSIST ephemeris files.

## How it is built

The pipeline is `extract-submitted-sources` → **`ssp-build-sssource`** (new console script, replacing `python -m ssp.sssource`) → `ssp-build-ssobject`, with these changes:

1. **Extractor:** emits `matchMethod`. Nothing else changes: it already writes every view column, renaming `id` to `diaSourceId`, plus `obsid`, `primary`, `submission_id`, `trksub` and `trkid`.
2. **SSSource:** writes the widened table.
   - **Measurement columns:** block 4 copied from `dia_sources.parquet`, cast to the schema types. The id split by `measuredOn` comes from the extractor's `diaSourceId` (the view's `id`) and the view's `parentId`.
   - **Link columns:** `status` from `obs_sbn`, which `ssp.sssource` already reads.
   - **`ssObjectId`:** NULL when unmatched (today it is 0).
   - **Ephemeris:** computed exactly as today, through `compute_ephemerides_one`, in parallel per object.
   - **Ellipse:** new. Per object, `ssp.nearbysso.propagate.coarse` at that object's observation times, then `ellipse_at(track, t, topo_pos)` along the precise pass's line of sight. This is the NearbySSO machinery, already reviewed and validated against Horizons.
     - The orbit's covariance comes from `ssp.nearbysso.orbits.load_orbits(with_filter=False)`, restricted to the objects present. The filter is off because SSSource keeps comets and short arcs.
     - The ellipse is NULL where there is no usable covariance.
   - Coarse costs about 3–5 ms per object here, a few seconds at 64 workers for about 300k objects. The ASSIST self-perturber handling (#32) applies to both passes.
3. **SSObject:** reads only the columns it uses. It treats a NULL `ssObjectId` as unmatched, where today it uses 0, and counts only `primary` rows, as today.

## Schema work (`sdm_schemas`, branch `tickets/DM-55375`)

The local branch is created from main 016e612.
- **`sso_base.yaml`:** rewrite `SSSource` as above, with the column definitions written out, not as `columnRefs`.
  - Descriptions come from the `SubmittableSources` column comments, which already say what each column means on DiaSource and on Source rows.
  - Units and UCDs come from DiaSource or Source.
  - The primary key is `obsid`.
  - The SSSource → DiaSource foreign key is removed. The `designation` → `mpc_orbits` foreign key stays, and `ssObjectId` → `SSObject` stays, now nullable.
  - Add the `NearbySSO` table, with its three ellipse columns after `ephRa`/`ephDec`.
- **`ppdb.yaml`:** update the `columnRefs` for SSSource and add NearbySSO.
- Version bump and news fragment per the repo's CONTRIBUTING guide.
- **Committed locally; pushed and a PR opened only when the owner says.**

## Validation

1. **Schema conformance:** a test reads `sso_base.yaml` directly, independently of the generated dtype, and checks the Parquet file against it: names, order, types, nullability, and `obsid` uniqueness.
2. **Copied values:** for a random sample of about 10k rows across every `processing` value, re-fetch the view row from ClickHouse by (`processing`, `id`) and compare every block-4 column.
   - Same type: exact.
   - Narrowed float64 → float32: equal at float32.
   - Ids: in the right column for their `measuredOn`.
3. **Regression:** on the same inputs, every ephemeris column of the widened table is bitwise equal to today's SSSource.
4. **The ellipse against NearbySSO:** for the AP-DS rows present in both (PPDB `DiaSource` = AP-DS), the two tables' ellipses must agree, since they use the same machinery and orbits.
5. **End to end** on a fresh `obs_sbn` dump: extract → SSSource → SSObject. Row counts against `obs_sbn`, the `I` rows and the #7 rows reported, timings and memory recorded here.

## Results (2026-10-01)

**The integrated chain on the full fixture.** `extract-submitted-sources` → `ssp-build-sssource` → `ssp-build-ssobject`, using the `obs_sbn` X05 dump of 2026-10-01 06:25 UTC with same-transaction MPC tables, at `sssource-widened` f02875f and 32 workers on sdfiana031. The runbook is `/sdf/data/rubin/user/mjuric/sssource-widened/integration/RUNBOOK.md`.

| stage | wall | peak RSS | output |
|---|---|---|---|
| `extract-submitted-sources` (8 ClickHouse queries at a time) | 27:00 (12:03 on an idle server; sdfiana035 was at load ~90) | 27 GB | `dia_sources.parquet`, 8,070,610 rows |
| `ssp-build-sssource` | 3:10 | 13.4 GB | `sssource.parquet`, 8,070,610 rows × 181 columns, 3.06 GB |
| `ssp-build-ssobject` | 1:21 | 5.7 GB | `ssobject.parquet`, 297,762 objects |

Today's SSSource build on the same inputs takes 1:42 and peaks at 15.6 GB. Most of the difference is the error ellipse:
- the ephemerides plus ellipse take 80 s, against about 45 s without the ellipse;
- assembling and writing the 181 columns takes about 55 s.

**Validation** (`bench/sssource_validate`): every check passes.

| check | result |
|---|---|
| `conformance` (19 checks) | schema names, order, types and nullability; `obsid` unique; sort order; id split; `ssObjectId` rules; ellipse sanity; one `primary` row per measurement (22 non-primary rows); zstd; dictionary columns |
| `copied` (5) | blocks 1, 3 and 4 equal `dia_sources.parquet` (126 block-4 columns × 8,070,610 rows; float64 → float32 after the cast) |
| `clickhouse` (6) | 10,000 rows stratified by `processing`, re-fetched from `ssp.SubmittableSources`: block 3, block 4 and the id split all equal |
| `regression` (8) | all 36 ephemeris and geometry columns bitwise equal to today's SSSource; `ssObjectId` NULL on exactly the 63,850 status-`I` rows and the 18 #7 rows |
| `counts` (4) | rows equal the 8,070,610 resolved X05 `obs_sbn` rows; `status` agrees |
| `ellipse` (3) | against NearbySSO (PPDB build, 2026-09-28 orbits), 214,882 AP-DS rows of 787 objects with identical orbits: \|ΔraErr\|/raErr median 1.7e-6, p99 4.8e-4, max 2.5e-3; \|Δρ\| max 1.9e-3. The residual is NearbySSO's interpolation between its samples; SSSource is evaluated at each exact time. |

Further results:
- **SSObject** from the widened SSSource is **byte-identical** to today's SSObject (md5 `645ccf34…`).
- The error ellipse is NULL on 71 of the 8,006,742 rows with an orbit, all of one object whose covariance is not positive semi-definite. No propagation hit the step cap.
- Rows by `matchMethod`: `obssubid` 8,070,562, `obssubid_trail` 40, `position` 8.

## Known limitations and open owner decisions

1. **The linear error ellipse understates the uncertainty of poorly constrained short arcs observed far from their epoch.**
   - The WP3 review's Monte Carlo of the full chain gave spreads 30–700× the linear ellipse in such cases. For example, 2025 NT344 observed about 300 days before its epoch: 58″ linear, against about 10° in the Monte Carlo.
   - NearbySSO has the same limitation, but its σ ≤ 10″ cut excludes those cases. SSSource publishes the ellipse as is.
2. **SSObject row-order dependence: resolved (2026-10-01; "Follow-ups").** The fits now take their observations in a canonical order, so SSObject is a function of the set of observations; it reads its photometry from SSSource, so it is reproducible from the published SSSource.
3. **Placeholder columns: resolved (2026-10-01).** `ephOffsetAlongTrack`/`CrossTrack` are computed; `diaDistanceRank` moved to NearbySSO, where it is computed.
4. **The Butler input path to `ssp.sssource` is gone.** The widened table needs the `obs_sbn` linkage, which only `extract-submitted-sources` output carries.
5. **Pushing the schema.** `tickets/DM-55375` (sso_base.yaml and ppdb.yaml) is committed locally and awaits the owner's go-ahead to push and open the `sdm_schemas` PR. The version is left at 10.0.0 (see "Phase 0 results").

## Implementation plan

The same pattern as NearbySSO: parallel subagents, each in its own worktree; the integrating session owns the contract, reviews, integrates and merges; the more complex packages also get an **independent review** by a fresh agent that tries to break them. Each work package merges through its own PR as a merge commit, after owner approval (or blanket approval).

### Phase 0: contract and fixtures (integrating session)

1. **Schema:** the `sso_base.yaml` and `ppdb.yaml` changes on `tickets/DM-55375`, as above, committed locally.
2. **The contract: `ssp/schema_ppdb.py`,** the generated widened-SSSource and NearbySSO dtypes. They are generated with the existing `ssp-generate-dtypes` from `sso_base.yaml`, which defines its columns directly, so no `columnRefs` support is needed.
   - It also holds a small hand-written `SSSOURCE_NONNULL` set, and the `matchMethod` values.
   - Agents may not change it.
3. **Fixtures** under `/sdf/data/rubin/user/mjuric/sssource-widened/fixtures/` (read-only; `/lscratch` is nearly full):
   - a fresh X05 `obs_sbn` export and same-day MPC tables;
   - a full `dia_sources.parquet` made from them with current master;
   - a small subset of a few hundred objects, covering every `processing` value, `-A`/`-B` pairs, position-fallback rows, `I` rows and the #7 rows;
   - today's SSSource on that subset, the reference for the regression check.

### Phase 0 results (2026-10-01)

- **Schema:** committed as `a8615ae` on `sdm_schemas` `tickets/DM-55375` (local only, not pushed). It covers `sso_base.yaml` (SSSource rewritten, 181 columns; NearbySSO added, 12 columns), `ppdb.yaml` (their `columnRefs`), and news fragments `DM-55375.ap.md` and `DM-55375.ssp.md`.
  - `felis validate` passes on both files, and `sdm_schemas`' tests pass.
  - The version stays 10.0.0: DM-55374 kept `ppdb.yaml` and `sso_base.yaml` matching the APDB version. This is to raise in the PR.
  - The table definitions come from `gen_sso_yaml.py` and `apply_schema.py` in `/sdf/data/rubin/user/mjuric/sssource-widened/work/`, driven by the fixture's column statistics (`stats.json`).
- **Nullability:** a column is non-null when every branch of the view populates it **by construction**, not merely in today's data. The non-null columns are `visit`, `detector`, `midpointMjdTai`, `ra`, `dec`, `band`, `psfFlux`, `psfFluxErr`, plus `obsid`, `status`, `primary`, `matchMethod`, `measuredOn`, `processing`, `processingTable` and today's non-null `eclLambda`, `eclBeta`, `galLon`, `galLat`.
  - `psfMag`, `exposureTime`, `trksub` and `trkid` have no NULLs today but can be NULL by definition (`psfFlux` ≤ 0, no ConsDB value), so they stay nullable.
- **Contract:**
  - `ssp/schema_ppdb.py`: generated from a copy of that `sso_base.yaml` in `tests/data/sdm_schemas/`;
  - `ssp/sssource_contract.py`: the rules and the WP1/WP2/WP3 interfaces;
  - `tests/test_sssource_contract.py`: checks the two agree with the YAML.
- **Fixtures:** `/sdf/data/rubin/user/mjuric/sssource-widened/fixtures/2026-09-30/`, read-only, with a README.
  - `obs_sbn` X05: 8,070,610 rows, with same-transaction MPC tables.
  - `dia_sources.parquet`: 8,070,610 rows, 12 min to extract; **542,796 of them Source-based**: NV-S, AP-S, DP2-S.
  - Today's SSSource on the full fixture: 1 min 42 s on 32 workers.
  - A 225-object subset covering every processing value, the trail pairs, position matches, the #7 rows, unidentified rows, repeat submissions and comets, with its own reference SSSource, bitwise equal to the full one.

### Phase 1: work packages in parallel

| WP | builds | depends on | independent review |
|---|---|---|---|
| **WP1 Extractor** | `matchMethod` (categorical) in `dia_sources.parquet`. Tests: each category, and the trail pair. | contract | no (small) |
| **WP2 Widened SSSource writer** | The `ssp-build-sssource` console script (`ssp.sssource:main`, registered in `pyproject.toml`; README updated). In `ssp.sssource`: assembling the six blocks; casts to the contract dtypes, with overflow and nullability checks; the id split by `measuredOn`; `status` from `obs_sbn`; `ssObjectId` NULL rules; the Parquet writer (dictionary encoding, sort order). SSObject reads only what it needs and handles a NULL `ssObjectId`. Tests: conformance on the fixture subset; the #7 and `I` rows; Source and DiaSource rows. | contract, fixtures | **yes:** casts and narrowing, null handling, row identity (`obsid`), the id split, SSObject compatibility |
| **WP3 Ephemeris ellipse** | Loading the covariances for the objects present (`load_orbits(with_filter=False)`); `coarse` at each object's observation times plus `ellipse_at(topo_pos)`, inside the existing parallel per-object pass; NULL where there is no covariance. Tests: against `coarse` directly, and against NearbySSO's ellipse for shared rows. | contract | **yes (light):** it reuses reviewed numerics, so the focus is the integration: times, frames, `topo_pos`, parallel determinism |
| **WP4 Validation harness** | `bench/sssource_validate.py`, black-box: the conformance check from YAML, ClickHouse value spot-checks (read-only, ≤ 8 concurrent queries), the ephemeris regression, the NearbySSO ellipse cross-check. | contract, fixtures | no; it is the independent check of WP1–3 |

### Phase 2: integration (integrating session)

- Merge WP1–3, then run the full chain on the Phase 0 fixtures with WP4's tools.
- Record row counts, timings and memory here, and write a runbook next to the outputs.

### Schema follow-up

When the owner approves, push `tickets/DM-55375` and open the `sdm_schemas` PR for DM-55375.

## Out of scope

- Loading into the PPDB (DM-55678 consumes the Parquet file).
- Incremental updates; the table is regenerated in full daily.
- Changing `ssp.SubmittableSources` itself.
- A widened SSObject.

## Follow-ups (2026-10-01; implemented)

### Owner decisions

| item | decision |
|---|---|
| `diaDistanceRank` | **Removed** from SSSource. |
| `ephOffsetAlongTrack`, `ephOffsetCrossTrack` | **Computed**, ported from pipe_tasks (`ssoAssociation.py`, upstream main), which ssp-tools never had. `along = (ephOffsetRa, ephOffsetDec) · (ephRateRa, ephRateDec)/ephRate` and `cross = (ephOffsetRa, ephOffsetDec) · (−ephRateDec, ephRateRa)/ephRate`, in arcsec. NULL where there is no orbit or `ephRate` is 0. |
| `NearbySSO.diaDistanceRank` | **Added** (named as in the old SSSource, now on NearbySSO where it belongs): the 1-based rank of the row's DiaSource by separation from its object's prediction in that visit. It ranks **all** of that visit's DiaSources within the 5″ radius of that prediction that pass the σ cut, whichever object each one's NearbySSO row names. Ties go to the lower `diaSourceId`. NearbySSO gets no along/cross-track columns. |
| SSObject's row-order dependence | **Find the root cause and make the fits order-independent**, rather than matching today's values by sorting. SSObject then reads its photometry from **SSSource** (the published float32 columns), so the published SSObject is reproducible from the published SSSource. Its values change for the objects whose fits were order-sensitive. |

### Plan

The same pattern as before: the integrator owns the contract, reviews and integrates; parallel subagents build the work packages; independent reviews where marked.

**Phase 0 (integrator):**
- `sdm_schemas` `tickets/DM-55375` (local): drop `SSSource.diaDistanceRank`; add `NearbySSO.diaDistanceRank` (short, non-null) after `ephOffset`.
- Re-vendor `sso_base.yaml`, regenerate `ssp/schema_ppdb.py`.
- `ssp/sssource_contract.py`: the along/cross formula and its NULL rules.
- `ssp/nearbysso/_contract.py`: add the rank to `NEARBYSSO_DTYPE`, and put the output columns in the schema's order (the ellipse columns next to `ephRa`/`ephDec`).

**Phase 1 (parallel):**

| WP | builds | independent review |
|---|---|---|
| **A SSSource** | Compute along/cross-track per the contract; drop `diaDistanceRank`; tests including pipe_tasks' worked example. | no (small; WP D checks it) |
| **B NearbySSO** | `diaDistanceRank` in pass 3, from every match within the radius of each (orbit, visit) prediction, ranked before the nearest-object reduction; output in schema order; tests. | light |
| **C SSObject** | Root-cause every source of row-order dependence in the photometric fits (`ssp/photfit.py`, `ssp/ssobject.py`) and why its effect is large. Fix it so the fits are mathematically order-independent. Read photometry from SSSource. Tests: random within-object permutations give the same SSObject (bitwise where order-insensitive arithmetic allows, else to a stated tolerance). Report how many objects change, and by how much, relative to today. | **yes** |
| **D Validation** | `bench/sssource_validate`: an independent along/cross check; regression without `diaDistanceRank` and the along/cross columns; an SSObject permutation-invariance check. `bench/nearbysso_validate`: a brute-force rank check against the DiaSources. | no (it is the check) |

**Phase 2 (integrator):** rerun the full chain on the 2026-09-30 fixture and NearbySSO on PPDB, run every check, and record the results here and in `nearbysso.md`.

### Further owner decisions (2026-10-01, during WP C)

| item | decision |
|---|---|
| `{band}_slope_fit_failed` | Implements the schema's meaning ("G12 fit failed; G12 contains a fiducial value used to fit H"). A band's slope fit fails when G12 ends at a bound (within 1e-5 of 0 or 1), the fit isn't invertible (including a non-finite G12Err at an interior G12), fewer than 3 points are used, or the phase span of the points used is below 2° (`--hg12MinPhaseSpan`). |
| Fallback | H is refit with G12 fixed at 0.5 (`--hg12FiducialG12`; DP2's value), clipping under the same condition as the free fit (more than 3 points). G12 = 0.5, G12Err and Cov NULL (stored as NaN, the SSObject convention), flag set. One surviving point still gives H; H is NULL only if no point survives. |
| Bands with 1–2 observations | Get H at the fiducial G12, flagged. |

### Results (2026-10-01)

**Root cause of the order dependence** (WP C, confirmed by its independent review). The fits' reductions (weighted means, IRLS sums, costs, JᵀJ) round differently, by an ulp, in another row order. In well-posed fits that moves G12 within the bounded search's tolerance (≤ 2e-5). In degenerate fits (2 points for 2 parameters, or a single phase angle) the cost is flat to rounding and JᵀJ is singular to about 1e16, so the rounding decided G12, whether it ended at a bound (which switches the HErr formula: 8e5 against 0.057), and whether the fit failed. With the canonical order, and the failure rules routing every degenerate fit to the fallback, no residual order effect remains beyond ~1e-6 (and none at all bitwise). Degenerate free fits are still decided by rounding for a given set of observations, so they may differ across platforms; the rules send them to the fallback.

**Integrated rerun** on the 2026-09-30 fixture (`sssource-followups` a9d3385, 32 workers; `/sdf/data/rubin/user/mjuric/sssource-widened/integration/2026-09-30-r2/`; the extract reused from the first run):

| stage | wall | peak RSS | output |
|---|---|---|---|
| `ssp-build-sssource` | 3:11 | 13.2 GB | 8,070,610 rows × 180 columns |
| `ssp-build-ssobject` (now `SSSOURCE MPCORB`) | 1:17 | 3.0 GB | 297,762 objects |

Every check passes: conformance (23), offsets (4: along/cross recomputed independently, NULL rules, rotation, separation), copied (5), clickhouse (6), regression (8; the 33 unchanged ephemeris and geometry columns bitwise equal), counts (4), ellipse (3), ssobject-permutation (5: byte-identical SSObject from permuted SSSource).

**SSObject against the previous build** (297,762 objects): 296,022 change.
- The order fix and float32 photometry alone change 112,071, mostly by ≤ 1e-4 relative; the large differences are all degenerate fits.
- The failure rules flag 852,924 of 1,091,782 band fits (78%; 67.6% of bands with ≥ 3 observations): G12 at a bound 458,829, fewer than 3 points 358,593, phase span < 2° 231,608, singular 3 (overlapping).
- 174,550 bands gain an H and none loses one; 6 bands end with no H (clipping keeps no point; bad photometry or linkage).
- The refit H moves by a median of 0.074 mag (p90 0.19, p99 0.26).
