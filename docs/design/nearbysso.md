# Design: building the NearbySSO table

Status: **proposed**, for review. Nothing here is implemented yet.

## Context

[RFC-1188](https://rubinobs.atlassian.net/browse/RFC-1188) (Proposed) decouples the PPDB Solar System tables from `DiaSource`:
- `SSSource` and `SSObject` are built from the record of what Rubin submitted to the MPC.
- `DiaSource` loses `ssObjectId` and becomes immutable.

What remains is a small, daily-regenerated table relating DiaSources to *predicted* Solar System object positions. The RFC calls it `NearbyAsteroids`; the draft schema (`lsst/sdm_schemas` branch `u/mjuric/ppdb-sso-ng`, 2025-12) calls it **`NearbySSO`**. It answers "which known object was near this detection", including the "asteroid photobombing a variable star" case. It is explicitly not the authoritative association; `SSSource`/`SSObject` are.

Also relevant:
- The RFC slide deck sizes it at ~2.4 GB/yr, bulk-dropped and re-ingested daily.
- DM-55678 is the ingestion path: Parquet files to a GCS bucket.

This doc proposes `ssp-build-nearbysso`, a utility in this repo that builds the table from any DiaSource catalog.

## Owner decisions (2026-09-28)

| question | decision |
|---|---|
| row content | the draft `NearbySSO` schema, plus the prediction's error ellipse (below) |
| multiplicity | **nearest** known object only, within the radius; one row per DiaSource |
| matching radius | **5″** |
| daily strategy | **full regeneration** from scratch |
| ephemerides | **ASSIST**, the same code and conventions as SSSource |
| DiaSource input | **any DiaSource catalog** (PPDB is the daily use) |
| orbits | `mpc_orbits`, quality-filtered as `lsst-gen-ephemcache` does |
| prediction eligibility | an orbit is a candidate at a visit only while its predicted **1σ on-sky semi-major axis ≤ 10″** (2× the radius) |
| runtime budget | **< 30 min on 64 cores** for a year of data |

## Output: `nearbysso.parquet`

One row per DiaSource that has an eligible known object's prediction within 5″, sorted by `diaSourceId`:

| column | type | meaning |
|---|---|---|
| `diaSourceId` | int64 | the input DiaSource (primary key) |
| `ssObjectId` | int64, nullable | the object's SSObject id, **only if** it has an `SSObject` row ("if any", per the draft) |
| `designation` | char(16) | primary provisional designation, unpacked (the `mpc_orbits` key) |
| `ephRa`, `ephDec` | double, deg | predicted topocentric ICRS position at the DiaSource's `midpointMjdTai`, light-time corrected |
| `ephOffset` | float, arcsec | DiaSource ↔ prediction separation (the "distance"), with SSSource's formula |
| `ephVmag` | float, mag | predicted V from `mpc_orbits` H, G (as SSSource) |
| `ephRateRa`, `ephRateDec` | float, deg/d | predicted on-sky rates (RA includes cos δ) |
| **`ephRaErr`, `ephDecErr`** | float, deg | **new:** 1σ prediction uncertainty in RA (with cos δ) and Dec |
| **`ephRa_ephDec_Cov`** | float, deg² | **new:** their covariance; with the two above, the full error ellipse (the same convention as DiaSource's `raErr`/`decErr`/`ra_dec_Cov`) |

- Ties for nearest are broken by `designation`, so the output is deterministic.
- About 32 bytes per row before compression, against ~20 in the RFC's estimate.
- The three new columns need a matching change to the draft schema.

## Inputs (read-only, all regenerated daily)

1. **DiaSources**, from any catalog: a Parquet file with at least `diaSourceId`, `visit`, `midpointMjdTai`, `ra`, `dec`. Examples:
   - PPDB DiaSource via the ssp ClickHouse AP-DS tables;
   - DP2 `dia_source`;
   - this repo's `extract-submitted-sources` output.

   Visits are derived from the DiaSources themselves: time, a field centre (the normalized mean unit vector) and a radius enclosing all of the visit's sources. No ConsDB dependency.
2. **`mpc_orbits`**, the same daily snapshot SSSource/SSObject use:
   - `q`, `e`, `i`, `node`, `argperi`, `peri_time`, `epoch_mjd`, H, G;
   - the `CAR` covariance from `mpc_orb_jsonb` (present for ~100% of orbits).

   The filter, as in `lsst-gen-ephemcache`'s `get-mpcorb.py`:
   - no comets (designations containing `/`, or packed designations starting with `_`), since their non-gravitational motion makes formal covariances unreliable;
   - all six elements present.

   The ">2-day arc" filter becomes redundant with the σ gate; it's kept anyway, since it's harmless.
3. **`SSObject`**, optional: only to fill `ssObjectId`.

## Uncertainty propagation

- **Source:** the MPC's epoch covariance, the 6×6 state block of `CAR`. It is propagated with ASSIST **variational equations**: six variational particles give the state-transition matrix Φ(t), and C(t) = Φ C₀ Φᵀ (as layup's `propagate_state` does).
- **Validation: Φ is correct only with `add_variation(testparticle=0)`.** REBOUND's default (`testparticle=-1`) makes ASSIST skip the variational accelerations and silently return Φ ≈ [[I, t·I], [0, I]]. Measured 2026-09-28:
  - with the fix, Φ matches central finite differences to 7×10⁻⁴ relative (their own error);
  - it costs ~1.6–2× a plain integration.
- **Onto the sky:** C(t) is projected to the topocentric tangent plane through the geometric Jacobian of (RA, Dec) with respect to the barycentric position, at the observer's position (X05). That gives the 1σ ellipse (`ephRaErr`, `ephDecErr`, `ephRa_ephDec_Cov`) and its semi-major axis σ.
- **Frames:** `CAR` is expected to be heliocentric ecliptic. Implementation step one is to confirm that from its `coefficient_values` against our `COM`-derived state (which the covariance must describe). The covariance is then rotated ecliptic → equatorial; the heliocentric → barycentric translation doesn't change it.
- **Linear propagation is accurate where it matters:** near σ ~ arcseconds. For orbits whose true uncertainty region is large and non-Gaussian, the linear σ is far above 10″, and those are excluded either way.
- **Orbits without a covariance** (~0.3% of arcs under 10 days) are ineligible, and counted in the run report.
- **Formal errors may be optimistic;** they're used as given. The report records the distribution of `normalized_rms`, to show whether scaling by it would matter.

## Algorithm: one parallel pass, per object

The work is grouped **per object**, not per visit, so each orbit is integrated once per stage. It reuses the fork-pool helpers from the SSSource/SSObject speed-ups (`util.run_chunks`, `util.balanced_chunks`).

**Parent (serial, fast):**
1. Read the DiaSources: 5 columns, sorted by visit. For each visit, derive its time, field centre and radius, and build its sky index: DiaSources binned into HEALPix cells of ~13″ (order 14, via `cdshealpix`; at order 15, ~6.4″, neighbour lookups miss at 5″).
2. Index visit centres per night, and compute the observer (X05) barycentric state at every visit time (vectorized, as SSSource does).
3. Read and filter the orbits, and convert them to barycentric ICRF states at epoch, with covariance.

Everything is shared with the workers through fork.

**Worker, per orbit (one orbit per ASSIST simulation):**
1. **Coarse pass:** integrate from the orbit epoch across the nights covered by the input, with six variational particles (`testparticle=0`) and IAS15 `adaptive_mode = 2` (set after attaching ASSIST, as layup does). At each night's reference time, record the topocentric position, the on-sky rate and σ.
2. **Candidates:** for each night where σ ≤ 10″, the visits whose field, widened by the object's motion within the night plus a safety margin, contains the coarse position. Typically a few to a few hundred visits per object and year.
3. **Precise pass:** `compute_ephemerides_one` at exactly those visit times: light-time corrected, topocentric, with rates and V, as SSSource. σ and the ellipse at each visit come from the coarse pass's Φ at that night, interpolated in time.
4. **Match:** look up the predicted position's HEALPix cell and its neighbours in that visit's DiaSource index. Emit `(diaSourceId, designation, ephOffset, eph…, ellipse)` for every DiaSource within 5″.

**Parent (reduce):**
1. Keep the nearest row per `diaSourceId`.
2. Attach `ssObjectId`.
3. Write `nearbysso.parquet`.
4. Write a run report: counts, eligibility statistics, orbits without a covariance, and timings.

**Memory bound:** DiaSources are processed in time slices, one month by default. At PPDB rates, 10M a night is ~150 GB a year for 5 columns, so orbits are re-integrated per slice. That's cheap next to holding all DiaSources in memory at once.

## Why one orbit per simulation (measured 2026-09-28)

Measured on a real `mpc_orbits` sample, integrating a year with nightly outputs, IAS15 `adaptive_mode = 2`; ms per orbit:

| | one per sim | N=8 | N=64 | N=512 |
|---|---|---|---|---|
| main belt, plain | 1.52 | 0.47 | 0.43 | 0.50 |
| + 5% NEOs, plain | 1.13 | 0.45 | 0.39 | 2.00 |
| main belt, variational | **2.46** | 2.29 | 7.61 | 58.4 |
| + 5% NEOs, variational | **2.39** | 2.20 | 7.32 | 233 |

- **Plain integrations batch well** (~3× at 32–64 per simulation), but not in large batches mixed with NEOs, whose close approaches force small time steps on the whole batch.
- **Variational integrations don't batch:** the cost grows ~N².

Since the coarse pass needs Φ for every orbit, it runs one orbit per simulation, at ~2.4 ms per orbit-year. Batching the precise pass (plain integrations) is a possible later optimization; it isn't needed for the budget.

`adaptive_mode = 2` made every case 1.3–2× faster than ASSIST's default controller. (It would also speed up the SSSource build, as a separate change, since it alters step choices at the numerical-noise level.)

## Performance estimate

For a year of data, ~1.4M orbits, at 64 workers:

| part | estimate |
|---|---|
| coarse pass with variational particles | ~2.4 ms × 1.4M ≈ 56 CPU-min ≈ **~1 min** |
| precise pass (candidates only; SSSource's ~5 ms per object-year as the bound) | ≈ 2 CPU-h ≈ **~2 min** |
| reading ~3.6B DiaSources (5 columns), indexing, matching | **several min** (I/O-bound) |
| **total** | **~5–10 min**, within the 30-min budget |

The cost grows about linearly with survey length. When full regeneration outgrows the budget (likely year 3–5), switch to incremental updates.

## Validation

**The references, and what each can show:**
- **JPL Horizons, with our own elements, is the arbiter for ephemerides.** `bench/ephem_bench.py`'s `horizons_observer` sends each orbit's own `mpc_orbits` osculating elements (`COMMAND=';'`, cometary TP/QR), so Horizons and ASSIST see the same orbit, and a difference is ours. The existing gate is < 1 mas RMS.
- **SSSource is a consistency reference, not truth,** and only when it was built from the same orbits (below).
- **Horizons etiquette:** queries are single-threaded, with a pause (1 s) between them and epochs chunked (≤ 60 per query), as ssp-submit does. It's never run in parallel or automatically in CI; parallel querying risks a JPL ban.

**Checks:**
1. **Agreement with SSSource, from the same orbits.** A DiaSource's SSSource ephemeris and its NearbySSO ephemeris are only comparable if both came from the **same `mpc_orbits` snapshot** with the same code. So:
   - **The main check** builds SSSource with this repo's `ssp.sssource` from the **same** `mpc_orbits` file NearbySSO uses, on the same DiaSource catalog. Every SSSource row should then appear in NearbySSO with the same designation and **identical** `eph*` values (same ASSIST code and conventions), apart from rows excluded by NearbySSO's own rules: the quality filter, the σ gate, the 5″ radius, and nearest-only.
   - **A comparison against DP2's published SSSource** is valid only with the orbit catalog DP2 used, and **DP2's quality cuts were different.** Its SSSource includes objects our filter excludes, and the reverse. So that comparison is restricted to the objects both keep, run with DP2's orbit snapshot, and reported, not gated. Designations are reconciled via `current_identifications` first.
   - **Every discrepancy is checked against Horizons** with that orbit's elements: which side matches Horizons? A NearbySSO discrepancy that Horizons confirms is a bug. One where SSSource disagrees with Horizons goes to the SSSource code.
2. **Horizons spot checks, independent of SSSource,** run as part of validating each release of the tool, not every day:
   - a stratified random sample of ~100 NearbySSO rows: main belt, NEOs (including one at close approach), Jupiter Trojans, TNOs, short-arc orbits, and rows near the 5″ radius and near the σ = 10″ gate;
   - for each, Horizons' astrometric RA/Dec at the DiaSource's time and observer X05, from the orbit's own elements;
   - gated at the existing < 1 mas RMS criterion, and the per-row `ephOffset`, rates and `ephVmag` compared too.
3. **Coarse-pass safety.** On a sample of visits, compare the candidate list against brute force (all eligible orbits, exact ephemerides) to show the margins lose nothing.
4. **Uncertainty checks:**
   - Φ from the variational particles against finite differences, for a sample including NEOs;
   - the propagated σ against a Monte Carlo sample of orbits drawn from C₀ (each integrated), for a few short-arc and long-arc objects;
   - **Against Horizons, using JPL's orbits.** Horizons reports uncertainties only for JPL's own orbits, so use those as the input:
     - fetch JPL's elements **and covariance** from the JPL Small-Body Database API (`sbdb.api`, `cov=mat`);
     - propagate them with our code;
     - compare our on-sky ellipse with Horizons' plane-of-sky 3σ ellipse for the same object, epoch and observer (quantity 37: semi-major and semi-minor axes and orientation; quantity 36: RA/Dec 3σ).

     Stratified like the spot checks, including short-arc objects and epochs far from their observations. Gate: σ within ~10% where it's below ~1′ (the linear regime); reported beyond that. The same Horizons etiquette applies.
5. **Serial against parallel:** byte-identical output.
6. **Tests:** synthetic orbits with a fake ephemeris, as E1's tests use, for the matching, reduction and σ gate. ASSIST-dependent tests are skipped when `SSP_ASSIST_*` isn't set; Horizons checks are never in the test suite.

## Implementation plan

Built by parallel subagents, each in its own git worktree, with the integrating session defining interfaces, reviewing, integrating and merging. The more complex work packages also get **independent reviews**: a fresh reviewer agent that sees only the design, the interface contract and the code, and tries to break it.

### Phase 0: interfaces and fixtures (integrating session, before any agent starts)

So that the work packages can be built in parallel against a fixed contract:

1. **Module layout:** `ssp/nearbysso/`, containing:
   - `orbits.py` (WP1), `propagate.py` (WP2), `visits.py` (WP3), `build.py` and the CLI (WP4);
   - `_contract.py`: the shared dtypes and function signatures, with docstrings, and `NotImplementedError` stubs where no implementation exists yet.
2. **The contract:**
   - **`OrbitSet`**, a structured array:
     - `designation`, `packed`, `epoch` (ASSIST time);
     - `state0` (6, barycentric ICRF, AU and AU/d) and `cov0` (6×6, the same frame);
     - `H`, `G` and a `has_cov` flag.
   - **`Visits`**: `visit`, `t` (ASSIST time, TDB), `night`, `center` (unit vector), `radius` (rad), `obs_pos`/`obs_vel` (X05, barycentric ICRF).
   - **`coarse(orbit, t_nights, obs_pos_nights) → CoarseTrack`**: topocentric unit vectors, on-sky rates, the 1σ ellipse (`raErr`, `decErr`, `ra_dec_Cov`) and σ_major, per night.
   - **`ellipse_at(track, t) → ellipse`:** interpolated to exact visit times.
   - **`candidates(visits, track, sigma_max, margin) → visit indices`** and **`match(dia_index, visit, radec, radius) → (dia row, sep)`**.
   - **The `NearbySSO` output dtype.**
3. **Fixtures under `/lscratch/mjuric/sspwt/nearbysso/fixtures/`**, read-only for the agents:
   - a 3-night DP2-DS DiaSource slice and a 3-night PPDB AP-DS slice, both from ClickHouse;
   - the 2026-09-26 `mpc_orbits` snapshot;
   - SSSource built by `ssp.sssource` from that same snapshot on the DP2-DS slice, for WP5's same-orbits comparison.
4. **Confirm the `CAR` covariance frame** (the design's first implementation step). This fixes WP1's conversion, so it has to be settled before WP1 starts.

### Phase 1: four work packages in parallel

| WP | builds | depends on | independent review |
|---|---|---|---|
| **WP1 Orbits** | Read `mpc_orbits`; apply the filter (no comets, elements present, arc > 2 d); parse the `CAR` covariance from `mpc_orb_jsonb` quickly for ~1.5M rows (vectorized or a fast JSON parser, not a per-row Python loop); convert to the barycentric ICRF state and covariance at epoch; report orbits without a covariance and the `normalized_rms` distribution. Tests: the state matches our `COM`-derived one, covariance symmetry and positivity, the filter. | contract | no; small, but its output checks are part of WP2's review |
| **WP2 Propagation and uncertainty** | `coarse()` (one orbit per ASSIST simulation, six variational particles with `testparticle=0`, IAS15 `adaptive_mode = 2` after attaching ASSIST, nightly outputs); the projection of C(t) onto the topocentric sky; `ellipse_at()`. Tests: Φ against finite differences (including an NEO); σ against a Monte Carlo sample. | contract | **yes:** numerics, frames, the Jacobian, the interpolation, the regimes where linearization fails |
| **WP3 Visits, candidates and matching** | Read any DiaSource Parquet (5 columns, in time slices); derive visits (centre, radius, night) and observer states; per-visit HEALPix order-15 DiaSource indices; `candidates()` (field plus a motion margin plus a safety margin, with the σ gate); `match()` (the cell and its neighbours, 5″); nearest-per-DiaSource with deterministic ties. Tests: fake tracks; coverage at cell and field edges, the RA wrap and the poles. | contract | **yes:** completeness at every edge, the margin reasoning, memory at PPDB scale |
| **WP5 Validation harness** | Black-box tools run on outputs only:<br>(a) comparison against SSSource built from the same orbits;<br>(b) comparison against DP2 SSSource on the objects both keep, reported not gated;<br>(c) Horizons adjudication of every discrepancy;<br>(d) the stratified Horizons position spot check;<br>(e) the Horizons uncertainty check with SBDB elements and covariance;<br>(f) brute-force coarse-pass safety.<br>Horizons strictly serial and polite; reuses `bench/ephem_bench.py`. | contract and fixtures only | no; it *is* an independent check of WP1–4, written without seeing their code |

The agents may not change `_contract.py`. A needed contract change comes back to the integrating session, which applies it for everyone.

### Phase 2: integration (WP4)

Starts once WP1–3 have landed.
- **`build.py` and the CLI `ssp-build-nearbysso`:** the parallel per-orbit pass with `util.run_chunks`: coarse, then candidates, then the precise `compute_ephemerides_one`, then matching. Also time slicing, the reduction, attaching `ssObjectId`, the Parquet writer and the run report.
- **Tests:** serial against parallel byte-identical; an end-to-end run on the fixtures.
- **Independent review: yes,** of the whole pipeline: fork sharing, determinism, error handling, the time-slice boundaries (a night split across slices), and memory.

### Phase 3: validation and performance (integrating session, with WP5's tools)

1. **On the fixtures:** every WP5 check. Discrepancies are adjudicated with Horizons and fixed in the relevant work package.
2. **A full year of PPDB AP-DS** (or the largest available span): the run time against the 30-minute budget on 64 cores, peak memory, the row count against the RFC's ~120M/yr estimate, and the run report.
3. **Results go into this doc,** as for the SSSource/SSObject speed-ups.

### Reviews and merging

- **Each work package gets:**
  - my review against this design and the contract;
  - an independent review for WP2, WP3 and WP4;
  - fixes sent back to the author until both reviews are clean.
- **Each work package merges through its own PR,** as a merge commit.
- **The owner approves before each merge,** unless blanket approval is given for this project.

### Schema follow-up

Draft the `sdm_schemas` change adding `ephRaErr`, `ephDecErr` and `ephRa_ephDec_Cov` to `NearbySSO` on `u/mjuric/ppdb-sso-ng`, for the owner to take forward.

## Out of scope

- Incremental daily updates (a later change, when needed).
- Matching on trail centroids; the 5″ radius applies to the DiaSource PSF position.
- Loading into the PPDB (DM-55678 consumes the Parquet file).
- The `sdm_schemas` change for the three new columns (to be proposed alongside).
