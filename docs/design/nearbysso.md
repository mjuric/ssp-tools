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
1. Read the DiaSources: 5 columns, sorted by visit. For each visit, derive its time, field centre and radius, and build its sky index: DiaSources binned into HEALPix cells of ~10″ (order 15, via `cdshealpix`).
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

1. **Agreement with SSSource.** On a catalog with SSSource rows (e.g. DP2-DS), every associated DiaSource should appear in NearbySSO with the same designation and identical `eph*` values (same ASSIST code). Report the recovered fraction and account for every miss (quality filter, σ gate, radius).
2. **Coarse-pass safety.** On a sample of visits, compare the candidate list against brute force (all eligible orbits, exact ephemerides) to show the margins lose nothing.
3. **Uncertainty checks:**
   - Φ from the variational particles against finite differences, for a sample including NEOs;
   - the propagated σ against a Monte Carlo sample of orbits drawn from C₀, for a few short-arc and long-arc objects.
4. **Serial against parallel:** byte-identical output.
5. **Tests:** synthetic orbits with a fake ephemeris, as E1's tests use, for the matching, reduction and σ gate. ASSIST-dependent tests are skipped when `SSP_ASSIST_*` isn't set.

## Out of scope

- Incremental daily updates (a later change, when needed).
- Matching on trail centroids; the 5″ radius applies to the DiaSource PSF position.
- Loading into the PPDB (DM-55678 consumes the Parquet file).
- The `sdm_schemas` change for the three new columns (to be proposed alongside).
