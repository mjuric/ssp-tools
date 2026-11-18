# Double-precision positions, ranges and arc

Status: **approved** (2026-11-18); F-16 folded in, implementation in progress.

## Context

sdm_schemas PR #549 (`tickets/DM-55375`, commit d6e8100, `sso_base.yaml`) changes the datatypes of 11 columns. The owner approved the change on 2026-11-18.

| Table | Columns | Was | Now |
|---|---|---|---|
| SSObservation | `helio_x`, `helio_y`, `helio_z`, `topo_x`, `topo_y`, `topo_z` | float | double |
| SSObservation | `helioRange`, `topoRange` | float | double |
| SSObject | `arc` | float | double |
| SSObservation | `ephOffsetRa`, `ephOffsetDec` | double | float |

Nothing else in the schema changes. `elongation`, `phaseAngle`, the range rates and the velocities stay float, by the owner's decision.

Why:
- **Direction:** float32 `topo_x/y/z` give the topocentric direction to 3 mas median and 11.5 mas at most, against the double `ephRa`/`ephDec` (2026-11-17 delivery). That is worse than Rubin's ~1 mas astrometry.
- **Rebuilt vectors are worse:** a topocentric vector rebuilt as helio minus observer is off by 23–390 mas, and by ~2.5″ for an NEO at 0.01 AU.
- **Arc:** over ten years, a float32 `arc` rounds to ~21 s.
- **Offsets:** `ephOffsetRa`/`ephOffsetDec` become float, like `ephOffset`, `ephOffsetAlongTrack` and `ephOffsetCrossTrack`.

The request came through the ssp-daily session. It times the pin with River's table-file type change (`etc/ppdb.table.yaml`).

## Where float32 is assumed today

- **`ssp.ssobservation`:**
  - writes the float64 ephemeris (`EphResult.helio_pos`, `topo_pos`) into the float32 columns of the row array;
  - then computes from those float32 columns:
    - `helioRange` and `topoRange`;
    - the range rates;
    - `ephVmag` (from the ranges).
  - computes `ephOffsetAlongTrack`/`CrossTrack` from the row array's float64 `ephOffsetRa`/`Dec`. Once those columns are float32, the inputs would be rounded first.
- **`ssp.ssobject`:**
  - `arc` is computed into `schema.SSObjectDtype`'s float32 field;
  - the photometric fits read SSObservation's `topoRange` and `helioRange`.
  - `ssp/schema.py` is generated from an old `lsstcam.yaml`. Its SSObjectDtype matches d6e8100's SSObject exactly, except for `arc`.
- **`ssp.nearbysso.build`:** computes NearbySSO's V "exactly as SSObservation computes it: from its float32 helio/topo columns". It casts the float64 vectors to float32 to match.
- **`ssp.delivery_check`:** the elongation tolerance includes twice the float32 rounding bound of `topo − helio`.
- **Not affected:**
  - `ssp.ssobservation_ellipse` already uses the float64 `topo_pos`.
  - The tail angles already come from the float64 vectors.
  - No code on the MPC or submission side (`ssp.export`) reads these columns.
  - NearbySSO_internal has no position, range or offset column.

## Owner decisions

| Date | Decision |
|---|---|
| 2026-11-18 | **The schema change** (above), in sdm_schemas d6e8100. Relayed by ssp-daily. |
| 2026-11-18 | **Design approved** as written, with D1 and D2. River loads the new types from 2026-11-18 (ssp-daily PR #37), and mppdb casts float32 files up, so the pin has no timing constraint. |
| 2026-11-18 | **F-16 fix approved** (wrap the RA difference, a new offsets-vs-separation check, a straddling test). Relayed by ssp-daily; folded into this branch. |
| 2026-11-18 | **Master merge approved**, including F-16, once the F-16 WP passes its review. |
| 2026-11-18 | **Release:** earlier deliveries stay as they are; the owner approves the master merge; ssp-daily times the pin. Relayed by ssp-daily. |
| 2026-11-18 | **D1, approved. Derived values computed from the float64 ephemeris.** The range rates, `ephVmag`, NearbySSO's `vmag`, and through the ranges SSObject's fits, would be computed from the float64 vectors instead of float32-rounded ones. Their stored values would change at the float32 rounding level, and the fits by about that much. Otherwise the new double ranges would be upcast float32, and V would be computed from less precise inputs than the ones delivered. |
| 2026-11-18 | **D2, approved. The elongation check's tolerance.** The rounding term uses the epsilon of the stored type: 2⁻²⁴ for float32 vectors, 2⁻⁵³ for float64. The same code then checks both old and new deliveries correctly. For new deliveries the tolerance becomes 1″ plus the light-time term, about 1″ nearby and up to ~2″ at 90 AU. |
| 2026-11-18 | **D2 amended (approved): the light-time term covers both legs.** `helio_*` is relative to the Sun as the object saw it (`ephem_assist._apparent_sun`, at t − τ − τ_refl), so `topo − helio` is the Sun's position delayed by τ + τ_refl, not τ alone. The term becomes 16 m/s × 499 s/AU × (\|topo\| + \|helio\|) instead of × \|topo\|. The tolerance for float64 vectors is ~1.03″ nearby and ~2.98″ at 90 AU (D2 above said ~2″); for float32 deliveries it is ~7.4″ at 90 AU (was ~6″). Found in the WP's real-data check: with \|topo\| alone, the new build used 0.74 of the tolerance at 89 AU, and would fail beyond ~190 AU. |

## F-16: `ephOffsetRa` across RA 0/360 (folded in 2026-11-18)

A QA finding, F-16, relayed by ssp-daily with the owner's approval of the fix. `ssp/ssobservation.py` computes `ephOffsetRa = (ra − ephRa) · cos(ephDec) · 3600` without wrapping `ra − ephRa`.
- **The bug:** an observation and a prediction on opposite sides of RA 0/360 get about ±1.3e6″ instead of a few arcsec.
- **What else it affects:** `ephOffsetAlongTrack`/`CrossTrack` come from `ephOffsetRa`, so they are wrong in the same rows. `ephOffset`, an independent Vincenty separation, is right.
- **Today it is latent:** on 2026-11-17 no row straddles RA 0.
- **Rows that change:** wrapping changes only 2025 NO124's 18 rows. Its orbit predicts positions 174–178° from the observations (finding F-12). `ephOffsetRa` there goes from about +6.1e5″ to about −5.7e5″; at that separation the tangent-plane offsets have no meaning either way.
- **Why the current check misses it:** the delivery check's along/cross-track test (along² + cross² = offsetRa² + offsetDec²) can't see it, because both sides are wrong together.
- **Packaged here** because it touches the same lines, and PR #124 was not yet merged.

Changes:
- **Wrap:** `ra − ephRa` to [−180°, 180°) before the cos(dec) factor.
- **New delivery check:** `hypot(ephOffsetRa, ephOffsetDec)` must agree with `ephOffset`, within a tolerance that covers:
  - the tangent-plane approximation, which grows with the separation and with |tan dec|;
  - the float32 storage of the offsets and of `ephOffset`.

  Measured on the new 2026-11-17 build: below 1″ the relative difference is at most 1.0e-6, at 1–60″ at most 1.5e-5, and at 1′–1° at most 6.2e-5. Above about 1°, where the approximation itself fails, the check only requires the offsets to stay within the half-sky bound (|ephOffsetRa| ≤ 180° · 3600). That still catches an unwrapped difference wherever the separation is small.
- **Test:** a synthetic object straddling RA 0/360 (e.g. 359.9999° and 0.0001°).

## Changes

1. **Schema copies:**
   - refresh `tests/data/sdm_schemas/sso_base.yaml` and `ppdb.yaml` to d6e8100;
   - regenerate `ssp/schema_ppdb.py`, also generating SSObject in it;
   - `ssp.ssobject` uses `schema_ppdb.SSObjectDtype`;
   - `ssp/schema.py`'s other tables stay.
2. **SSObservation** (`ssp.ssobservation`):
   - compute the ranges, range rates, `ephVmag` and the along/cross-track offsets from float64 arrays (`EphResult` and the float64 offsets);
   - then store them. Each value is rounded once, to its column's type.
3. **SSObject:** `arc` is computed and stored in float64 (`np.ptp` of the float64 `midpointMjdTai`).
4. **NearbySSO:** `vmag` from the float64 ranges, as SSObservation (D1).
5. **Delivery check:** the tolerance uses the stored type's epsilon (D2). The type check already follows Felis, so it requires the new types.

## Validation

**Tests (network-free):**
- the written Arrow types match d6e8100;
- the doubles carry float64 values, not upcast float32: `x != float32(x)` on typical rows;
- `helioRange`/`topoRange` equal the norm of the float64 vectors;
- along/cross-track are unchanged when the offsets become float32;
- `arc` keeps sub-second precision over ten years;
- NearbySSO `vmag` equals SSObservation's `ephVmag` for the same orbit and time;
- the tolerance is right for both float32 and float64 vectors.

**Real data:**
- rebuild 2026-11-17 (or the latest delivery) on a scratch root with old and new code, and compare column by column:
  - the 9 doubles agree with the old float32 to within float32 rounding;
  - the offsets agree with the old doubles to within float32 rounding;
  - every other column is bitwise equal, except the rounding-level changes of D1;
  - report their counts and maxima.
- Run `delivery_check` on the new build and record the elongation margin with the new tolerance.
- Run it on the 11-17 and 11-18 deliveries as delivered (float32 vectors), to show they still pass.
- Record the change in SSObservation's file size and the build's peak memory.

## Implementation plan

- **Integrator:** this document; then the schema refresh and regeneration (change 1), which is the contract.
- **Wave 1, one WP:** changes 2–5 with their tests.
- **Review:** an independent review of the WP, including the real-data rebuild.
- **Then:**
  - record the results here;
  - merge to master after the owner's approval;
  - send the master commit to ssp-daily.

## Record

Integrated 2026-11-18. The WP: 0c8829d, 3169e0c, 1079d0c (tolerance) and 6c8380e (review fixes); PR #126, merged into this branch as 9edd638.

**Review.**
- The integrator's review found the one-leg light-time term (D2 amended, above). The fix was verified on real data: on 3,000 rows at 40–89 AU, the check's residual tracks the Sun's motion over τ + τ_refl (correlation 0.9993, slope 1.017). With that motion removed, the maximum residual drops from 1.26″ to 0.04″, the float32 rounding of `elongation`.
- An independent review, in two rounds, found no correctness bugs; round 2 found nothing.
  - It recomputed, on 450,000 rows of the new build, in its own code:
    - the ranges equal the norms of the stored vectors, bitwise;
    - the speeds and range rates agree within 1.24 float32 ulps (the velocities are float32);
    - `ephVmag` from mpc_orbits' H and G within 0.52 ulps;
    - along/cross-track within 1.1e-7 × `ephOffset`.
  - None of the 450,000 rows has a vector or range equal to its float32 rounding, so the doubles are genuine.
  - On 7 objects, from 0.047 to 76 AU, an independent ephemeris run agrees bitwise on `helio_*` and to ≤ 8e-15 AU on `topo_*`.
  - The Sun's barycentric speed from DE440 is at most 14.61 m/s over 2025–2036, so the 16 m/s bound holds.
  - On synthetic cases from 0.001 to 500 AU, the Sun's real shift equals the light-time term to 4 digits.
  - Its low findings (a NaN vector passed the elongation check, which the no-NaN check catches anyway; the bench's legacy mode accepted a column left float32; a test that didn't always tell the paths apart) were fixed. `ssp/schema.py`, now unused, is issue #125.

**Tests.** The full suite passes: 1528 tests.

**Real data.** 2026-11-17 was rebuilt from its inputs with master a34126d (base) and with the WP (new), one after the other, at 32 workers.

| Step | Wall (s), base → new | Peak RSS (GB), base → new |
|---|---|---|
| mpc | 45.8 → 45.4 | 5.78 → 5.79 |
| ssobservation | 312 → 334 | 13.52 → 14.02 |
| ssobject | 74.3 → 74.2 | 2.95 → 3.07 |
| nearbysso | 452 → 461 | 11.24 → 11.03 |
| check | 77 → 83 | 5.36 → 5.28 |
| whole build | 16m18s → 16m52s | |

SSObservation, 8,129,545 rows, 8,067,419 of them with an orbit:
- **The vectors:** float32 of the new values equals the old ones bitwise.
- **The ranges:** float32 of the new values differs from the old on 2.2M rows, by at most 2 ulps, the error of the old float32 arithmetic.
- **The offsets:** float32 of the old doubles equals the new values bitwise.
- **D1 columns,** values changed and largest change:

  | Column | Values changed | Largest change |
  |---|---|---|
  | `topoRangeRate` | 4.71M | 7.6e-6 km/s (4 ulps of `topo_vtot`) |
  | `helioRangeRate` | 6.81M | 3.8e-6 km/s (2 ulps of `helio_vtot`) |
  | `topo_vtot` | 2.29M | 2 ulps |
  | `helio_vtot` | 1.99M | 2 ulps |
  | `ephVmag` | 2.75M | 3.8e-6 mag |

- **Along/cross-track:** unchanged.
- **The other 166 columns and the sidecar:** bitwise equal.
- **Size:** 3.33 → 3.52 GB (+5.7%).

SSObject, 299,690 rows:
- `arc` equals max − min of `midpointMjdTai` exactly, and float32 of it equals the old value.
- **The fits move with the ranges:** p99.9 \|ΔH\| is 1.9e-6, the largest ΔH 2.5e-5. Changes are larger only on degenerate fits, e.g. the H/G12 covariance by up to 0.12 absolute, 5e-4 relative.
- The other 43 columns are bitwise equal.

NearbySSO:
- `vmag` changed on 1.43M of 4.17M rows, by at most 2 ulps.
- On 249,186 rows matched to SSObservation, it equals `ephVmag` bitwise on 247,418, and within 1 ulp on the rest, which come from separate integrations.
- The other 16 columns and the sidecar are bitwise equal.
- mpc_orbits and the identification tables are identical.

`delivery_check` with the new code:

| Build | Result | Elongation: largest difference, worst fraction of tolerance |
|---|---|---|
| new (float64 vectors) | 6/6 pass | 1.458″ at 89.02 AU; at most 0.497 |
| base (float32 vectors) | fails only the types: the 10 retyped SSObservation columns and SSObject `arc` | 2.682″ at 88.95 AU; at most 0.439 |

Worst fraction by distance, new build: 0–2 AU 0.086, 2–5 AU 0.111, 5–10 AU 0.148, 10–30 AU 0.337, 30–60 AU 0.442, 60+ AU 0.497.

The delivered 11-17 run predates the elongation and NULL fix (a34126d), so it also fails those checks. That says nothing about this change; the base rebuild is the float32 comparison. No 11-18 delivery existed yet.

The bench regression check (`bench/ssobservation_validate.py`) passes new against base. It allows a pre-d6e8100 reference 8 float32 ulps on the retyped and D1 columns; the largest seen is 4.
