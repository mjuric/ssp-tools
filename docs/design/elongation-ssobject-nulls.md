# Fixing SSObservation.elongation and SSObject's NULLs

Status: **approved** (2026-11-17); implementation in progress.

## Context

An independent QA of the 2026-11-16 delivery, built by 2f013b1, found two defects. The code in these paths is the same in 6c95217.

**F-1: SSObservation.elongation is wrong on every row.**
- `ssp.util.solar_elongation_ndarray` takes the Sun from `get_sun(t).icrs`. `get_sun` gives the geocentric (GCRS) Sun with its distance. Transforming that to ICRS moves the origin to the solar-system barycentre, so the code used the direction from the barycentre to the Sun. That direction has nothing to do with the Sun's position in the observer's sky.
- The bug is in the original code (2025-11). Every delivery so far has it.
- The tests compared the function only with itself: the time-shifted against the exact evaluation.
- Measured on 2,965 delivered rows with an orbit, against the elongation computed from the delivered vectors (the angle between `topo` and `topo − helio`):

  | Sun direction | Difference |
  |---|---|
  | delivered | median −81° |
  | geocentric apparent Sun (GCRS, from `get_sun`) | median 7″, max 27″ |
  | geocentric geometric Sun (DE440, barycentric Sun minus Earth) | median −2.7″, max 8.3″ |

**F-4: SSObject stores NULL as NaN, and as False in the flags.**
- `util.struct_to_parquet` writes SSObject, with types inferred from numpy and every field nullable.
- 51 of the 80 columns have NaN, and none has a NULL. `{band}_slope_fit_failed` is False on every row with `{band}_nObs = 0`.
- The 15 `nullable: false` columns are declared nullable.
- SSObservation and NearbySSO already write NULLs, with the Felis nullability.
- The design docs meant these values as NULL already: sssource-widened.md says "NULL (stored as NaN, the SSObject convention)".

## Owner decisions

| Date | Decision |
|---|---|
| 2026-11-17 | ~~**Elongation uses the geometric Sun direction from the observatory**: DE440 barycentric Sun minus the observatory's barycentric position, at the observation time, in ICRS.~~ Superseded the same day (next row). |
| 2026-11-17 | **Elongation is apparent, as Horizons' S-O-T.** The geometric directions to the Sun (DE440 Sun minus the observatory) and to the object (the observed `ra`, `dec`) are both aberrated by the observatory's barycentric velocity before taking the angle. It is the standard definition (Horizons, MPC), and it is the convention `phaseAngle` already follows: it matches Horizons' `phi` (`ssp.ephem_assist._phase_angle_deg`). Geometric elongation differs from it by up to 41″ (2v/c). The extra cost is negligible: the velocity comes with the position. |
| 2026-11-17 | **The delivery check compares with the observed direction.** It compares `elongation` with the angle between the observed (`ra`, `dec`) and `topo − helio`, apparent in the same way, using the observatory's velocity relative to the Sun, `helio_v − topo_v`. The angle to `topo` would fail on rows whose orbit prediction is far from the observation (111 of 8.07 M on 2026-11-16, from 15 objects, off by 36″ to 178°). Their elongation is right. That count is still reported in the check's detail. |
| 2026-11-17 | **The MPC tables' NaN stays.** `mpc_orbits` mirrors the replica: on 2026-11-16, `mean_anomaly`, `period` and `mean_anomaly_unc` had 246 NaN each, and `period_unc` 1,946. The no-NaN check covers SSObservation, SSObject and NearbySSO (and its sidecar) only. |
| 2026-11-17 | **Earlier deliveries, and the int PPDB dataset, stay as they are.** The fix applies from the first run with the new pin. |
| 2026-11-17 | **Approach approved:** NULL for NaN in SSObject, NULL flags for bands without observations, the Felis-derived Arrow schema, and new delivery checks for both defects. |

## Changes

### 1. Elongation (`ssp.util.solar_elongation_ndarray`, `ssp.ssobservation`)

The Sun direction becomes `r_sun(t) − r_obs(t)`:
- `r_sun` is the barycentric position of the Sun from DE440;
- `r_obs` is the observatory's barycentric position, from `util.observatory_barycentric_posvel`, the same function `observer_states` uses;
- both that direction and the unit vector of (`ra`, `dec`) are aberrated, to first order in v/c, by `v_obs`, the observatory's barycentric velocity, which the same function returns. The rule is the one `ssp.ephem_assist._phase_angle_deg` applies: `u' = u + β − (u·β) u`, normalised, with `β = v_obs / c`;
- the elongation is the angle between the two aberrated directions. This is the apparent elongation, Horizons' S-O-T.

The rest stays as it is:
- the Sun and the observatory's position and velocity are evaluated once per unique time;
- with `dt_s`, the direction is shifted linearly, as now;
- the cost is of the same order.

The result reproduces Horizons' S-O-T to about 0.2″, and it applies the same way to rows without an orbit, which have no vectors.

### 2. SSObject NULLs (`ssp.ssobject`)

- **Arrow schema:** SSObject is written with the schema derived from Felis, as SSObservation is: its columns in order, its types, and non-nullable exactly for the `nullable: false` columns.
- **NaN:** in every float column, NaN is written as NULL. ±Inf is not converted.
- **Flags:** `{band}_slope_fit_failed` is NULL where `{band}_nObs = 0`. With observations but no usable point, the flag stays True, as now.
- **Failure:** the writer fails on a NULL in a non-nullable column.
- **The numbers do not change:** every non-NULL value is bitwise as before.

### 3. Delivery checks (`ssp.delivery_check`)

- **Elongation:** on SSObservation rows with non-NULL `topo_*` and `helio_*`, recompute the elongation from delivered columns and compare it with `elongation`; they must match to within 0.01°.
  - The recomputed elongation is the angle between the observed (`ra`, `dec`) and `topo − helio`, both aberrated by `helio_v − topo_v` (the observatory's velocity relative to the Sun).
  - The check reports the count and the maximum difference, plus, for information, the number of rows where the angle to `topo` would differ.
- **No NaN:** no float column of any delivered table may hold a NaN. If a table from the MPC replica (`mpc_orbits`, the identifications) has NaN in real data, the work package reports it before deciding the scope.

## Loaders

- **mppdb:** every ClickHouse column is `Nullable`, and the ingest config sets no `nan_to_null`, so SSObject's NaN currently loads as NaN. NULLs load with no change to the loader. The mppdb session is told before the pin moves.
- **PPDB / BigQuery:** the columns are nullable as in Felis. The new SSObject has the same shape as SSObservation, which already loads.

## Validation

**Tests (network-free):**
- **Elongation:** `tests/data/elongation/horizons_sot.csv` gives 319 Horizons S-O-T values for 81 objects. The function, given Horizons' own RA and Dec, must reproduce them to within 0.001°. The current code fails this test.
- **Elongation, other cases:**
  - the `dt_s` shift agrees with exact evaluation;
  - a case with known geometry, e.g. a target at the Sun's own position gives about 0°.
- **SSObject:**
  - NULLs appear where the values were NaN before;
  - the flags are NULL for bands without observations;
  - the Arrow nullability matches Felis;
  - every other value is unchanged.
- **Delivery checks:** each new check fails on a broken file and passes on a correct one.

**Real data:**
- Rebuild the 2026-11-16 SSObservation and SSObject from its inputs on a scratch root.
- Compare with the delivery:
  - elongation is the only SSObservation column that changes;
  - SSObject's values are bitwise the same where they were finite;
  - NaN became NULL.
- Run the full delivery check, with the new checks.

## Implementation plan

- **Wave 1 (one WP):** sections 1–3 with their tests.
- **Review:** an independent review of the WP: probes, comparison with Horizons and with the vectors, and the real-data rebuild.
- **Then:** merge to master, after the owner's approval.
- **ssp-daily:** moves the pin, after the owner's approval as usual, and tells the mppdb session first.

## Record

Integrated 2026-11-17: WP PR #123, merged into this branch as 034a611.

**Review.** An independent review ran in two rounds.
- Round 1 found two low-severity items, both fixed: scalar and array inputs no longer broadcast against each other, and the documentation was out of date.
- Round 2 found nothing.
- The reviewer computed everything independently, with jplephem DE440, erfa for the site, and `erfa.ab` for exact aberration:
  - production agrees with it to 5e-4″;
  - Horizons' S-O-T to 0.18″ (its printed precision) on the 319 fixture rows.

**Tests.** The full suite passes: 1501 tests. Run offline (no network, no ASSIST), 1410 pass and 85 skip.

**Real data.** 2026-11-16 was rebuilt on a scratch root with `ssp-build-sso --workers 32`, using the same inputs plus the 2026-11-17 IERS-A file. It was rebuilt twice, once with the new code and once with the old code (c181bb5).

SSObservation:
- `elongation` is the only column that differs, on all 8,129,545 rows. The sidecar is identical.
- Old minus new: median 89.6°, range −64° to 174°.

Elongation, against independent computations:
- Apparent minus geometric: up to ±41.6″, median |diff| 35″.
- A sample of 12,000 rows, against the reviewer's independent apparent elongation: at most 0.028″, i.e. float32 resolution.
- A sample of 20,004 rows, against astropy (`get_body('sun')` in the site's GCRS): at most 0.42″.

SSObject:
- 7,515,591 NaN became NULL, and all 10,166,119 finite float values are bitwise equal.
- 699,781 `slope_fit_failed` flags are NULL, exactly where `nObs` = 0; the other 1,098,359 are unchanged.
- The non-nullable fields are exactly the 15 Felis `nullable: false` columns.

`delivery_check`, all 6 tables pass:
- The elongation check covers 8,067,419 rows. The largest difference is 2.68″, against a 0.01° limit: it comes from the float32 vectors of objects at 66–89 AU, and from light-time.
- 111 rows, from 15 objects, are more than 0.01° from the angle computed with `topo`. The check reports them for information. Their orbit predictions are 36″ to 178° from the observations.
- The original delivery fails both new checks: elongation on 8,067,171 rows, and SSObject no-NaN in 51 columns.
- A geometric elongation fails the elongation check on 2.96 M rows, so the check tells the two definitions apart.

Cost:
- `solar_elongation_ndarray` on 8.1 M rows takes 16.8 s, against 17.6 s before.
- The ssobservation step's peak RSS rises by about 0.2–0.7 GB, from 14.4 GB.

**Open (optional).** The elongation check's tolerance could be tightened to about 5″. That would still pass 2026-11-16, and would also catch a geometric/apparent mix-up.
