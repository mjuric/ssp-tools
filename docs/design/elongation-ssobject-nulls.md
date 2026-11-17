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
| 2026-11-17 | **Elongation uses the geometric Sun direction from the observatory**: DE440 barycentric Sun minus the observatory's barycentric position, at the observation time, in ICRS. |
| 2026-11-17 | **Earlier deliveries, and the int PPDB dataset, stay as they are.** The fix applies from the first run with the new pin. |
| 2026-11-17 | **Approach approved:** NULL for NaN in SSObject, NULL flags for bands without observations, the Felis-derived Arrow schema, and new delivery checks for both defects. |

## Changes

### 1. Elongation (`ssp.util.solar_elongation_ndarray`, `ssp.ssobservation`)

The Sun direction becomes `r_sun(t) − r_obs(t)`:
- `r_sun` is the barycentric position of the Sun from DE440;
- `r_obs` is the observatory's barycentric position, from `util.observatory_barycentric_posvel`, the same function `observer_states` uses;
- the elongation is the angle between this vector and the unit vector of (`ra`, `dec`).

The rest stays as it is:
- the Sun and the observatory are evaluated once per unique time;
- with `dt_s`, the direction is shifted linearly, as now;
- the cost is of the same order.

The result should agree with the elongation computed from the delivered `topo`/`helio` vectors to well under 1″. It applies the same way to rows without an orbit, which have no vectors.

### 2. SSObject NULLs (`ssp.ssobject`)

- **Arrow schema:** SSObject is written with the schema derived from Felis, as SSObservation is: its columns in order, its types, and non-nullable exactly for the `nullable: false` columns.
- **NaN:** in every float column, NaN is written as NULL. ±Inf is not converted.
- **Flags:** `{band}_slope_fit_failed` is NULL where `{band}_nObs = 0`. With observations but no usable point, the flag stays True, as now.
- **Failure:** the writer fails on a NULL in a non-nullable column.
- **The numbers do not change:** every non-NULL value is bitwise as before.

### 3. Delivery checks (`ssp.delivery_check`)

- **Elongation:** on SSObservation rows with non-NULL `topo_*` and `helio_*`, the elongation computed from the vectors must match `elongation` to within 0.01°. The check reports the count and the maximum difference.
- **No NaN:** no float column of any delivered table may hold a NaN. If a table from the MPC replica (`mpc_orbits`, the identifications) has NaN in real data, the work package reports it before deciding the scope.

## Loaders

- **mppdb:** every ClickHouse column is `Nullable`, and the ingest config sets no `nan_to_null`, so SSObject's NaN currently loads as NaN. NULLs load with no change to the loader. The mppdb session is told before the pin moves.
- **PPDB / BigQuery:** the columns are nullable as in Felis. The new SSObject has the same shape as SSObservation, which already loads.

## Validation

**Tests (network-free):**
- **Elongation:** `tests/data/elongation/horizons_sot.csv` gives 319 Horizons S-O-T values for 81 objects. The function must reproduce them to within 0.01°. The current code fails this test.
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

(To be filled in after integration.)
