# Double-precision positions, ranges and arc

Status: **approved** (2026-11-18); implementation in progress.

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
| 2026-11-18 | **Release:** earlier deliveries stay as they are; the owner approves the master merge; ssp-daily times the pin. Relayed by ssp-daily. |
| 2026-11-18 | **D1, approved. Derived values computed from the float64 ephemeris.** The range rates, `ephVmag`, NearbySSO's `vmag`, and through the ranges SSObject's fits, would be computed from the float64 vectors instead of float32-rounded ones. Their stored values would change at the float32 rounding level, and the fits by about that much. Otherwise the new double ranges would be upcast float32, and V would be computed from less precise inputs than the ones delivered. |
| 2026-11-18 | **D2, approved. The elongation check's tolerance.** The rounding term uses the epsilon of the stored type: 2⁻²⁴ for float32 vectors, 2⁻⁵³ for float64. The same code then checks both old and new deliveries correctly. For new deliveries the tolerance becomes 1″ plus the light-time term, about 1″ nearby and up to ~2″ at 90 AU. |

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
