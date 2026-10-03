# Design: tail position angles in SSSource and NearbySSO

## Context

Comets' ion and dust tails point, to a first approximation, along two directions that follow from geometry alone:
- the **anti-Sun direction:** the extension of the Sun→object vector, along which the ion/gas tail points;
- the **negative heliocentric velocity:** the direction the dust trail lags towards.

JPL Horizons publishes both as observer-table quantity 27 (`PsAng`, `PsAMV`).

Adding their position angles to SSSource and NearbySSO lets users:
- tell a comet's tail or coma DiaSources from unrelated sources;
- orient cutouts;
- study activity.

Both angles are defined for every object, so they're filled for asteroids too, which helps with main-belt comets and active asteroids.

This is the first step of the comet work discussed on 2026-10-03. Comet magnitudes, Afρ and activity measures are later steps.

## Owner decisions (2026-10-03)

| item | decision |
|---|---|
| Columns | The two position angles only. `ephTimeFromPerihelion` was considered and dropped. |
| Tables | SSSource **and** NearbySSO. |
| Objects | All objects with an orbit, comets or not. |
| Schema | Changed on the existing `sdm_schemas` branch `tickets/DM-55375` (lsst/sdm_schemas#549). |
| Names and type (2026-10-03) | `ephAntiSunPA` and `ephAntiMotionPA`, both float32 (Felis `float`). |
| Plan (2026-10-03) | Approved, with ICRF north, and filled at every phase angle (no opposition cut). |

## Outputs

Two new nullable `float` (float32) columns, in degrees, in both tables. In SSSource and NearbySSO they go right after `ephRateDec`.

| column | definition (Horizons quantity 27) | UCD |
|---|---|---|
| `ephAntiSunPA` | Position angle of the extended Sun→object radius vector, projected onto the sky at the object, measured from north through east, in [0, 360). Horizons `PsAng`. | `pos.posAng;pos.ephem` |
| `ephAntiMotionPA` | Position angle of the negative of the object's heliocentric velocity vector, projected the same way. Horizons `PsAMV`. | `pos.posAng;pos.ephem` |

Names and type as decided above.

**Conventions,** chosen to match Horizons, so that users can compare directly:
- The vectors are the ones already published in SSSource's `helio_*` columns: at light-emission time, relative to the apparent Sun (`EphResult.helio_pos`/`helio_vel`).
- They are projected onto the plane of the sky at the object's astrometric direction (`EphResult.topo_pos`).
- North is the ICRF pole, as for `ephRa`/`ephDec`.
- Horizons may report `PsAng`/`PsAMV` in the equator of date. If so, its angles differ from ICRF ones by up to about 0.4° in 2026, from precession. The validation must settle which frame Horizons uses, and the column descriptions must state ours.

**NULL** where the row has no orbit, like every other `eph*` column.

**Near opposition** (phase angle → 0°) or conjunction, the Sun→object vector lies along the line of sight and `ephAntiSunPA` becomes ill-conditioned. We still fill it: it is what Horizons does, and `phaseAngle` tells users when to distrust it. The column description says so.

## Approach

- One shared function, `ssp.ephem_assist.tail_position_angles(helio_pos, helio_vel, topo_pos) -> (anti_sun_pa, neg_vel_pa)` on (3, N) arrays.
- Project each vector onto the tangent plane at `topo_pos`, then take `atan2(east, north)`.
- SSSource calls it in `compute_sssource_entry`, and NearbySSO in its precise pass. Both already have the `EphResult`, so no new integration is needed.
- **Cost:** negligible, about 2 × 4 bytes per row, i.e. about 65 MB for SSSource's 8M rows before compression.

## Validation

- **Unit tests** on synthetic geometry, with known answers:
  - a vector pointing due north or due east;
  - the pole;
  - the RA wrap-around;
  - an opposition case.
- **Against Horizons quantity 27** for about 8 objects (comets and asteroids, a range of phase angles), at real Rubin times, with JPL's own orbits so that only the geometry is tested. The comparison reuses `bench/jpl_compare.py`'s serial, cached client. It costs about 10 requests, strictly serial.
  - **Pass:** agreement to better than 0.01° away from opposition, after settling the frame.
- **Consistency:** SSSource's and NearbySSO's angles agree at the same DiaSource. Every other column of both tables is bitwise unchanged on the full 2026-10-01 rerun.
- **The delivery checks** (`delivery_check`, `sssource:conformance`) pass with the new schema.

## Implementation plan

**First, the integrator:**
- add the two columns to `sso_base.yaml` on `tickets/DM-55375` (local commit);
- refresh `tests/data/sdm_schemas/` and regenerate `ssp/schema_ppdb.py`;
- the contract: the definitions and the shared function's signature, in `ssp/sssource_contract.py` and `ssp/nearbysso/_contract.py`.

**Then, in parallel:**

| WP | builds | review |
|---|---|---|
| **T1 the angles** | `tail_position_angles`, with SSSource and NearbySSO filling the columns; unit tests; a fixture run. | light (T2 is the independent check) |
| **T2 validation (black box)** | A `bench/jpl_compare.py` subcommand against Horizons quantity 27, including the frame question, plus the SSSource–NearbySSO consistency check. Built from the contract alone. | — |

**Last, the integrator:**
- the full 2026-10-01 rerun with every check;
- results recorded here, and a runbook note;
- **push** the `sdm_schemas` branch, which updates lsst/sdm_schemas#549, and tell the DM-55678 owners that two columns were added;
- the PR to master, for the owner's approval.

## Open

- Which frame Horizons reports `PsAng`/`PsAMV` in (settled by T2); ours is ICRF.
