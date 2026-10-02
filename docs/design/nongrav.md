# Design: non-gravitational forces (comets and Yarkovsky asteroids)

Status: **in progress.** The scope is decided (2026-10-01); the units check comes first, and the implementation plan follows it, for approval.

## Context

The ephemerides (`ssp.ephem_assist`, for SSSource and NearbySSO's precise pass; `ssp.nearbysso.propagate`, the coarse pass) integrate every orbit with gravity only.
- **NearbySSO** excludes comets altogether.
- **SSSource** carries comets, but without their non-gravitational accelerations.

Measured on the 2026-10-01 daily run (observed − predicted, SSSource `ephOffset`):

| | objects | median | p90 | max |
|---|---|---|---|---|
| comets, no non-grav fit | 31 | 0.25″ | 0.66″ | 356″ (P/1999 RO28; P/1994 P1 62″: orbit or linkage, not non-gravs) |
| comets with a non-grav fit | 15 | 0.65″ | 2.1″ | 6″ |
| asteroids with a Yarkovsky fit | 24 | 0.076″ | 0.20″ | 1″ |
| other asteroids | ~297k | 0.060″ | 0.18″ | — |

## What is there

**Software: ASSIST 1.2.3.**
- **Per-particle coefficients:** Marsden-style A1, A2 and A3 via `extras.particle_params`. The `NON_GRAVITATIONAL` force is on by default.
- **g(r):** one per simulation (`alpha`, `r0`, `nm`, `nn`, `nk`), g(r) = alpha·(r/r0)^−nm·(1 + (r/r0)^nn)^−nk. Its default (1, 1, 2, 5.093, 0) is 1/r², which suits Yarkovsky. Comets need the Marsden (1973) water-ice g(r): alpha 0.1112620426, r0 2.808 AU, nm 2.15, nn 5.093, nk 4.6142. We integrate one orbit per simulation, so g(r) can be set per object.
- **No DT** (the asymmetric time delay). No current MPC orbit uses DT.
- **Not yet verified:** whether ASSIST's variational equations include the non-gravitational terms.

**Data: `mpc_orbits` (2026-10-01).**
- **Comet-style designations: 4,608.**
  - **2,527** have no elements and no JSON: placeholders, which can't be computed.
  - **138** are `S/` natural satellites, not comets, and are excluded.
  - **27** are `A/` objects, asteroids on comet-like orbits.
  - **About 2,080 have orbits.**
- **Non-grav fits: 184 comets** (A1 and A2, model "yc", never A3 or DT) and **454 asteroids** (one Yarkovsky coefficient, model "yarkovski" or "yarkovsky").
  - The values and their covariance are in `mpc_orb_jsonb.CAR`: `coefficient_names` beyond `x…vz`, `coefficient_values`, and an 8×8 or 7×7 `covariance`.
  - The `a1`/`a2`/`a3`/`dt` columns are populated for only 40 comets, so the JSON is the source.
- **No comet total-magnitude parameters** (M1, K1): only H, G (`magnitude_data`).

## Owner decisions (2026-10-01)

| item | decision |
|---|---|
| Scope | Comets **and** the Yarkovsky asteroids. |
| NearbySSO | **Comets enabled** (the comet exclusion removed). The `S/` natural satellites and the element-less placeholders stay out. |
| Comets without non-grav fits (~1,900) | Integrated with gravity only. |
| First step | Confirm the units and definitions of the MPC's coefficients. |

## Step 1: the units check

For a few objects with both an MPC non-grav fit and a JPL fit, compare the MPC's coefficients (`CAR.coefficient_values`) with JPL SBDB's (`sbdb.api?…&phys-par`, the `model_pars`). This establishes:
- the units of A1 and A2 (AU/day² expected);
- the MPC Yarkovsky coefficient's definition and normalization, against JPL's A2 with g = 1/r²;
- that "yc" with DT absent means the standard Marsden g(r).

The queries to SBDB are serial and polite. The results go below.

## Open (for the plan)

- **Covariance:** the error ellipse uses the 6×6 state block only. Propagating the A1/A2 (or Yarkovsky) partials is needed so that σ isn't understated. The options are finite differences in the parameters, or the variational equations if ASSIST covers them.
- **Validation:** Horizons with MPC elements and non-gravs (if Horizons accepts non-gravs with user-supplied elements), or the change in the offsets above.
- **Comet magnitudes:** `ephVmag` from HG is wrong for active comets, and SSObject's HG12 fits for comets aren't meaningful. To be decided.
