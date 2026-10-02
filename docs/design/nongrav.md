# Design: non-gravitational forces (comets and Yarkovsky asteroids)

Status: **proposed.** The scope is decided and the units check done (2026-10-01); the implementation plan is below, for approval.

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

Six serial SBDB queries were made (2026-10-01), comparing the MPC's coefficients (2026-10-01 snapshot) with JPL's:

| object | MPC (`CAR.coefficient_values`) | JPL SBDB (`model_pars`) | result |
|---|---|---|---|
| Apophis (2004 MN4) | `yarkovski` −2.869e-4 | A2 −2.902e-14 au/d²; ALN 1, NM 2, NK 0, R0 1 | ×1e-10 matches (1.1%) |
| 1862 Apollo (1932 HA) | `yarkovsky` −3.647e-5 | A2 −3.657e-15 au/d², g = 1/r² | ×1e-10 matches (0.3%) |
| 29075 (1950 DA) | `yarkovsky` −4.538e-5 | A2 −5.203e-15 au/d², g = 1/r² | ×1e-10, 13% (different fits) |
| 145P (P/1991 T1) | A1 1.869e-9, A2 −1.814e-10 | A1 1.798e-9, A2 −1.826e-10 au/d², default (Marsden) g(r) | same units (4%, 0.7%) |
| C/2022 N2 | A1 7.42e-8, A2 3.93e-10 | A1 1.11e-7, A2 1.32e-9, with a non-standard g(r) (R0 5 au, NK 2.6, NN 3, ALN 0.0408) | not comparable: different models |
| 402P (P/2002 T5) | A1, A2 | no non-grav fit | — |

**Conclusions:**
- **Comets ("yc"):** A1 and A2 are in **au/day²**, with the **standard Marsden water-ice g(r)** (alpha 0.1112620426, r0 2.808 au, m 2.15, n 5.093, k 4.6142). JPL's 145P uses the same g(r) and agrees within a few percent.
- **Asteroids ("yarkovski" or "yarkovsky", both spellings occur):** the coefficient is a **transverse A2 in units of 1e-10 au/day²**, with **g = (r / 1 au)⁻²**, which is ASSIST's default g(r). A1 = A3 = 0.
- **Left open:** that the MPC uses the standard g(r) for every "yc" comet. It can't be checked against JPL where JPL uses another model, so the validation step tests it empirically (the comets' offsets should shrink).


## Implementation plan

The work is split into work packages built by subagents in worktrees, per `CLAUDE.md`.

**First (integrator): the contract.**
- `ssp/nongrav.py`: `nongrav_params(mpc_orb_jsonb) -> NonGrav(A1, A2, A3, model)`, the parsing and units rules above, and `G_OF_R` per model (`"comet"`: Marsden; `"yarkovsky"`: 1/r²).
- The `ORBIT_DTYPE` change: the non-grav coefficients, the model, and the full covariance of the fitted parameters.
- Fixtures: the 184 comets and 454 Yarkovsky asteroids, plus a gravity-only control sample.

**Then, in parallel:**

| WP | builds | independent review |
|---|---|---|
| **N1 precise pass** | `ephem_assist._propagate_one` and `compute_ephemerides_one` take the non-grav coefficients and g(r) (`particle_params`; `alpha`, `r0`, `nm`, `nn`, `nk`), including the self-perturber path. Gravity-only orbits must stay **bitwise unchanged**. SSSource reads the coefficients for every object. | yes |
| **N2 coarse pass and uncertainty** | `nearbysso.propagate.coarse` with the non-gravs. The covariance includes the fitted A's (the 8×8 or 7×7 CAR block). Φ is extended with ∂state/∂A by finite differences, or by the variational equations if ASSIST covers non-gravs; that has to be checked first. `orbits.load_orbits` carries the A's and the full covariance. | yes |
| **N3 NearbySSO comets** | Remove the comet exclusion from the orbit filter. Keep the `S/` satellites and the element-less placeholders out. Check the candidate tolerance for comets: near-parabolic and hyperbolic orbits, close approaches. | light |
| **N4 validation (black box)** | SSSource offsets before and after, for the comets and Yarkovsky asteroids, against the gravity-only controls. A Horizons spot check with MPC elements plus non-gravs, if Horizons accepts them for user-supplied elements (strictly serial). The uncertainty against a Monte Carlo over the fitted A's. | — |

**Last (integrator):** a full daily rerun with every check, results recorded here.

**Out of scope:** comet magnitudes. `ephVmag` from HG stays wrong for active comets, and SSObject's HG12 fits for comets aren't meaningful; to be decided separately.

## Open (for the plan)

- **Covariance:** the error ellipse uses the 6×6 state block only. Propagating the A1/A2 (or Yarkovsky) partials is needed so that σ isn't understated. The options are finite differences in the parameters, or the variational equations if ASSIST covers them.
- **Validation:** Horizons with MPC elements and non-gravs (if Horizons accepts non-gravs with user-supplied elements), or the change in the offsets above.
- **Comet magnitudes:** `ephVmag` from HG is wrong for active comets, and SSObject's HG12 fits for comets aren't meaningful. To be decided.
