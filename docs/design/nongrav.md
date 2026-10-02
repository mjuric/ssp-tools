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
- **Variational equations:** they include the non-gravitational terms, both the partials with respect to the state and those with respect to A1–A3 (checked by WP N2 and its review against the 1.2.3 source; see "WP N2 results").

**Data: `mpc_orbits` (2026-10-01).**
- **Comet-style designations: 4,608.**
  - **2,527** have no elements and no JSON: placeholders, which can't be computed.
  - **138** are `S/` natural satellites, not comets, and are excluded.
  - **27** are `A/` objects, asteroids on comet-like orbits.
  - **About 2,080 have orbits,** but that count includes the 138 `S/` satellites. NearbySSO keeps **1,942 comets** (1,188 C/, 728 P/, 26 A/; WP N3).
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
| **N1 precise pass** | `ephem_assist._propagate_one` and `compute_ephemerides_one` take the non-grav coefficients and g(r) (`particle_params`; `alpha`, `r0`, `nm`, `nn`, `nk`). (As built, per the contract: the self-perturber paths ignore non-gravs, since none of ASSIST's 16 perturbing asteroids or Pluto has a non-grav fit.) Gravity-only orbits must stay **bitwise unchanged**. SSSource reads the coefficients for every object. | yes |
| **N2 coarse pass and uncertainty** | `nearbysso.propagate.coarse` with the non-gravs. The covariance includes the fitted A's (the 8×8 or 7×7 CAR block). Φ is extended with ∂state/∂A by finite differences, or by the variational equations if ASSIST covers non-gravs; that has to be checked first. `orbits.load_orbits` carries the A's and the full covariance. | yes |
| **N3 NearbySSO comets** | Remove the comet exclusion from the orbit filter. Keep the `S/` satellites and the element-less placeholders out. Check the candidate tolerance for comets: near-parabolic and hyperbolic orbits, close approaches. | light |
| **N4 validation (black box)** | SSSource offsets before and after, for the comets and Yarkovsky asteroids, against the gravity-only controls. A Horizons spot check with MPC elements plus non-gravs, if Horizons accepts them for user-supplied elements (strictly serial). The uncertainty against a Monte Carlo over the fitted A's. | — |

**Last (integrator):** a full daily rerun with every check, results recorded here.

**Out of scope:** comet magnitudes. `ephVmag` from HG stays wrong for active comets, and SSObject's HG12 fits for comets aren't meaningful; to be decided separately.

## WP N1 results (precise pass, 2026-10-01)

- `ephem_assist._propagate_one` and `compute_ephemerides_one` take `nongrav=` and call `ssp.nongrav.apply` right after the ASSIST Extras is attached. Gravity-only orbits never call it.
- SSSource reads each object's NonGrav from `mpc_orb_jsonb` (`ssp.sssource.load_nongravs`). That adds 5–15 s to a full run.
- **ASSIST 1.2.3's force, checked by the independent review against the installed library:**
  - the frame is heliocentric RTN, with A1 along r̂, A2 along ĥ×r̂ (positive in the direction of motion) and A3 along ĥ;
  - the acceleration matches A_i·g(r) to ~5e-13 relative;
  - the `particle_params` setter keeps its own copy, so a temporary array is safe.
- **Fixture** (`/sdf/data/rubin/user/mjuric/nongrav/fixtures/2026-10-01/`, 370 objects):
  - the control and gravity-only comet rows are bitwise identical to the gravity-only reference in all 180 columns;
  - ephOffset:

| class | objects | rows | median | p95 | max |
|---|---|---|---|---|---|
| comets with a non-grav fit | 15 | 205 | 0.651″ → 0.360″ | 3.005″ → 0.750″ | 6.389″ → 0.962″ |
| Yarkovsky asteroids | 24 | 290 | 0.076″ → 0.074″ | 0.279″ → 0.276″ | 0.943″ → 0.995″ |
| comets without one (unchanged) | 31 | 456 | 0.252″ | 1.196″ | 356.2″ |
| controls (unchanged) | 300 | 7,493 | 0.068″ | 0.301″ | 5.620″ |

- **Per object:**
  - the largest gains are P/2003 K2 (median 5.97″ → 0.60″), P/1973 S1 (3.02″ → 0.55″) and P/2005 N3 (2.13″ → 0.38″);
  - eight comets move by less than 1 mas: they are observed far from the Sun or close to their epoch;
  - Yarkovsky changes are at the mas level, as expected for observations within about a year of the epoch.
- **The MPC fits every "yc" comet with the standard Marsden g(r)** (the open assumption of the units check). The comet offsets support this: no comet gets worse, and the large residuals collapse.
- **Data to watch:**
  - 2025 QH138's Yarkovsky A2 is −4.1e-10 au/d² (σ 4.1e-10), about 1000× typical, at q = 3.2 au;
  - P/2010 J5's A1 is about 3% of solar gravity at its q = 3.7 au.
  - Both are applied as the MPC fits them.

## WP N3 results (comets in NearbySSO, 2026-10-01)

- **Filter:** `filter_masks` drops only `S/` natural satellites, whose elements are heliocentric two-body fits that can't follow the motion about the planet.
  - C/, P/, D/ and A/ are kept. A/ objects are inactive, with ordinary orbits and no non-grav fits.
  - Element-less placeholders still drop through `has_elements`.
  - Numbered periodic comets appear under their provisional primary designation ("P/1991 T1" is 145P).
- **Robustness:**
  - The vectorized and scalar element conversions agree to 8e-14 on all 1,942 comets. The catalog reaches e = 6.12 (C/2025 N1, 3I/ATLAS), with 306 orbits at e > 1, 378 with |1−e| < 1e-3, and q up to 14 au.
  - The hyperbolic Kepler solver's arcsinh(M/e) start diverges for e → 1 at small |M|. Both the vectorized and the scalar solver now fall back to Danby's start. No 2026-10-01 orbit needs the fallback.
- **Candidates:** a brute force of every comet at every visit (1,942 × 32,845) found **no missed candidate**. The largest gap between the precise position and the coarse track's extrapolation is 23.5″ (C/2025 N1), well inside the 90″ margin.
- **Sungrazers:** five Kreutz-type sungrazers with q < 0.02 au fail the coarse pass by design (`propagate._Q_MIN_AU`). None falls in a visit today. See issue #62.
- **Comets-only NearbySSO on the DiaSources SSSource is built from (gravity-only):**
  - 435 of 462 gravity-only comet SSSource rows, and 199 of 205 non-grav ones, have a NearbySSO row for the same object, with NearbySSO's offset equal to SSSource's within 1e-5″.
  - Every miss is explained: P/1999 RO28 (356″), P/1994 P1 (62″), P/2025 OZ695 (no usable orbit), and P/2003 K2 at 6″, which the non-gravs bring to 0.6″.

## Integration (2026-10-01)

- NearbySSO's precise pass gets each orbit's NonGrav (`ssp.nongrav.from_orbit`). The contract named this wiring, but no WP owned it; `tests/test_nearbysso_build.py::test_precise_pass_gets_nongrav` covers it.
- The scalar `ephem_assist.solve_kepler_hyperbolic` got N3's fallback. Converged results are unchanged.
- Merged into `nongrav`: N3 (#60), N1 (#61), N2 (#63).

## WP N2 results (coarse pass and uncertainty, 2026-10-01)

**ASSIST 1.2.3 covers the non-gravs in its variational equations** (`assist_additional_force_non_gravitational`, `src/forces.c`):
- each `testparticle` variation gets the non-grav acceleration's partials with respect to position and velocity, plus `dA1·∂a/∂A1 + dA2·∂a/∂A2 + dA3·∂a/∂A3`;
- `particle_params` holds one (A1, A2, A3) triple per real particle, then one per variational configuration: configuration v reads its (dA1, dA2, dA3) from `particle_params[3·(N_real + v)]`;
- so `coarse` adds three variational particles seeded at zero, with unit dA's, and gets ∂state/∂A directly; the six state variations carry dA = 0;
- a particle whose three A's are all 0 is skipped altogether, variations included. 2018 CW2's fitted A2 is exactly 0, so for it one fitted A is set to 1e-300 (no effect on the orbit).

**Checks** (`tests/test_nearbysso_nongrav.py`; scripts and outputs in `/sdf/data/rubin/user/mjuric/nongrav/work/n2/`):
- ∂state/∂A against central differences in A: agrees to 1e-6–1e-8 at steps of 1000σ(A). At 1σ the plain integrations' own noise limits the difference to ~1e-3.
- Phi's non-grav part (P/2010 J5, the largest A1: 2e-2 at a year) against central differences in the state: agrees to 2e-5.
- The states are bitwise those of a plain integration with `ssp.nongrav.apply`. The variational particles don't change IAS15's steps.
- Gravity-only orbits are bitwise unchanged against the previous code: `coarse` on 1,000 catalog orbits, and `load_orbits` on the full catalog, with and without the filter.

**Monte Carlo** (500 draws of (state0, A) from `cov_full`, integrated with ASSIST). The table is the ratio of the sample σ to the predicted σ: the largest position σ, and σ_major on the sky seen from the geocentre.

| set (samples) | times | MC / C(t), position: median (range) | MC / C(t), sky: median (range) | MC / state-only (Φ cov0 Φᵀ), sky: range |
|---|---|---|---|---|
| fixture comet_ng (15 orbits, 168 times) | epoch ±30, 182, 365 d; up to 6 SSSource epochs each | 1.00 (0.93–1.08) | 1.00 (0.92–1.08) | 0.26–1.27 |
| fixture yarkovsky (24, 264) | the same | 0.99 (0.92–1.08) | 1.00 (0.94–1.08) | 0.77–1.10 |
| the 10 comets of the catalog where the A's matter most (100) | ±30 d to ±3 yr | 1.01 (0.93–1.04) | 1.01 (0.93–1.05) | 0.09–15.7 (P/1983 V1, +3 yr) |
| the 10 such Yarkovsky orbits (100) | ±30 d to ±3 yr | 0.98 (0.94–1.04) | 0.98 (0.94–1.05) | 0.29–3.5 |

With 500 draws the sampling error of a σ is ~3%. The mean sample offset is under 0.11σ everywhere, so the problem stays linear. The state-only covariance is wrong in both directions, by up to 16× too small and 10× too large, because the state–A correlations can shrink σ as well as grow it.

**Load and runtime:**
- `load_orbits` on the full catalog: +1 s (14.0 s against 12.8 s), for the scan and parsing the 638 non-grav rows. Its peak RSS rises from about 9.6 to 10.7 GB, because `cov_full` (9×9 doubles) is written for every row.
- **Selecting the non-grav rows:** a row is parsed when its JSON matches either the `non_gravs` flag (`"non_gravs"\s*:\s*true`) or a CAR coefficient name after `vz` (`"vz"\s*,\s*"`). Both are regular expressions, so compact or indented JSON is found too. A row where the two disagree is parsed anyway, with a warning naming it. On the 2026-10-01 catalog both mark the same 638 orbits. The test is shared with SSSource's `load_nongravs` (`ssp.nearbysso.orbits.nongrav_marks`).
- `coarse`, 31 nightly samples: comets 2.8 against 2.2 ms per orbit, Yarkovsky 11.8 against 9.4 ms, both ×1.25.
- SSSource's error ellipses (`ssp.sssource_ellipse`, through `coarse`) now include the A's for the 638 non-grav orbits.

## Open (for the plan)

- ~~**Covariance:** the error ellipse uses the 6×6 state block only.~~ Done in WP N2: the A's are in the covariance, through ASSIST's variational equations.
- **Validation:** WP N4, the black-box harness (`bench/nongrav_validate.py`), and the full daily rerun.
- **Comet magnitudes:** `ephVmag` from HG is wrong for active comets, and SSObject's HG12 fits for comets aren't meaningful. To be decided.
