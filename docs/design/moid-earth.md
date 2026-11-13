# SSObject's Earth MOID: Earth's real orbit, and a better solver

## Context

SSObject's five `MOIDEarth*` columns come from `ssp/moid.py`, which `compute_ssobject` calls for every object with an `mpc_orbits` row.

**The errors.** The ssp-vet session compared the solver with JPL SBDB on 10 NEAs (2026-11-09):

| object | JPL MOID [au] | ours − JPL [au] |
|---|---|---|
| Ganymed | 0.3419550 | +4.6e-4 |
| Apollo | 0.0263183 | −1.7e-4 |
| Phaethon | 0.0187322 | −1.3e-4 |
| Toutatis | 0.0065060 | +7.0e-5 |
| Apophis | 0.0001079 | −1.3e-5 (12%) |

They have three causes:
1. **The Earth orbit** (most of it): `earth_orbit` is Standish's (1992) linear secular model of the Earth–Moon barycentre's *mean* orbit. JPL uses Earth's osculating orbit. With Earth's osculating orbit from DE440, the same solver agrees with JPL to ≤ 1e-7 au on 9 of 10.
2. **One basin:** the solver zooms only around the best cell of its first grid. Phaethon (e = 0.89) is then still off by 1.2e-5 au, because its true MOID is in another basin.
3. **Hyperbolic orbits:** the grid samples true anomalies beyond the asymptotes, where r < 0. At e = 1, p is NaN.

**ssp-vet's fix.** ssp-vet vendored our solver (`mjuric/ssp-vet` `src/ssp_vet/moid.py`, efcfe4a, attributed to ssp-tools 70ea6a4) and fixed all three:
- Earth from DE440;
- zooming from the 4 closest local minima;
- the physical branch only for e ≥ 1;
- plain constants instead of astropy.

Against SBDB on 12 cases (10 NEAs, 'Oumuamua, Borisov) it agrees to ≤ 4e-7 au, SBDB's own 6-digit rounding.

**Our data** (2026-10-09 delivery):
- all 298,117 SSObject rows have an orbit, from 3 distinct epochs (MJD 60800–61200);
- 4 hyperbolic, none parabolic;
- 79 with e > 0.8, 849 NEOs (q < 1.3), and 218 with MOID < 0.05 au.

## Owner decisions

| Decision (date) | Choice |
|---|---|
| Fix (2026-11-09) | Fix SSObject's MOIDEarth as a project. |
| Speed (2026-11-09) | Execution speed is critical: measure it before and after. |

## Outputs

- **The same five columns,** with the same meaning and units: `MOIDEarth`, `MOIDEarthDeltaV`, `MOIDEarthEclipticLongitude`, `MOIDEarthTrueAnomaly` (Earth's) and `MOIDEarthTrueAnomalyObject`. Only the values change.
- **The four hyperbolic orbits** now get correct values.

## Approach

1. **The solver** (`ssp/moid.py`): take ssp-vet's version back, with its attribution kept.
   - It searches the object's orbit as the first orbit (elliptic, parabolic or hyperbolic) and Earth's as the second (elliptic).
   - It zooms from the `n_basins` = 4 closest local minima of the first 128×128 grid, to an absolute and relative tolerance of 1e-10, in at most 16 levels.
   - It uses plain constants: GM_sun from DE440 and the IAU au.
   - **One change from ssp-vet's version:** the object's orbit is given by q, not a (p = q(1 + e)), so e = 1 is no special case. SSObject has q from `mpc_orbits`; today it computes a = q/(1 − e).
2. **Earth's orbit** (`earth_orbit`): Earth's heliocentric osculating elements at the orbit's epoch.
   - Earth's geocentre minus the Sun, from ASSIST's DE440, rotated to the J2000 ecliptic, with GM_sun only, as ssp-vet and JPL do.
   - Computed once per distinct epoch in the parent (3 today), and passed to the workers.
   - The Standish model is removed.
3. **The column mapping.** The object becomes the solver's first orbit and Earth its second, the reverse of today. `MOIDEarthTrueAnomaly` stays Earth's and `MOIDEarthTrueAnomalyObject` the object's.
4. **Speed.** The per-call cost of ssp-vet's solver is measured at about today's (below), and Earth's elements are computed 3 times rather than per object, so no slowdown is expected. If the end-to-end measurement shows one, the first levers are:
   - fewer basins (zoom only basins whose coarse distance can beat the best);
   - a smaller zoom grid than 128×128 after the first level;
   - looser tolerances (1e-10 au is far below the 1e-7 au the data support).

## Speed: before and after

Measured on the same inputs: the 2026-10-09 delivery, 32 workers, sdfiana031.

| | Before | After |
|---|---|---|
| Per call, one core, 2,000 random SSObject orbits (`/sdf/data/rubin/user/mjuric/moid/bench_solvers.py`) | mean 2.34 ms, median 2.54, p99 2.67, max 3.18 | ssp-vet's solver as is: mean 1.95 ms, median 1.97, p99 3.24, max 14.28 (with the final code: to measure) |
| SSObject's MOID stage, 298,117 objects, 32 workers (`[MOID]` in `ssobject.log`) | 17.8 s | to measure |
| The whole `ssobject` step | 80.0 s, 2.94 GB | to measure |

**Acceptance:** the MOID stage no slower than before (within run-to-run noise, measured over 3 runs each on the same node).

## Validation

- **Tests (network-free):**
  - the analytic cases of `tests/test_moid.py`;
  - ssp-vet's 12 SBDB cases (elements, epochs and JPL MOIDs as data) against Earth from DE440, to max(5e-7 au, 6e-6 × MOID); these skip without the ASSIST data, as the other ASSIST tests do;
  - a hyperbolic and a parabolic orbit;
  - the column mapping.
- **Independent review:** against a brute-force MOID (a dense grid with local optimisation from every grid minimum) on a few thousand real orbits, weighted to high e, low MOID and the hyperbolic ones. No mutation testing.
- **Real data:** a full `ssp-build-sso` run with every check; the before/after distribution of the change in MOIDEarth; the speed table above.
- **Against JPL SBDB:** a sample of real SSObject orbits, at SBDB's own elements and epoch: serial, at least 1 s apart, never in tests or CI. The sample size is an owner decision (below).

## Open decisions

1. **The JPL SBDB sample.** Recommended: 100 objects, about 2 minutes of serial queries. It would include all 4 hyperbolic orbits, the 79 with e > 0.8, and NEOs with MOID < 0.05 au, plus a random draw.

## Implementation plan

- **One work package (WP-M), in a worktree, on the feature branch `moid-earth`:**
  - the solver;
  - Earth from DE440;
  - SSObject's call (q, the mapping, Earth per epoch);
  - the tests;
  - the per-call benchmark before and after.
- **Then an independent review,** as above.
- **Then the integrator's work:** the end-to-end run, 3 timed runs before and after for the MOID stage, the SBDB comparison, the results here and the runbook.
- **Merge to master** with the owner's approval. Then tell ssp-daily, since SSObject's values change, and ssp-vet, which can then use ssp-tools' solver again if it wants.
