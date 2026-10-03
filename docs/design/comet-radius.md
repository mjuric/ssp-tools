# Design: a larger NearbySSO match radius for comets and ISOs

## Context

NearbySSO matches each DiaSource to the nearest eligible predicted position within `MATCH_RADIUS_ARCSEC` = 5″, the same radius for every object. Comets and interstellar objects (ISOs) call for a larger one:
- **Their positions are less well known.** Non-gravitational forces and comet astrometry make their orbits worse. In the 2026-10-01 SSSource, comet offsets are a median 0.29″ (p95 0.90″), about 5× the asteroids' (0.06″, p95 0.26″). They also run 3–18× the predicted σ, so the error ellipse doesn't capture the extra error.
- **They're extended.** The median `extendedness` of comet detections is 1.00, against 0.04 for asteroids. A comet's DiaSource centroid can sit off the nucleus, and its coma or tail can produce extra DiaSources.

## Owner decisions (2026-10-03)

| item | decision |
|---|---|
| Which objects | Comets and ISOs, by designation: the unpacked primary provisional designation starts with `C/`, `P/`, `D/` or `I/`. ISOs carry comet designations (3I/ATLAS is C/2025 N1 in `mpc_orbits`), so no eccentricity criterion is needed. `A/` objects (inactive, on comet-like orbits) and `S/` satellites (already excluded from NearbySSO) keep the asteroid rules. |
| Radius | One radius for comets and ISOs. The owner's prior was 15″, to be checked by measurement (below). |
| Measure first | Yes. |

## The measurement (2026-10-03)

- **Method:** NearbySSO for the 1,942 comets only, with the match radius raised to 60″, on the 2026-10-01 PPDB DiaSources: 13.1M DiaSources, complete for each of 4,422 visits.
- **Size:** 730 eligible comet predictions, 344 matches within 60″.
- **Background:** the chance density, from the 30–60″ ring, is 3.0e-5 matches per prediction per arcsec².
- **Outputs and script:** `/sdf/data/rubin/user/mjuric/comet-radius/measure/`.

**P/2002 T6: a comet missed entirely at 5″.**
- **Found:** 92 of its DiaSources sit at 6.6–6.9″ from its prediction, over three nights.
- **They are the comet:** they move with the prediction at 20.8″/h.
- **Coma or tail pieces:** 34 more of its DiaSources, mostly second DiaSources of the same visits (`diaDistanceRank` 2), sit at 11–20″.
- **Never submitted:** it has no SSSource rows, so none of these detections has been reported.
- **The 6.8″ is a systematic error in the MPC orbit,** constant across the nights.

**How likely a match is to be real, by radius** (cumulative), with P/2002 T6 left out so that it doesn't dominate:

| radius | matches | expected by chance | fraction real | chance match per comet prediction |
|---|---|---|---|---|
| 5″ | 7 | 0.6 | 0.91 | 0.2% |
| 10″ | 9 | 2.5 | 0.72 | 0.9% |
| **15″** | 12 | 5.7 | **0.53** | 2.1% |
| 20″ | 14 | 10.1 | 0.28 | 3.8% |
| 30″ | 19 | 22.7 | ≈ 0 | 8.5% |

**Conclusion: 15″.**
- It's the largest radius at which a match is still more likely real than chance.
- It recovers P/2002 T6's main detections.
- Beyond 15″ chance matches dominate.

The sample is small: besides P/2002 T6, 26 comets and about 12 real matches. So 15″ is supported, not finely tuned. Rerunning the measurement on more nights would firm it up (see "Validation").

## The change

- **Contract** (`ssp/nearbysso/_contract.py`):
  - `MATCH_RADIUS_COMET_ARCSEC = 15.0`;
  - `match_radius(designation) -> float`: 15″ for the comet designations above, else `MATCH_RADIUS_ARCSEC` (5″);
  - `MATCH_RADIUS_ARCSEC` keeps its value and meaning for every other object.
- **Matching** (`ssp/nearbysso/build.py`, pass 3): each prediction is matched within its own object's radius.
  - One way to do it: match every prediction at the larger radius, then drop the matches beyond the prediction's own radius. This keeps `DiaIndex.match`'s scalar interface.
- **The nearest-object reduction is unchanged:** it compares separations in arcsec. A DiaSource 4″ from an asteroid and 10″ from a comet stays with the asteroid.
- **`diaDistanceRank`:** its pool is all of the visit's DiaSources within *that prediction's* radius. So a comet's rank counts the DiaSources within 15″ of it, and its coma and tail fragments get ranks 2, 3 and so on.
- **Unchanged:** the σ gate (`SIGMA_MAX_ARCSEC` = 10″), and the candidate margin (90″). N3's worst coarse-to-precise gap was 23.5″; with a 15″ radius that makes 38.5″, well inside the margin.
- **Asteroid rows don't change.** A DiaSource gains a comet row only if no asteroid is within 5″ of it, or the comet is nearer than that asteroid. So every row that names an asteroid stays bitwise identical. The comet radius can only add comet rows, or move a DiaSource from a farther asteroid to a nearer comet; at most a handful of DiaSources should move this way.
- **Schema** (`sso_base.yaml` NearbySSO, on `tickets/DM-55375`): text only.
  - The table description gives the radii: 5″, and 15″ for comets and ISOs (designations C/, P/, D/, I/).
  - The `diaDistanceRank` description says "within the object's matching radius".
  - No new column: the designation identifies the class.
- **SSSource is unaffected:** its associations are the MPC's.

## Validation

- **Unit tests:**
  - `match_radius` for each prefix, including A/, S/ and asteroids;
  - a comet matched at 10″, an asteroid not matched at 10″;
  - the comet's rank pool at 15″;
  - the nearest rule between a comet and an asteroid.
- **The full 2026-10-01 rerun:**
  - every pre-existing asteroid row is bitwise unchanged;
  - the added comet rows are listed by comet and offset;
  - P/2002 T6's detections at about 6.8″ appear;
  - any DiaSource that moves from an asteroid to a comet is listed and explained.
- **The harnesses use the per-class radius:**
  - `bench/nearbysso_validate.py`: its brute force and its explanation of misses;
  - `bench/nongrav_validate.py nearbysso`.
- **Optional, for the owner:** rerun the 60″ measurement on more nights (e.g. a DP2 slice of the PPDB, if available) to confirm 15″.

## Implementation plan

**First, the integrator:**
- the contract (`MATCH_RADIUS_COMET_ARCSEC`, `match_radius`, the `diaDistanceRank` rule);
- the schema text on `sdm_schemas` (local commit, refreshed copies).

**Then, in parallel:**

| WP | builds | review |
|---|---|---|
| **R1 matching** | Per-prediction radius in pass 3 and `diaDistanceRank`; the report counts comet matches; unit tests; a fixture run (the non-grav fixture plus P/2002 T6's visits). | light, plus the rerun's bitwise check |
| **R2 harnesses (black box)** | `bench/nearbysso_validate.py` (brute force, `filter_reason`, the explanation of misses) and `bench/nongrav_validate.py nearbysso` use `match_radius`. Tests with a comet at 10″. | light |

**Last, the integrator:**
- the full 2026-10-01 rerun with every check, and the comparison above;
- results recorded here, and a runbook note;
- push `sdm_schemas` (text only) and tell the DM-55678 owners;
- the PR to master, for the owner's approval.

## Related, not in scope

P/2002 T6's 92 unsubmitted detections are good candidates for MPC submission, through ssp-submit's own process. That is outside ssp-tools; it's for the owner to raise there.
