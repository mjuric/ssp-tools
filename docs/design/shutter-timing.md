# Design: shutter-motion-corrected times in SSSource

## Context

A DiaSource's `midpointMjdTai` is the visit's midpoint. LSSTCam's shutter blades take a finite time to cross the focal plane, so each source's actual exposure midpoint differs from it by up to 0.24 s, depending on where on the focal plane it lies. For a fast-moving object, that time error becomes an along-track position error.

The `shutter-timing` package ([mjuric/shutter-timing#25](https://github.com/mjuric/shutter-timing/pull/25); see its `docs/design/packaging.md`, "Interface to ssp-tools") builds per-night correction tables from the raw data. It also gives the corrected midpoint for any (visit, detector, x, y).

This project makes SSSource carry the corrected time, and computes its ephemerides at that time. It also prepares NearbySSO to follow DiaSource's own time once AP corrects DiaSource.

## Owner decisions (2026-10-05)

The plan was agreed in the shutter-timing session, then reviewed and approved here.

| item | decision |
|---|---|
| Scope | SSSource gets corrected times. NearbySSO evaluates its predictions at each DiaSource's own `midpointMjdTai`, as read (no correction applied by us). ssp-submit is out of scope. |
| SSSource time | `midpointMjdTai` is **overwritten** with the corrected time. The original is kept as the internal column `midpointMjdTaiVisit` in `dia_sources.parquet`, and not published (no warning when it is dropped). |
| NearbySSO time | Read from the input DiaSource's `midpointMjdTai`, so NearbySSO stays consistent with the `DiaSource` it is joined to. Today that is the visit time; once DiaSource carries corrected times, NearbySSO follows automatically. No NearbySSO schema change. |
| New SSSource columns | `midpointMjdTai_flag` (the time is the visit midpoint, not corrected) and `midpointMjdTai_flag_degraded` (corrected, with reduced accuracy). Booleans, **computed** for every row, right after `midpointMjdTai`. Added to lsst/sdm_schemas `tickets/DM-55375` (#549) in `sso_base.yaml` and `ppdb.yaml`. No sidecar file. |
| MPC matching | Correction is applied **before** matching. An `obs_sbn` row matches a measurement if its time is within 10 ms of **either** the visit time or the corrected time; the 1-minute bucket fallback checks both. The basis (`visit`, `corrected`, `both`) is recorded internally per row, with counts in the input manifest. |
| Missing corrections | Status 3 (`NOT_BUILT`): the visit's night has no table, or the night exists but the visit is missing from its exposure log (a late raw). Both mean a stale table: **the run fails.** A visit the table deliberately omits: the visit time, with `midpointMjdTai_flag` = True. A degraded correction: the corrected time, with `midpointMjdTai_flag_degraded` = True. |
| Daily stage 0 | `ssp-sso-daily` first runs `shutter-timing-table --out /sdf/data/rubin/user/mjuric/shutter-timing/corrections --refresh-recent 3` from ssp-tools' venv. It resumes, skips nights already built, re-checks the last 3 nights written and rebuilds any with raws that arrived later (without it, late raws would never be processed), reads only the raw zips under `/sdf/data/rubin/lsstdata/offline/instrument/LSSTCam`, and uses at most 32 workers. A calibration mismatch stops the run with a clear message. |
| Dependency | `shutter-timing`, tag `v0.2.0`, from its private GitHub repository (`[tool.uv.sources]`), installed over HTTPS with the `gh` credential helper; `requires-python >= 3.11`; re-locked. Left as is although ssp-tools is public: a temporary situation. |
| Degraded | Defined by shutter-timing (its contract: `CorrectionStatus`, `DEGRADED_QC`, `DEGRADED_RESIDUAL_S`): any source outside the raytrace table's coverage counts as degraded. Expected on about 13% of post-June 2026 sources, and on 99.7% of earlier sources (whose times come from the header). |
| Provenance | The correction table's `table_format`, `calibration_id` and `package_version` go into the run manifest. |
| PPDB notice | None needed: the owner owns DM-55678. |

Accepted risks:
- The daily run fails when the correction tables lag.
- Every SSSource ephemeris column changes for nearly every row, so the regression against the previous delivery is no longer bitwise.
- The extract's matching rules and the input manifest are in `ssp/delivery_contract.py`, so the contract changes.

## The shutter-timing interface (v0.2.0)

```python
from shutter_timing.corrections import corrected_midpoints
r = corrected_midpoints(visit, detector, x, y, table_dir=..., require_uniform=True)
r.t_mid_mjd_tai   # NaN where not corrected
r.status          # 0 ok, 1 degraded, 2 omitted, 3 not built (night or visit missing)
r.flag, r.flag_degraded, r.table_format, r.calibration_id, r.package_version
```

`x`, `y` are DiaSource pixel coordinates (LSST convention); visit = `day_obs * 100000 + seq`. Raises `TableFormatError` for an unknown format; under `require_uniform`, a table that mixes calibrations is refused.

## The approach

**SSSource (extract stage).** `ssp-extract-sso-inputs` corrects the times of the measurements it resolves, before matching them to `obs_sbn`:
- `dia_sources.parquet` gets the corrected `midpointMjdTai`, `midpointMjdTaiVisit`, the two flags, and the matching basis;
- status 3 (night not built) fails the run.

The build copies the time and the flags into SSSource. It computes every ephemeris column at the corrected time, as it does today for whatever time a row carries.

**NearbySSO.** Candidate selection and the match within the radius still use a visit-level prediction. But each matched row's prediction (position, ellipse, rates, angles, magnitude) is evaluated at that row's own `midpointMjdTai`.
- **Today** every source of a visit carries the visit time, so the output must be **bitwise unchanged**.
- **The fix it replaces:** today `build_visits` warns and takes the median when a visit's sources' times differ by more than ~86 ms. With per-source times, that median would silently misplace the predictions; this replaces it.
- **What it needs:** no correction step, no new input columns, no schema change.

**SSSource vs. NearbySSO.** Until AP corrects DiaSource times, the two tables predict at different times for the same detection. Their positions differ by about the object's rate × the shift (≤ 0.24 s), up to a few tens of mas for fast objects. Each table is consistent with its own time: SSSource carries `midpointMjdTai`, and NearbySSO goes with `DiaSource`'s. The checks that compare the two (`bench/tail_angles_validate.py consistency`, `bench/nearbysso_validate.py`, `bench/nongrav_validate.py nearbysso`) allow for |rate × Δt| on top of their current tolerances.

**Performance.** The observer state (`ssp/sssource.py`, about line 776) and the solar elongation (`ssp/util.py`, about line 90) are each computed once per unique time. Per-source times multiply the number of unique times. Measure the cost; if needed, evaluate at the visit time and shift linearly (|Δt| ≤ 0.24 s).

## Validation

- Rebuild SSSource for a recent day: the along-track residuals (`ephOffsetAlongTrack`) should tighten, and the cross-track residuals shouldn't change.
- Count the two flags and the matching bases (`visit`, `corrected`, `both`); report any `obs_sbn` row matched before but not now.
- NearbySSO bitwise unchanged on today's inputs, plus a synthetic test with per-source times.
- The SSSource/NearbySSO consistency checks pass with the time-shift tolerance.
- The delivery checks pass with the new columns.

## Implementation plan

**First, the integrator (now, while v0.2.0 is untagged):**
- the two columns on `tickets/DM-55375` (local commit, validated with Felis);
- the refreshed `tests/data/sdm_schemas/` and `ssp/schema_ppdb.py`, and the contract tests' counts;
- the rules in `ssp/sssource_contract.py` (where the time and the flags come from), and the matching and manifest changes in `ssp/delivery_contract.py`;
- a fixture correction table from the shutter-timing session.

**Then, once v0.2.0 is tagged, in parallel:**

| WP | builds | review |
|---|---|---|
| **S1 extract** | Correction before matching in `ssp/export/submittable.py` / `ssp/sso_inputs.py`: the two-basis 10 ms match and bucket fallback, the basis column and manifest counts, `midpointMjdTaiVisit`, the flags, the provenance, failure on status 3. | independent |
| **S2 SSSource build** | Copy the corrected time and flags; the ephemerides at that time; the performance measurement and, if needed, the linear shift. | light |
| **S3 NearbySSO** | Evaluate each matched row at its own `midpointMjdTai`; bitwise unchanged today; a synthetic per-source-time test. | independent |
| **S4 stage 0 and checks** | `shutter-timing-table` in `ssp-sso-daily` (the failure modes, the runbook); the consistency checks' time-shift tolerance. | light |

**Last, the integrator:**
- the dependency pin and re-lock;
- a full rerun with every check, and the results here;
- push `tickets/DM-55375`;
- the PR to master, for the owner's approval.
