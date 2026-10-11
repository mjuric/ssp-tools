# NearbySSO: unmatched predictions, placement on detectors, and shutter-corrected times

## Context

NearbySSO (`ssp/nearbysso/`, `docs/design/nearbysso.md`) has one row per DiaSource that a known Solar System object's prediction matched: the nearest object within its matching radius, with the predicted position, uncertainty ellipse, rates and offset. Predictions that matched nothing are dropped.

This design keeps them. Every eligible prediction that falls on the focal plane, or within 30″ of it, gets a row, matched or not. Each row is also placed on a detector: (detector, x, y), from the per-(visit, detector) WCSs that the visit-table project extracts. And each row's values are moved from the visit's time to the shutter-corrected exposure midpoint at its pixel.

History:
- Issue #86 recorded the first design, proposed at the RFC-1188 schema review (2026-10-08) and deferred.
- A simplified version (one circle around the focal plane, no detectors) was proposed on 2026-10-09 and not pursued.
- This design (2026-10-11) supersedes both: the WCS store now exists (visit-table), and so does the PPDB TAP extract with every PPDB visit (`docs/design/ppdb-tap.md`, v0.2.0).

Related:
- DM-56570 (the stack's per-pixel shutter time for DiaSources and SSO association), whose method this design follows;
- visit-table issue #22 (the nearest-detector placement mode);
- issue #96 (predictions on detectors with no WCS).

## Owner decisions (2026-10-11)

| Topic | Decision |
|---|---|
| Unmatched predictions | Rows of the official NearbySSO table, with NULL `diaSourceId`, `ephOffset` and `diaDistanceRank`. |
| New official columns | Only `visit` and `detector`, both non-null. Everything else (placement, time, flags) goes to a sidecar. |
| Sidecar | `NearbySSO_internal.parquet`: an exact 1:1 join to NearbySSO (one row per row, same order, by the key), never uploaded. Not extra rows. |
| Key | (`diaSourceId`, `designation`, `visit`) with `diaSourceId` nullable (sdm_schemas #549, `6d7c9a1`). Felis accepts it. Postgres would refuse a NULL in a key column; BigQuery (the PPDB) doesn't enforce keys, so it works there. Fallback if a nullable key is ever a problem: `diaSourceId` = 0 as a sentinel. No surrogate key (owner, 2026-10-11). |
| `detector` | The detector the prediction falls on, or the nearest one for gaps and the margin. For a matched row in a visit with no WCS: its DiaSource's detector. |
| Placement | The prediction's position (also for matched rows), through the visit-table WCSs. |
| Margin | 30″ around the detectors. Beyond it, no unmatched row. |
| Gaps | The nearest detector by distance to its outline in the visit's tangent plane. x, y from the detector's cubic at the closest edge point, extended linearly with the local Jacobian, so they fall outside the pixel range. |
| Overlaps | The detector the position is deepest inside, with a deterministic tie-break. |
| Placement code | Upstream in visit-table (`locate`, issue #22), pinned as a dependency. |
| Processed detectors | No filtering. Whether a detector was processed is the user's judgement. |
| Pixel names | `eph_x`, `eph_y`. |
| Times | Every row (matched or not) is evaluated at the shutter-corrected exposure midpoint of its predicted pixel, as DM-56570 does. |
| Pixel correction | `eph_x`/`eph_y` are also moved linearly to the corrected time (only above a 1 mas move, if it proves slow). |
| Corrected PPDB times | Detected per visit: DiaSource `midpointMjdTai` within 1 ms of the header midpoint for all of a visit's DiaSources = uncorrected. Otherwise matched rows use their DiaSource's own time. No per-source comparison with our table. |
| TAP extract | Gains `detector` (6 columns). No other new columns. |
| Missing WCSs | Never inferred. Counted; rescued later (#96). |
| Missing detectors (revised 2026-10-11, after U0) | The only rule: keep a prediction if it lands on a detector with a WCS, or within 30″ of one; drop everything else. A prediction on a missing detector (no WCS, rejected, or one of the 17 never processed) is dropped automatically, since most of its area is more than 30″ from any placed detector. No reference set of expected detectors. Accepted edge case: a prediction on a missing detector within 30″ of a neighbour's edge is kept and assigned to the neighbour like a gap prediction, with `eph_x`/`eph_y` outside its pixel range. Handling missing detectors more carefully is for later (#96). |
| Matched rows far from any WCS (2026-10-11, after U2) | A matched row whose prediction is more than 30″ from every detector with a WCS (its DiaSource's detector has none) is treated as a matched row in a visit with no WCS: its DiaSource's `detector`, NULL `eph_x`/`eph_y`/`ephDetectorEdgeDist`, the visit's time flagged `no_wcs`, counted in `matched_without_wcs`. (14 rows on the 2026-10-09 inputs.) |
| Times beyond 100 px (2026-10-11, after U0) | shutter-timing gives no corrected time more than 100 px off a detector (`OFF_DETECTOR_OMIT_PIX`); the 30″ margin is ~150 px. Accepted: those rows keep the visit's time, with `midpointMjdTai_flag` true (≤ ~0.1″ error for fast objects over 0.25 s). |
| WCS store | visit-table's store at `/sdf/home/m/mjuric/ssp_daily_data/visit-table/wcs/`, refreshed hourly by ssp-daily, read only through visit-table's reader. ssp-tools assumes it is current: no freshness checks; what is missing today is picked up by tomorrow's rebuild. |
| Reviews | No mutation testing (2026-10-11): probes, comparison with the stack and with exact evaluation, real-data checks. |
| Validation | Don't re-validate what is validated upstream (visit-table's placement accuracy, `at_source_time`'s moves); check only what is new here. |

## Outputs

**NearbySSO** (`run/delivery/NearbySSO.parquet`):
- 15 → 17 columns: `visit` (long, non-null) and `detector` (short, non-null) added.
- `diaSourceId` and `diaDistanceRank` become nullable (`ephOffset` already is).
- The primary key changes as above.
- Rows: one per matched (DiaSource, nearest object), as today, plus one per unmatched eligible prediction on the focal plane or within 30″ of it.
- Sorted by `diaSourceId` (matched rows) and then, for the unmatched rows, by (`visit`, `designation`).
- `ephRa`, `ephDec`, the ellipse, the rates, `ephVmag`, the position angles and `ephOffset` are all evaluated at the row's corrected time (below).

**Sidecar** (`run/delivery/NearbySSO_internal.parquet`), one row per NearbySSO row, same order:

| column | type | null | meaning |
|---|---|---|---|
| key columns | as NearbySSO | as NearbySSO | (`diaSourceId`, `designation`, `visit`) |
| `eph_x`, `eph_y` | double | yes | pixel position of the prediction on `detector` at its corrected time; outside the pixel range for gaps and the margin; NULL where there is no WCS |
| `ephDetectorEdgeDist` | float | yes | signed distance to `detector`'s edge [arcsec], positive inside; NULL where there is no WCS |
| `midpointMjdTai` | double | no | the TAI MJD the row's values are evaluated at |
| `midpointMjdTai_flag` | boolean | no | true where the time is not shutter-corrected (no correction for this pixel: NOT_BUILT, outside coverage, no WCS) |

**Run report:** counts of matched and unmatched rows, and of predictions dropped:
- beyond the margin;
- in visits without a WCS;
- matched rows without a WCS.

And the rows whose time is not corrected (`midpointMjdTai_flag`), by cause.

Also the visits whose PPDB times are corrected, and the WCS store snapshot the build read.

## Inputs

**`ppdb_dia_sources.parquet`** gains `detector` (int64): `SELECT diaSourceId, visit, detector, midpointMjdTai, ra, dec`. Same for the `--tap-url none` ClickHouse query. `REQUIRED_INPUT_COLUMNS` and the TAP checks follow.

**The WCS store** (new input, referenced, not copied):
- Read with `visit_table.locate.WcsStore([...per-night files...])` and `store.locator(visit)` (rejected detectors left out), plus the nearest-detector mode of visit-table #22.
- Stack-free; pinned at a visit-table tag.

**Its state is not ssp-tools' concern** (owner, 2026-10-11). ssp-daily keeps it current. The build opens whatever nights the store has for the nights it covers, and checks nothing about the store's freshness:
- a night not in the store yet, a held night, a visit missing from a night, a detector without a WCS or rejected: all handled as "no WCS" (below and #96) and counted;
- NearbySSO is rebuilt from scratch every day, so whatever is missing today is picked up tomorrow;
- the run report records the store snapshot the build read, so a delivery's WCSs can be identified, and the "no WCS" counts make a stalled store visible.

**The correction table** (already an input to the extract; path in the manifest's `shutter_timing` entry) becomes an input to the NearbySSO build as well, through `ssp.export.submittable.Corrections` (shutter-timing v0.5.0's API).

## Approach

Everything below runs in NearbySSO's matching pass. It already works per slice of whole nights, on the eligible predictions sorted by (visit, orbit). Each slice opens its own nights' WCS files.

1. **Visit footprint.** The visit's center and radius come from its detectors' corners (the WCS), plus 30″, instead of from its DiaSources. This changes which predictions are eligible: candidate selection in the orbit pass uses the same footprint. Visits without a WCS keep the DiaSource-derived footprint, for matching only.
2. **Placement at the visit's time.** Per visit, the visit-table locator gives each eligible prediction (detector, x, y, edge distance), in the nearest-detector mode. Predictions beyond the 30″ margin are dropped (no unmatched row; a matched one keeps its row and placement).
3. **Missing detectors.** No special rule: step 2's 30″ cut drops predictions on a missing detector, except within 30″ of a neighbour, where they are placed like gap predictions (owner, 2026-10-11). A visit with no WCS at all gets no unmatched rows; its matched rows take their DiaSource's `detector`, with NULL `eph_x`/`eph_y`/`ephDetectorEdgeDist`.
4. **Matching** as today (radius, nearest per DiaSource, `diaDistanceRank`), at the visit's time.
5. **Corrected time.** Per visit, decide whether the PPDB times are corrected:
   - **Uncorrected:** every DiaSource's `midpointMjdTai` is within 1 ms (`MAX_HEADER_MISMATCH_S`) of the header midpoint, (MJD-BEG + MJD-END)/2 from the correction table's exposure log. Then every row's time t is our table's corrected midpoint at its predicted (detector, x, y), from `Corrections`:
     - off-detector positions within the margin use the timing model extrapolated there, as DM-56570 does, up to shutter-timing's 100 px limit; beyond it, the visit's time with the flag (accepted);
     - positions without a correction (NOT_BUILT, outside coverage, no WCS) keep the visit's time, with `midpointMjdTai_flag` true.
   - **Otherwise** (the PPDB has started recording corrected times): matched rows take their DiaSource's own `midpointMjdTai`, and unmatched rows our table's time as above. The run report counts these visits, and the build logs the first.
6. **Move to t.** Every row's published values move from the visit's time to t with `at_source_time`:
   - along the great circle, with rates at dt/2;
   - the ellipse, rates, `ephVmag` and position angles by their rates of change;
   - `ephOffset` re-measured.

   This needs the rates of change (`DOT_DTYPE`) for every candidate visit, not only for visits whose sources carry their own times. That means a second `compute_ephemerides_one` (and two `ellipse_at`) at every candidate visit: 6.0% of the orbit pass's worker CPU, ~21–25 s of wall at 32 workers (U0).
7. **Pixel correction.** `eph_x`/`eph_y` move by the cubic's analytic Jacobian times the change in (ra, dec) from step 6. `ephDetectorEdgeDist` follows. Placement isn't redone: |dt| ≤ ~0.25 s, so at most ~0.5 px even for fast NEOs, which the linear move captures.
8. **Write** NearbySSO (17 columns) and the sidecar, in the same order; the run report.

## Validation

- **Placement** is visit-table's (validated there against the stack: 0 detector mismatches, ≤ 0.005 mas). Here only a spot check that NearbySSO calls it correctly: a few visits' predictions placed again independently, through `VisitLocator`.
- **Matched rows' `detector`** against their DiaSource's detector: they agree, except predictions near an edge, which are counted and listed.
- **Times** against `Corrections` at the same pixels: bit-identical. The uncorrected/corrected visit test checked on fabricated corrected times (a copy of an input with its DiaSource times replaced by our table's).
- **Moves** are `at_source_time`'s, validated in `docs/design/nearbysso.md`. New here: `eph_x`/`eph_y` after the linear pixel correction, against a fresh placement at the moved position.
- **The sidecar** is checked by the delivery check: same row count and key order as NearbySSO, the columns' types and nulls (as SSObservation's sidecar).
- **End to end** on the 2026-10-09 inputs (TAP extract with `detector`):
  - matched and unmatched row counts, NearbySSO's wall time and memory against today's (379 s, 11.9 GB);
  - every check passes;
  - SSObservation, SSObject and the MPC tables unchanged.

## Implementation plan

| Wave | Work | Who |
|---|---|---|
| 0 | This design; owner approval | integrator |
| 1 | **The contract:** `ssp/nearbysso/_contract.py` additions (sidecar dtype, key, report fields, margin, the uncorrected test), `ssp/delivery_contract.py` (`detector` in `ppdb_dia_sources`). **Schema:** NearbySSO in sdm_schemas #549 (`visit`, `detector`, nullability, key; the Felis question), copies and `ssp/schema_ppdb.py`. | integrator |
| 1 | **U0, the speed gate:** on the 2026-10-09 inputs, measure the extra cost of (a) the rates of change at every candidate visit, (b) placing every eligible prediction with today's `VisitLocator` (whichDetector + pixels; the gap mode comes later), (c) the time lookup. If the total exceeds ~10% of NearbySSO's time, stop and report. | WP agent |
| 2 | **U1, inputs:** `detector` in the TAP and ClickHouse extracts (small; may be folded into the contract wave). | integrator |
| 2 | **U2, time and moves:** per-visit uncorrected test; `Corrections` at predicted pixels; rates of change everywhere; `at_source_time` for all rows; the pixel correction. | WP agent |
| 3 | **U3, placement and rows:** the WCS footprint; nearest-detector placement (needs visit-table #22's tag); the 30″ rule; unmatched rows; NearbySSO's 17 columns; the sidecar; the report. | WP agent |
| 3 | **U4, checks:** the delivery check and bench for the new NearbySSO and its sidecar, written black-box from this design and the contract. | WP agent |
| 4 | Independent reviews of U2 and U3 (probes, comparison with the stack and with exact evaluation) | review agents |
| 5 | End-to-end run, results recorded here, runbook; feature branch → master | integrator; owner approval |

Dependencies outside this repository:
- visit-table's first tag (the reader, packaged);
- visit-table #22 (the gap mode) before U3's placement;
- ssp-daily keeping the WCS store current (not checked here);
- sdm_schemas #549.

## Results

**U0, the speed gate (2026-10-11):** ~32 s, ~8% of the NearbySSO step (379–414 s at 32 workers): passed. Details: `/sdf/data/rubin/user/mjuric/nearbysso-unmatched/u0/REPORT.md`.
- (a) rates of change at every candidate visit: +25 s wall (6.0% of the orbit pass's CPU); output unchanged today (byte-identical, dt = 0).
- (b)+(c) placement and the time lookup: +7 s wall over 33 slices on 8 processes (29.9 s CPU: WCS read 10.4, locator 2.6, whichDetector 4.7, pixels 1.3, margin distance 3.9, corrected_midpoints 7.1); +0.9 GB per pass-3 worker.
- Rows: of 3,912,084 eligible predictions, 69.4% on a detector, 6.8% in a gap or the margin, 23.7% beyond 30″. Unmatched rows ≈ 1,271,015; NearbySSO ≈ 2.98M rows (+74%).
- Corrected times at those positions: OK 2,550,074, DEGRADED 404,422, OMITTED 29,409 (beyond 100 px), NOT_BUILT 0.
- Findings folded in: 172 detectors appear in the store, not 189 (hence the revised missing-detector rule); the store holds every visit, so each slice reads only its PPDB visits' rows (half the memory); the build stalls under Python 3.14, so WP environments pin Python 3.12.


**Wave 1 (2026-10-11):**
- The contract (`ssp/nearbysso/_contract.py`, "Unmatched predictions, detector placement and corrected times"): `PLACEMENT_MARGIN_ARCSEC`, `NEARBYSSO_DTYPE_NEXT` (17 columns), `NEARBYSSO_KEY`, `NEARBYSSO_NONNULL_NEXT`, `NEARBYSSO_UNMATCHED_NULL`, the sidecar (`NEARBYSSO_SIDECAR_FILE`, `_DTYPE`, `_NONNULL`), the time rules and `PLACEMENT_REPORT_FIELDS`. The `*_NEXT` names replace the current ones when U3 switches the output. Checked against sdm_schemas `6d7c9a1`: column order, non-null columns and key match.
- The schema copies (`tests/data/sdm_schemas/`, `ssp/schema_ppdb.py`) move to `6d7c9a1` with U3, which changes NearbySSO's output; before that they would fail five tests.
- U1 (`detector` in `ppdb_dia_sources`): in the TAP query (int64), the ClickHouse query (`toInt64(detector)`) and `REQUIRED_INPUT_COLUMNS`. On data-int: 17,863,456 rows in 321 s; the other five columns equal the earlier file and `detector` equals the 2026-10-09 full export (2..186). ClickHouse: 13,086,925 rows, other columns unchanged.
- Sequencing (owner, 2026-10-11): U2 and U3 both need the placement and edit the same part of `build.py`, so they wait for visit-table #22's tag and then run one after the other (U2, then U3), not in parallel.

