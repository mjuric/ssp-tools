# SSObservation: the rename, internal columns and the partitioned delivery

## Context

The RFC-1188 schema review (meeting "RFC-1188 changes finalization", 2026-10-08) accepted the RFC's concept and the proposed schema (lsst/sdm_schemas#549), with three changes for the SSP side:

1. **Rename.** The widened PPDB table and the alert's SSSource fragment now differ in structure and meaning: in an alert, SSSource says "a known object is nearby"; in the PPDB, "this is the object". Renaming the alert's version would break backward compatibility, so the PPDB table becomes **SSObservation**: one row per MPC-accepted Rubin observation, keyed by the MPC's `obsid`. NearbySSO stays the PPDB counterpart of the alert's SSSource.
2. **Internal columns.** Some columns are for our own use. They leave the Felis schema and the delivered table for now, to unblock the loader, and may come back later (having them in BigQuery is useful even when they are not exposed).
3. **Partitioned delivery.** The table is delivered as a series of Parquet parts, partitioned by `ssObjectId`, with a JSON manifest that describes the partitioning. This also fits BigQuery's `LOAD DATA ... FROM FILES (uris = ['gs://.../SSObservation.part*.parquet'])`.

The meeting also proposed keeping unassociated predictions in NearbySSO (rows with a NULL `diaSourceId`). That is **deferred** (owner, 2026-10-08): it has enough downstream consequences to need more thought. The design so far is recorded in a GitHub issue. NearbySSO's schema does not change here.

## Owner decisions (2026-10-08)

| Topic | Decision |
|---|---|
| Name | The PPDB's SSSource becomes **SSObservation**. The alert's SSSource is unchanged. |
| Rename scope | Everything, now: the schema, the delivered files, the code (modules, contract names, CLI), tests, bench, docs and the RFC drafts. Otherwise the code invites confusion with the alert's SSSource entity. No backward-compatible aliases. |
| Internal columns | `matchMethod` and `midpointMjdTai_flag_degraded`, **configurable**. Removed from the Felis schema and from SSObservation; written to a **sidecar** Parquet. |
| Sidecar | One file (not partitioned). |
| Felis | The internal columns do not need to be in Felis: both are computed by the extract stage and carried in `dia_sources.parquet`; the contract types them (as it already does for the shutter-timing internals). |
| Partitioning | By `ssObjectId`, ~**2 million rows** per part, contiguous `ssObjectId` ranges. |
| Manifest | Ours to define; the PPDB loader owners review it. |
| Which tables | SSObservation only; the other tables stay single files. |
| Upload | No GCS upload (no credentials yet). A delivery under `/sdf/data/rubin/user/mjuric/...` for the loader owners suffices. Uploads stay dry-run. |
| NearbySSO unassociated predictions | Deferred; recorded in an issue. |

## Outputs

`RUN_DIR/delivery/` becomes:

```
SSObservation.part0000.parquet      ssObjectId range 1
SSObservation.part0001.parquet      ...
...
SSObservation.partNNNN.parquet      rows with NULL ssObjectId (last)
SSObservation.manifest.json         the partitioning (below)
SSObservation_internal.parquet      sidecar: obsid + the internal columns
SSObject.parquet  NearbySSO.parquet  mpc_orbits.parquet
current_identifications.parquet  numbered_identifications.parquet
```

**SSObservation parts.** Columns, order, types and nullability are ppdb.yaml's SSObservation (#549) — today's SSSource less the internal columns (182 − 2 = 180). Rows sorted as today, by (`ssObjectId`, `midpointMjdTai`, `obsid`), NULL `ssObjectId` last; zstd. Parts are cut from that order:

- a part holds rows of whole objects: a boundary never splits an `ssObjectId`, so each part's range `[min, max]` is disjoint from the others;
- a part is closed at the first object boundary at or after `part_rows` (default 2,000,000, configurable);
- rows with a NULL `ssObjectId` (observations the MPC has not linked to an object) go in their own part(s) at the end, cut every `part_rows` rows;
- part numbers are zero-padded to four digits and contiguous from 0000.

On the 2026-10-01 build (8,070,610 rows, 63,868 with NULL `ssObjectId`, 3.13 GB) this gives four or five ranged parts of ~0.8 GB and one NULL part.

**Manifest** (`SSObservation.manifest.json`):

```json
{
  "table": "SSObservation",
  "format_version": 1,
  "schema": {"source": "lsst/sdm_schemas tickets/DM-55375", "file": "sso_base.yaml", "md5": "..."},
  "ssp_tools_commit": "<sha>",
  "created_utc": "2026-10-08T12:00:00Z",
  "partition_key": "ssObjectId",
  "sort": ["ssObjectId", "midpointMjdTai", "obsid"],
  "part_rows": 2000000,
  "rows": 8070610,
  "parts": [
    {"file": "SSObservation.part0000.parquet", "rows": 2000123,
     "ssObjectId_min": 1, "ssObjectId_max": 4417, "null_ssObjectId": false,
     "bytes": 812345678, "md5": "..."},
    {"file": "SSObservation.part0005.parquet", "rows": 63868,
     "ssObjectId_min": null, "ssObjectId_max": null, "null_ssObjectId": true,
     "bytes": 25000000, "md5": "..."}
  ],
  "sidecar": {"file": "SSObservation_internal.parquet", "key": "obsid",
              "columns": ["matchMethod", "midpointMjdTai_flag_degraded"],
              "rows": 8070610, "bytes": 0, "md5": "..."}
}
```

**Sidecar** (`SSObservation_internal.parquet`): `obsid` plus the configured internal columns, one row per SSObservation row, in the same order; types from the contract. Not uploaded to the PPDB.

**report.json.** `tables.SSObservation` gains `parts` and `manifest`; its `rows`/`bytes` are totals.

## Inputs

Unchanged: the stage-1 inputs (`obs_sbn`, `dia_sources`, the MPC tables, the PPDB DiaSources) and the shutter-timing correction table.

## Approach

### A0. The rename (integrator, before the WPs)

A mechanical rename across the repository (today: 69 files, ~1,000 lines mention SSSource):

- **Schema.** In #549 (`sso_base.yaml`, `ppdb.yaml`): the table `SSSource` → `SSObservation`, its column descriptions, and references from NearbySSO and the MPC tables. Refresh `tests/data/sdm_schemas/` and regenerate `ssp/schema_ppdb.py` (`SSSourceDtype` → `SSObservationDtype`).
- **Code.** `ssp/sssource.py` → `ssp/ssobservation.py`, `ssp/sssource_contract.py` → `ssp/ssobservation_contract.py`, `ssp/sssource_ellipse.py` → `ssp/ssobservation_ellipse.py`; `SSSOURCE_*` → `SSOBSERVATION_*`; CLI `ssp-build-sssource` → `ssp-build-ssobservation`; the build step `sssource` → `ssobservation`; `DELIVERY_TABLES`; `ssp-upload-sso` and `config/sso-upload/*`; `bench/sssource_validate.py` → `bench/ssobservation_validate.py`; the tests (`tests/test_sssource_*` → `tests/test_ssobservation_*`).
- **Not renamed:** the alert's SSSource; mentions of the historical name in history (design docs keep their file names and gain a note at the top: "the PPDB table described here is now SSObservation"); upstream names we do not own.
- **Docs:** README, runbook, CLAUDE.md, the RFC drafts.

Behaviour is unchanged: the full suite passes, and a rebuild from the same inputs gives byte-identical table contents under the new name.

### A1. Internal columns and the sidecar (WP)

*Amended 2026-10-08, during A0:* dropping the two columns from the schema made 38 tests fail, since they check the columns' values. Rather than leave the suite red until W1, A0 writes the sidecar itself (the default columns, a single file next to `ssobservation.parquet`, carried into the delivery by `ssp-build-sso`). The bench reads the internal columns from the sidecar next to the file it checks. W1 keeps the rest: the configurable set and its checks.

- The contract gets `SSOBSERVATION_INTERNAL_DEFAULT = ("matchMethod", "midpointMjdTai_flag_degraded")` and the internal columns' types (from today's schema), plus a rule: an internal column must be one the builder computes; the delivered table is the Felis table, the sidecar is `obsid` + the internal set.
- Configurable through `ssp-build-ssobservation --internal-columns` and the matching `ssp-build-sso`/`ssp-sso-daily` option (and its config). A column moved back from internal requires it to be in Felis again: the build refuses a configuration where a column is in neither, or in both.
- `SSOBSERVATION_NONNULL` loses the two columns; the sidecar keeps their non-null rule.
- The bench reads the internal columns from the sidecar (joined on `obsid`).

### A2. Partitioned writing, the manifest and the checks (WP)

- The builder writes the parts and the manifest directly (streaming by object, not a second pass over a single file), then the sidecar. Peak memory must not exceed today's.
- `ssp/delivery_check.py` checks SSObservation through its manifest: every part present, md5/bytes/rows match, parts disjoint and ordered by range, no object split across parts, NULL parts last, each part's schema equal to Felis, the sort order within and across parts, the totals, and the sidecar (same `obsid`s, in order, its types).
- SSObject and NearbySSO read the parts (through the manifest) instead of the single file.
- `ssp-upload-sso` (dry run only) uploads the parts and the manifest under the prefix; the Pub/Sub message keeps `uploaded_tables` and gains `files` (`{Table: [object names]}`). The sidecar is not uploaded. The message change goes to the PPDB loader owners with the manifest, for review.

### A3. Docs

The RFC drafts (PR #77): the rename, the dropped columns, the partitioned delivery, and NearbySSO's unassociated predictions as a planned future option, not part of this revision. The design docs, the runbook and README.

### A4. The delivery

An end-to-end daily run, with every check, writing a partitioned delivery under `/sdf/data/rubin/user/mjuric/sso-delivery/...`; its path goes to the PPDB loader owners.

## Validation

- A0: the full suite; a rebuild from a fixed input directory compared with master's output, column by column (contents equal, names changed).
- A1/A2 (independent review): the SSObservation parts concatenated plus the sidecar joined on `obsid` equal master's SSSource exactly; the manifest validates; mutation tests on the check (a dropped part, a swapped part, a split object, a bad md5, a missing sidecar row) all FAIL it.
- Partition edge cases: `part_rows` smaller than one object's rows; no NULL rows; only NULL rows; an empty table.
- A4: all delivery checks PASS; timings and peak memory against today's (~25 minutes for the daily run).

## Implementation plan

| Wave | Work | Who |
|---|---|---|
| 0 | This design; owner approval | integrator |
| 1 | **A0** the rename: schema (#549, copies, generated dtypes), code, tests, bench, docs; contract additions for A1/A2 (internal set, manifest fields, part naming); the sidecar with the default internal columns | integrator |
| 2 | Three WPs in parallel, each in its own worktree and its own files, all working from the contract's manifest and internal-column rules: **W1** the builder (`ssp/ssobservation.py`): internal columns, sidecar, partitioned writer, manifest (A1 + A2's writing); **W2** the delivery check (`ssp/delivery_check.py`) and the bench, written as a black box from this design and the contract; **W3** the readers (SSObject, NearbySSO), `ssp-build-sso`/`ssp-sso-daily` options and `ssp-upload-sso` (dry run) | WP agents |
| 3 | Independent review of A1/A2 (the black-box comparison with master above) | review agent |
| 4 | **A3** docs and RFC drafts; **A4** end-to-end run and delivery; results recorded here | integrator |
| 5 | Feature branch → master | owner approval |

Follow-ups: a GitHub issue for NearbySSO's unassociated predictions (footprint test with a 30″ margin, the detector and an edge distance for unmatched rows, the key (`diaSourceId`, `designation`, `visit`), the option on by default, the σ ≤ 10″ cut).

## Results (2026-10-08)

**What was built.** The plan ran as designed, with three changes:
- A0 also wrote the sidecar, so the suite stayed green after the two columns left the schema (see A1).
- The WPs were regrouped by file: W1 the builder, W2 the checks and bench, W3 the build, readers and uploader.
- `report.json` gained `manifest_md5`, from W3's question.

Merges:
- A0: `c5b4ed8` (rename and sidecar) and `8ffbf54` (contract, `ssp.ssobservation_parts`).
- W1: #87. W3: #88. W2: #89.
- #90: status-'I' rows with a designation (below).

The schema rename is in lsst/sdm_schemas#549 at `27a404c`. The RFC drafts (PR #77) are updated.

**Reviews.**
- **W1.** The independent review found no correctness bugs:
  - `part_bounds` matched a brute-force reference on ~370k cases;
  - builds from the same inputs equal A0's output exactly, with byte-identical parts under every `--internal-columns` setting.

  It did find robustness gaps, all fixed: stale files left by a rebuild, unchecked preconditions in `write_partitioned`, and the commit recorded from an enclosing checkout.
- **W2.** The independent review found no false passes in the partitioning. It found gaps, all fixed:
  - `midpointMjdTai_flag_degraded` was no longer compared with the input once it moved to the sidecar;
  - manifest value types; stray files; crashes on badly typed inputs; the empty table failing conformance.

  80 of 81 mutants are killed; the survivor can't change a result.
- **W3.** Reviewed by the integrator; one fix round (`manifest_md5`).
- **Full suite** on the integrated branch: 1175 passed. ruff is at the baseline.

**Equivalence.** On the 2026-10-06 inputs (3,000 objects, 79,511 rows):
- the parts concatenated equal master's SSSource column for column, minus the two internal columns;
- the sidecar equals master's values of those columns, matched by `obsid`;
- SSObject built from the parts is byte-identical to SSObject built from the single file.

**Finding: status-'I' rows with a designation.** The MPC snapshot of 2026-10-08 has 4 `obs_sbn` rows with status 'I' that carry a provid (2015 GF54, from one Rubin submission of 2026-10-07, without a `trksub`).
- The builder refused them, as master's would have.
- **Owner decision (2026-10-08):** trust the status. They are kept unidentified (NULL `ssObjectId` and `designation`), with a warning that names them. More than `MAX_DESIGNATED_I_ROWS` (100) still fail the build (#90).

**End-to-end run (A4).**
- **Command:** `ssp-sso-daily /sdf/data/rubin/user/mjuric/ssobservation-delivery/daily --upload dev --dry-run`.
- **Inputs:** extracted 2026-10-09 UTC in 1012 s (dia_sources 757 s). That run stopped on the 'I' rows.
- **The build** was rerun at `ad8ab9c` from the same inputs (`--reuse-inputs ... --stamp 2026-10-09-r2`): deliverable, all 8 checks PASS, build 13.0 min.

  | step | wall | peak RSS |
  |---|---|---|
  | mpc | 44 s | 7.3 GB |
  | ssobservation | 222 s | 14.1 GB |
  | ssobject | 86 s | 2.9 GB |
  | nearbysso | 345 s | 11.7 GB |
  | check | 58 s | 5.3 GB |

- **The delivery,** `/sdf/data/rubin/user/mjuric/ssobservation-delivery/daily/2026-10-09-r2/run/delivery/`:
  - SSObservation: 8,078,546 rows, 3.24 GB, in 6 parts:
    - four of ~2.0M rows (0.80 GB each);
    - a fifth ranged part of 14,564 rows;
    - one of 63,849 NULL-`ssObjectId` rows.
  - The manifest, and the sidecar (`matchMethod`, `midpointMjdTai_flag_degraded`; 19 MB).
  - SSObject 298,117 rows; NearbySSO 1,295,205; mpc_orbits 1,576,334; current_identifications 2,100,962; numbered_identifications 896,611.
  - The dry-run upload lists the parts, then the manifest, and not the sidecar.
- **The delivery check** on W1's full build: 14 s and 2.6 GB. Conformance: 37 s and 6.3 GB.
- **Remainder parts.** With parts closed at the first object boundary after 2M rows, the last ranged part is a small remainder (14,564 rows here). That is by the rules, and harmless for loading.
