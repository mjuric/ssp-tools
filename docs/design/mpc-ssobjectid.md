# ssObjectId in the MPC tables

## Context

The PPDB's Solar System tables carry three tables replicated from the MPC: `mpc_orbits`, `current_identifications` and `numbered_identifications` (`sso_base.yaml`, `ppdb.yaml`; lsst/sdm_schemas#549, DM-55375). They are keyed by designation only. SSObject, SSObservation and NearbySSO are keyed by the integer `ssObjectId`.

The owner wants one integer key across all the Solar System tables, so that any designation (secondary, or permanent number) resolves to the object with a join. ssp-daily relayed this (2026-10-12); the owner confirmed it here.

ssp-daily checked on that day's data (ClickHouse ppdb, generation 43):
- every row of the three tables has a non-empty `packed_primary_provisional_designation` of at most 8 characters;
- the id SSObservation computes from it reproduces all 298,117 SSObject ids;
- so every row gets an id: `mpc_orbits` 1,578,004 (unique), `current_identifications` 2,102,622 rows over 1,612,118 primaries, `numbered_identifications` 896,611.

## Owner decisions

| Decision (date) | Choice |
|---|---|
| Upstream ticket (2026-10-12) | In DM-55375, lsst/sdm_schemas#549, which is still open, so the PPDB dataset is rebuilt only once. |
| Semantics (2026-10-12) | Every row carries the `ssObjectId` of its **primary** provisional designation. In `current_identifications` a secondary designation, and in `numbered_identifications` a permanent number, resolves to the object's id. |
| Description (2026-10-12) | Brief. The schema doesn't describe how the id is computed: that is an implementation detail and may change. Users shouldn't rely on the ids outside the data; to keep a reference to an object, they keep its designation. |

## Outputs

- **Schema** (`sso_base.yaml`): `ssObjectId` (`long`, not nullable, `meta.id;src`) in the three tables, placed right after `id`. Proposed description: "The SSObject id of this row's object (SSObject.ssObjectId), for joins within this release. Not every object has an SSObject row. To refer to an object elsewhere, use its designation." `ppdb.yaml`'s `columnRefs` take it in.
- **Delivered tables:** the three gain the column, computed in the `mpc` step. Their row order is unchanged.

`ppdb.yaml` builds the BigQuery tables, so the loader needs nothing more than the schema rebuild DM-55375 already needs (runbook, "Open items with the DM-55678 owners").

## Approach

- **The `mpc` step:** today `MPC_DERIVED` maps a delivered column to the input column it copies. It is extended so a derived column can also be computed from one: `ssObjectId` from `packed_primary_provisional_designation`, with the same function SSObservation uses (`util.packed_ascii_to_uint64_le`), so the ids agree by construction.
- **A row the id can't be computed for** (a NULL, empty, or over-8-character designation) fails the step loudly rather than getting a NULL or a guessed id. None exist today.
- **The schema copies:** `tests/data/sdm_schemas/` and `ssp/schema_ppdb.py` are refreshed from the updated #549 branch, all three together.

## Validation

- **Tests:** the column's values and type; the primary's id on rows with a secondary designation; agreement with SSObservation's ids for the same designation; the loud failure.
- **Delivery check:** every row has an id, and it equals the function of its primary designation.
- **Real data:** a full `ssp-build-sso` run on current inputs, with every check. Every SSObject id is found in `mpc_orbits`, and every SSObservation/NearbySSO id that has an SSObject row is found there too.

## Implementation plan

The integrating session alone; the project is small and its parts depend on each other in order.
1. **Upstream:** a commit on lsst/sdm_schemas `tickets/DM-55375` (#549) adding the column and descriptions. Shown to the owner before it is pushed.
2. **ssp-tools**, on the branch `mpc-ssobjectid`: the schema copies, the derived column, the tests and the delivery check.
3. **Review:** an independent review (probes, a comparison against a reference, real data; no mutation testing).
4. **End to end** on real data; results here; the runbook.
5. **Merge to master** with the owner's approval; then tell ssp-daily the commit or tag.
