# RFC-1188 revision (drafts)

Drafts of an update to [RFC-1188](https://rubinobs.atlassian.net/browse/RFC-1188), "Decouple PPDB solar system and diasource/object tables", for the owner to review before anything is posted to Jira. All files are in Jira wiki markup.

| file | what |
|---|---|
| `description.2026-06-25.jira` | the description as it stands (fetched 2026-10-05), for diffing |
| `description.jira` | the proposed new description: replaces the whole body |
| `comment.jira` | a comment to post with the edit. Jira doesn't notify watchers of description edits, so the comment says what changed and answers the open comments |

## What changes, and why

The decisions were made with the owner on 2026-10-05, and updated after the schema review on 2026-10-08:

| item | decision |
|---|---|
| Durable record of submitted measurements | Stated as a requirement; how it's kept is an implementation detail. |
| `SSSource`, `SSObject` | Summarized as built (one row per MPC-accepted observation, keyed by `obsid`; the full submitted measurement; `SSObject` from `SSSource`), with the details left to lsst/sdm_schemas#549. |
| `DiaSource.ssObjectId` | The RFC proposes removing it, with `ssObjectReassocTimeMjdTai`, and `SSSource`/`SSObject` from the APDB. |
| `NearbyAsteroids` | Renamed `NearbySSO` and defined: the nearest known object within an AP-determined matching radius, possibly different between classes of objects (e.g. asteroids and comets). No link to `SSSource`. |
| Undetected predictions | The option is left open for `NearbySSO` to also list objects predicted in the field but not matched, with a NULL `diaSourceId`. |
| Matching radius, uncertainty cut | Set by the AP team, tunable without an RFC; no values in the RFC. |
| Measured position errors | Not mentioned. |
| Level of detail | A summary that points to lsst/sdm_schemas#549 for the columns. |
| Body vs. comment | The body is rewritten (it is what gets adopted); one comment summarizes the change and answers the open comments. |
| PPDB `SSSource` → `SSObservation` (schema review, 2026-10-08) | Renamed, because the PPDB table and the alert's `SSSource` now differ in structure and meaning; the alert's `SSSource` is unchanged, and `NearbySSO` is its PPDB counterpart. |
| Undetected predictions (2026-10-08) | Deferred: described as a possible future extension, not part of this revision (ssp-tools issue #86). |
| Delivery format (2026-10-08) | `SSObservation` is delivered as Parquet parts partitioned by `ssObjectId`, with a JSON manifest: one line in the Implementation Notes. Its internal columns are not mentioned. |

## Before posting

- Add the link to the NearbySSO paper (marked "link to be added").
- Replace the 2026-06 diagram image. The `{code}` block in the description is a text stand-in.
- Comments are referred to by date, not by name, because this repository is public. Add @-mentions in Jira if wanted.
