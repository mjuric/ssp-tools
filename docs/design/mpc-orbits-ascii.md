# mpc_orbits.mpc_orb_jsonb: ASCII only

## Context

Querying `mpc_orbits` through the data-int PPDB TAP fails with `'ascii' codec can't decode byte 0xe2`.
- `sso_base.yaml` declares `mpc_orb_jsonb` as Felis `text`, which is VOTable `char`: ASCII only.
- Our delivered values carry the MPC database's literal UTF-8. In the 2026-10-12 delivery, 3,206 of 1,578,004 rows have non-ASCII text, all in the MPC's naming citations (`designation_data.citation`, `name`). The most common characters are –, é, “ ”, ’, á, ö, ü and š.
- Every value is valid UTF-8 and valid JSON. `mpc_orb_jsonb` is the only column of the six tables with non-ASCII text.

## Owner decisions

| Decision (date) | Choice |
|---|---|
| Fix (2026-11-12, via ssp-daily's brief) | Escape the non-ASCII characters as JSON `\uXXXX` escapes. No schema change to `unicode`: no sdm_schemas change and no PPDB table rebuild. |
| Check (2026-11-12, via the brief) | A delivery check: the text columns of mpc_orbits, and, if cheap, every string column of all delivered tables, are ASCII; the delivery fails otherwise. |
| Build (2026-11-12) | The owner asked for it to be built by a subagent, as usual. |

## Approach

- **Where:** in the build's `mpc` step (`ssp/sso_build.py`, `shape_mpc_table`), on `mpc_orbits.mpc_orb_jsonb` only, before writing `delivery/mpc_orbits.parquet`.
- **How: the text, not a parse and re-serialise.** Each non-ASCII character in the value is replaced by its JSON escape: `\uXXXX`, lower-case hex as Python's `json` writes it, and a surrogate pair above U+FFFF. Nothing else changes.
  - In valid JSON, non-ASCII characters can only occur inside strings, where these escapes mean exactly the same characters. So the result is valid JSON, and `json.loads` of it equals `json.loads` of the original.
  - `json.dumps(json.loads(s))` is not used. It would rewrite the MPC's number literals, e.g. `0.000000000397875827896837` as `3.97875827896837e-10`, in every row, and possibly change their digits. Escaping the text changes only the rows that have non-ASCII characters, byte for byte nowhere else.
  - Rows that are already ASCII are untouched, and NULLs stay NULL.
- **Speed:** only rows with a non-ASCII byte are touched. They are found vectorised, and only those 0.2% of rows are rewritten in Python.
- **The check** (`ssp/delivery_check.py`, run by build step `check`): every string column of every delivered table is ASCII, with the failing column, a row count and examples. It should cost seconds, not minutes, on the 8 M-row SSObservation; if it's slow, it is restricted to mpc_orbits as the brief allows. For SSObservation it runs on every part.

## Validation

- **Tests:**
  - a value with –, curly quotes, an accented name and a character beyond the BMP comes out ASCII and `json.loads`-equal;
  - ASCII-only values come out byte-identical, and NULLs stay NULL;
  - the check fails on a non-ASCII string in any delivered table and passes otherwise.
- **Real data:** rebuild only `mpc_orbits` from the 2026-10-12 inputs into a scratch run directory, never the production one. Then check:
  - 1,578,004 rows;
  - 0 non-ASCII values;
  - exactly 3,206 rows changed, each `json.loads`-equal to the original;
  - every other row byte-identical;
  - the check passing;
  - the step's time before and after.

## Implementation plan

- **One WP:** the escape, the check and their tests.
- **Then a review by the integrator,** with the real-data comparison.
- **Merge to master** with the owner's approval. Then tell ssp-daily the commit to pin.

## Results (2026-11-13)

The WP was merged into `mpc-orbits-ascii` through PR #110 (f10f41c), after the integrator's review.
- **Tests:** the full suite, 1328 passed.

**Real data:** `mpc_orbits` rebuilt from the 2026-10-12 inputs into `/sdf/data/rubin/user/mjuric/mpc-orbits-ascii/`, compared with that day's delivered file.

| | |
|---|---|
| rows | 1,578,004, in 13 row groups, as before |
| changed | exactly 3,206 rows. Each is the original with every non-ASCII character replaced by its `json.dumps` escape and is `json.loads`-equal to it (checked by the WP and independently by the integrator). |
| unchanged | the other 1,574,798 rows byte-identical; the NULLs (2,624) and every other column equal |
| non-ASCII values after | 0 |
| step time (`mpc`, 32 workers) | old code 105.7–137.4 s (3 runs), new 95.7 s: no slowdown within the filesystem's noise. The escape itself takes about 4.5 s. Peak RSS 8.0 GB, unchanged. |

**The check, `ascii`:**
- It passes on the rebuilt delivery and fails on the original, naming `mpc_orb_jsonb` with 3,206 values.
- It costs about 20 s (19–36 s across runs), nearly all of it reading and decompressing `mpc_orb_jsonb`'s ~11 GB of text; all the other tables together add about 1 s.
- Restricting it to `mpc_orbits` would save nothing, so it covers every string column of every delivered table. That's slightly over the design's 30 s guide when the cache is cold; the integrator accepted it, since the build itself takes over an hour.
