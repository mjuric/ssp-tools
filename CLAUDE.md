# Working in this repository

The rules here are for development in ssp-tools, by Claude sessions and their subagents. They come from how the NearbySSO, widened-SSSource and SSO-delivery work was done (`docs/design/*.md`).

## How a project is run

**1. Understand, then interview.**
- Read the relevant RFCs, tickets, schemas and code.
- Ask the owner about every ambiguity, one decision at a time, with options and a recommendation.
- Don't start building on assumptions.

**2. Design document on a new branch.**
- Write `docs/design/<topic>.md` with:
  - the context;
  - an **owner-decisions table**, recording each decision and its date;
  - the outputs and inputs;
  - the algorithm or approach;
  - the validation;
  - an **implementation plan**.
- Commit it on a new branch, open a draft PR, and **ask the owner to approve.** Revise and re-ask until approved.

**3. The implementation plan follows the dependencies.**
- Break the work into **work packages (WPs)** that subagents can build independently.
- Group them into waves by what each needs from the others. A project may have two waves or many; there is no fixed set of phases.
- Typical work for the integrating session itself, before the WPs that depend on it:
  - **the contract:** a module of shared dtypes, function signatures and rules, e.g. `ssp/sssource_contract.py`, `ssp/delivery_contract.py`, `ssp/nearbysso/_contract.py`;
  - **schema changes;**
  - **fixtures:** read-only, with a README of facts.
- **Work packages must not change the contract.** A needed change goes back to the integrator, who applies it for everyone and tells the affected agents.
- A **validation harness** is built as its own WP, as a black box: it works from the design, the contract and the schema, without reading the implementation.

**4. Subagents build the WPs, each in its own git worktree.**
- **Worktrees come up on master.** Every WP prompt must start with `git fetch && git reset --hard origin/<feature-branch>`.
- Each prompt gives the agent:
  - the files to read;
  - the task and the tests;
  - a real-data check with the paths;
  - the environment;
  - the shared-resource limits;
  - "commit with the trailers, don't push";
  - what to report.
- Independent WPs run in parallel.

**5. Review.**
- The integrator reviews every WP.
- WPs with non-trivial logic get an **independent review**, by a fresh agent that tries to break them: probes, mutation testing, comparison against brute force or a reference. Small WPs get a light review.
- Fixes go back to the WP's author, round after round, until the review is clean.
- Findings that change a design assumption go to the owner before going further.

**6. Merge.**
- Each WP merges into the feature branch **through its own PR, as a merge commit, never a squash.** Rebase it onto the feature branch and run the full test suite first.
- After integration, the feature branch merges to master through its PR.
- **Approvals:** the owner gives blanket approval for WP merges into the feature branch. Merge each one once it has passed its review and the full test suite, without asking.
- The owner reviews and approves only the final merge of the feature branch to master: ask before that one.

**7. Integrate and record.**
- Run the integrated result end to end on real data, with every check.
- Record the results and measurements in the design doc: what was run, timings, memory, counts, check results and findings.
- Write or update a runbook in `docs/runbooks/` for anything operational.
- Report to the owner, separating what's done from the decisions still open.

**Throughout.**
- **Keep the owner informed** as WPs land.
- **Stop and ask** about major findings, deviations from agreed assumptions, or anything that changes outputs beyond what was approved.
- **Record decisions in the design doc** as they're made.
- **Open GitHub issues** for follow-ups and external dependencies.

## Conventions

- **Commits** end with the attribution trailers, and **PR descriptions** with the generation line and session link, as the session's system instructions specify.
- **The repository is public:** no people's names in issues, commits, PRs or docs. Refer to roles, e.g. "the DM-55678 owners".
- **Tests:** network-free by default. Tests that need ASSIST data or fixtures skip without them, using the `SSP_ASSIST_PLANETS`/`SSP_ASSIST_ASTEROIDS` variables. Use `uv tool run ruff check` on changed files.
- **Schema:** `sdm_schemas` (DM-55375, lsst/sdm_schemas#549) is the source of truth.
  - Copies live in `tests/data/sdm_schemas/`; `ssp/schema_ppdb.py` is generated from them with `ssp-generate-dtypes`.
  - Refresh all three together. Don't edit the generated file.
- **Scratch:** large scratch and run outputs go under `/sdf/data/rubin/user/mjuric/...`, not `/lscratch`, which is shared and often full.

## Shared resources: etiquette

- **ClickHouse** (HTTP only; the host moves, so it is read from `~/.clickhouse.host`, a line `river:<host>` kept up to date by its operators; `sdfiana032.sdf.slac.stanford.edu:8123` since 2026-10-05): read-only, at most **8 concurrent queries**.
  - The tools read the host with `ssp.export.submittable.current_host`; an explicit `--host` wins. `~/.chpass` needs a line for the current host.
  - The code excludes it from SDF's HTTP proxy (`ssp.export.submittable.bypass_proxy`); with curl, use `--noproxy '*'`.
  - If a query hangs with no progress, report it rather than retrying in a loop. The server's data sits on NFS, which has stalled reads before.
  - Credentials come from `~/.chpass` through `ssp.export.submittable.credentials`.
  - Admin (`default`) access is the owner's to use, not ours.
- **MPC replica** (`mpcorb-db.slac.stanford.edu`, `mpc_sbn`, user `rubin`): read-only, in one transaction when tables must be consistent.
- **JPL Horizons and SBDB:** strictly serial, at least 1 s apart, a small total budget, never in parallel or in CI.
- **Never submit anything to the MPC.**
- **Shared nodes:** keep workers at or under the stated limit, 32 unless told otherwise. Check `uptime` before heavy runs.
- **GCS and Pub/Sub uploads** (`ssp-upload-sso`): dry-run unless the owner asks for a real upload.

## Environment

- **Venv:** `.venv`, created with `uv sync --extra all`. After pulling new console scripts, refresh them with `VIRTUAL_ENV=$PWD/.venv uv pip install --no-deps -e .`
- **ASSIST data:** `data/assist/linux_p1550p2650.440` and `data/assist/sb441-n16.bsp`. Export the two `SSP_ASSIST_*` variables, and `OMP_NUM_THREADS=1`.
- **Runbooks:** `docs/runbooks/sso-daily.md` (the daily PPDB Solar System tables).
