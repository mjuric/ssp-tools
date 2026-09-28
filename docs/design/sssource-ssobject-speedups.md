# Design: faster SSSource/SSObject builds

Status: **proposed**, for review. Nothing here is implemented yet.
Tracks items 3 and 4 of [#9](https://github.com/mjuric/ssp-tools/issues/9).

Three changes:
- **A:** run the SSObject per-object work in parallel;
- **B:** `sssource.py` reads only the DiaSource columns it uses;
- **C:** cheaper H/G12 fits, with G12 bounded to [0, 1].

See *Plan* for the order and who does what.

## Plan

1. **B first,** by the integrating session: a small, separate PR, merged to
   `master` before anything else starts. It shrinks the SSSource build's
   memory, which the A and C benchmarks both depend on.
2. **Then A and C in parallel,** each by a subagent in its own git worktree,
   branched from `master` after B.
   - A touches `ssp/ssobject.py`; C touches `ssp/photfit.py` and its tests.
     The overlap is small: A never changes what `compute_ssobject_entry` or
     `fitHG12` compute, and C never changes how they're called.
   - Each subagent works to the spec below, commits on its branch, and
     reports back with test and benchmark results. It doesn't push or merge.
3. **Integration,** by the integrating session:
   - review each branch against its spec;
   - send back fixes until each is right;
   - merge A and C into one integration branch, resolving any conflicts;
   - re-run the full verification (A's equality tests, C's comparison, and a
     full build benchmark with both);
   - open one PR per change (B, A, C), or a combined A+C PR if they're easier
     to review together, and merge only after owner approval.
4. **Order of effect:**
   - A alone is ~30–60× (per-object work across ~64 workers);
   - C alone is ~5–10× per fit;
   - together, the fits should drop from ~2.5 h to on the order of a minute,
     leaving the fixed loading and join costs, ~20–40 s at full size, as the
     floor.

## Why (measurements)

A full `ssp-build-ssobject` takes ~2.5 h, on one core of a 128-core machine.

Measured 2026-09-27 on an SSSource of 3,000 random objects (80,114
observations). The SSSource was built with `python -m ssp.sssource
--max-objects 3000 --seed 1` from the `extract-submitted-sources` output for
the 2026-09-25 obs_sbn dump, with MPC tables exported 2026-09-26.

| | |
|---|---|
| wall time (not profiled) | 98.6 s; user 113 s, sys 1 s (one core) |
| per object | ~31 ms |
| extrapolated to 297,747 objects | ~2.5 h |

Where the time goes (cProfile, which inflates everything to 155 s):

| part | time | share |
|---|---|---|
| per-object loop (`util.group_by` → `compute_ssobject_entry`) | 144.0 s | 93% |
| &nbsp;&nbsp;of which `photfit.fitHG12` (9,149 fits) | 134.9 s | 87% |
| MOID loop (`MOIDSolver.compute`, 3,000 calls) | 4.8 s | 3% |
| SSSource ↔ DiaSource join | 1.6 s | 1% |
| reading SSSource, DiaSource and orbits | 0.4 s | <1% |

Two things to take from this:
- **The work is per object and independent,** so it is embarrassingly
  parallel.
- **The MOID loop is small in this sample but not at full size.** At
  ~1.6 ms per object it is ~8 min serially for 297,747 objects, so it must be
  parallelized too, or it becomes the bottleneck once the fits are.

## A. Parallel SSObject build

### Goals

- **A full build takes minutes, not hours,** on the 50+ core machines this
  repo assumes.
- **The output is identical to the serial build:** every column of every row,
  NaN-aware and bitwise for floats. No change to what is computed.
- **The change is small and local to `ssp/ssobject.py`.** No new dependencies,
  and `--workers 1` keeps today's code path.

### Non-goals

- **Faster fits.** That's C, specified separately below.
- **Changes to what SSObject computes,** its schema or its inputs.
- **Distributing across machines.**

### Design

#### Where the parallelism goes

`compute_ssobject()` keeps its current structure: filter the rows (orbit-less
and non-primary), join to DiaSource, add the magnitude columns, then
**two parallel stages** instead of two serial loops:

1. **Per-object quantities.** This is what `util.group_by(..., compute_ssobject_entry)`
   does today.
2. **MOID** for the objects that have an `mpc_orbits` row.

The Tisserand parameter, the `argjoin` against `mpc_orbits` and the output
assembly stay in the parent process. They are vectorized and cheap.

#### Mechanism

- **`multiprocessing` with the `fork` start method** (Linux), via
  `concurrent.futures.ProcessPoolExecutor(mp_context=get_context("fork"))`.
- **Workers read the parent's already-loaded tables through module-level
  globals** set just before the pool is created, so they are inherited by the
  fork, not pickled. The joined SSSource frame is pyarrow-backed. Its buffers
  are never written, so copy-on-write keeps them shared across workers.
- **Tasks and results are small.**
  - A task is a range of group indices (stage 1) or of orbit indices
    (stage 2).
  - A stage-1 result is a structured `SSObjectDtype` array for that range,
    370 bytes per object.
  - A stage-2 result is the five MOID output columns for that range.

  At full size that is ~110 MB of results in total, which is negligible.
- **`spawn`/`forkserver` are rejected.** Each worker would have to reload or
  receive the 8M-row DiaSource table.

#### Partitioning (stage 1)

- **Group boundaries are computed once in the parent,** exactly as
  `util.group_by` does today: `np.unique(keys, return_index=True,
  return_counts=True)` on `ssObjectId`, with the same "is grouped" check. The
  output row order is therefore the same as today's: ascending `ssObjectId`.
- **Chunks are contiguous runs of groups, balanced by observation count, not
  object count.** Objects range from a handful of observations to thousands,
  and the fit cost grows with them. The target is about `8 × workers` chunks
  (`--chunk-factor`, default 8), so a slow chunk can't leave the other
  workers idle at the end.
- **Each worker loops over its groups** with the same slicing and the same
  `compute_ssobject_entry(row, sss_slice, ...)` call as today, writing into
  its local result array.
- **The parent copies each result into `obj[start:end]` as it arrives,** and
  prints progress (objects done, elapsed time, rough time left) once per
  finished chunk. This replaces `group_by`'s print every 100 objects, which
  can't be ordered sensibly across processes.

#### Stage 2 (MOID)

- **The parent computes `a, e, i, node, argperi, epoch_mjd` for the matched
  objects** as today, and splits them into index ranges.
- **Each worker builds one `MOIDSolver`** (per process, not per call) and runs
  the existing per-object loop over its range, including `earth_orbit(epoch)`.
- **The parent writes the five MOID columns back through `oidx`.**

#### Interface

- **`ssp-build-ssobject` gets `--workers N`.** The default is
  `min(64, os.cpu_count())`: past ~64 the fixed loading and assembly costs
  dominate, and the machines are shared.
- **`--workers 1` runs the current serial code path unchanged,** with no pool
  and no fork. It is also the fallback on platforms without `fork`.
- **`compute_ssobject()` gets a `workers=1` keyword,** so library callers,
  including the tests, get today's behaviour unless they ask.

#### Threads inside workers

numpy here uses OpenBLAS. The fits' linear algebra is on 2×2 systems, well
below OpenBLAS's multithreading thresholds, so N workers shouldn't each spawn
BLAS threads. This is measured, not assumed: the benchmark checks total CPU
(`user + sys`) against `workers × wall`. If there is oversubscription, the
parent sets `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1` and
`MKL_NUM_THREADS=1` before numpy/scipy are imported (the CLI entry point is
the one place that can). No `threadpoolctl` dependency is added unless that
proves insufficient.

#### Errors

- **An exception in any worker aborts the build** with that worker's
  traceback, and no output file is written. Same as today's serial failure;
  no partial SSObject.
- **Warnings stay as they are.** Workers inherit the parent's warning filters,
  so the existing `RuntimeWarning` noise from divergent fits (#9, item 3) is
  unchanged, just interleaved.

### Verification

1. **Equality test (new, no network).**
   - Input: a synthetic SSSource/DiaSource/orbit set of ~40 objects with
     uneven observation counts, including one object with no orbit and one
     with non-primary rows.
   - Check: `compute_ssobject(workers=1)` and `compute_ssobject(workers=3)`
     are identical in every column (NaN-aware, bitwise).
   - This also exercises uneven chunks and more chunks than workers.
2. **Real-data equality.** On the 3,000-object subset above, the serial and
   the parallel `ssobject.parquet` are identical in every column and row.
3. **Benchmark**, reported in the PR:
   - wall time and CPU use for the 3,000-object subset at 1, 8, 32 and 64
     workers;
   - one full build: all 297,747 objects from a full SSSource, at the default
     worker count, with wall time and peak memory.
4. **The existing tests pass**, including `tests/test_ssobject_primary.py`,
   which runs the serial path.

**Expected result:** ~2.5 h → about **3–5 min** at 64 workers, of which
~20–40 s is fixed (reading 8M DiaSource rows and 1.6M orbits, the join, and
writing the output). Peak memory about what the serial build uses plus a small
per-worker overhead.

### Risks

- **Load imbalance** from a few very large objects. Mitigated by chunking on
  observation counts and by using many more chunks than workers. If one
  object alone dominates, it's visible in the per-chunk progress lines.
- **Copy-on-write erosion.** Python refcount updates on shared objects touch
  pages, which then get copied per worker. The large data is in Arrow buffers
  that refcounting never touches, so this should stay small. It will be
  measured as peak memory at 64 workers.
- **Non-bitwise differences** would mean nondeterminism in the fit (for
  example BLAS thread count affecting a reduction). The equality tests would
  catch it, and the thread limits above would be the fix.

## B. `sssource.py` reads only the DiaSource columns it uses

- **Today:** `build_sssource` loads all 148 columns of `dia_sources.parquet`
  (2.6 GB since the view's v2). That peaked at **19.7 GB** of memory even for
  the 3,000-object subset.
- **What it uses:** 12 columns: `diaSourceId`, `ra`, `dec`,
  `midpointMjdTai`, and when present `obsid`, `processing`, `primary`,
  `submission_id`, `trksub`, `trkid`, `sep_mas`, `dt_ms`.
- **Change:** read the file's schema, then read the intersection of those
  names. That works for the Butler path (4 columns) and the extractor path
  alike.
  - `--dia-sample-frac` keeps working unchanged, since it samples after
    loading.
  - The column list sits next to the code that uses it, with a comment to
    extend it when a new column is used.
- **Verification:**
  - `sssource.parquet` identical before and after, every column, for the
    3,000-object subset and for a Butler-path run (`--max-objects 10`);
  - peak memory and wall time reported before and after.

## C. Cheaper H/G12 fits (G12 bounded to [0, 1])

**Owner decision (2026-09-27):** G12 is bounded to **[0, 1]**. This
intentionally changes results for fits that today diverge outside that range
(#9, item 3, 30–40% of objects on narrow phase-angle ranges).

### Approach

- **The model is linear in H.** In magnitudes, for fixed G12,
  `m_i = H + f_i(G12)` with `f_i = -2.5 log10(G1 Φ1 + G2 Φ2 + (1 - G1 - G2) Φ3)`.
  With weights `w_i = 1/σ_i²`, the best H is the weighted mean
  `H*(G12) = Σ w_i (m_i - f_i) / Σ w_i`.
- **So the fit is a one-parameter search** minimizing
  `χ²(G12) = Σ w_i (m_i - f_i - H*)²` over `G12 ∈ [0, 1]`.
  - It uses the existing `_HG1G2_basis` (Φ1, Φ2, Φ3 evaluated once per fit)
    and the existing piecewise `G12 → (G1, G2)` mapping, with its kink at
    0.2.
  - **Coarse grid first:** evaluate χ² on a fixed grid of G12 values over
    [0, 1] (e.g. 101 points), vectorized in one numpy expression.
  - **Then refine** the best grid cell with a bounded scalar minimizer
    (`scipy.optimize.minimize_scalar(method="bounded")`, or a golden-section
    search) to a tolerance of 1e-6 in G12. The grid makes it safe against
    local minima; the refinement gives the precision.
- **`fixedG12` stays exact:** just `H*(fixedG12)`, no search.
- **The robust path is unchanged:** with `nSigmaClip` (off by default), the
  `soft_l1` first stage stays `least_squares`, since its loss isn't
  quadratic. The final linear-loss fit then uses the new method.

### Outputs (same fields and schema as today)

- **`H`, `G12` and `chi2dof`** come from the minimum.
- **Uncertainties when G12 is inside (0, 1):** `H_err`, `G12_err` and `HG_cov`
  come from `inv(JᵀJ)` of the two-parameter model at the solution, using the
  existing analytic Jacobian. That's the same formula as today, so interior
  fits are directly comparable.
- **Uncertainties when G12 is at a bound:**
  - `G12` is the bound (0 or 1);
  - `G12_err` and `HG_cov` are **NaN**, since a Gaussian error at a hard
    bound isn't meaningful;
  - `H_err` is the fixed-G12 error `1/sqrt(Σ w_i)`.

  **Flagged for owner review:** the alternative is to report the
  two-parameter formula even at a bound.
- **Failure:** `nobs = 0` and NaNs, exactly as today (no finite
  observations, a singular `JᵀJ`).

### Verification

1. **Unit tests** in `tests/test_photfit.py`:
   - noiseless synthetic data at several G12 values in [0, 1], including near
     0.2 and at 0 and 1, recover H and G12 to 1e-6;
   - data generated with G12 outside [0, 1] gives the bound, with NaN
     `G12_err`/`HG_cov`;
   - `fixedG12` gives the closed-form H;
   - the error handling is unchanged.
2. **Agreement with today's fits,** on every (object, band) fit of the
   3,000-object subset, old `fitHG12` against new:
   - where today's fit converged with G12 in [0, 1]: `|ΔH| < 1e-4` mag,
     `|ΔG12| < 1e-3`, and `H_err`/`G12_err`/`chi2dof` within 1%;
   - for the rest: report how many there are and how H and G12 move. These
     are the intended changes.
3. **Speed:** time per fit old against new on the same subset, and the
   3,000-object build's wall time.

## Follow-ups (not in this change)


**D. Per-object numpy instead of pandas slices.** A few percent at most. It
would be done only alongside A, and only if the benchmark shows the slicing
matters once the work runs in parallel.
