# Design: faster SSSource/SSObject builds

Status: **proposed**, for review. Nothing here is implemented yet.
Tracks items 3 and 4 of [#9](https://github.com/mjuric/ssp-tools/issues/9).

Four changes:
- **A:** run the SSObject per-object work in parallel;
- **B:** `sssource.py` reads only the DiaSource (and obs_sbn) columns it uses
  (done: #13);
- **C:** cheaper H/G12 fits, with G12 bounded to [0, 1], and DP2's robust
  defaults (0.05 mag floor, 10σ clipping);
- **E:** a faster SSSource build: E1 runs the per-object loop in parallel,
  E2 replaces per-object astropy bookkeeping with numpy.

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
4. **E in two steps.** E2 is independent of A and runs now, in parallel with
   A and C (one subagent, branch `sssource-numpy`). E1 follows once A and E2
   are merged, reusing A's pool and chunking. With E2 already in, E1's
   serial-against-parallel equality check compares identical code. The same
   review, integration and merge flow as above.
5. **Order of effect:**
   - A alone is ~30–60× (per-object work across ~64 workers);
   - C alone is ~5–10× per fit, with the robust stage included, since both
     stages become one-parameter searches;
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

## C. Cheaper H/G12 fits, and DP2's robust defaults

**Owner decisions (2026-09-27):**
- **G12 is a free fit bounded to [0, 1].** This intentionally changes results
  for fits that today diverge outside that range (#9, item 3, 30–40% of
  objects on narrow phase-angle ranges). DP2 instead fixed G12 at 0.5; that
  remains available with `--hg12FixedG12 0.5`.
- **Robust fitting is on by default, as in DP2:** a 0.05 mag error floor in
  quadrature, a robust `soft_l1` first fit, rejection of residuals beyond
  10σ, then an ordinary least-squares fit on the retained points.

### DP2 comparison

DP2's SSObject used pipe_tasks `fitHG12`
([DM-54843, pipe_tasks#1300](https://github.com/lsst/pipe_tasks/pull/1300)),
configured in `lsst/drp_pipe` `pipelines/_ingredients/LSSTCam/DRP.yaml`
(commit `299697d7a6`) with `hg12FixedG12: 0.5`, `hg12MagSigmaFloor: 0.05`
and `hg12NSigmaClip: 10`. Described in RTN-115 §4.5.2.

- **Our `fitHG12` is a backport of that function.** The only differences are
  performance ones: a precomputed basis and an analytic Jacobian. The flux →
  magnitude conversion (31.4 − 2.5 log10 f; σ = 1.085736 σ_f/f), the distance
  reduction, the `soft_l1` stage with `f_scale=1`, the clipping and the final
  fit are the same.
- **So the gap to DP2 is only the defaults:**

| option | DP2 | today | after C |
|---|---|---|---|
| error floor (`--hg12MagSigmaFloor`) | 0.05 | 0.0 | **0.05** |
| clip threshold (`--hg12NSigmaClip`) | 10 | off | **10** |
| G12 (`--hg12FixedG12`) | fixed 0.5 | free, unbounded | **free, bounded [0, 1]** |

- **The floor also matters for the clipping.** `f_scale=1` and the 10σ
  threshold are both in units of the magnitude errors. Without the floor,
  bright detections' small formal errors make both far stricter than
  intended.
- **Where the defaults go:** the CLI (`ssp-build-ssobject`) and the
  `compute_ssobject()` keywords. `fitHG12`'s own keyword defaults stay neutral
  (no floor, no clipping), matching pipe_tasks, so the function stays a
  faithful building block.

### Approach

The weighted residuals are `r_i = (m_i − H − f_i(G12)) / σ_i`, with
`f_i = −2.5 log10(G1 Φ1 + G2 Φ2 + (1 − G1 − G2) Φ3)`, and `m_i` the
distance-reduced magnitudes with the floor already applied to `σ_i`. For a
fixed G12 the model is linear in H, so in both stages H is solved per G12
value and only G12 is searched.

- **Final stage (linear loss).**
  - For a given G12, the best H is the weighted mean
    `H*(G12) = Σ w_i (m_i − f_i) / Σ w_i`, with `w_i = 1/σ_i²`.
  - That leaves a one-parameter search minimizing
    `χ²(G12) = Σ w_i (m_i − f_i − H*)²` over [0, 1].
- **Robust stage (`soft_l1`)**, with `ρ(z) = 2(√(1+z) − 1)` of `z = r²`,
  and the same scale as today (`f_scale = 1`).
  - For a given G12, the best H minimizes `Σ ρ(r_i²)`. That's a
    one-dimensional convex problem, solved by iteratively reweighted least
    squares: weights `w_i ∝ (1 + r_i²)^(−1/2)`, a few iterations to a
    tolerance of 1e-9 mag.
  - The profiled robust cost is then minimized over G12 in [0, 1], the same
    way as the final stage.
- **The search over G12** is the same in both stages:
  - a coarse fixed grid over [0, 1] (e.g. 101 points), evaluated vectorized
    across the grid, including the per-G12 H solve;
  - then a bounded scalar refinement of the best cell
    (`scipy.optimize.minimize_scalar(method="bounded")`, or golden-section)
    to 1e-6 in G12.

  The grid guards against local minima, and the piecewise G12 → (G1, G2)
  mapping with its kink at 0.2 is used unchanged. The refinement gives the
  precision.
- **Clipping is unchanged:** keep `|r_i| < nSigmaClip`, with the residuals
  taken at the robust stage's solution. `nObsUsed` is the retained count.
- **`fixedG12`:** both stages reduce to the H solve alone, with no search. So
  DP2's configuration is also fast.
- **Reference implementation.** The existing `least_squares` path stays in
  `photfit.py` as a private reference (`_fitHG12_reference`), extended with
  bounds `[0, 1]` on G12 in both stages. It is used only by tests and the
  verification below, to check the fast path.

### Outputs (same fields and schema as today)

- **`H`, `G12` and `chi2dof`** come from the final stage's minimum.
- **Uncertainties when G12 is inside (0, 1):** `H_err`, `G12_err` and
  `HG_cov` come from `inv(JᵀJ)` of the two-parameter model at the solution,
  using the existing analytic Jacobian. That's the same formula as today, so
  interior fits are directly comparable.
- **Uncertainties when G12 is at a bound:**
  - `G12` is the bound (0 or 1);
  - `G12_err` and `HG_cov` are **NaN**, since a Gaussian error at a hard
    bound isn't meaningful;
  - `H_err` is the fixed-G12 error `1/sqrt(Σ w_i)`.

  **Owner decision (2026-09-27):** NaN at a bound, as above.
- **Failure:** `nobs = 0` and NaNs, as today (no finite observations, too
  few points left after clipping, a singular `JᵀJ`).

### Verification

1. **Unit tests** in `tests/test_photfit.py`:
   - noiseless synthetic data at several G12 values in [0, 1], including
     near 0.2 and at 0 and 1, recover H and G12 to 1e-6;
   - data generated with G12 outside [0, 1] gives the bound, with NaN
     `G12_err`/`HG_cov`;
   - with injected outliers, the right points are rejected;
   - `fixedG12` gives the closed-form H;
   - the error handling is unchanged.
2. **Fast path against the bounded reference,** on every (object, band) fit
   of the 3,000-object subset, with the new defaults (floor 0.05, clip 10σ):
   - the retained sets after clipping are identical in ≥ 99.9% of fits, with
     each exception listed;
   - where both converge: `|ΔH| < 1e-4` mag, `|ΔG12| < 1e-3`, and
     `H_err`/`G12_err`/`chi2dof` within 1%;
   - robust stage alone: the same `|ΔH|`/`|ΔG12|` tolerances.
3. **The effect of the new defaults** (reported, not asserted). Against
   today's build of the same subset (free unbounded fit, no floor, no clip):
   - how many fits end at a G12 bound;
   - how many points get clipped;
   - the distributions of ΔH and ΔG12.

   Also compared against a run with `--hg12FixedG12 0.5`, i.e. DP2's
   configuration.
4. **Speed:** time per fit, old reference against new fast path, with the new
   defaults (robust stage included), and the 3,000-object build's wall time.

## E. Faster SSSource build

### Measurements

- **The full SSSource build takes ~34 min** (2,062 s for 297,749 objects,
  2026-09-26), about 6.6 ms per object, on one core.
- **Profile on the 3,000-object subset** (cProfile, which inflates the total
  to 42.9 s against 31.5 s unprofiled):

| part | time | calls |
|---|---|---|
| per-object loop (`util.group_by` → `compute_sssource_entry`) | 31.0 s (72%) | 3,000 |
| &nbsp;&nbsp;ASSIST ephemerides (`compute_ephemerides_one`) | 13.8 s | 3,000 |
| &nbsp;&nbsp;astropy `SkyCoord` construction + `separation` | 4.8 s | 9,006 |
| &nbsp;&nbsp;pandas slicing | 1.9 s | 6,014 |
| &nbsp;&nbsp;other per-object Python | ~10 s | |
| vectorized: observatory positions, elongation, ecl/gal transforms | 4.5 s | |
| input joins | 1.9 s | |
| reading inputs | 0.8 s | |

### E1. Parallel per-object loop

- **Where:** in `build_sssource`, the `util.group_by([sss[:n_orbit], assoc.iloc[:n_orbit]], "ssObjectId", compute_sssource_entry…)`
  call. Each object's ephemerides are independent.
- **Mechanism, as in A:**
  - a fork-based `ProcessPoolExecutor`;
  - inputs shared through module-level globals set before the fork: the
    `sss` structured array, `assoc`, `dia_eph` and `mpcorb`;
  - contiguous chunks of groups, balanced by observation count, about
    `8 × workers` chunks.
  - **Reuse A's code.** If it factors naturally, move A's chunking and pool
    helper into `ssp/util.py` and use it from both. A's tests must still
    pass. Don't build a framework.
- **Results.** Today `compute_sssource_entry` writes into slices of `sss`. In
  a forked worker those writes land in its private copy-on-write pages, so
  each worker returns **only the fields it computes**:
  - the eph*, helio*, topo* and range/rate columns, `phaseAngle` and
    `ephVmag`, as one structured array per chunk;
  - kept as an explicit module-level list (`EPH_FIELDS`) next to
    `compute_sssource_entry`, with a test that it matches what the function
    writes.

  The parent assigns each chunk back into `sss`.
- **ASSIST ephemeris:**
  - each worker opens its own `open_ephem()` in the pool initializer, rather
    than sharing the parent's C-level object across the fork;
  - the parent doesn't open one when `workers > 1`;
  - measure per-worker memory; if the ephemeris files are read into memory
    per worker, report it and cap the default worker count to fit.
- **Interface:**
  - `python -m ssp.sssource --workers N`, default `min(64, os.cpu_count())`;
  - `--workers 1` runs today's serial code path unchanged;
  - library default `workers=1`.
- **Output and errors:**
  - the per-object `max/median separation` lines are kept (interleaved
    across workers);
  - progress is printed per chunk;
  - any worker exception fails the build with no output written.
- **Verification:**
  - `sssource.parquet` from `--workers 1` and `--workers 64` is
    **identical** (`Table.equals`) on the 3,000-object subset;
  - a new synthetic test: `workers=1` against `workers=3`;
  - a benchmark at 1, 8, 32 and 64 workers (wall time, CPU, peak RSS);
  - one full build at the default worker count, identical to the serial
    full build at `/lscratch/mjuric/sspwt/full/sssource.parquet` (B's code)
    apart from E2's tolerance below.

### E2. Numpy instead of per-object astropy and pandas bookkeeping

- **`ephRa`/`ephDec`:** replace `SkyCoord(ra=e.ra_deg, dec=e.dec_deg)` →
  `.ra.deg`/`.dec.deg` with the equivalent numpy normalization (RA wrapped to
  [0, 360), Dec unchanged). The result must be **bitwise identical** to
  today's.
- **`ephOffset`:** replace the second `SkyCoord` and `eph.separation(obsv)`
  with a float64 haversine separation, as in
  `ssp/export/submittable.py:sep_mas`. That's not bitwise, so the tolerance
  is `|ΔephOffset| ≤ 1e-9″`, with the maximum difference reported. Add a
  unit test of the haversine against `SkyCoord.separation` on random points,
  including the poles and the RA wrap.
- **Per-object slicing:** prepare `dia_eph` and the observer state in `assoc`
  as numpy arrays once, before the loop, instead of
  `dia.iloc[...]`/`assoc[[...]].to_numpy()` per object. The values are
  identical.
- **Unchanged:** the `Time` construction and the `compute_ephemerides_one`
  call. ASSIST's API needs them, and that's the inherent work.
- **Verification:** `sssource.parquet` on the 3,000-object subset against
  B's serial output (`/lscratch/mjuric/sspwt/bbench/new_ext/sssource.parquet`):
  every column identical except `ephOffset`, which must be within
  tolerance.
- **Expected:** roughly 15–25% off the per-object cost.

**Expected combined:** ~34 min → about 1 min at 64 workers (E1), a little
less with E2, plus ~10–20 s of fixed cost at full size.

## Follow-ups (not in this change)


**D. Per-object numpy instead of pandas slices.** A few percent at most. It
would be done only alongside A, and only if the benchmark shows the slicing
matters once the work runs in parallel.
