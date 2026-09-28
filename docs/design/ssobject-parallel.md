# Design: parallel SSObject build

Status: **proposed**, for review. Nothing here is implemented yet.
Tracks item 4 of [#9](https://github.com/mjuric/ssp-tools/issues/9).

## Why

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

## Goals

- **A full build takes minutes, not hours,** on the 50+ core machines this
  repo assumes.
- **The output is identical to the serial build:** every column of every row,
  NaN-aware and bitwise for floats. No change to what is computed.
- **The change is small and local to `ssp/ssobject.py`.** No new dependencies,
  and `--workers 1` keeps today's code path.

## Non-goals

- **Faster fits** (see *Follow-ups*, C). That can change results, so it is a
  separate change that needs its own decision.
- **Changes to what SSObject computes,** its schema or its inputs.
- **Distributing across machines.**

## Design

### Where the parallelism goes

`compute_ssobject()` keeps its current structure: filter the rows (orbit-less
and non-primary), join to DiaSource, add the magnitude columns, then
**two parallel stages** instead of two serial loops:

1. **Per-object quantities.** This is what `util.group_by(..., compute_ssobject_entry)`
   does today.
2. **MOID** for the objects that have an `mpc_orbits` row.

The Tisserand parameter, the `argjoin` against `mpc_orbits` and the output
assembly stay in the parent process. They are vectorized and cheap.

### Mechanism

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

### Partitioning (stage 1)

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

### Stage 2 (MOID)

- **The parent computes `a, e, i, node, argperi, epoch_mjd` for the matched
  objects** as today, and splits them into index ranges.
- **Each worker builds one `MOIDSolver`** (per process, not per call) and runs
  the existing per-object loop over its range, including `earth_orbit(epoch)`.
- **The parent writes the five MOID columns back through `oidx`.**

### Interface

- **`ssp-build-ssobject` gets `--workers N`.** The default is
  `min(64, os.cpu_count())`: past ~64 the fixed loading and assembly costs
  dominate, and the machines are shared.
- **`--workers 1` runs the current serial code path unchanged,** with no pool
  and no fork. It is also the fallback on platforms without `fork`.
- **`compute_ssobject()` gets a `workers=1` keyword,** so library callers,
  including the tests, get today's behaviour unless they ask.

### Threads inside workers

numpy here uses OpenBLAS. The fits' linear algebra is on 2×2 systems, well
below OpenBLAS's multithreading thresholds, so N workers shouldn't each spawn
BLAS threads. This is measured, not assumed: the benchmark checks total CPU
(`user + sys`) against `workers × wall`. If there is oversubscription, the
parent sets `OPENBLAS_NUM_THREADS=1`, `OMP_NUM_THREADS=1` and
`MKL_NUM_THREADS=1` before numpy/scipy are imported (the CLI entry point is
the one place that can). No `threadpoolctl` dependency is added unless that
proves insufficient.

### Errors

- **An exception in any worker aborts the build** with that worker's
  traceback, and no output file is written. Same as today's serial failure;
  no partial SSObject.
- **Warnings stay as they are.** Workers inherit the parent's warning filters,
  so the existing `RuntimeWarning` noise from divergent fits (#9, item 3) is
  unchanged, just interleaved.

## Verification

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

## Risks

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

## Follow-ups (not in this change)

**B. `sssource.py` reads only the DiaSource columns it uses.**
- Today it loads all 148 columns of `dia_sources.parquet` (2.6 GB since the
  view's v2). That peaked at 19.7 GB even for the 3,000-object subset.
- It uses 12: `diaSourceId`, `ra`, `dec`, `midpointMjdTai`, and when present
  `obsid`, `processing`, `primary`, `submission_id`, `trksub`, `trkid`,
  `sep_mas`, `dt_ms`.
- The fix: read the file schema, then read the intersection of those names.
  That serves the Butler path (4 columns) and the extractor path alike. It's a
  small separate PR, with an equality check of `sssource.parquet` before and
  after.

**C. Cheaper fits.**
- Each fit makes ~118 model evaluations (1,075,157 over 9,149 fits) for a
  two-parameter problem.
- H enters the model linearly in magnitude, so the best H for a given G12 has
  a closed form. The fit then becomes a one-parameter search over G12, likely
  10–20 evaluations and 5–10× faster per fit. It stacks with the
  parallelization.
- **Needs a decision first:** whether G12 is bounded. Today's free fits
  diverge for 30–40% of objects on narrow phase ranges (#9, item 3), and a
  bounded search would change those results. So this is its own design.

**D. Per-object numpy instead of pandas slices.** A few percent at most. It
would be done only alongside A, and only if the benchmark shows the slicing
matters once the work runs in parallel.
