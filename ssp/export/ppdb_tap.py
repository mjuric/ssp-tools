"""``ppdb_dia_sources.parquet`` from a PPDB TAP service.

See docs/design/ppdb-tap.md.

The five ``ppdb.DiaSource`` columns NearbySSO reads, fetched in visit-range
chunks of about CHUNK_ROWS rows as UWS async jobs (pyvo, with
RESPONSEFORMAT=parquet), one at a time (concurrent jobs can stay in
EXECUTING indefinitely on data-int, DM-56627):

1. a sync ``GROUP BY visit`` count fixes the visits and their row counts;
2. each chunk is one job; its rows must equal the counted rows of its
   visits (a difference fails the step: the PPDB changed during the
   extract, and a rerun picks it up), with the five columns cast exactly
   to their types and no NULLs;
3. a job still QUEUED or EXECUTING after JOB_TIMEOUT_S is aborted and
   retried once; an ERROR phase or an HTTP error is retried once too, a
   refused token never;
4. the parts are concatenated, checked (unique diaSourceId, every counted
   visit present, the total), sorted by (visit, diaSourceId) and written
   with zstd.

Every job is deleted, also on failure. The bearer token is read from a
file of mode 0600 and appears nowhere else (not in messages, logs or the
manifest).
"""

from __future__ import annotations

import stat
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DEFAULT_TAP_URL = "https://data-int.lsst.cloud/api/ppdbtap"
DEFAULT_TOKEN_FILE = "~/.data-int.token"
TABLE = "ppdb.DiaSource"
SCHEMA = pa.schema([("diaSourceId", pa.int64()), ("visit", pa.int64()), ("midpointMjdTai", pa.float64()),
                    ("ra", pa.float64()), ("dec", pa.float64())])
SELECT = f"SELECT {', '.join(SCHEMA.names)} FROM {TABLE}"
#: Rows per chunk: 2M-row jobs hung on 2026-10-09; 1M-row ones went through.
CHUNK_ROWS = 1_000_000
#: A job not finished after this long is aborted (a chunk alone takes
#: 10-40 s).
JOB_TIMEOUT_S = 300
POLL_S = 5
HTTP_TIMEOUT_S = 120


class PPDBTapError(RuntimeError):
    """The PPDB could not be read over TAP, or its rows failed a check."""


class TokenRefusedError(PPDBTapError):
    """HTTP 401/403: not retried."""


def read_token(path):
    """The bearer token in ``path``, which must be mode 0600 (or stricter)."""
    p = Path(path).expanduser()
    try:
        mode = p.stat().st_mode
    except OSError as e:
        raise PPDBTapError(f"the TAP token file {p}: {e.strerror}") from None
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise PPDBTapError(f"the TAP token file {p} is readable by others (mode {stat.S_IMODE(mode):o}); "
                           "chmod 600 it")
    token = p.read_text().strip()
    if not token:
        raise PPDBTapError(f"the TAP token file {p} is empty")
    return token


class _Tap:
    """The PPDB's TAP service, through pyvo, with the bearer token on a
    requests session."""

    def __init__(self, url, token, token_file):
        self.url, self._token, self.token_file = url.rstrip("/"), token, token_file
        self._svc = None

    def _service(self):
        """(TAPService, requests session)."""
        import pyvo
        import requests

        if self._svc is None:
            s = requests.Session()
            s.headers["Authorization"] = "Bearer " + self._token
            self._svc = (pyvo.dal.TAPService(self.url, session=s), s)
        return self._svc

    def _error(self, e, what):
        """``e`` (from pyvo or requests) as a PPDBTapError; a refused token
        as TokenRefusedError."""
        status = getattr(getattr(e, "response", None), "status_code", None)
        status = status or getattr(getattr(getattr(e, "cause", None), "response", None), "status_code", None)
        if status in (401, 403):
            return TokenRefusedError(f"{what}: HTTP {status}: the token in {self.token_file} was "
                                     "refused (expired?)")
        return PPDBTapError(f"{what}: {type(e).__name__}: {e}")

    def sync(self, query):
        """A small sync query, as an Arrow table (VOTable on the wire)."""
        try:
            return pa.Table.from_pandas(self._service()[0].run_sync(query).to_table().to_pandas(),
                                        preserve_index=False)
        except Exception as e:  # (pyvo's DAL errors and requests')
            raise self._error(e, f"sync query {query!r}") from None

    def run_job(self, query, dst, label):
        """One async job with a Parquet result, streamed to ``dst``; the job
        is deleted whatever happens. Returns the job URL."""
        try:
            job = self._service()[0].submit_job(query, maxrec=10 * CHUNK_ROWS, RESPONSEFORMAT="parquet")
        except Exception as e:
            raise self._error(e, f"{label}: create job") from None
        try:
            job.run()
            t0 = time.monotonic()
            while (phase := job.phase) in ("PENDING", "QUEUED", "EXECUTING", "RUN", "UNKNOWN"):
                if time.monotonic() - t0 > JOB_TIMEOUT_S:
                    job.abort()
                    raise PPDBTapError(f"{label}: job {job.url} still {phase} after {JOB_TIMEOUT_S} s: "
                                       "aborted")
                time.sleep(POLL_S)
            if phase != "COMPLETED":
                job.raise_if_error()
                raise PPDBTapError(f"{label}: job {job.url} ended {phase}")
            tmp = dst.with_suffix(".tmp")
            with self._service()[1].get(job.result_uri, stream=True, timeout=HTTP_TIMEOUT_S) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for b in r.iter_content(1 << 24):
                        f.write(b)
            tmp.rename(dst)
            return job.url
        except PPDBTapError:
            raise
        except Exception as e:
            raise self._error(e, f"{label}: job {job.url}") from None
        finally:
            try:
                job.delete()
            except Exception as e:  # (report it; the job's outcome stands)
                print(f"warning: {label}: could not delete {job.url}: {e}", flush=True)


def visit_chunks(visits, counts, target=CHUNK_ROWS):
    """[(lo, hi, rows)]: contiguous ranges of the sorted ``visits``, closed
    at the first visit where the rows reach ``target`` (a visit is never
    split)."""
    out, lo, acc = [], None, 0
    for v, n in zip(visits.tolist(), counts.tolist()):
        lo = v if lo is None else lo
        acc += n
        if acc >= target:
            out.append((lo, v, acc))
            lo, acc = None, 0
    if lo is not None:
        out.append((lo, int(visits[-1]), acc))
    return out


def _cast(t, label):
    """``t`` as exactly SCHEMA, with no NULLs; an unsafe cast fails."""
    missing = [n for n in SCHEMA.names if n not in t.column_names]
    if missing:
        raise PPDBTapError(f"{label}: no column(s) {missing} (got {t.column_names})")
    cols = []
    for f in SCHEMA:
        c = t[f.name]
        if c.null_count:
            raise PPDBTapError(f"{label}: {c.null_count} NULL {f.name}")
        try:
            cols.append(c.cast(f.type, safe=True))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
            raise PPDBTapError(f"{label}: {f.name} ({c.type}) does not cast to {f.type}: {e}") from None
    return pa.Table.from_arrays(cols, schema=SCHEMA)


def export(out_path, tap_url=DEFAULT_TAP_URL, token_file=DEFAULT_TOKEN_FILE, log=print):
    """Write ``ppdb_dia_sources.parquet`` to ``out_path`` from the TAP
    service at ``tap_url``. Returns the manifest's ``ppdb_tap`` details."""
    out_path = Path(out_path)
    tap = _Tap(tap_url, read_token(token_file), Path(token_file).expanduser())

    # 1. the visits and their counts
    t0 = time.monotonic()
    c = tap.sync(f"SELECT visit, COUNT(*) AS n FROM {TABLE} GROUP BY visit")
    if c["visit"].null_count:
        raise PPDBTapError(f"{TABLE} has rows with a NULL visit")
    visits = c["visit"].cast(pa.int64()).to_numpy()
    counts = c["n"].cast(pa.int64()).to_numpy()
    o = np.argsort(visits)
    visits, counts = visits[o], counts[o]
    if not len(visits):
        raise PPDBTapError(f"{TABLE} is empty")
    expected = dict(zip(visits.tolist(), counts.tolist()))
    chunks = visit_chunks(visits, counts)
    log(f"ppdb_dia_sources: {int(counts.sum()):,} rows in {len(visits):,} visits "
        f"({visits[0]}..{visits[-1]}), {len(chunks)} chunks of ~{CHUNK_ROWS:,} rows, "
        f"one job at a time (counted in {time.monotonic() - t0:.0f} s)")

    # 2-3. the chunks
    parts = out_path.parent / (out_path.stem + ".parts")
    parts.mkdir(exist_ok=True)
    retried = []

    def one(k):
        lo, hi, rows = chunks[k]
        label = f"chunk {k + 1}/{len(chunks)} (visits {lo}..{hi})"
        dst = parts / f"part-{k:04d}.parquet"
        query = f"{SELECT} WHERE visit BETWEEN {lo} AND {hi}"
        for attempt in (1, 2):
            t1 = time.monotonic()
            try:
                tap.run_job(query, dst, label)
            except TokenRefusedError:
                raise
            except Exception as e:  # (one retry, then fail)
                if attempt == 2:
                    raise PPDBTapError(f"{label}: failed twice; last: {e}") from None
                log(f"{label}: {e}; retrying once")
                retried.append(k)
                continue
            t = _cast(pq.read_table(dst), label)
            v = t["visit"].to_numpy()
            if len(t) and (v.min() < lo or v.max() > hi):
                raise PPDBTapError(f"{label}: rows outside the visit range")
            if len(t) != rows:
                raise PPDBTapError(f"{label}: got {len(t):,} rows, {rows:,} counted: the PPDB changed "
                                   f"during the extract; rerun")
            pq.write_table(t, dst)
            log(f"{label}: {len(t):,} rows in {time.monotonic() - t1:.0f} s")
            return len(t)

    for k in range(len(chunks)):
        one(k)

    # 4. assemble and check
    t = pa.concat_tables([pq.read_table(parts / f"part-{k:04d}.parquet") for k in range(len(chunks))])
    n_unique = pc.count_distinct(t["diaSourceId"]).as_py()
    if n_unique != len(t):
        raise PPDBTapError(f"{len(t) - n_unique:,} duplicate diaSourceId")
    got = pc.value_counts(t["visit"])
    got = dict(zip(got.field("values").to_pylist(), got.field("counts").to_pylist()))
    if got != expected:
        bad = sorted(v for v in expected.keys() | got.keys() if got.get(v) != expected.get(v))
        raise PPDBTapError(f"{len(bad)} visit(s) with rows other than counted, e.g. {bad[:5]}")
    t = t.sort_by([("visit", "ascending"), ("diaSourceId", "ascending")])
    pq.write_table(t, out_path, compression="zstd")
    for p in parts.iterdir():
        p.unlink()
    parts.rmdir()
    return {"url": tap.url, "count_star": int(counts.sum()), "visits": len(visits),
            "visit_min": int(visits[0]), "visit_max": int(visits[-1]), "chunks": len(chunks),
            "retried_jobs": len(retried)}
