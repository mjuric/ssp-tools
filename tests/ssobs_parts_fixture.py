"""Hand-made partitioned SSObservation deliveries for the tests: parts, a
sidecar and a manifest, written from the contract's rules
(ssp.ssobservation_contract, "The partitioned delivery") and not by the
builder, so that the checks can be tested independently of it.

Not a test module (no test_ prefix); imported by test_delivery_check.py and
test_ssobservation_validate.py.
"""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ssp.ssobservation_contract import (
    MANIFEST_FORMAT_VERSION,
    PART_FILE_FORMAT,
    SIDECAR_FILE,
    SIDECAR_KEY,
    SSOBSERVATION_DICTIONARY,
    SSOBSERVATION_MANIFEST_FILE,
    SSOBSERVATION_SORT,
)


def md5(path):
    return hashlib.md5(Path(path).read_bytes()).hexdigest()


def cuts(sid, part_rows):
    """[(start, stop, null_part)] by the contract: ranged parts close at the
    first object boundary at or after part_rows rows; the NULL-ssObjectId
    rows (which must be last) are cut every part_rows rows; an empty table is
    one empty part. ``sid``: a list of ssObjectIds (None for NULL)."""
    n = len(sid)
    if n == 0:
        return [(0, 0, False)]
    n_ranged = next((i for i, s in enumerate(sid) if s is None), n)
    out, start = [], 0
    while start < n_ranged:
        stop = min(start + part_rows, n_ranged)
        while stop < n_ranged and sid[stop] == sid[stop - 1]:
            stop += 1
        out.append((start, stop, False))
        start = stop
    for start in range(n_ranged, n, part_rows):
        out.append((start, min(start + part_rows, n), True))
    return out


def write_parts(t, d, part_rows=2, sidecar=None, splits=None, compression="zstd", dictionary=True,
                manifest=None):
    """Write table ``t`` (in its row order) as a partitioned SSObservation in
    directory ``d``: parts, then the sidecar (``sidecar``: a table of obsid
    + internal columns, in t's row order; default: obsid alone), then the
    manifest. ``splits`` overrides the cutting ([(start, stop, null_part)]).
    ``manifest``: fields to override. Returns the manifest."""
    d = Path(d)
    d.mkdir(parents=True, exist_ok=True)
    sid = t["ssObjectId"].to_pylist() if "ssObjectId" in t.column_names else [None] * len(t)
    splits = splits if splits is not None else cuts(sid, part_rows)
    use_dict = [c for c in SSOBSERVATION_DICTIONARY if c in t.column_names] if dictionary else False
    parts = []
    for k, (a, b, null_part) in enumerate(splits):
        f = PART_FILE_FORMAT.format(k)
        pq.write_table(t.slice(a, b - a), d / f, compression=compression, use_dictionary=use_dict)
        s = [x for x in sid[a:b] if x is not None]
        parts.append({"file": f, "rows": b - a,
                      "ssObjectId_min": None if null_part or not s else min(s),
                      "ssObjectId_max": None if null_part or not s else max(s),
                      "null_ssObjectId": null_part,
                      "bytes": (d / f).stat().st_size, "md5": md5(d / f)})
    if sidecar is None:
        sidecar = pa.table({SIDECAR_KEY: t[SIDECAR_KEY]})
    pq.write_table(sidecar, d / SIDECAR_FILE, compression=compression,
                   use_dictionary=[c for c in SSOBSERVATION_DICTIONARY if c in sidecar.column_names])
    m = {
        "table": "SSObservation",
        "format_version": MANIFEST_FORMAT_VERSION,
        "schema": {"source": "lsst/sdm_schemas tickets/DM-55375", "file": "sso_base.yaml", "md5": None},
        "ssp_tools_commit": None,
        "created_utc": "2026-10-08T12:00:00Z",
        "partition_key": "ssObjectId",
        "sort": list(SSOBSERVATION_SORT),
        "part_rows": part_rows,
        "rows": len(t),
        "parts": parts,
        "sidecar": {"file": SIDECAR_FILE, "key": SIDECAR_KEY,
                    "columns": [c for c in sidecar.column_names if c != SIDECAR_KEY],
                    "rows": len(sidecar), "bytes": (d / SIDECAR_FILE).stat().st_size,
                    "md5": md5(d / SIDECAR_FILE)},
    }
    m.update(manifest or {})
    write_manifest(d, m)
    return m


def read_manifest(d):
    return json.loads((Path(d) / SSOBSERVATION_MANIFEST_FILE).read_text())


def write_manifest(d, m):
    (Path(d) / SSOBSERVATION_MANIFEST_FILE).write_text(json.dumps(m, indent=1))


def refresh(d, m=None):
    """Recompute the manifest's rows/bytes/md5 (parts and sidecar) from the
    files on disk, so that a mutation shows up only where it is meant to."""
    d = Path(d)
    m = m if m is not None else read_manifest(d)
    for p in [*m["parts"], m["sidecar"]]:
        f = d / p["file"]
        if f.exists():
            p.update(rows=pq.ParquetFile(f).metadata.num_rows, bytes=f.stat().st_size, md5=md5(f))
    m["rows"] = sum(p["rows"] for p in m["parts"])
    write_manifest(d, m)
    return m


def rename_part(d, m, k, new_k):
    """Rename part k's file (and its manifest entry) to part number new_k."""
    d = Path(d)
    old = m["parts"][k]["file"]
    new = PART_FILE_FORMAT.format(new_k)
    (d / old).rename(d / new)
    m["parts"][k]["file"] = new
    return m
