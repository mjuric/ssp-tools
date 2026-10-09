"""Reading a partitioned SSObservation (docs/design/ssobservation-delivery.md).

SSObservation is written as parts plus a manifest and a sidecar of internal
columns (ssp.ssobservation_contract, "The partitioned delivery"). These
helpers read it back for the code that consumes it (SSObject, NearbySSO,
the build, the bench tools). They trust the manifest; checking it against
the files is ssp.delivery_check's job.
"""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .ssobservation_contract import SIDECAR_KEY, SSOBSERVATION_MANIFEST_FILE


def manifest_path(path):
    """The manifest of the SSObservation at ``path``: a directory holding
    SSOBSERVATION_MANIFEST_FILE, or the manifest file itself."""
    p = Path(path)
    return p / SSOBSERVATION_MANIFEST_FILE if p.is_dir() else p


def read_manifest(path):
    """The manifest (a dict) of the SSObservation at ``path`` (see
    manifest_path)."""
    with open(manifest_path(path)) as f:
        return json.load(f)


def part_paths(path):
    """The part files of the SSObservation at ``path``, in part order."""
    m = manifest_path(path)
    return [m.parent / p["file"] for p in read_manifest(m)["parts"]]


def sidecar_path(path):
    """The sidecar file of the SSObservation at ``path``."""
    m = manifest_path(path)
    return m.parent / read_manifest(m)["sidecar"]["file"]


def parquet_schema(path):
    """The Arrow schema of the parts (every part has the same one)."""
    return pq.read_schema(part_paths(path)[0])


def num_rows(path):
    """The total number of rows, from the manifest."""
    return read_manifest(path)["rows"]


def read_ssobservation(path, columns=None, filters=None, internal=False):
    """The SSObservation at ``path`` as one table, the parts concatenated
    in part order (so in SSOBSERVATION_SORT order).

    ``columns`` and ``filters`` are as for pyarrow.parquet.read_table and
    apply to every part (filters only on delivered columns). With
    ``internal``, the sidecar's internal columns are appended, matched on
    obsid: all of them, or with ``columns``, those it names.
    """
    parts = part_paths(path)
    delivered = set(pq.read_schema(parts[0]).names)
    side_cols = []
    if internal:
        side_names = pq.read_schema(sidecar_path(path)).names
        side_cols = [c for c in side_names if c != SIDECAR_KEY and (columns is None or c in columns)]
    read_cols = None
    if columns is not None:
        unknown = [c for c in columns if c not in delivered and c not in side_cols]
        if unknown:
            raise ValueError(f"SSObservation has no columns {unknown}"
                             + ("" if internal else " (internal columns need internal=True)"))
        read_cols = [c for c in columns if c in delivered]
        if side_cols and SIDECAR_KEY not in read_cols:
            read_cols.append(SIDECAR_KEY)
    tables = [pq.read_table(p, columns=read_cols, filters=filters) for p in parts]
    t = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    if side_cols:
        side = pq.read_table(sidecar_path(path), columns=[SIDECAR_KEY, *side_cols])
        if filters is None and side[SIDECAR_KEY].equals(t[SIDECAR_KEY]):
            take = None
        else:
            take = pc.index_in(t[SIDECAR_KEY], value_set=side[SIDECAR_KEY])
            if take.null_count:
                raise ValueError(f"{sidecar_path(path)}: {take.null_count:,} obsids not in the sidecar")
        for c in side_cols:
            t = t.append_column(c, side[c] if take is None else side[c].take(take))
        if columns is not None:
            t = t.select(list(columns))
    return t
