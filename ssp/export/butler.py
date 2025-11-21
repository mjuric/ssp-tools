"""Utilities and CLI for extracting LSST Butler catalogs to Parquet.

This module provides:
- query_tables_generator_and_count: stream Butler tables with optional
  ID filtering
- write_tables_to_parquet: stream-append tables into a single Parquet file
- main(): CLI entry-point `extract-catalog`

Notes
-----
- Requires the Rubin Observatory LSST Science Pipelines environment
  (lsst.daf.butler and related packages available in the runtime).
- Writes a single Parquet file with one row-group per yielded table.
"""

from __future__ import annotations

import argparse
from typing import Callable, Generator, Iterable, Optional, Tuple

import numpy as np
import pyarrow.parquet as pq
from lsst.daf.butler.formatters.parquet import astropy_to_arrow


def write_tables_to_parquet(
    table_generator: Iterable,
    output_file: str,
    compression: str = "zstd",
    on_batch: Optional[Callable[[], None]] = None,
) -> int:
    """Write tables from a generator to a single Parquet file.

    Parameters
    ----------
    table_generator : iterable
        Iterable or generator that yields Astropy Table-like objects.
    output_file : str
        Path to output Parquet file.
    compression : str, optional
        Parquet compression codec. Default: "zstd".

    Returns
    -------
    int
        Total number of rows written.
    """
    writer: Optional[pq.ParquetWriter] = None
    total_rows = 0

    for table in table_generator:
        try:
            # Convert and write when non-empty; still count as processed
            # even if empty
            if len(table) > 0:
                arrow_table = astropy_to_arrow(table)

                if writer is None:
                    writer = pq.ParquetWriter(
                        output_file,
                        arrow_table.schema,
                        compression=compression,
                        use_dictionary=False,
                    )

                # Append this batch as a row group
                writer.write_table(arrow_table)
                total_rows += len(table)
        finally:
            # Invoke progress callback once per processed dataRef
            if on_batch is not None:
                try:
                    on_batch()
                except Exception:
                    # Progress should not break the export
                    pass

    if writer is not None:
        writer.close()

    return total_rows


def query_catalogs(
    repo: str,
    collection: str,
    datasetType: str,
    filter_ids: Optional[np.ndarray] = None,
    target_column: str = "diaSourceId",
) -> Tuple[Generator, int]:
    """Query Butler registry and return a generator yielding catalog tables.

    This function queries the Butler registry for all datasets of a given type
    in a collection, and returns a generator that yields each catalog table
    (optionally filtered by ID), along with the total count of datasets for
    progress tracking.

    Parameters
    ----------
    repo : str
        Path to Butler repository (e.g., "/repo/main").
    collection : str
        Butler collection name (e.g.,
        "LSSTCam/runs/DRP/FL/w_2025_19/DM-50795").
    datasetType : str
        Dataset type to query (e.g., "dia_source_visit").
    filter_ids : numpy.ndarray, optional
        If provided, only rows whose ``target_column`` value exists in this
        array will be yielded. IDs must be int64-compatible. If None, all
        rows from each dataset are yielded.
    target_column : str, optional
        Column name in each catalog table to match against ``filter_ids``.
        Default: "diaSourceId".

    Returns
    -------
    generator
        Generator that yields Astropy Table-like objects
        (potentially filtered).
    int
        Total number of datasets that will be yielded (for progress bars).

    Notes
    -----
    - The registry query is executed immediately to determine the total count.
    - Each dataset is loaded lazily via ``butler.get()`` as the generator is
      consumed.
    - ID filtering uses vectorized ``numpy.isin()`` for performance.
    """
    import lsst.daf.butler as dafButler

    butler = dafButler.Butler(repo, collections=collection)
    dataRefs = list(
        butler.registry.queryDatasets(datasetType=datasetType, collections=collection)
    )

    def _gen():
        for dataRef in dataRefs:
            table = butler.get(dataRef)
            if filter_ids is not None:
                mask = np.isin(table[target_column], filter_ids)
                yield table[mask]
            else:
                yield table

    return _gen(), len(dataRefs)


def _read_filter_ids_from_parquet(path: str, column: Optional[str] = None) -> np.ndarray:
    """Load a list of IDs from a Parquet file into a numpy array.

    Parameters
    ----------
    path : str
        Parquet file path containing ID values.
    column : str, optional
        Column name to read. If not provided, the first column is used.

    Returns
    -------
    numpy.ndarray
        Array of int64 IDs.

    Raises
    ------
    ValueError
        If the IDs cannot be converted to int64.
    """
    t = pq.read_table(path)
    if t.num_columns == 0:
        raise ValueError("Filter Parquet file has no columns")
    if column is None:
        col = t.column(0)
    else:
        try:
            col = t.column(column)
        except KeyError as e:
            raise ValueError(f"Column '{column}' not found in {path}") from e

    # Convert to numpy
    ids = col.to_numpy(zero_copy_only=False)

    # Require int64 conversion for ID filtering
    try:
        ids = ids.astype(np.int64, copy=False)
    except (ValueError, TypeError, OverflowError) as e:
        raise ValueError(
            f"Filter IDs in '{path}' (column: {column or 'first column'}) "
            f"must be convertible to int64. Got dtype: {ids.dtype}. "
            f"Conversion error: {e}"
        ) from e

    return ids


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="extract-catalog",
        description="Stream LSST Butler datasets into a single Parquet file",
    )
    parser.add_argument(
        "output_file", help="Path to output Parquet file (single file, many row-groups)"
    )
    parser.add_argument("repo", help="Butler repository path (e.g., /repo/main)")
    parser.add_argument("collection", help="Butler collection name")
    parser.add_argument(
        "--dataset-type",
        dest="dataset_type",
        default="dia_source_visit",
        help="Dataset type to export (default: dia_source_visit)",
    )
    parser.add_argument(
        "--filter-ids",
        dest="filter_ids",
        help="Optional Parquet file containing IDs to filter on (IDs must be "
             "int64-convertible; first column will be used unless "
             "--filter-column is specified)",
    )
    parser.add_argument(
        "--filter-column",
        dest="filter_column",
        help="Column in --filter-ids Parquet to read (defaults to first column)",
    )
    parser.add_argument(
        "--target-column",
        dest="target_column",
        default="diaSourceId",
        help="Column in dataset tables to match against filter IDs (default: diaSourceId)",
    )
    parser.add_argument(
        "--compression",
        default="zstd",
        help="Parquet compression codec (default: zstd)",
    )
    parser.add_argument(
        "--silent",
        action="store_true",
        help="Disable progress bar (progress is shown by default)",
    )
    return parser


def _extract_with_progress(
    gen: Generator,
    total: int,
    output_file: str,
    compression: str,
) -> int:
    """Write tables with a progress bar."""
    from tqdm import tqdm

    pbar = tqdm(total=total, unit="dataset")
    try:
        return write_tables_to_parquet(
            gen,
            output_file=output_file,
            compression=compression,
            on_batch=lambda: pbar.update(1), # type: ignore
        )
    finally:
        pbar.close()


def main(argv: Optional[Iterable[str]] = None) -> None:
    """CLI entry-point for extracting Butler catalogs to Parquet.

    Usage (examples)
    ----------------
    extract-catalog dia_sources.parquet /repo/main \
        LSSTCam/runs/DRP/FL/w_2025_19/DM-50795 \
        --filter-ids=obs_sbn.parquet \
        --filter-column=obssubid
    """
    args = _build_argument_parser().parse_args(list(argv) if argv is not None else None)

    # Load filter IDs if specified
    filter_ids = (
        _read_filter_ids_from_parquet(args.filter_ids, args.filter_column)
        if args.filter_ids
        else None
    )

    # Get generator and total count
    gen, total = query_catalogs(
        repo=args.repo,
        collection=args.collection,
        datasetType=args.dataset_type,
        filter_ids=filter_ids,
        target_column=args.target_column,
    )

    # Write with or without progress
    if args.silent:
        total_written = write_tables_to_parquet(
            gen, output_file=args.output_file, compression=args.compression
        )
    else:
        total_written = _extract_with_progress(
            gen, total, output_file=args.output_file, compression=args.compression
        )

    print(f"Wrote {total_written:,} rows to {args.output_file}")


if __name__ == "__main__":
    main()
