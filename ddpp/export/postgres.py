# pip install psycopg2-binary pyarrow

import os
import argparse
import tempfile
import psycopg2
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DEFAULT_ROW_GROUP_SIZE = 1_000_000                # rows per Parquet row-group
DEFAULT_BLOCK_SIZE = 1 << 26                      # ~67MB

# --- Postgres OID -> Arrow type map (extend as needed) ---
PGOID_TO_ARROW = {
    16:  pa.bool_(),        # bool
    20:  pa.int64(),        # int8
    21:  pa.int16(),        # int2
    23:  pa.int32(),        # int4
    700: pa.float32(),      # float4
    701: pa.float64(),      # float8
    1700: pa.float64(),     # numeric -> use decimal128 if exactness required
    25:  pa.string(),       # text
    1043: pa.string(),      # varchar
    1042: pa.string(),      # char
    114: pa.string(),       # json
    3802: pa.string(),      # jsonb
    1082: pa.date32(),      # date
    1114: pa.timestamp('us'),            # timestamp w/o tz
    1184: pa.timestamp('us', tz='UTC'),  # timestamptz (choose your TZ policy)
    1186: pa.duration('us'),             # interval (approx)
}


def arrow_type_for_oid(oid):
    """Map Postgres OID to Arrow type, defaulting to string for unknown types."""
    return PGOID_TO_ARROW.get(oid, pa.string())


def build_dsn(args: argparse.Namespace) -> str:
    """Build a Postgres DSN string from args/env, ensuring extra_float_digits=3."""
    # Highest priority: explicit --dsn
    if args.dsn:
        dsn = args.dsn.strip()
        if "extra_float_digits" not in dsn:
            # add options while preserving any existing options
            opt = "options='-c extra_float_digits=3'"
            dsn = f"{dsn} {opt}" if "options=" not in dsn else f"{dsn} -c extra_float_digits=3"
        return dsn

    # Next: parts from flags or env
    host = args.host or os.getenv("PGHOST")
    port = args.port or os.getenv("PGPORT")
    dbname = args.dbname or os.getenv("PGDATABASE")
    user = args.user or os.getenv("PGUSER")
    password = args.password or os.getenv("PGPASSWORD")

    if not all([host, port, dbname, user]):
        raise ValueError("Database connection parameters must be provided via --dsn, CLI flags, or environment variables (PGHOST, PGPORT, PGDATABASE, PGUSER)")

    parts = [
        f"host={host}",
        f"port={port}",
        f"dbname={dbname}",
        f"user={user}",
        "options='-c extra_float_digits=3'",
    ]
    if password:
        parts.append(f"password={password}")
    return " ".join(parts)


def main():
    """CLI entry point for exporting Postgres tables to Parquet."""
    parser = argparse.ArgumentParser(description="Stream Postgres table to Parquet via Arrow")
    # Connection
    parser.add_argument("--dsn", help="Full Postgres DSN string (overrides other connection flags)")
    parser.add_argument("--host", help="Postgres host (env PGHOST)")
    parser.add_argument("--port", default="5432", help="Postgres port (default: 5432, env PGPORT)")
    parser.add_argument("--dbname", help="Postgres database name (env PGDATABASE)")
    parser.add_argument("--user", help="Postgres user (env PGUSER)")
    parser.add_argument("--password", help="Postgres password (env PGPASSWORD)")
    # Export config
    parser.add_argument("--sql", required=True, help="SQL SELECT to export")
    parser.add_argument("--out", required=True, dest="parquet_out", help="Output Parquet file path")
    parser.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE, help="Rows per Parquet row group")
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE, help="Arrow CSV block size in bytes")
    parser.add_argument("--keep-temp", action="store_true", help="Keep the intermediate CSV for debugging")

    args = parser.parse_args()

    DSN = build_dsn(args)
    SQL = args.sql
    PARQUET_OUT = args.parquet_out
    ROW_GROUP_SIZE = args.row_group_size
    BLOCK_SIZE = args.block_size
    # --- COPY CSV to temp file ---
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT * FROM ({SQL}) t LIMIT 0")
        colnames = [d.name for d in cur.description]
        column_types = {
            name: arrow_type_for_oid(d.type_code)
            for name, d in zip(colnames, cur.description)
        }

        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp_name = tmp.name
            print(tmp_name)
            cur.copy_expert(
                f"COPY ({SQL}) TO STDOUT WITH (FORMAT CSV, HEADER FALSE)", tmp
            )

    # --- Stream CSV → Parquet ---
    reader = pacsv.open_csv(
        tmp_name,
        read_options=pacsv.ReadOptions(column_names=colnames, block_size=BLOCK_SIZE),
        convert_options=pacsv.ConvertOptions(
            column_types=column_types,
            null_values=["", "NULL"],
            true_values=["t"],
            false_values=["f"],
            strings_can_be_null=True,
            quoted_strings_can_be_null=True,
        ),
    )

    writer = None
    batches, rows_accum = [], 0

    def flush():
        nonlocal batches, rows_accum, writer
        if not batches:
            return
        tbl = pa.Table.from_batches(batches)
        if writer is None:
            writer = pq.ParquetWriter(
                PARQUET_OUT, tbl.schema, compression="zstd", use_dictionary=False
            )
        writer.write_table(tbl, row_group_size=ROW_GROUP_SIZE)
        batches.clear()
        rows_accum = 0

    try:
        for rb in reader:
            batches.append(rb)
            rows_accum += rb.num_rows
            if rows_accum >= ROW_GROUP_SIZE:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()
        if not args.keep_temp:
            try:
                os.remove(tmp_name)
            except OSError:
                pass


if __name__ == "__main__":
    main()
