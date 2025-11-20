# pip install psycopg2-binary pyarrow

import os
import argparse
import tempfile
import psycopg2
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import yaml

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

    # Next: service name from ~/.pg_service.conf
    if args.service:
        return f"service={args.service} options='-c extra_float_digits=3'"

    # Last: parts from flags or env
    host = args.host or os.getenv("PGHOST")
    port = args.port or os.getenv("PGPORT", "5432")
    dbname = args.dbname or os.getenv("PGDATABASE")
    user = args.user or os.getenv("PGUSER")
    password = args.password or os.getenv("PGPASSWORD")

    if not all([host, dbname, user]):
        raise ValueError("Database connection parameters must be provided via --service, --dsn, CLI flags, or environment variables (PGHOST, PGDATABASE, PGUSER)")

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


def export_query_to_parquet(
    cur,
    sql: str,
    parquet_out: str,
    row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    block_size: int = DEFAULT_BLOCK_SIZE,
    keep_temp: bool = False,
):
    """Export a single SQL query to a Parquet file using the provided cursor.

    Args:
        cur: Active psycopg2 cursor within a transaction
        sql: SQL SELECT query to export
        parquet_out: Output Parquet file path
        row_group_size: Rows per Parquet row group
        block_size: Arrow CSV block size in bytes
        keep_temp: Keep the intermediate CSV for debugging
    """
    # Introspect column names and types
    cur.execute(f"SELECT * FROM ({sql}) t LIMIT 0")
    colnames = [d.name for d in cur.description]  # type: ignore
    column_types = {
        name: arrow_type_for_oid(d.type_code)
        for name, d in zip(colnames, cur.description)  # type: ignore
    }

    # COPY to temp CSV
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        tmp_name = tmp.name
        cur.copy_expert(
            f"COPY ({sql}) TO STDOUT WITH (FORMAT CSV, HEADER FALSE)", tmp
        )

    # Stream CSV → Parquet
    reader = pacsv.open_csv(
        tmp_name,
        read_options=pacsv.ReadOptions(column_names=colnames, block_size=block_size),
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
                parquet_out, tbl.schema, compression="zstd", use_dictionary=False
            )
        writer.write_table(tbl, row_group_size=row_group_size)
        batches.clear()
        rows_accum = 0

    try:
        for rb in reader:
            batches.append(rb)
            rows_accum += rb.num_rows
            if rows_accum >= row_group_size:
                flush()
        flush()
    finally:
        if writer is not None:
            writer.close()
        if not keep_temp:
            try:
                os.remove(tmp_name)
            except OSError:
                pass


def main():
    """CLI entry point for exporting Postgres tables to Parquet."""
    parser = argparse.ArgumentParser(description="Stream Postgres table to Parquet via Arrow")

    # Connection options
    conn_group = parser.add_argument_group('connection options')
    conn_group.add_argument("--service", help="PostgreSQL service name from ~/.pg_service.conf (recommended)")
    conn_group.add_argument("--dsn", help="Full Postgres DSN string (overrides other connection flags)")
    conn_group.add_argument("--host", help="Postgres host (env PGHOST)")
    conn_group.add_argument("--port", help="Postgres port (default: 5432, env PGPORT)")
    conn_group.add_argument("--dbname", help="Postgres database name (env PGDATABASE)")
    conn_group.add_argument("--user", help="Postgres user (env PGUSER)")
    conn_group.add_argument("--password", help="Postgres password (env PGPASSWORD)")

    # Export config
    parser.add_argument("--sql", help="SQL SELECT to export (required for single export)")
    parser.add_argument("--out", dest="parquet_out", help="Output Parquet file path (required for single export)")
    parser.add_argument("--config", help="YAML or JSON config file for batch exports")
    parser.add_argument("--row-group-size", type=int, default=DEFAULT_ROW_GROUP_SIZE, help="Rows per Parquet row group (default for all exports)")
    parser.add_argument("--block-size", type=int, default=DEFAULT_BLOCK_SIZE, help="Arrow CSV block size in bytes")
    parser.add_argument("--keep-temp", action="store_true", help="Keep the intermediate CSV for debugging")

    args = parser.parse_args()

    # Validate argument combinations
    if args.config:
        # Batch mode
        if args.sql or args.parquet_out:
            parser.error("When using --config, do not specify --sql or --out")
    else:
        # Single export mode
        if not args.sql or not args.parquet_out:
            parser.error("Either --config must be specified, or both --sql and --out are required")

    DSN = build_dsn(args)

    if args.config:
        # Batch export mode: multiple exports in a single transaction
        exports = load_config(args.config)

        with psycopg2.connect(DSN) as conn:
            # Start explicit transaction
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ)
            with conn.cursor() as cur:
                # BEGIN transaction implicitly started
                print(f"Starting batch export of {len(exports)} tables in single transaction...")

                for i, export_spec in enumerate(exports, 1):
                    sql = export_spec["sql"]
                    out = export_spec["out"]
                    row_group_size = export_spec.get("row_group_size", args.row_group_size)

                    print(f"[{i}/{len(exports)}] Exporting to {out}...")
                    export_query_to_parquet(
                        cur=cur,
                        sql=sql,
                        parquet_out=out,
                        row_group_size=row_group_size,
                        block_size=args.block_size,
                        keep_temp=args.keep_temp,
                    )
                    print(f"✓ Exported to {out}")

                # Commit transaction
                conn.commit()
                print(f"All {len(exports)} exports completed successfully in single transaction!")
    else:
        # Single export mode (original behavior)
        with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
            export_query_to_parquet(
                cur=cur,
                sql=args.sql,
                parquet_out=args.parquet_out,
                row_group_size=args.row_group_size,
                block_size=args.block_size,
                keep_temp=args.keep_temp,
            )
            print(f"✓ Exported to {args.parquet_out}")


def load_config(config_path: str) -> list:
    """Load export configuration from YAML or JSON file.

    Returns a list of dicts, each with keys: 'sql', 'out', and optionally 'row_group_size'.
    """
    with open(config_path, 'r') as f:
        if config_path.endswith('.json'):
            import json
            data = json.load(f)
        else:
            # Assume YAML (also handles .yml, .yaml)
            data = yaml.safe_load(f)

    if not isinstance(data, list):
        raise ValueError("Config file must contain a list of export specifications")

    # Validate each export spec
    for i, spec in enumerate(data):
        if not isinstance(spec, dict):
            raise ValueError(f"Export spec {i} must be a dictionary")
        if "sql" not in spec or "out" not in spec:
            raise ValueError(f"Export spec {i} must have 'sql' and 'out' keys")

    return data


if __name__ == "__main__":
    main()
