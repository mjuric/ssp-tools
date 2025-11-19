#!/bin/bash

# Script to dump specified tables from mpc_sbn database using fast-export
# Usage: ./dump_tables.sh

set -e  # Exit on any error

# Database connection parameters
HOST="mpc-usdf.sp.mjuric.org"
DBNAME="mpc_sbn"
USER="rubin"

echo "Starting table export from $DBNAME database..."

echo "Exporting table: current_identifications"
fast-export \
    --sql "SELECT * FROM current_identifications" \
    --out "current_identifications.parquet" \
    --host "$HOST" \
    --dbname "$DBNAME" \
    --user "$USER"
echo "✓ Exported current_identifications to current_identifications.parquet"

echo "Exporting table: mpc_orbits"
fast-export \
    --sql "SELECT * FROM mpc_orbits" \
    --out "mpc_orbits.parquet" \
    --host "$HOST" \
    --dbname "$DBNAME" \
    --user "$USER"
echo "✓ Exported mpc_orbits to mpc_orbits.parquet"

echo "Exporting table: numbered_identifications"
fast-export \
    --sql "SELECT * FROM numbered_identifications" \
    --out "numbered_identifications.parquet" \
    --host "$HOST" \
    --dbname "$DBNAME" \
    --user "$USER"
echo "✓ Exported numbered_identifications to numbered_identifications.parquet"

echo "Exporting table: obs_sbn (filtered for stn='X05')"
fast-export \
    --sql "SELECT * FROM obs_sbn WHERE stn='X05'" \
    --out "obs_sbn.parquet" \
    --host "$HOST" \
    --dbname "$DBNAME" \
    --user "$USER"
echo "✓ Exported obs_sbn to obs_sbn.parquet"

echo "All tables exported successfully!"