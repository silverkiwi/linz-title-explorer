#!/usr/bin/env bash
# snowsql_build.sh — Run the LINZ Snowflake build using SnowSQL
#
# Prerequisites:
#   - SnowSQL installed: https://docs.snowflake.com/en/user-guide/snowsql-install-config
#   - Connection configured in ~/.snowsql/config (see snowsql.config.example)
#     OR connection params supplied via environment variables below.
#
# Usage:
#   # Full build + validation
#   ./scripts/snowsql_build.sh
#
#   # Validation only (smoke-check an existing deployment)
#   ./scripts/snowsql_build.sh --validate-only
#
#   # Use a named connection from ~/.snowsql/config
#   SNOWSQL_CONNECTION=linz ./scripts/snowsql_build.sh
#
# Environment variables (override ~/.snowsql/config defaults):
#   SNOWSQL_CONNECTION   Named connection to use (default: linz)
#   SNOWSQL_ACCOUNT      Snowflake account identifier
#   SNOWSQL_USER         Snowflake username
#   SNOWSQL_DATABASE     Database name (default: L)
#   SNOWSQL_WAREHOUSE    Warehouse name (default: COMPUTE_WH)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/../sql"

SNOWSQL_CONNECTION="${SNOWSQL_CONNECTION:-linz}"
SNOWSQL_DATABASE="${SNOWSQL_DATABASE:-L}"
SNOWSQL_WAREHOUSE="${SNOWSQL_WAREHOUSE:-COMPUTE_WH}"

# Build base SnowSQL options.  Connection name handles auth; individual flags
# override only if the env vars are explicitly set.
SNOWSQL_OPTS="-c $SNOWSQL_CONNECTION -d $SNOWSQL_DATABASE -w $SNOWSQL_WAREHOUSE"
[[ -n "${SNOWSQL_ACCOUNT:-}" ]] && SNOWSQL_OPTS="$SNOWSQL_OPTS -a $SNOWSQL_ACCOUNT"
[[ -n "${SNOWSQL_USER:-}"    ]] && SNOWSQL_OPTS="$SNOWSQL_OPTS -u $SNOWSQL_USER"

VALIDATE_ONLY=false
for arg in "$@"; do
    [[ "$arg" == "--validate-only" ]] && VALIDATE_ONLY=true
done

# ─── Helpers ──────────────────────────────────────────────────────────────────

step() {
    echo
    echo "══════════════════════════════════════════════════"
    echo "  $1"
    echo "══════════════════════════════════════════════════"
}

run_sql() {
    local file="$1"
    if [[ ! -f "$file" ]]; then
        echo "ERROR: SQL file not found: $file" >&2
        exit 1
    fi
    echo "  → $(basename "$file")"
    # shellcheck disable=SC2086
    snowsql $SNOWSQL_OPTS -f "$file" --stop-on-error
}

# ─── Build ────────────────────────────────────────────────────────────────────

if [[ "$VALIDATE_ONLY" == "false" ]]; then
    step "Step 1/4 — Schemas (OPS, BRONZE, SILVER, GOLD)"
    run_sql "$SQL_DIR/01_create_schemas.sql"

    step "Step 2/4 — SILVER tables + core GOLD views"
    run_sql "$SQL_DIR/09_create_loading_and_datamodel_snowflake.sql"

    step "Step 3/4 — GOLD layer (full views + reporting)"
    run_sql "$SQL_DIR/15_gold_layer.sql"

    step "Step 4/4 — Materialise DIM_ADDRESS_SEARCH + nightly task"
    run_sql "$SQL_DIR/16_materialize_dim_address.sql"
fi

# ─── Validation ───────────────────────────────────────────────────────────────

step "Validation — smoke checks"
run_sql "$SQL_DIR/18_validate_build.sql"

echo
echo "✔  Build complete."
