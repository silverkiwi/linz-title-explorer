#!/usr/bin/env bash
set -euo pipefail

# Upload extracted LINZ 7-layer + 14-layer directories to a UC Volume via Databricks CLI.
#
# Usage:
#   ./scripts/upload_linz_layer_dirs.sh \
#     --profile dbc-4910bda0-398c \
#     --volume dbfs:/Volumes/main/default/linz_data \
#     --src7 /Users/adrianwhite/linz/lds-world-7layers-SHP \
#     --src14 /Users/adrianwhite/linz/lds-world-14layers-SHP

PROFILE=""
VOLUME_PATH="dbfs:/Volumes/main/default/linz_data"
SRC7="/Users/adrianwhite/linz/lds-world-7layers-SHP"
SRC14="/Users/adrianwhite/linz/lds-world-14layers-SHP"
TIMEOUT_SECS="3600"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --volume) VOLUME_PATH="$2"; shift 2 ;;
    --src7) SRC7="$2"; shift 2 ;;
    --src14) SRC14="$2"; shift 2 ;;
    --timeout) TIMEOUT_SECS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if ! command -v databricks >/dev/null 2>&1; then
  echo "ERROR: databricks CLI not found"
  exit 1
fi

for d in "$SRC7" "$SRC14"; do
  [[ -d "$d" ]] || { echo "ERROR: directory not found: $d"; exit 1; }
done

DBX=(databricks)
[[ -n "$PROFILE" ]] && DBX+=(--profile "$PROFILE")

export DATABRICKS_HTTP_TIMEOUT_SECONDS="$TIMEOUT_SECS"

echo "== Uploading 7-layer directory =="
"${DBX[@]}" fs cp "$SRC7" "$VOLUME_PATH/$(basename "$SRC7")" --overwrite -r

echo "== Uploading 14-layer directory =="
"${DBX[@]}" fs cp "$SRC14" "$VOLUME_PATH/$(basename "$SRC14")" --overwrite -r

echo "== Remote check =="
"${DBX[@]}" fs ls "$VOLUME_PATH/$(basename "$SRC7")"
"${DBX[@]}" fs ls "$VOLUME_PATH/$(basename "$SRC14")"

echo "Done."
