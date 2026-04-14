#!/usr/bin/env bash
set -euo pipefail

# Upload LINZ layer folders one-by-one to UC Volume (table-by-table style).
#
# Usage:
#   ./scripts/upload_linz_tables_one_by_one.sh \
#     --profile dbc-4910bda0-398c \
#     --volume dbfs:/Volumes/main/default/linz_data \
#     --src7 /Users/adrianwhite/linz/lds-world-7layers-SHP \
#     --src14 /Users/adrianwhite/linz/lds-world-14layers-SHP
#
# Optional:
#   --only landonline-title-parcel-association,landonline-appellation
#   --retries 3
#   --timeout 3600

PROFILE=""
VOLUME_PATH="dbfs:/Volumes/main/default/linz_data"
SRC7="/Users/adrianwhite/linz/lds-world-7layers-SHP"
SRC14="/Users/adrianwhite/linz/lds-world-14layers-SHP"
ONLY=""
RETRIES=3
TIMEOUT_SECS=3600

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --volume) VOLUME_PATH="$2"; shift 2 ;;
    --src7) SRC7="$2"; shift 2 ;;
    --src14) SRC14="$2"; shift 2 ;;
    --only) ONLY="$2"; shift 2 ;;
    --retries) RETRIES="$2"; shift 2 ;;
    --timeout) TIMEOUT_SECS="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if ! command -v databricks >/dev/null 2>&1; then
  echo "ERROR: databricks CLI not found"
  exit 1
fi

[[ -d "$SRC7" ]] || { echo "ERROR: dir not found: $SRC7"; exit 1; }
[[ -d "$SRC14" ]] || { echo "ERROR: dir not found: $SRC14"; exit 1; }

DBX=(databricks)
[[ -n "$PROFILE" ]] && DBX+=(--profile "$PROFILE")

export DATABRICKS_HTTP_TIMEOUT_SECONDS="$TIMEOUT_SECS"

# Build table folder list (macOS bash 3 compatible; no mapfile)
ALL_DIRS=()
while IFS= read -r line; do
  ALL_DIRS+=("$line")
done < <(find "$SRC7" "$SRC14" -mindepth 1 -maxdepth 1 -type d | sort)

should_upload() {
  local name="$1"
  if [[ -z "$ONLY" ]]; then
    return 0
  fi
  IFS=',' read -ra want <<< "$ONLY"
  for w in "${want[@]}"; do
    [[ "$name" == "$w" ]] && return 0
  done
  return 1
}

upload_dir() {
  local local_dir="$1"
  local parent_name="$2"
  local table_name
  table_name="$(basename "$local_dir")"
  local remote_dir="$VOLUME_PATH/$parent_name/$table_name"

  local attempt=1
  while (( attempt <= RETRIES )); do
    echo "[$table_name] upload attempt $attempt/$RETRIES"
    if "${DBX[@]}" fs cp "$local_dir" "$remote_dir" --overwrite -r; then
      echo "[$table_name] OK -> $remote_dir"
      return 0
    fi
    echo "[$table_name] failed"
    (( attempt++ ))
    sleep 20
  done
  echo "[$table_name] FAILED after $RETRIES attempts"
  return 1
}

fail_count=0
for d in "${ALL_DIRS[@]}"; do
  table_name="$(basename "$d")"
  parent_name="$(basename "$(dirname "$d")")"

  if ! should_upload "$table_name"; then
    continue
  fi

  if ! upload_dir "$d" "$parent_name"; then
    ((fail_count++))
  fi
done

echo
if (( fail_count > 0 )); then
  echo "Completed with failures: $fail_count"
  exit 1
else
  echo "All selected table folders uploaded successfully."
fi

echo
"${DBX[@]}" fs ls "$VOLUME_PATH/$(basename "$SRC7")"
"${DBX[@]}" fs ls "$VOLUME_PATH/$(basename "$SRC14")"
