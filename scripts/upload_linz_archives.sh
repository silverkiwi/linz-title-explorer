#!/usr/bin/env bash
set -euo pipefail

# Upload LINZ zip archives to a Unity Catalog Volume via Databricks CLI.
#
# Usage:
#   ./scripts/upload_linz_archives.sh \
#     --volume dbfs:/Volumes/main/default/linz_data \
#     --zip7 lds-world-7layers-SHP.zip \
#     --zip14 lds-world-14layers-SHP.zip
#
# Optional:
#   --profile DEFAULT
#   --trigger-job-id 1234567890

VOLUME_PATH="dbfs:/Volumes/main/default/linz_data"
ZIP7="lds-world-7layers-SHP.zip"
ZIP14="lds-world-14layers-SHP.zip"
PROFILE=""
TRIGGER_JOB_ID=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --volume) VOLUME_PATH="$2"; shift 2 ;;
    --zip7) ZIP7="$2"; shift 2 ;;
    --zip14) ZIP14="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --trigger-job-id) TRIGGER_JOB_ID="$2"; shift 2 ;;
    *) echo "Unknown arg: $1"; exit 1 ;;
  esac
done

if ! command -v databricks >/dev/null 2>&1; then
  echo "ERROR: databricks CLI not found. Install with: brew install databricks/tap/databricks"
  exit 1
fi

for f in "$ZIP7" "$ZIP14"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: file not found: $f"
    exit 1
  fi
done

DBX=(databricks)
if [[ -n "$PROFILE" ]]; then
  DBX+=(--profile "$PROFILE")
fi

echo "== Local checksums =="
shasum -a 256 "$ZIP7" "$ZIP14"

echo
printf "Uploading %s -> %s/%s\n" "$ZIP7" "$VOLUME_PATH" "$(basename "$ZIP7")"
"${DBX[@]}" fs cp "$ZIP7" "$VOLUME_PATH/$(basename "$ZIP7")" --overwrite

printf "Uploading %s -> %s/%s\n" "$ZIP14" "$VOLUME_PATH" "$(basename "$ZIP14")"
"${DBX[@]}" fs cp "$ZIP14" "$VOLUME_PATH/$(basename "$ZIP14")" --overwrite

echo
echo "== Remote volume listing =="
"${DBX[@]}" fs ls "$VOLUME_PATH"

echo
echo "Upload complete."
echo "Next, unzip in Databricks notebook:"
cat <<'PY'
import os, zipfile

for zip_path, extract_dir in [
    ("/Volumes/main/default/linz_data/lds-world-7layers-SHP.zip", "/Volumes/main/default/linz_data/lds-world-7layers-SHP"),
    ("/Volumes/main/default/linz_data/lds-world-14layers-SHP.zip", "/Volumes/main/default/linz_data/lds-world-14layers-SHP"),
]:
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    print("Unzipped:", zip_path, "->", extract_dir)
PY

if [[ -n "$TRIGGER_JOB_ID" ]]; then
  echo
  echo "Triggering Databricks job run-now for job_id=$TRIGGER_JOB_ID"
  "${DBX[@]}" jobs run-now "$TRIGGER_JOB_ID"
fi
