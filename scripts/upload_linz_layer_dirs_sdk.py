#!/usr/bin/env python3
"""
Upload extracted LINZ 7-layer + 14-layer directories to UC Volume using Databricks SDK.

Requires:
  pip install databricks-sdk
  export DATABRICKS_HOST=...
  export DATABRICKS_TOKEN=...

Usage:
  python scripts/upload_linz_layer_dirs_sdk.py \
    --src7 /Users/adrianwhite/linz/lds-world-7layers-SHP \
    --src14 /Users/adrianwhite/linz/lds-world-14layers-SHP \
    --volume /Volumes/main/default/linz_data
"""

from __future__ import annotations

import argparse
import os
from databricks.sdk import WorkspaceClient


def iter_files(root: str):
    for base, _, files in os.walk(root):
        for f in files:
            yield os.path.join(base, f)


def upload_tree(w: WorkspaceClient, src_root: str, volume_root: str):
    src_root = os.path.abspath(src_root)
    root_name = os.path.basename(src_root)
    count = 0
    for src_file in iter_files(src_root):
        rel = os.path.relpath(src_file, src_root)
        dst = f"{volume_root}/{root_name}/{rel}".replace("\\", "/")
        with open(src_file, "rb") as fh:
            w.files.upload(file_path=dst, contents=fh, overwrite=True)
        count += 1
        if count % 200 == 0:
            print(f"Uploaded {count} files from {root_name}...")
    print(f"Uploaded {count} files from {root_name}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--src7", required=True)
    p.add_argument("--src14", required=True)
    p.add_argument("--volume", default="/Volumes/main/default/linz_data")
    args = p.parse_args()

    for d in (args.src7, args.src14):
        if not os.path.isdir(d):
            raise FileNotFoundError(d)

    w = WorkspaceClient()
    upload_tree(w, args.src7, args.volume)
    upload_tree(w, args.src14, args.volume)

    print("Done")


if __name__ == "__main__":
    main()
