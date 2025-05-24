# src/github_commit_pipeline/restore_folder.py
"""
Restore all files from a commit snapshot (before / after) into a local folder.

Example
-------
poetry run python -m github_commit_pipeline.restore_folder \
    --repo awesome-selfhosted/awesome-selfhosted \
    --sha 044ef9dd08be2185d89a6aa0a53e903422a79707 \
    --direction after \
    --out ./restored/awesome-selfhosted
"""

from __future__ import annotations

import argparse
import os
from pathlib import PurePosixPath
from typing import List

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# ──────────────────────────── ENV & CLIENT ────────────────────────────────
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL", "localhost:9000").rstrip("/")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = os.getenv("S3_BUCKET", "commit-data")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)


# ──────────────────────────── FUNCTIONS ────────────────────────────────────
def restore_folder(repo: str, sha: str, direction: str, out_dir: str) -> None:
    """
    Download every object whose key starts with
    ``blobs/<repo>/<sha[:2]>/<sha>/<direction>/``.
    """
    prefix: str = f"blobs/{repo}/{sha[:2]}/{sha}/{direction}/"
    print(f"Looking for objects with prefix: {prefix}")

    objects: List = list(
        client.list_objects(BUCKET, prefix=prefix, recursive=True)
    )
    if not objects:
        print(f"No objects found with prefix {prefix}")
        return

    os.makedirs(out_dir, exist_ok=True)
    downloaded = 0

    for obj in objects:
        object_path = PurePosixPath(obj.object_name)
        prefix_path = PurePosixPath(prefix)

        try:
            rel_path = object_path.relative_to(prefix_path)
        except ValueError:
            continue  # object not under prefix (should not happen)

        if not rel_path or str(rel_path).endswith("/"):
            continue  # skip “folders”

        dst = os.path.join(out_dir, str(rel_path))
        os.makedirs(os.path.dirname(dst), exist_ok=True)

        try:
            client.fget_object(BUCKET, obj.object_name, dst)
            downloaded += 1
        except S3Error as exc:
            print(f"Failed to download {obj.object_name}: {exc}")

    print(f"Restored {downloaded} files to {out_dir}")


# ────────────────────────────── CLI ────────────────────────────────────────
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restore before/after snapshot of a commit."
    )
    parser.add_argument("--repo", required=True, help="owner/repo")
    parser.add_argument("--sha", required=True, help="full commit SHA")
    parser.add_argument(
        "--direction",
        choices=["before", "after"],
        default="after",
        help="snapshot side to restore (default: after)",
    )
    parser.add_argument(
        "--out",
        required=True,
        help="destination directory on local filesystem",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    restore_folder(args.repo, args.sha, args.direction, args.out)


if __name__ == "__main__":
    main()
