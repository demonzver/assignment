"""
Wrapper around boto3 for uploading blobs to MinIO / S3.
All configuration is taken from the .env
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import PurePosixPath

import boto3
import botocore
from botocore.config import Config

LOG = logging.getLogger(__name__)

# Config
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL") or os.getenv("S3_ENDPOINT") or "http://localhost:9000"
S3_BUCKET = os.getenv("S3_BUCKET", "commit-data")
S3_PREFIX = os.getenv("S3_PREFIX", "blobs/")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

# boto3 client
S3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    config=Config(signature_version="s3v4"),
)

# Ensure the configured S3 bucket exists before uploading objects
try:
    S3.head_bucket(Bucket=S3_BUCKET)
except botocore.exceptions.ClientError as e:
    code = e.response["Error"]["Code"]
    if code in ("404", "NoSuchBucket"):
        S3.create_bucket(Bucket=S3_BUCKET)
        LOG.info("created bucket %s", S3_BUCKET)
    else:
        raise


def make_key(repo: str, sha: str, direction: str, path: str) -> str:
    """
    Build an S3 object key:
    blobs/<repo>/<sha[0:2]>/<sha>/<direction>/<basename>

    Shard by the first two hex digits of the commit SHA to spread
    objects evenly across many prefixes, preventing “hot” S3 partitions
    and improving list/read performance at scale.
    """
    return str(
        PurePosixPath(
            S3_PREFIX,
            repo,
            sha[:2],
            sha,
            direction,
            os.path.basename(path),
        )
    )


def put_blob(buffer: bytes, key: str) -> str:
    """Upload non-empty buffer to S3, return s3:// URI or ''."""
    if not buffer:
        return ""
    S3.upload_fileobj(io.BytesIO(buffer), S3_BUCKET, key)
    return f"s3://{S3_BUCKET}/{key}"
