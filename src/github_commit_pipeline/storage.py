"""MinIO helper with YYYY/MM/shard/xx/ prefixing."""
from __future__ import annotations

import hashlib
import os
import tempfile
from datetime import datetime
from pathlib import PurePosixPath

from dotenv import load_dotenv
from minio import Minio

load_dotenv()

client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", ""),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False,
)

BUCKET = os.getenv("MINIO_BUCKET", "commit-data")
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)


def _prefix(timestamp: datetime, logical_key: str) -> str:
    """Return YYYY/MM/shard/xx prefix."""
    digest = hashlib.sha256(logical_key.encode()).hexdigest()[:2]
    return f"{timestamp.year:04d}/{timestamp.month:02d}/shard/{digest}"


def upload_bytes(data: bytes, logical_key: str, ts: datetime) -> str:
    """
    Upload `data` under a deterministic, date-sharded key.

    logical_key example:
        apache/airflow/<sha>/after/path/to/file.py
    final object key:
        2025/05/shard/af/apache/airflow/<sha>/after/path/to/file.py
    """
    full_key = str(PurePosixPath(_prefix(ts, logical_key), logical_key))

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
        tmp.flush()
    client.fput_object(BUCKET, full_key, tmp.name)
    os.unlink(tmp.name)

    return f"s3://{BUCKET}/{full_key}"
