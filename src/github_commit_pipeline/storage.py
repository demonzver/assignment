import os, tempfile
from minio import Minio
from dotenv import load_dotenv

load_dotenv()

client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT").replace("http://", ""),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)

BUCKET = os.getenv("MINIO_BUCKET", "commit-data")
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)


def upload_bytes(data: bytes, key: str) -> str:
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(data)
    client.fput_object(BUCKET, key, tmp.name)
    os.unlink(tmp.name)
    return f"s3://{BUCKET}/{key}"