from sqlalchemy import (
    MetaData, Table, Column,
    String, Integer, Text, DateTime, ForeignKey
)

metadata = MetaData()

# high-level commit info
commits = Table(
    "commits",
    metadata,
    Column("commit_hash",      String, primary_key=True),
    Column("repository",       String,  nullable=False),   # e.g. "apache/spark"
    Column("author",           String),
    Column("author_email",     String),
    Column("commit_message",   Text),
    Column("commit_timestamp", DateTime),                  # keeps TZ info
    Column("files_changed",    Integer),
    Column("lines_added",      Integer),
    Column("lines_removed",    Integer),
)

# per-file diff info
commit_files = Table(
    "commit_files",
    metadata,
    Column("id",             Integer, primary_key=True, autoincrement=True),
    Column("commit_hash",    String, ForeignKey("commits.commit_hash")),
    Column("file_path",      String),     # src/module/foo.py
    Column("file_extension", String),     # .py
    Column("change_type",    String),     # added | modified | removed | renamed
    Column("lines_added",    Integer),
    Column("lines_removed",  Integer),
    Column("before_uri",     String),     # s3://commit-data/.../before/...
    Column("after_uri",      String),     # s3://commit-data/.../after/...
)

__all__ = ["metadata", "commits", "commit_files"]
