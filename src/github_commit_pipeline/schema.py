from sqlalchemy import (
    MetaData, Table, Column,
    String, Integer, Text, DateTime, ForeignKey
)

metadata = MetaData()

# Commits (repo-level)
commits = Table(
    "commits",
    metadata,
    Column("commit_hash",      String, primary_key=True),
    Column("repository",       String,  nullable=False),
    Column("author",           String),
    Column("author_email",     String),
    Column("commit_message",   Text),
    Column("commit_timestamp", DateTime),
    Column("files_changed",    Integer),
    Column("lines_added",      Integer),
    Column("lines_removed",    Integer),
)

# Files (per-file diff)
commit_files = Table(
    "commit_files",
    metadata,
    Column("commit_hash",   String, ForeignKey("commits.commit_hash"), primary_key=True),
    Column("file_path",     String, primary_key=True),
    Column("file_extension",String),
    Column("change_type",   String),     # added | modified | removed | renamed
    Column("lines_added",   Integer),
    Column("lines_removed", Integer),
    Column("before_uri",    String),
    Column("after_uri",     String),
)

__all__ = ["metadata", "commits", "commit_files"]
