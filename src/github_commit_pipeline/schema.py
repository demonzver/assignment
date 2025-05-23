from sqlalchemy import (
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Text,
    DateTime,
    ForeignKey,
    Index,
)

metadata = MetaData()

# catalog of repositories
repositories = Table(
    "repositories",
    metadata,
    Column("full_name", String, primary_key=True),  # e.g. apache/airflow
    Column("topic", String),
    Column("stars", Integer),
    Column("inserted_at", DateTime),
    Column("updated_at", DateTime),
)

# commits (one row = one commit)
commits = Table(
    "commits",
    metadata,
    Column("commit_hash", String, primary_key=True),
    Column("repository", String, ForeignKey("repositories.full_name")),
    Column("author", String),
    Column("author_email", String),
    Column("commit_message", Text),
    Column("commit_timestamp", DateTime),
    Column("files_changed", Integer),
    Column("lines_added", Integer),
    Column("lines_removed", Integer),
    Column("repo_stars", Integer),
    Column("collected_at", DateTime),
)
Index("idx_commits_repo_ts", commits.c.repository, commits.c.commit_timestamp)

# file-level diff table
commit_files = Table(
    "commit_files",
    metadata,
    Column(
        "commit_hash",
        String,
        ForeignKey("commits.commit_hash"),
        primary_key=True,
    ),
    Column("file_path", String, primary_key=True),
    Column("file_extension", String),
    Column("change_type", String),
    Column("lines_added", Integer),
    Column("lines_removed", Integer),
    Column("before_uri", String),
    Column("after_uri", String),
)
Index("idx_files_ext", commit_files.c.file_extension)

# last processed commit per repo
last_commits = Table(
    "last_commits",
    metadata,
    Column("repository", String, primary_key=True),
    Column("last_commit_hash", String),
    Column("last_collected_at", DateTime),
)

__all__ = [
    "metadata",
    "repositories",
    "commits",
    "commit_files",
    "last_commits",
]
