"""
Collect new commits and before/after blobs for each GitHub repo stored in
DuckDB `repositories`.  Upload blobs to S3/MinIO using storage.py and
write metadata to `commits`, `commit_files`, `last_commits`.

Only processes commits newer than `last_commits.last_commit_hash`.
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, UTC
from typing import Iterator, Tuple

from dotenv import load_dotenv
from git import Commit, GitCommandError, Repo
from sqlalchemy import create_engine, insert, select, text

from github_commit_pipeline.schema import (
    repositories,
    commits,
    commit_files,
    last_commits,
    metadata,
)

import github_commit_pipeline.storage as storage

# Config
load_dotenv()

DB_PATH = os.getenv("DB_PATH", "./data_db/commits.duckdb")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "1"))
MAX_COMMITS_PER_REPO = int(os.getenv("MAX_COMMITS_PER_REPO", "100"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

SINCE = datetime.now(UTC) - timedelta(days=HISTORY_DAYS)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
)
LOG = logging.getLogger("collector")

# Database
engine = create_engine(f"duckdb:///{DB_PATH}", future=True)
metadata.create_all(engine)


# Helpers
def fetch_repositories() -> list[Tuple[str, int]]:
    """Return list of full_name, stars ordered by star count"""
    with engine.connect() as conn:
        rows = conn.execute(
            select(repositories.c.full_name, repositories.c.stars)
            .order_by(repositories.c.stars.desc())
        ).fetchall()
    return [(r.full_name, r.stars or 0) for r in rows]


def get_last_sha(conn, repo: str) -> str | None:
    row = conn.execute(
        select(last_commits.c.last_commit_hash)
        .where(last_commits.c.repository == repo)
    ).fetchone()
    return row.last_commit_hash if row else None


def save_last_sha(conn, repo: str, sha: str) -> None:
    now = datetime.now(UTC)
    conn.execute(
        text(
            """
            INSERT INTO last_commits (repository, last_commit_hash, last_collected_at)
            VALUES (:repo, :sha, :now)
            ON CONFLICT(repository) DO UPDATE SET
              last_commit_hash = excluded.last_commit_hash,
              last_collected_at = excluded.last_collected_at
            """
        ),
        {"repo": repo, "sha": sha, "now": now},
    )


def iter_new_commits(repo: Repo, since_sha: str | None) -> Iterator[Commit]:
    """
    Yield commits from newest to oldest until we reach `since_sha`
    or HISTORY_DAYS boundary.
    """
    seen_previous = since_sha is None
    for c in repo.iter_commits("HEAD", since=SINCE.isoformat()):
        if not seen_previous:
            if c.hexsha == since_sha:
                seen_previous = True
            continue
        yield c


# Per-repo worker
def process_repository(full_name: str, stars: int) -> None:
    LOG.info("clone %s", full_name)
    tmp_dir = tempfile.mkdtemp(prefix="bare_")
    processed = 0

    try:
        token_prefix = f"{GITHUB_TOKEN}@" if GITHUB_TOKEN else ""
        origin_url = f"https://{token_prefix}github.com/{full_name}.git"

        repo = Repo.clone_from(
            origin_url,
            tmp_dir,
            bare=True,
            filter="blob:limit=50m",  # avoids huge downloads
        )

        with engine.begin() as conn:
            since_sha = get_last_sha(conn, full_name)

            commit_rows = []
            file_rows = []
            newest_sha: str | None = None

            for cm in iter_new_commits(repo, since_sha):
                if processed >= MAX_COMMITS_PER_REPO:
                    break
                if not cm.parents:
                    continue

                parent = cm.parents[0]
                try:
                    diffs = parent.diff(cm, create_patch=False, ignore_submodules="all")
                except GitCommandError as exc:
                    LOG.warning("diff failed %s@%s: %s", full_name, cm.hexsha[:7], exc)
                    continue

                stats_total = cm.stats.total
                stats_files = cm.stats.files

                for d in diffs:
                    # Path used for stats lookup
                    stats_key = d.b_path if d.b_path else d.a_path
                    file_stat = stats_files.get(stats_key, {})
                    before_uri = ""
                    after_uri = ""

                    if d.a_blob:
                        before_uri = storage.put_blob(
                            d.a_blob.data_stream.read(),
                            storage.make_key(full_name, cm.hexsha, "before", d.a_path),
                        )
                    if d.b_blob:
                        after_uri = storage.put_blob(
                            d.b_blob.data_stream.read(),
                            storage.make_key(full_name, cm.hexsha, "after", d.b_path),
                        )

                    file_rows.append(
                        {
                            "commit_hash": cm.hexsha,
                            "file_path": stats_key,
                            "file_extension": os.path.splitext(stats_key)[1]
                            .lstrip(".")
                            .lower(),
                            "change_type": d.change_type,
                            "lines_added": file_stat.get("insertions", 0),
                            "lines_removed": file_stat.get("deletions", 0),
                            "before_uri": before_uri,
                            "after_uri": after_uri,
                        }
                    )

                commit_rows.append(
                    {
                        "commit_hash": cm.hexsha,
                        "repository": full_name,
                        "author": cm.author.name if cm.author else None,
                        "author_email": cm.author.email if cm.author else None,
                        "commit_message": cm.message.strip(),
                        "commit_timestamp": datetime.fromtimestamp(
                            cm.committed_date, tz=UTC
                        ),
                        "files_changed": stats_total["files"],
                        "lines_added": stats_total["insertions"],
                        "lines_removed": stats_total["deletions"],
                        "collected_at": datetime.now(UTC),
                    }
                )
                newest_sha = cm.hexsha
                processed += 1

            if commit_rows:
                conn.execute(insert(commits), commit_rows)
            if file_rows:
                conn.execute(insert(commit_files), file_rows)
            if newest_sha:
                save_last_sha(conn, full_name, newest_sha)

        LOG.info("done %s – %d commits", full_name, processed)

    except Exception:
        LOG.exception("failed %s", full_name)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def main() -> None:
    repos = fetch_repositories()
    if not repos:
        LOG.warning("repository table is empty")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        tasks = {pool.submit(process_repository, n, s): n for n, s in repos}
        for fut in as_completed(tasks):
            fut.result()

    LOG.info("ALL DONE")


if __name__ == "__main__":
    main()
