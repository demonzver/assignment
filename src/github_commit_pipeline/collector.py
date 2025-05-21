"""
src/github_commit_pipeline/collector.py
--------------------------------------

One-shot collector that

1. Reads the GitHub repo list (hard-coded for the first run)
2. Downloads the last `COMMITS_LIMIT` commits per repo
3. Uploads file snapshots *before* / *after* each commit to MinIO
4. Stores commit- and file-level metadata in DuckDB

Run once:

    poetry run python -m github_commit_pipeline.collector
"""

import os
from datetime import datetime, UTC
from pathlib import Path

from dotenv import load_dotenv
from github import Github
from minio.error import S3Error
from sqlalchemy import create_engine
from sqlalchemy.dialects.sqlite import insert  # works with DuckDB

from github_commit_pipeline.schema import metadata, commits, commit_files
from github_commit_pipeline.storage import upload_bytes

# ─────── CONFIG ────────────────────────────────────────────────────────────────
load_dotenv()

REPOSITORIES   = ["octocat/Hello-World"]   # small public repo for first test
COMMITS_LIMIT  = 20

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
if not GITHUB_TOKEN:
    raise RuntimeError("GITHUB_TOKEN missing in .env")

DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "./data/commits.duckdb")).expanduser()
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ─────── CLIENTS ───────────────────────────────────────────────────────────────
github  = Github(GITHUB_TOKEN)
engine  = create_engine(f"duckdb:///{DUCKDB_PATH}")
metadata.create_all(engine)        # create tables if they don't exist

# ─────── MAIN LOGIC ────────────────────────────────────────────────────────────
def process_repository(repo_name: str, limit: int = COMMITS_LIMIT) -> None:
    repo = github.get_repo(repo_name)
    timestamp = datetime.now(UTC).isoformat(timespec="seconds")
    print(f"[{timestamp}] → {repo_name}")

    with engine.begin() as conn:
        for commit in repo.get_commits()[:limit]:
            sha = commit.sha

            # materialise files list (PaginatedList -> list)
            file_list     = list(commit.files)
            files_changed = len(file_list)
            lines_added   = sum(f.additions for f in file_list)
            lines_removed = sum(f.deletions for f in file_list)

            # 1 ▸ upsert commit metadata
            conn.execute(
                insert(commits).prefix_with("OR IGNORE"),
                {
                    "commit_hash":      sha,
                    "repository":       repo_name,
                    "author":           commit.commit.author.name,
                    "author_email":     commit.commit.author.email,
                    "commit_message":   commit.commit.message,
                    "commit_timestamp": commit.commit.author.date,
                    "files_changed":    files_changed,
                    "lines_added":      lines_added,
                    "lines_removed":    lines_removed,
                },
            )

            # 2 ▸ per-file info
            for f in file_list:
                before_uri = after_uri = None

                # a) BEFORE snapshot (unless file is new)
                if f.status != "added":
                    try:
                        content_before = repo.get_contents(
                            f.filename, ref=commit.parents[0].sha
                        ).decoded_content
                        before_uri = upload_bytes(
                            content_before,
                            f"{repo_name}/{sha}/before/{f.filename}",
                        )
                    except Exception as exc:
                        print(f"   ⚠ cannot fetch BEFORE {f.filename}: {exc}")

                # b) AFTER snapshot (unless file is removed)
                if f.status != "removed":
                    try:
                        content_after = repo.get_contents(
                            f.filename, ref=sha
                        ).decoded_content
                        after_uri = upload_bytes(
                            content_after,
                            f"{repo_name}/{sha}/after/{f.filename}",
                        )
                    except Exception as exc:
                        print(f"   ⚠ cannot fetch AFTER  {f.filename}: {exc}")

                conn.execute(
                    commit_files.insert(),
                    {
                        "commit_hash":   sha,
                        "file_path":     f.filename,
                        "file_extension": os.path.splitext(f.filename)[1],
                        "change_type":   f.status,
                        "lines_added":   f.additions,
                        "lines_removed": f.deletions,
                        "before_uri":    before_uri,
                        "after_uri":     after_uri,
                    },
                )
    print(f"✔ finished {repo_name}")


def main() -> None:
    for repo in REPOSITORIES:
        try:
            process_repository(repo)
        except S3Error as e:
            print(f"MinIO error: {e}")
        except Exception as e:
            print(f"Unexpected error on {repo}: {e}")


if __name__ == "__main__":
    main()
