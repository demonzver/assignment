"""
Load NEW repositories for each topic defined in topics.yml.

* Reads a YAML file with a list of topics/keywords.
* For every keyword, fetches the most-starred public repos
  that satisfy stars >= STAR_THRESHOLD.
* Inserts up to NEW_LIMIT per topic unseen repositories into the
  `repositories` table (DuckDB). Existing rows are skipped.

Usage
-----
poetry run python -m github_commit_pipeline.repo_loader
"""

from __future__ import annotations

import os
from datetime import datetime, UTC
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from github import Github
from sqlalchemy import create_engine, insert, select, update

from github_commit_pipeline.schema import metadata, repositories

#  Configuration
CONFIG_FILE = Path(__file__).resolve().parent / "topics.yml"

# fallback defaults (in case fields absent in yaml)
DEFAULT_STAR_THRESHOLD = 1000
DEFAULT_NEW_LIMIT = 50


# Env / clients
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
DUCK_PATH = os.getenv("DUCKDB_PATH", "./data/commits.duckdb")


# Ensure the directory for the DuckDB file exists
Path(DUCK_PATH).parent.mkdir(parents=True, exist_ok=True)

if not TOKEN:
    raise RuntimeError("GITHUB_TOKEN not set in .env")

engine = create_engine(f"duckdb:///{DUCK_PATH}")
metadata.create_all(engine)
gh = Github(TOKEN)


# Helpers
def load_config(path: Path) -> tuple[list[str], int, int]:
    if not path.is_file():
        raise FileNotFoundError(f"{path} not found")

    data: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    topics = [t.strip() for t in data.get("topics", []) if t.strip()]

    star_thr = int(data.get("star_threshold", DEFAULT_STAR_THRESHOLD))
    limit = int(data.get("new_limit_per_topic", DEFAULT_NEW_LIMIT))
    return topics, star_thr, limit


def repo_exists(conn, name: str) -> bool:
    return bool(
        conn.execute(
            select(repositories.c.full_name).where(repositories.c.full_name == name)
        ).first()
    )


def merge_topics(cur: str | None, new_kw: str) -> str:
    parts = {*(cur or "").split(","), new_kw}
    parts = {p for p in parts if p}
    return ",".join(sorted(parts))


# Main
def main() -> None:
    keywords, star_threshold, limit_per_topic = load_config(CONFIG_FILE)

    if not keywords:
        print("[repo_loader] topics list empty → nothing to do")
        return

    total_added = 0
    for kw in keywords:
        query = f"{kw} in:description,topics,readme stars:>={star_threshold}"
        added = 0

        for repo in gh.search_repositories(query, sort="stars", order="desc"):
            with engine.begin() as conn:
                if repo_exists(conn, repo.full_name):
                    # обновим метаданные (звёзды, topics, updated_at)
                    merged = merge_topics(
                        conn.execute(
                            select(repositories.c.topic).where(
                                repositories.c.full_name == repo.full_name
                            )
                        ).scalar_one(),
                        kw,
                    )
                    conn.execute(
                        update(repositories)
                        .where(repositories.c.full_name == repo.full_name)
                        .values(
                            topic=merged,
                            stars=repo.stargazers_count,
                            updated_at=datetime.now(UTC),
                        )
                    )
                    continue

                now = datetime.now(UTC)
                conn.execute(
                    insert(repositories).values(
                        full_name=repo.full_name,
                        topic=kw,
                        stars=repo.stargazers_count,
                        inserted_at=now,
                        updated_at=now,
                    )
                )
                added += 1
                total_added += 1

            if added >= limit_per_topic:
                break

        print(f"[repo_loader] '{kw}': +{added} new repositories")

    print(f"[repo_loader] total new repos this run: {total_added}")


if __name__ == "__main__":
    main()
