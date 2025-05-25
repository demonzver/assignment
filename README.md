# ML DE Test Assignment

Our ML team wants to train our Next Edit Prediction model on commit data from GitHub.  
For each commit, they need all file states before the commit and after the commit, commit message and author.  
It would be great if they can restore repository state on some commit. They also want to filter commits based on number 
of files/lines changed and based on file types changed.

TODO:  
    1. Create a data schema to store commit data to manage the requests from ML team, take into account that 
data storage need to address possible future commit data requests, not like current ones, and we do not know 
yet which exactly.  
    2. Collect commit data from as many repositories as you can. Store it into S3-like storage.   
    3. (Nice to have) Create an AirFlow pipeline to collect more commit data on a regular basis.  


## Implementation plan
- **poetry** - project dependency
- **DuckDB** - database for the schema (`commits.duckdb`)
- **MinIO** - S3-like storage (`before/after`)
- **docker-compose** - deployment MinIO + Airflow
- **Apache Airflow** - scheduled execution of the commit-collector pipeline

## Notes
- [GitHub Rate limits](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28): 
All of these requests count towards your personal rate limit of 5,000 requests per hour.

## Project structure
```text
assignment/
│
├── .gitignore
├── .env.example
├── docker-compose.yml          # MinIO and Airflow
├── pyproject.toml              # poetry manifest (duckdb, minio, etc.)
│
├── src/
│   └── github_commit_pipeline/
│       ├── __init__.py      
│       ├── topics.yml          # repos topics and config  
│       ├── schema.py           # DuckDB DDL
│       ├── storage.py          # MinIO helpers
│       ├── repo_loader.py      # New repos
│       ├── collector.py        # fetch commits (new and existing repos)
│       └── restore.py          # restore repo state for a given repo name/SHA
│
├── dags/
│   └── commit_collector.py     # Airflow dag
│
└── scripts/
    ├── run_repo_loader.sh      # adds new repositories to DuckDB
    ├── run_collector.sh        # collects commits for both new repos and updates existing ones
    └── run_restore.sh          # restores the workspace to a given commit’s before/after state
```

## Quick Start (Ununtu)
- `git clone https://github.com/demonzver/assignment.git`
- `cd assignment`
- `cp .env.example .env` 
- edit .env (GITHUB_TOKEN, etc.)
- poetry:  `curl -sSL https://install.python-poetry.org | python3 -`
- python install (`python3.12`)
- installing dependencies: `poetry install`
- install docker
- `sudo snap install duckdb`
- `mkdir -p data`
- `docker compose up -d minio` -> http://localhost:9001 (local Minio)
- run repo_loader.py: `poetry run python -m github_commit_pipeline.repo_loader`
- `duckdb "${DUCKDB_PATH:-./data/commits.duckdb}" -c "SELECT * FROM repositories limit 20"`
- `duckdb "${DUCKDB_PATH:-./data/commits.duckdb}" -c "SELECT count(*) FROM repositories"`
- run collector.py: `poetry run python -m github_commit_pipeline.collector` (parallel processing)
- `duckdb "${DUCKDB_PATH:-./data/commits.duckdb}" -c "SELECT * FROM commits limit 20"`
- `duckdb "${DUCKDB_PATH:-./data/commits.duckdb}" -c "SELECT * FROM commit_files limit 20"`
- `duckdb "${DUCKDB_PATH:-./data/commits.duckdb}" -c "SELECT * FROM last_commits limit 20"`
- `poetry run python -m github_commit_pipeline.restore <owner/repo> <commit_sha> [before|after] [dest_dir]`


## Restore example
```text
poetry run python -m github_commit_pipeline.restore \
    --repo kamranahmedse/developer-roadmap \
    --sha 78e62f9de5bfb2fe884d85294d6d77f3d9b65f62 \
    --direction after \
    --out ./restored/kamranahmedse/developer-roadmap/after
```

```text
poetry run python -m github_commit_pipeline.restore \
    --repo kamranahmedse/developer-roadmap \
    --sha 78e62f9de5bfb2fe884d85294d6d77f3d9b65f62 \
    --direction before \
    --out ./restored/kamranahmedse/developer-roadmap/before
```

## Airflow (optional)
- `mkdir -p logs dags data`
- `sudo chown -R $(id -u):$(id -g) logs dags data scripts`
- `echo "AIRFLOW_UID=$(id -u)" >> .env`
- `docker compose up -d postgres`
- `docker compose run --rm airflow db init`
- `docker compose run --rm airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com`
- `docker compose up -d` http://localhost:8080 (local Airflow) 