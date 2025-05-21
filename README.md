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


## Project structure
```text
github-commit-pipeline/
│
├── .gitignore
├── .env.example
├── docker-compose.yml          # MinIO and Airflow
├── requirements_airflow.txt    # Airflow libs
├── pyproject.toml              # poetry manifest (duckdb, minio, etc.)
│
├── src/
│   └── github_commit_pipeline/
│       ├── schema.py           # DuckDB DDL
│       ├── storage.py          # MinIO helpers
│       ├── collector.py        # fetch commits
│       └── restore.py          # restore repo state for a given SHA
│
├── dags/
│   └── commit_collector.py     # Airflow dag calls collector.py
│
└── scripts/
    └── run_collector.sh        # single local run
```

## Quick Start (Ununtu)
- `git clone https://github.com/demonzver/assignment.git`
- `cd assignment`
- `cp .env.example .env` 
- edit .env
- poetry:  `curl -sSL https://install.python-poetry.org | python3 -`
- python install (`python3.12`)
- installing dependencies: `poetry install`
- run collector.py  
- ...