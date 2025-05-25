from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "ml-team",
    "depends_on_past": False,
    "catchup": False,
    "max_active_runs": 1,
}

with DAG(
    dag_id="github_commit_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 5, 24),
    schedule_interval=None,  # TODO: for now trigger manually, than @hourly, @daily or CRON
    catchup=False,
    tags=["github", "ml"],
) as dag:
    load_repos = BashOperator(
        task_id="load_repos",
        bash_command="./scripts/run_repo_loader.sh ",
        cwd="/opt/airflow"
    )

    collect_commits = BashOperator(
        task_id="collect_commits",
        bash_command="./scripts/run_collector.sh ",
        cwd="/opt/airflow"
    )

    load_repos >> collect_commits
