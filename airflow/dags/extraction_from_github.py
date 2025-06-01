from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
}

with DAG(
    dag_id="github_ingestion_pipeline",
    default_args=default_args,
    description="A DAG to extract and curate GitHub data",
    schedule_interval='@daily',
    catchup=False,
) as dag:
    install_deps = BashOperator(
        task_id="install_dependencies",
        bash_command="pip install -r /opt/airflow/dags/requirements.txt",
        dag=dag,
    )
    extract_repos = BashOperator(
        task_id="extract_repos_from_github",
        bash_command="python /opt/airflow/scripts/extract_repos_from_github.py"
    )

    extract_commits = BashOperator(
        task_id="extract_commits_from_github",
        bash_command="python /opt/airflow/scripts/extract_commits_from_github.py"
    )

    extract_commit_details = BashOperator(
        task_id="extract_commits_details_from_github",
        bash_command="python /opt/airflow/scripts/extract_commits_details_from_github.py"
    )

    extract_blobs = BashOperator(
        task_id="extract_blob_files",
        bash_command="python /opt/airflow/scripts/extract_blob_files.py"
    )

    curate_layer = BashOperator(
        task_id="create_curated_layer",
        bash_command="python /opt/airflow/scripts/create_curated_layer.py"
    )

    (
        install_deps
        >> extract_repos 
        >> extract_commits 
        >> extract_commit_details 
        >> curate_layer 
        >> extract_blobs
    )