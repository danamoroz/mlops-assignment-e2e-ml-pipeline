from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import BashOperator


with DAG(
    dag_id="mini-swebench",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_script = BashOperator(
        task_id="mini-swe-bench-single",
        bash_command=f"{PROJECT_ROOT}/mini-swe-bench-single.sh ",
        cwd=str(PROJECT_ROOT),
        env={
            "NEBIUS_API_KEY": "{{ var.value.NEBIUS_API_KEY }}",
        },
        append_env=True,
    )
