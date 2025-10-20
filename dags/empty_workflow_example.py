import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="basic_three_empty_tasks",
    start_date=datetime.datetime(2025, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "basic"],
) as dag:
    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")
    task_c = EmptyOperator(task_id="task_c")

    task_a >> task_b >> task_c
