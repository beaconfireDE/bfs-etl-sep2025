# dags/example_complex_demo.py
import pendulum
from airflow import DAG
from airflow.models.baseoperator import chain

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_complex_demo",
    schedule=None,
    start_date=pendulum.datetime(2025, 7, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    start = BashOperator(task_id="start", bash_command="echo start")
    branch_a = BashOperator(task_id="branch_a", bash_command="echo branch A")
    branch_b = BashOperator(task_id="branch_b", bash_command="echo branch B")
    merge = BashOperator(task_id="merge", bash_command="echo merge")
    end = BashOperator(task_id="end", bash_command="echo end")

    chain(start, [branch_a, branch_b], merge, end)
