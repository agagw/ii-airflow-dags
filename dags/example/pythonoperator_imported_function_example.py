"""
Objective: 
To show an example of a dag made up of PythonOperator, that uses a function imported from a self-defined module

Description:
- Airflow variable `k8s_namespace` is imported as python variable `k8s_namespace`
- Import of python library `aicoe_shared_services` is done in imported example function

Example conf when triggering this dag: N.A

Notes:
- You can use REST API to read xcom values or use the UI to view xcom
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from custom_module.custom_function import _example_fn

k8s_namespace = Variable.get("k8s_namespace")

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="pythonoperator_imported_function_example",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["test", "example"],
) as dag:
    run_example_fn_task = PythonOperator(
        task_id="run_example_fn",
        python_callable=_example_fn,
    )
