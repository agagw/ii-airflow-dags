"""
Objective: 
To show an example of a dag made up of PythonOperator and PythonVirtualenvOperator

Description:
- Airflow variable `k8s_namespace` is imported as python variable `k8s_namespace`
- Import of python library `ai_bus_commons` from nexus works

Example conf when triggering this dag: N.A

Notes:
- You can use REST API to read xcom values or use the UI to view xcom
"""

import time
from pprint import pprint

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from kubernetes.client import models as k8s

k8s_namespace = Variable.get("k8s_namespace")

args = {
    "owner": "airflow",
}

with DAG(
    dag_id="pythonoperator_example",
    default_args=args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["test", "example"],
) as dag:

    # [START howto_operator_python]
    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        from business_ocr import Client as BOCRClient
        from aicoe_shared_services.s3.s3store import S3Store
        from aicoe_shared_services.connectors.cms_connector import CmsConnector

        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
        task_id="import_python_libraries",
        python_callable=print_context,
    )
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    def my_sleeping_function(random_base):
        """This is a function that will run within the DAG execution"""
        time.sleep(random_base)

    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):
        task = PythonOperator(
            task_id="sleep_for_" + str(i),
            python_callable=my_sleeping_function,
            op_kwargs={"random_base": float(i) / 10},
        )

        run_this >> task
    # [END howto_operator_python_kwargs]

    # [START howto_operator_python_venv]
    def callable_virtualenv():
        """
        Example function that will be performed in a virtual environment.
        Importing at the module level ensures that it will not attempt to import the
        library before it is installed.
        """
        from time import sleep

        from colorama import Back, Fore, Style

        print(Fore.RED + "some red text")
        print(Back.GREEN + "and with a green background")
        print(Style.DIM + "and in dim text")
        print(Style.RESET_ALL)
        for _ in range(10):
            print(Style.DIM + "Please wait...", flush=True)
            sleep(10)
        print("Finished")

    virtualenv_task = PythonVirtualenvOperator(
        task_id="virtualenv_python",
        python_callable=callable_virtualenv,
        requirements=["colorama==0.4.0"],
        system_site_packages=False,
    )
    # [END howto_operator_python_venv]
