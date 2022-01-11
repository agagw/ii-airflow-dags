"""
Objective: 
To show an example of a dag made up of PythonOperator and S3Hook

Description:
- Airflow variable `s3hook_example_prefix` is imported as python variable `s3hook_example_prefix`
- This dag list the keys under a given prefix, in sapit-core-dev cf s3 instance

Example conf when triggering this dag: N.A

Notes:
- Refer to https://airflow.apache.org/docs/apache-airflow/1.10.6/_api/airflow/hooks/S3_hook/index.html 
for more S3hook methods
"""

import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable

def list_keys():
    hook = S3Hook(aws_conn_id="aws_credentials")
    bucket = "hcp-bdf790a7-b7e7-482e-abda-613871248c65"
    prefix = Variable.get("s3hook_example_prefix")
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")


dag = DAG(
    "s3hook_example",
    start_date=datetime.datetime.now(),
    schedule_interval=None,
    tags=["test", "example"],
)

list_keys = PythonOperator(task_id="list_keys", python_callable=list_keys, dag=dag)
