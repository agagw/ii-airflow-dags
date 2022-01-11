"""
Objective: 
To write and read to/from xcom

Description:
- This dag shows how to write and read to/from xcom

Example conf when triggering this dag: N.A

Notes:
- You can use REST API to read xcom values or use the UI to view xcom
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow import configuration as conf
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("k8spodoperator_xcom_example", schedule_interval="@once", default_args=default_args, tags=["test", "example"])

compute_resources = {"request_cpu": "100m", "request_memory": "0.2Gi", "limit_cpu": "200m", "limit_memory": "0.2Gi"}
k8s_namespace = Variable.get("k8s_namespace")

with dag:
    # [START howto_operator_k8s_write_xcom]
    write_xcom = KubernetesPodOperator(
        namespace=k8s_namespace,
        image="alpine",
        cmds=["sh", "-c", "mkdir -p /airflow/xcom/;echo '[1,2,3,4]' > /airflow/xcom/return.json"],
        name="pod_write_xcom",
        labels={"release": "aicoe-airflow-release"},
        annotations={"co.elastic.logs/enabled": "false"},
        do_xcom_push=True,
        in_cluster=True,
        resources=compute_resources,
        service_account_name="aicoe-airflow-release-worker",
        task_id="task_write_xcom_example",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    pod_task_xcom_result = BashOperator(
        bash_command="echo \"{{ task_instance.xcom_pull('task_write_xcom_example')[0] }}\"",
        task_id="task_echo_xcom_result",
    )
    # [END howto_operator_k8s_write_xcom]

    write_xcom >> pod_task_xcom_result
