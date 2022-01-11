from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": True,
}

dag = DAG(
    "k8spodoperator_json_log_example",
    schedule_interval=None,
    default_args=default_args,
    tags=["test", "example"],
    concurrency=2,
    max_active_runs=4,
)

# This is where we define our desired resources.
compute_resources = {"request_cpu": "250m", "request_memory": "500Mi", "limit_cpu": "800m", "limit_memory": "1Gi"}
k8s_namespace = Variable.get("k8s_namespace")

with dag:
    index_case = KubernetesPodOperator(
        namespace=k8s_namespace,
        image="acrmlh.azurecr.io/json-log:v211013",
        labels={"release": "aicoe-airflow-release"},
        annotations={"co.elastic.logs/enabled": "false"},
        name="pod_json_log",
        task_id="task_json_log",
        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        resources=compute_resources,
        service_account_name="aicoe-airflow-release-worker",
        is_delete_operator_pod=True,
        get_logs=True,
        execution_timeout=timedelta(minutes=20),
        retries=2,
        retry_delay=timedelta(seconds=10),
        image_pull_policy="Always",
    )
