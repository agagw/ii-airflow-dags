from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow import configuration as conf
from airflow.models import Variable

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("k8spodoperator_acr_example", schedule_interval=None, default_args=default_args, tags=["test", "example"])

# This is where we define our desired resources.
compute_resources = {"request_cpu": "100m", "request_memory": "0.2Gi", "limit_cpu": "800m", "limit_memory": "1Gi"}
k8s_namespace = Variable.get("k8s_namespace")

with dag:
    task_one = KubernetesPodOperator(
        namespace=k8s_namespace,
        image="acrmlh.azurecr.io/hello-world:v1",
        labels={"app": f"airflow-task-{k8s_namespace}", "release": "aicoe-airflow-release"},
        annotations={"co.elastic.logs/enabled": "false"},
        name="private-test-pod-hello-world",
        task_id="task-one",
        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        resources=compute_resources,
        service_account_name="aicoe-airflow-release-worker",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    task_two = KubernetesPodOperator(
        namespace=k8s_namespace,
        image="ubuntu:16.04",
        labels={"app": f"airflow-task-{k8s_namespace}", "release": "aicoe-airflow-release"},
        annotations={"co.elastic.logs/enabled": "false"},
        cmds=["bash", "-cx"],
        arguments=["sleep 30", "echo 10"],
        name="public-test-pod-bash",
        task_id="task-two",
        in_cluster=True,  # if set to true, will look in the cluster, if false, looks for file
        resources=compute_resources,
        service_account_name="aicoe-airflow-release-worker",
        is_delete_operator_pod=True,
        get_logs=True,
    )

    task_one >> task_two
