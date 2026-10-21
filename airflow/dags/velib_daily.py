"""Pipeline quotidien Vélib' : Bronze → Silver → Gold, exécuté sur le cluster Spark."""
import os

import pendulum
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG

# Chemin du projet sur la machine hôte : c'est Docker (sur Windows) qui fait les montages
HOST_DIR = os.environ["VELIB_HOST_DIR"]

SPARK_IMAGE = "apache/spark:4.0.4-python3"   # la même image que le cluster : versions alignées
SPARK_SUBMIT = "/opt/spark/bin/spark-submit --conf spark.cores.max=1"

SPARK_MOUNTS = [
    Mount(source=f"{HOST_DIR}/spark_jobs", target="/opt/spark/jobs", type="bind", read_only=True),
    Mount(source=f"{HOST_DIR}/data", target="/opt/spark/data", type="bind"),
    Mount(source=f"{HOST_DIR}/config/spark-defaults.conf",
          target="/opt/spark/conf/spark-defaults.conf", type="bind", read_only=True),
]


def spark_job(task_id, script):
    """Tâche Airflow qui lance un job Spark dans un conteneur éphémère."""
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=f"{SPARK_SUBMIT} /opt/spark/jobs/{script}",
        mounts=SPARK_MOUNTS,
        network_mode="velib-data-pipeline_default",   # pour joindre spark-master:7077
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",                         # supprime le conteneur s'il a réussi
        mount_tmp_dir=False,
    )


with DAG(
    dag_id="velib_daily",
    schedule=None,   
    start_date=pendulum.datetime(2026, 10, 18, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["velib", "spark"],
) as dag:

    silver = spark_job("build_silver", "silver_station_status.py")
    gold = spark_job("build_gold", "gold_velib.py")

    silver >> gold
