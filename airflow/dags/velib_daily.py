"""Pipeline quotidien Vélib' : Bronze → Silver → Gold, exécuté sur le cluster Spark."""
import json
import os
import pathlib

import pendulum
from docker.types import Mount

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, task

# Chemin du projet sur la machine hôte : c'est Docker (sur Windows) qui fait les montages
HOST_DIR = os.environ["VELIB_HOST_DIR"]

# Journal Delta de Bronze, monté en lecture seule dans les conteneurs Airflow
BRONZE_DELTA_LOG = pathlib.Path("/opt/airflow/data/bronze/station_status/_delta_log")

SPARK_IMAGE = "apache/spark:4.0.4-python3"   # la même image que le cluster : versions alignées
SPARK_SUBMIT = "/opt/spark/bin/spark-submit --conf spark.cores.max=1"

# Jour traité par le run = le début de sa période (Jinja : remplacé par Airflow au lancement)
DAY = "{{ data_interval_start | ds }}"

SPARK_MOUNTS = [
    Mount(source=f"{HOST_DIR}/spark_jobs", target="/opt/spark/jobs", type="bind", read_only=True),
    Mount(source=f"{HOST_DIR}/data", target="/opt/spark/data", type="bind"),
    Mount(source=f"{HOST_DIR}/config/spark-defaults.conf",
          target="/opt/spark/conf/spark-defaults.conf", type="bind", read_only=True),
]


def spark_job(task_id, script):
    """Tâche Airflow qui lance un job Spark dans un conteneur éphémère, pour le jour du run."""
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=f"{SPARK_SUBMIT} /opt/spark/jobs/{script} --date {DAY}",
        mounts=SPARK_MOUNTS,
        network_mode="velib-data-pipeline_default",   # pour joindre spark-master:7077
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",                         # supprime le conteneur s'il a réussi
        mount_tmp_dir=False,
    )


def latest_bronze_timestamp():
    """Heure Kafka du message le plus récent de Bronze, lue dans les statistiques du journal Delta."""
    latest = None
    for commit in sorted(BRONZE_DELTA_LOG.glob("*.json"))[-10:]:   # les 10 derniers commits suffisent
        for line in commit.read_text().splitlines():
            action = json.loads(line)
            if "add" in action:   # un fichier Parquet ajouté, avec le min / max de chaque colonne
                max_timestamp = pendulum.parse(json.loads(action["add"]["stats"])["maxValues"]["kafka_timestamp"])
                latest = max_timestamp if latest is None else max(latest, max_timestamp)
    return latest


with DAG(
    dag_id="velib_daily",
    # Période déclarée explicitement : le run du 20/10 traite [20/10 00:00 ; 21/10 00:00[ (UTC)
    # et démarre le 21/10 à 00:00. Sans ça, Airflow 3 ne donne pas de période (voir fiche, Q4)
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 10, 11, tz="UTC"),   # premier jour de données
    catchup=False,       # les jours passés sont rattrapés par un backfill lancé à la main
    max_active_runs=1,   # un jour après l'autre : Gold du jour J lit aussi le Silver de J-1
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["velib", "spark"],
) as dag:

    @task.sensor(poke_interval=60, timeout=2 * 60 * 60, mode="reschedule", retries=0)
    def wait_for_bronze(data_interval_end=None):
        """Attend que Bronze contienne des messages arrivés APRÈS la fin de la journée traitée."""
        latest = latest_bronze_timestamp()
        print(f"Message le plus récent dans Bronze : {latest} (attendu : après {data_interval_end})")
        return latest is not None and latest >= data_interval_end

    silver = spark_job("build_silver", "silver_station_status.py")
    gold = spark_job("build_gold", "gold_velib.py")

    wait_for_bronze() >> silver >> gold
