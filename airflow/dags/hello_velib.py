"""DAG d'apprentissage : 3 tâches Bash qui s'enchaînent."""
import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="hello_velib",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 18, tz="UTC"),
    catchup=False,
    tags=["apprentissage"],
) as dag:

    annoncer = BashOperator(
        task_id="annoncer",
        bash_command="echo 'Début du traitement Vélib'",
    )

    afficher_periode = BashOperator(
        task_id="afficher_periode",
        bash_command="echo 'Réparé ! Jour traité : {{ ds }}'",

        retries=2,
        retry_delay=pendulum.duration(seconds=20),
    )


    terminer = BashOperator(
        task_id="terminer",
        bash_command="echo 'Traitement terminé'",
    )

    annoncer >> afficher_periode >> terminer
