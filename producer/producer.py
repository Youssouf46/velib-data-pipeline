"""Producer Vélib' : interroge l'API toutes les 60 s et envoie chaque station dans Kafka."""
import json
import logging
import os
import time

import requests
from confluent_kafka import Producer

# --- Configuration : tout est modifiable par variable d'environnement, rien en dur
API_URL = os.getenv(
    "VELIB_API_URL",
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json",
)
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "velib-station-status")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

# --- Logs : plus pro que print(), avec la date et le niveau (INFO, WARNING, ERROR)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("velib-producer")


def fetch_station_status() -> dict:
    """Appelle l'API Vélib' et renvoie le JSON complet."""
    response = requests.get(API_URL, timeout=10)  # timeout : ne jamais attendre indéfiniment
    response.raise_for_status()                    # lève une erreur si code HTTP 4xx / 5xx
    return response.json()


def delivery_report(err, msg):
    """Appelée par Kafka pour chaque message : on ne log que les échecs."""
    if err is not None:
        logger.error("Échec d'envoi pour la station %s : %s", msg.key(), err)


def publish_stations(producer: Producer, payload: dict) -> int:
    """Envoie 1 message par station dans Kafka. Renvoie le nombre de stations envoyées."""
    ingestion_time = int(time.time())               # quand NOUS avons reçu la donnée
    api_last_updated = payload.get("lastUpdatedOther")
    stations = payload["data"]["stations"]

    for station in stations:
        message = {
            **station,                              # tous les champs d'origine de la station
            "api_last_updated": api_last_updated,
            "ingestion_time": ingestion_time,
        }
        producer.produce(
            topic=TOPIC,
            key=str(station["station_id"]),          # clé = station → ordre garanti par station
            value=json.dumps(message),               # dict Python → texte JSON
            callback=delivery_report,
        )
        producer.poll(0)                             # traite les accusés de réception en attente

    producer.flush()                                 # attend que tout soit bien arrivé
    return len(stations)


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    logger.info("Démarrage : topic=%s, intervalle=%ss, kafka=%s", TOPIC, POLL_INTERVAL_SECONDS, BOOTSTRAP_SERVERS)

    while True:
        try:
            payload = fetch_station_status()
            count = publish_stations(producer, payload)
            logger.info("%d stations envoyées", count)
        except (requests.RequestException, ValueError, KeyError) as exc:
            # API en panne, JSON invalide ou structure changée → on log et on réessaie au prochain tour
            logger.warning("Cycle échoué, nouvel essai dans %ss : %s", POLL_INTERVAL_SECONDS, exc)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
