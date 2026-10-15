# Vélib' Data Pipeline 🚴

Pipeline de données temps réel sur la disponibilité des stations Vélib' Paris :
ingestion streaming avec **Kafka**, traitement avec **Spark**, architecture médaillon (Bronze / Silver / Gold)
sur **Databricks**, orchestration avec **Airflow**, le tout conteneurisé avec **Docker**.

## Architecture

```
API Vélib' (GBFS) ──► Producer Python ──► Kafka ──► Spark Streaming ──► Bronze
                                                                          │
                                         Airflow (orchestration quotidienne)
                                                                          ▼
                                         Spark batch ──► Silver ──► Gold ──► Databricks (Delta Lake, Dashboard)
```

## Source de données

API open data Vélib' Métropole (format GBFS), sans clé d'API, mise à jour toutes les 1 à 3 minutes :

| Flux | Contenu |
|---|---|
| `station_status.json` | Vélos disponibles (mécaniques / électriques), places libres, état de la station |
| `station_information.json` | Nom, coordonnées GPS, capacité de chaque station |

Les deux flux se joignent sur `station_id`. Environ 1 500 stations.

### Ce que révèle l'exploration (`spark_jobs/explore_kafka.py`)

Analyse de **1 069 376 messages** (704 cycles × 1 519 stations, aucun message perdu) :

- **Seulement 3,6 % de mesures réellement nouvelles** : chaque station publie environ toutes les 28 minutes, alors que l'API est interrogée chaque minute. La couche Silver dédoublonnera sur (`station_id`, `last_reported`). La couche Bronze conserve tout, pour pouvoir rejouer les traitements.
- **Stations hors service** : 15 en maintenance et 2 non installées au dernier cycle. Elles sont conservées avec un indicateur, pour analyser les pannes, et exclues des calculs de disponibilité.
- **Traçabilité** : le couple (`partition`, `offset`) identifie de façon unique chaque message Kafka. Il sera conservé en Bronze.

## Avancement

- [x] Kafka en mode KRaft avec Docker Compose (volume persistant + Kafka UI)
- [x] Test du partitionnement par clé (ordre garanti par station)
- [x] Producer Python (API Vélib' → Kafka, 1 message par station, clé = `station_id`)
- [x] Conteneurisation du producer (Dockerfile + service Docker Compose)
- [x] Cluster Spark (1 master + 1 worker) avec connecteur Kafka et Delta Lake
- [x] Exploration des données Kafka avec PySpark (data profiling)
- [ ] Spark Structured Streaming (couche Bronze)
- [ ] Spark batch (couches Silver / Gold)
- [ ] Orchestration Airflow
- [ ] Databricks (Delta Lake, Workflows, Dashboard)

## Choix de conception

**Kafka**
- **Mode KRaft** : le broker gère lui-même ses métadonnées, sans Zookeeper.
- **Deux listeners** : `localhost:9092` pour la machine hôte, `kafka:29092` pour les autres conteneurs du réseau Docker.
- **Volume persistant** : les messages survivent à la suppression du conteneur (`docker compose down`).
- **Topic `velib-station-status` en 3 partitions** pour permettre une lecture parallèle par Spark.

**Producer**
- **1 message par station** plutôt qu'un seul gros message : chaque message reste petit, a sa propre clé et correspond directement à une ligne dans Spark.
- **Clé = `station_id`** : Kafka garantit l'ordre uniquement au sein d'une partition. Toutes les mesures d'une même station vont dans la même partition, donc restent dans l'ordre chronologique.
- **Données brutes conservées** : le message contient tous les champs de l'API, sans transformation (principe de la couche Bronze).
- **Ajout de `ingestion_time`** (timestamp Unix, UTC) : identique pour toutes les stations d'un même appel. Il servira à dédoublonner les mesures dans la couche Silver.
- **Résilience** : timeout sur les appels HTTP. Une erreur (réseau, HTTP, JSON invalide, format modifié) est journalisée et le cycle suivant réessaie, sans arrêter le producer.
- **Configuration par variables d'environnement** (`VELIB_API_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `POLL_INTERVAL_SECONDS`) : rien n'est écrit en dur dans le code.

**Conteneurisation**
- **Dockerfile optimisé pour le cache** : les dépendances sont installées avant la copie du code. Modifier le code ne relance pas `pip install`.
- **Utilisateur non-root** dans l'image du producer.
- **Healthcheck sur Kafka** + `depends_on: condition: service_healthy` : le producer ne démarre que lorsque le broker répond réellement.
- **`restart: unless-stopped` sur tous les services** : ils redémarrent automatiquement après un crash ou un redémarrage de Docker. Au départ, seul le producer avait cette politique : après un redémarrage, il tournait seul, sans Kafka.

**Spark**
- **Cluster standalone** : un `spark-master` (gestion du cluster + driver des jobs) et un `spark-worker` (2 cœurs, 2 Go pour les executors).
- **Versions alignées** : Spark 4.0.4, connecteur `spark-sql-kafka-0-10_2.13:4.0.4` et `delta-spark_2.13:4.0.1`.
- **Configuration centralisée** dans `config/spark-defaults.conf` (master, packages, extensions Delta), chargée automatiquement à chaque lancement.
- **Dossier `data/` partagé** entre master et worker : le driver écrit le journal Delta, les executors écrivent les fichiers Parquet. Il est exclu de Git (les données sont générées, pas versionnées).

## Lancer le projet

```bash
docker compose up -d --build
```

Cette commande démarre Kafka, Kafka UI, le producer et le cluster Spark. Le topic `velib-station-status` est créé automatiquement au premier message, avec 3 partitions.

| Service | Accès |
|---|---|
| Kafka UI | http://localhost:8085 (**Topics → velib-station-status → Messages**) |
| Logs du producer | `docker compose logs -f producer` |
| Spark UI (cluster) | http://localhost:9090 |
| Spark UI (application en cours) | http://localhost:4040 |
| Console PySpark | `docker exec -it spark-master /opt/spark/bin/pyspark` |

Le producer envoie environ 1 500 messages par minute.

Pour tout arrêter : `docker compose down`. Les messages Kafka sont conservés dans le volume `kafka_data`.

## Structure du projet

```
velib-data-pipeline/
├── docker-compose.yml      # Kafka (KRaft) + Kafka UI + producer + Spark
├── config/
│   └── spark-defaults.conf # Configuration Spark (master, packages Kafka/Delta)
├── producer/
│   ├── Dockerfile          # Image du producer
│   ├── producer.py         # API Vélib' → Kafka, toutes les 60 s
│   ├── test_keys.py        # Démonstration du partitionnement par clé
│   └── requirements.txt
├── spark_jobs/
│   └── explore_kafka.py    # Exploration batch du topic (volume, doublons, état des stations)
├── data/                   # Données générées : Bronze / Silver / Gold (non versionné)
├── airflow/                # (à venir)
├── databricks/             # (à venir)
└── docs/
```

## Stack technique

Python 3.12 · Apache Kafka 4.3 (KRaft) · confluent-kafka · Apache Spark 4.0 (PySpark) · Delta Lake 4.0 · Docker Compose · *à venir : Apache Airflow, Databricks*
