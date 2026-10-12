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

## Avancement

- [x] Kafka en mode KRaft avec Docker Compose (volume persistant + Kafka UI)
- [x] Test du partitionnement par clé (ordre garanti par station)
- [x] Producer Python (API Vélib' → Kafka, 1 message par station, clé = `station_id`)
- [x] Conteneurisation du producer (Dockerfile + service Docker Compose)
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
- **`restart: unless-stopped`** : le producer est relancé automatiquement en cas de crash imprévu.

## Lancer le projet

```bash
docker compose up -d --build
```

Cette commande démarre Kafka, Kafka UI et le producer. Le topic `velib-station-status` est créé automatiquement au premier message, avec 3 partitions.

| Service | Accès |
|---|---|
| Kafka UI | http://localhost:8085 (**Topics → velib-station-status → Messages**) |
| Logs du producer | `docker compose logs -f producer` |

Le producer envoie environ 1 500 messages par minute.

Pour tout arrêter : `docker compose down`. Les messages Kafka sont conservés dans le volume `kafka_data`.

## Structure du projet

```
velib-data-pipeline/
├── docker-compose.yml      # Kafka (KRaft) + Kafka UI + producer
├── producer/
│   ├── Dockerfile          # Image du producer
│   ├── producer.py         # API Vélib' → Kafka, toutes les 60 s
│   ├── test_keys.py        # Démonstration du partitionnement par clé
│   └── requirements.txt
├── spark_jobs/             # (à venir)
├── airflow/                # (à venir)
├── databricks/             # (à venir)
└── docs/
```

## Stack technique

Python 3.12 · Apache Kafka 4.3 (KRaft) · confluent-kafka · Docker Compose · *à venir : Apache Spark, Apache Airflow, Databricks, Delta Lake*
