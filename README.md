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

- **`last_reported` n'identifie pas une mesure** : il ne change en moyenne que toutes les 28 minutes, alors que le nombre de vélos change entre-temps. Sur 63 746 couples (`station_id`, `last_reported`), **48 268 regroupent des valeurs différentes**. Dédoublonner sur cette clé aurait supprimé environ **3 changements réels sur 4**. En réalité, **15,2 %** des relevés montrent un changement d'état par rapport au relevé précédent de la station.
- **Granularité retenue en Silver** : un **relevé par station et par cycle** (`station_id`, `ingestion_time`), chaque minute étant une observation valide. Le dédoublonnage ne retire que les éventuels doublons techniques.
- **Panne partielle de l'API** : lors de 10 cycles, le détail mécanique / électrique était vide (`[{}, {}]`). Silver met alors `nb_mechanical` et `nb_ebike` à **NULL** (inconnu) plutôt qu'à 0 (faux).
- **Stations hors service** : 15 en maintenance et 2 non installées au dernier cycle. Elles sont conservées avec un indicateur, pour analyser les pannes, et exclues des calculs de disponibilité.
- **Traçabilité** : le couple (`partition`, `offset`) identifie de façon unique chaque message Kafka. Il sera conservé en Bronze.

## Avancement

- [x] Kafka en mode KRaft avec Docker Compose (volume persistant + Kafka UI)
- [x] Test du partitionnement par clé (ordre garanti par station)
- [x] Producer Python (API Vélib' → Kafka, 1 message par station, clé = `station_id`)
- [x] Conteneurisation du producer (Dockerfile + service Docker Compose)
- [x] Cluster Spark (1 master + 1 worker) avec connecteur Kafka et Delta Lake
- [x] Exploration des données Kafka avec PySpark (data profiling)
- [x] Spark Structured Streaming Kafka → Bronze (Delta Lake, checkpoint, exactly-once), en service permanent
- [x] Spark batch Bronze → Silver (parsing, typage, aplatissement, broadcast join avec le référentiel des stations)
- [x] Spark batch Silver → Gold (4 tables d'indicateurs, heure de Paris, complétude des données)
- [x] Airflow 3.3 déployé dans Docker Compose (LocalExecutor + PostgreSQL)
- [ ] DAG quotidien Silver → Gold orchestré par Airflow
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
- **`spark.log.level WARN`** : des logs lisibles pour tous les jobs.

**Couche Bronze (`spark_jobs/stream_bronze.py`)**
- **Structured Streaming** de Kafka vers une table **Delta Lake**, avec un micro-batch par minute.
- **Données brutes** : le JSON est stocké tel quel (`raw_json`), sans schéma imposé (*schema-on-read*). Bronze ne casse jamais si l'API change de format, et Silver peut toujours être recalculé.
- **Traçabilité** : `topic`, `partition` et `offset` identifient chaque message. `kafka_timestamp` et `bronze_ingested_at` permettent de mesurer la latence.
- **Partitionnement par `ingestion_date`** : un dossier par jour, pour lire uniquement les dates utiles.
- **Exactly-once** : la source est rejouable (offsets Kafka), le checkpoint enregistre l'intention (`offsets/`) et la validation (`commits/`) de chaque micro-batch, et Delta garantit des écritures atomiques et idempotentes. C'est vérifié après plusieurs arrêts brutaux : autant de couples (`partition`, `offset`) distincts que de lignes, et un batch planifié mais non validé rejoué à l'identique au redémarrage.
- **`maxOffsetsPerTrigger`** : au premier lancement, l'historique (1,6 million de messages) est absorbé par lots de 300 000.
- **Service Docker dédié** (`spark-bronze-stream`) avec `restart: unless-stopped`, et limité à **1 cœur** (`spark.cores.max=1`) pour laisser des ressources aux jobs batch.

**Couche Silver (`spark_jobs/silver_station_status.py`)**
- **Parsing de `raw_json`** avec un schéma explicite (*schema-on-read*).
- **Granularité** : un relevé par station et par cycle (`station_id`, `ingestion_time`). Seuls les doublons techniques sont retirés (voir « Ce que révèle l'exploration »).
- **Aplatissement** du détail `[{"mechanical": n}, {"ebike": m}]` en `nb_mechanical` et `nb_ebike`. Ils valent **NULL** quand l'API renvoie un détail vide : une valeur inconnue n'est jamais remplacée par 0, qui fausserait les moyennes.
- **Typage** : timestamps Unix convertis en `TIMESTAMP` UTC (`last_reported_at`, `snapshot_at`), indicateurs 0 / 1 convertis en booléens, et ajout de `is_operational`.
- **Enrichissement par broadcast join** avec le référentiel `station_information` (nom, GPS, capacité, 1 519 lignes), lui-même stocké dans `silver/station_information`. La table de 1,8 million de relevés n'est pas redistribuée sur le réseau.
- **Partitionnement par `snapshot_date`** et **recalcul complet idempotent** (`overwrite`) : le job peut être relancé sans créer de doublons. Delta conserve les versions précédentes (*time travel*).
- **Contrôle** : 1 790 901 relevés = 1 179 cycles × 1 519 stations, et 100 % des stations trouvées dans le référentiel.

**Couche Gold (`spark_jobs/gold_velib.py`)**

| Table | Question métier | Granularité |
|---|---|---|
| `station_hourly` | Évolution de chaque station heure par heure | station × jour × heure |
| `hourly_profile` | Heures de pointe du réseau | heure de la journée |
| `station_ranking` | Stations souvent vides, pleines ou hors service | station |
| `network_daily` | Santé du réseau et **complétude des données** | jour |

- **UTC jusqu'en Silver, heure de Paris en Gold** : l'heure locale est ambiguë au changement d'heure (la même minute existe deux fois). Elle n'est utilisée que dans la couche de présentation.
- **Taux de remplissage calculé sur les seules stations en service** : une station en panne n'est pas « vide ».
- **`data_coverage`** (cycles observés / 1 440 par jour) : un indicateur de qualité pour ne pas tirer de conclusions sur des journées incomplètes.

**Airflow (orchestration)**
- **Airflow 3.3 en `LocalExecutor`** : les tâches s'exécutent dans le conteneur du scheduler. Pas besoin de Celery ni de Redis à cette échelle.
- **Un conteneur par composant** : `airflow-apiserver` (interface web + API utilisée par les tâches), `airflow-scheduler`, `airflow-dag-processor` (lecture des DAGs) et `airflow-triggerer` (tâches différées). La base de métadonnées est un **PostgreSQL** dédié (`airflow-postgres`) avec un volume persistant, non exposé hors du réseau Docker.
- **`airflow-init`** : un conteneur éphémère crée les tables et le compte admin, puis s'arrête. Les autres services attendent qu'il se termine sans erreur (`service_completed_successfully`). Il est idempotent : le relancer ne change rien.
- **Configuration factorisée** : une ancre YAML (`x-airflow-common`) partage l'image, les réglages et les dossiers entre les 5 services Airflow.
- **Secrets hors du code** : la clé JWT (authentification entre composants), la clé Fernet (chiffrement des connexions) et le compte admin sont dans un fichier `.env`, exclu de Git.
- **DAGs en pause à leur création** et exemples d'Airflow désactivés.
- **Spark lancé par `DockerOperator`** *(choix retenu pour la suite)* : les conteneurs Spark sont en Python 3.10, alors que le provider Spark d'Airflow exige Python 3.11 ou plus. Or le driver PySpark doit avoir la même version de Python que les executors. Airflow lancera donc un conteneur Spark éphémère, avec la même image que le cluster, pour chaque job. Airflow orchestre, Spark calcule.

## Premiers résultats

- **L'effet colline** : les stations le plus souvent vides sont en hauteur (Porte de Ménilmontant 55 % du temps, Tourlaque - Damrémont à Montmartre 52 %, Porte de Bagnolet 50 %). Les plus souvent pleines sont dans le centre, près de la Seine (Place Alphonse Deville 41 %, Beaux-Arts - Bonaparte 37 %, Oratoire - Rivoli 34 %). Les vélos descendent les collines mais les remontent peu, d'où le besoin de régulation par camion.
- **Limite assumée** : le pipeline tourne sur un poste personnel, éteint la nuit. La couverture ne dépasse pas 23 % d'une journée, et la pointe du matin (2h-9h) n'est pas observée. Les heures de pointe ne peuvent donc pas encore être établies de façon fiable. Il faudrait une collecte 24h/24 sur plusieurs semaines.

## Lancer le projet

Créer d'abord un fichier `.env` à la racine du projet (il n'est pas versionné) :

```
AIRFLOW_UID=50000
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=<mot de passe>
AIRFLOW_JWT_SECRET=<python -c "import secrets; print(secrets.token_urlsafe(32))">
AIRFLOW_FERNET_KEY=<python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())">
```

Puis :

```bash
docker compose up -d --build
```

Cette commande démarre Kafka, Kafka UI, le producer, le cluster Spark, le streaming vers Bronze et Airflow. Le topic `velib-station-status` est créé automatiquement au premier message, avec 3 partitions.

| Service | Accès |
|---|---|
| Kafka UI | http://localhost:8085 (**Topics → velib-station-status → Messages**) |
| Logs du producer | `docker compose logs -f producer` |
| Spark UI (cluster) | http://localhost:9090 |
| Spark UI (jobs lancés depuis le master) | http://localhost:4040 |
| Spark UI du streaming Bronze | http://localhost:4041 (onglet **Structured Streaming**) |
| Logs du streaming Bronze | `docker compose logs -f spark-bronze-stream` |
| Console PySpark | `docker exec -it spark-master /opt/spark/bin/pyspark` |
| Airflow | http://localhost:8080 (compte défini dans `.env`) |
| Santé d'Airflow | http://localhost:8080/api/v2/monitor/health |

Construire la couche Silver (batch, environ 2 minutes) :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.cores.max=1 /opt/spark/jobs/silver_station_status.py
```

Construire la couche Gold (batch, après Silver) :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --conf spark.cores.max=1 /opt/spark/jobs/gold_velib.py
```

Lire les tables depuis la console PySpark :

```python
bronze = spark.read.format("delta").load("/opt/spark/data/bronze/station_status")
bronze.groupBy("ingestion_date").count().orderBy("ingestion_date").show()

silver = spark.read.format("delta").load("/opt/spark/data/silver/station_status")
silver.select("name", "nb_bikes", "nb_mechanical", "nb_ebike", "nb_free_docks", "is_operational", "snapshot_at").show(5)

from pyspark.sql import functions as F
ranking = spark.read.format("delta").load("/opt/spark/data/gold/station_ranking")
ranking.orderBy(F.desc("pct_time_empty")).select("name", "pct_time_empty", "pct_time_full").show(5, truncate=False)
```

Le producer envoie environ 1 500 messages par minute.

Pour tout arrêter : `docker compose down`. Les messages Kafka sont conservés dans le volume `kafka_data`.

## Structure du projet

```
velib-data-pipeline/
├── .env                    # Secrets Airflow (non versionné)
├── docker-compose.yml      # Kafka (KRaft) + Kafka UI + producer + Spark + streaming Bronze + Airflow
├── config/
│   └── spark-defaults.conf # Configuration Spark (master, packages Kafka/Delta)
├── producer/
│   ├── Dockerfile          # Image du producer
│   ├── producer.py         # API Vélib' → Kafka, toutes les 60 s
│   ├── test_keys.py        # Démonstration du partitionnement par clé
│   └── requirements.txt
├── spark_jobs/
│   ├── explore_kafka.py    # Exploration batch du topic (volume, doublons, état des stations)
│   ├── stream_bronze.py    # Streaming Kafka → Bronze (Delta Lake)
│   ├── silver_station_status.py  # Batch Bronze → Silver (nettoyage + broadcast join)
│   └── gold_velib.py       # Batch Silver → Gold (4 tables d'indicateurs)
├── data/                   # Données générées (non versionné)
│   ├── bronze/station_status/       # Table Delta, partitionnée par ingestion_date
│   ├── silver/station_status/       # Relevés nettoyés, partitionnés par snapshot_date
│   ├── silver/station_information/  # Référentiel des stations
│   ├── gold/                        # station_hourly, hourly_profile, station_ranking, network_daily
│   └── checkpoints/                 # Checkpoints du streaming
├── airflow/
│   ├── dags/               # DAGs Airflow (à venir)
│   └── logs/               # Logs des tâches (non versionné)
├── databricks/             # (à venir)
└── docs/
```

## Stack technique

Python 3.12 · Apache Kafka 4.3 (KRaft) · confluent-kafka · Apache Spark 4.0 (PySpark) · Delta Lake 4.0 · Apache Airflow 3.3 · PostgreSQL 16 · Docker Compose · *à venir : Databricks*
