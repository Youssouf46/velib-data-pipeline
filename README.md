# Vélib' Data Pipeline 🚴

Pipeline de données temps réel sur la disponibilité des stations Vélib' Paris.

## Architecture (en cours de construction)

API Vélib' → Producer Python → **Kafka** → **Spark** → **Databricks**, orchestré par **Airflow**

## Avancement

- [x] Kafka en mode KRaft avec Docker Compose (volume persistant + Kafka UI)
- [ ] Producer Python (API Vélib' → Kafka)
- [ ] Spark Structured Streaming (couche Bronze)
- [ ] Spark batch (couches Silver / Gold)
- [ ] Orchestration Airflow
- [ ] Databricks (Delta Lake, Workflows, Dashboard)

## Lancer le projet

docker compose up -d

Kafka UI : http://localhost:8085
