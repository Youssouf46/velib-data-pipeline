"""Contrôle qualité de Gold pour UNE journée : le job échoue (code 1) si une règle n'est pas respectée."""
import argparse
import datetime as dt
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

GOLD_PATH = "/opt/spark/data/gold"
EXPECTED_STATIONS = (1400, 1600)   # le réseau compte environ 1 500 stations


def parse_args():
    """Lit le jour à contrôler (--date, heure de Paris) et la couverture minimale acceptée."""
    parser = argparse.ArgumentParser(description="Contrôle qualité de Gold pour une journée")
    parser.add_argument("--date", required=True, help="jour à contrôler, format AAAA-MM-JJ (heure de Paris)")
    parser.add_argument("--min-coverage", type=float, default=0.05, help="part minimale de la journée observée")
    args = parser.parse_args()
    return dt.date.fromisoformat(args.date), args.min_coverage


day, min_coverage = parse_args()
spark = SparkSession.builder.appName(f"check-quality-{day}").getOrCreate()

network = spark.read.format("delta").load(f"{GOLD_PATH}/network_daily").filter(F.col("local_date") == F.lit(day))
stations = spark.read.format("delta").load(f"{GOLD_PATH}/station_daily").filter(F.col("local_date") == F.lit(day))

results = []   # (règle, réussie ?, détail)

# 1) La journée existe dans Gold
day_row = network.first()
results.append(("Journée présente dans Gold", day_row is not None, "network_daily"))

if day_row is not None:
    # 2) Assez de données pour que les indicateurs aient un sens
    results.append((
        "Couverture suffisante",
        day_row["data_coverage"] >= min_coverage,
        f"{day_row['data_coverage']:.1%} de la journée observée (minimum : {min_coverage:.0%})",
    ))

    # 3) Une seule ligne par station dans station_daily
    nb_rows, nb_stations = stations.agg(F.count("*"), F.countDistinct("station_id")).first()
    results.append(("Pas de doublon par station", nb_rows == nb_stations, f"{nb_rows} lignes, {nb_stations} stations"))

    # 4) Un nombre de stations plausible
    results.append((
        "Nombre de stations plausible",
        EXPECTED_STATIONS[0] <= nb_stations <= EXPECTED_STATIONS[1],
        f"{nb_stations} stations (attendu : entre {EXPECTED_STATIONS[0]} et {EXPECTED_STATIONS[1]})",
    ))

    # 5) Des comptes cohérents : vide + pleine <= en service <= relevés
    nb_incoherent = stations.filter(
        (F.col("nb_empty") + F.col("nb_full") > F.col("nb_operational"))
        | (F.col("nb_operational") > F.col("nb_snapshots"))
    ).count()
    results.append(("Comptes cohérents", nb_incoherent == 0, f"{nb_incoherent} station(s) incohérente(s)"))

spark.stop()

print(f"Contrôle qualité du {day} :")
for rule, passed, detail in results:
    print(f"  {'OK    ' if passed else 'ÉCHEC '} {rule} : {detail}")

failed = [rule for rule, passed, _ in results if not passed]
if failed:
    print(f"Qualité insuffisante pour le {day} : {', '.join(failed)}")
    sys.exit(1)   # code de sortie 1 → le DockerOperator, donc la tâche Airflow, échoue
print(f"Qualité validée pour le {day}")
