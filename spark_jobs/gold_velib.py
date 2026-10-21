"""Batch Silver → Gold pour UNE journée (heure de Paris) : indicateurs prêts pour l'analyse et le dashboard."""
import argparse
import datetime as dt

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_STATUS_PATH = "/opt/spark/data/silver/station_status"
SILVER_INFO_PATH = "/opt/spark/data/silver/station_information"
GOLD_PATH = "/opt/spark/data/gold"
LOCAL_TIMEZONE = "Europe/Paris"
CYCLES_PER_DAY = 24 * 60  # 1 relevé par minute


def parse_day():
    """Lit le jour à traiter : --date AAAA-MM-JJ (jour local, heure de Paris)."""
    parser = argparse.ArgumentParser(description="Silver → Gold pour une journée")
    parser.add_argument("--date", required=True, help="jour à traiter, format AAAA-MM-JJ (heure de Paris)")
    return dt.date.fromisoformat(parser.parse_args().date)


def save_day(df, name, day):
    """Remplace UNIQUEMENT le jour traité dans une table Gold journalière (idempotent)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"local_date = '{day}'")
        .partitionBy("local_date")
        .save(f"{GOLD_PATH}/{name}")
    )


def save_full(df, name):
    """Réécrit une petite table Gold calculée sur tout l'historique (idempotent)."""
    df.coalesce(1).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{GOLD_PATH}/{name}")


day = parse_day()
previous_day = day - dt.timedelta(days=1)

spark = SparkSession.builder.appName(f"gold-velib-{day}").getOrCreate()

# Une journée de Paris commence la veille en UTC (22h ou 23h selon l'heure d'été) :
# on lit les partitions Silver de la veille et du jour, puis on garde le jour local demandé
silver_day = (
    spark.read.format("delta").load(SILVER_STATUS_PATH)
    .filter(F.col("snapshot_date").between(F.lit(previous_day), F.lit(day)))   # partition pruning
    .withColumn("local_time", F.from_utc_timestamp("snapshot_at", LOCAL_TIMEZONE))
    .withColumn("local_date", F.to_date("local_time"))
    .filter(F.col("local_date") == F.lit(day))
    .withColumn("local_hour", F.hour("local_time"))
    .withColumn("fill_rate", F.try_divide("nb_bikes", F.col("nb_bikes") + F.col("nb_free_docks")))  # NULL si 0 / 0
    .withColumn("is_empty", (F.col("is_operational") & (F.col("nb_bikes") == 0)).cast("int"))
    .withColumn("is_full", (F.col("is_operational") & (F.col("nb_free_docks") == 0)).cast("int"))
    .cache()  # réutilisé par 3 agrégations
)
operational = silver_day.filter("is_operational")

# 1) Chaque station, heure par heure (tables journalières : seul le jour traité est remplacé)
station_hourly = operational.groupBy("station_id", "name", "local_date", "local_hour").agg(
    F.count("*").alias("nb_snapshots"),
    F.round(F.avg("nb_bikes"), 2).alias("avg_bikes"),
    F.round(F.avg("nb_ebike"), 2).alias("avg_ebikes"),          # avg() ignore les NULL
    F.round(F.avg("fill_rate"), 3).alias("avg_fill_rate"),
    F.round(F.avg("is_empty"), 3).alias("pct_time_empty"),
    F.round(F.avg("is_full"), 3).alias("pct_time_full"),
)
save_day(station_hourly, "station_hourly", day)

# 2) Chaque station, jour par jour : des COMPTES (additifs), pas des moyennes
station_daily = silver_day.groupBy("station_id", "local_date").agg(
    F.count("*").alias("nb_snapshots"),
    F.sum(F.col("is_operational").cast("int")).alias("nb_operational"),
    F.sum("is_empty").alias("nb_empty"),
    F.sum("is_full").alias("nb_full"),
)
save_day(station_daily, "station_daily", day)

# 3) Santé du réseau et complétude des données
network_daily = (
    silver_day.groupBy("local_date")
    .agg(
        F.countDistinct("snapshot_at").alias("nb_cycles"),
        F.round(F.countDistinct("snapshot_at") / CYCLES_PER_DAY, 3).alias("data_coverage"),
        F.countDistinct(F.when(F.col("is_operational"), F.col("station_id"))).alias("nb_stations_operational"),
        F.countDistinct(F.when(~F.col("is_operational"), F.col("station_id"))).alias("nb_stations_out_of_service"),
        F.sum("nb_bikes").alias("sum_bikes"),
    )
    .withColumn("avg_bikes_in_network", F.round(F.col("sum_bikes") / F.col("nb_cycles")).cast("int"))
    .drop("sum_bikes")
)
save_day(network_daily, "network_daily", day)

# 4) Tables sur TOUT l'historique, recalculées à partir des petites tables journalières (pas de Silver)
daily = spark.read.format("delta").load(f"{GOLD_PATH}/station_daily")
stations = spark.read.format("delta").load(SILVER_INFO_PATH).select("station_id", "name", "lat", "lon", "capacity")
station_ranking = (
    daily.groupBy("station_id")
    .agg(
        F.sum("nb_snapshots").alias("nb_snapshots"),
        F.sum("nb_operational").alias("nb_operational"),
        F.sum("nb_empty").alias("nb_empty"),
        F.sum("nb_full").alias("nb_full"),
    )
    .select(
        "station_id",
        "nb_snapshots",
        F.round(F.try_divide("nb_empty", "nb_operational"), 3).alias("pct_time_empty"),
        F.round(F.try_divide("nb_full", "nb_operational"), 3).alias("pct_time_full"),
        F.round(1 - F.col("nb_operational") / F.col("nb_snapshots"), 3).alias("pct_time_out_of_service"),
    )
    .join(stations, "station_id", "left")
)
save_full(station_ranking, "station_ranking")

hourly = spark.read.format("delta").load(f"{GOLD_PATH}/station_hourly")
weighted = lambda column: F.sum(F.col(column) * F.col("nb_snapshots")) / F.sum("nb_snapshots")  # noqa: E731
hourly_profile = hourly.groupBy("local_hour").agg(
    F.round(weighted("avg_fill_rate"), 3).alias("avg_fill_rate"),
    F.round(weighted("pct_time_empty"), 3).alias("pct_station_empty"),
    F.round(weighted("pct_time_full"), 3).alias("pct_station_full"),
    F.countDistinct("local_date").alias("nb_days_observed"),
).orderBy("local_hour")
save_full(hourly_profile, "hourly_profile")

print(f"Gold écrit pour le {day} : station_hourly, station_daily, network_daily, station_ranking, hourly_profile")
spark.stop()
