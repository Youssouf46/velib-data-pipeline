"""Batch Silver → Gold : indicateurs prêts pour l'analyse et le dashboard."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_STATUS_PATH = "/opt/spark/data/silver/station_status"
GOLD_PATH = "/opt/spark/data/gold"
LOCAL_TIMEZONE = "Europe/Paris"
CYCLES_PER_DAY = 24 * 60  # 1 relevé par minute


def save(df, name, partition_by=None):
    """Écrit une table Gold en Delta (recalcul complet, idempotent)."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(f"{GOLD_PATH}/{name}")


spark = SparkSession.builder.appName("gold-velib").getOrCreate()
silver = spark.read.format("delta").load(SILVER_STATUS_PATH)

# Relevés des stations EN SERVICE, en heure de Paris, avec les indicateurs de base
operational = (
    silver.filter("is_operational")
    .withColumn("local_time", F.from_utc_timestamp("snapshot_at", LOCAL_TIMEZONE))
    .withColumn("local_date", F.to_date("local_time"))
    .withColumn("local_hour", F.hour("local_time"))
    .withColumn("fill_rate", F.col("nb_bikes") / (F.col("nb_bikes") + F.col("nb_free_docks")))
    .withColumn("is_empty", (F.col("nb_bikes") == 0).cast("int"))
    .withColumn("is_full", (F.col("nb_free_docks") == 0).cast("int"))
    .cache()  # réutilisé par 2 agrégations
)

# 1) Chaque station, heure par heure
station_hourly = operational.groupBy("station_id", "name", "local_date", "local_hour").agg(
    F.count("*").alias("nb_snapshots"),
    F.round(F.avg("nb_bikes"), 2).alias("avg_bikes"),
    F.round(F.avg("nb_ebike"), 2).alias("avg_ebikes"),          # avg() ignore les NULL
    F.round(F.avg("fill_rate"), 3).alias("avg_fill_rate"),
    F.round(F.avg("is_empty"), 3).alias("pct_time_empty"),
    F.round(F.avg("is_full"), 3).alias("pct_time_full"),
)
save(station_hourly, "station_hourly", partition_by="local_date")

# 2) Profil horaire du réseau : les heures de pointe
hourly_profile = operational.groupBy("local_hour").agg(
    F.round(F.avg("fill_rate"), 3).alias("avg_fill_rate"),
    F.round(F.avg("is_empty"), 3).alias("pct_station_empty"),
    F.round(F.avg("is_full"), 3).alias("pct_station_full"),
    F.countDistinct("local_date").alias("nb_days_observed"),
).orderBy("local_hour")
save(hourly_profile.coalesce(1), "hourly_profile")

# 3) Classement des stations : souvent vides, pleines, en panne (sur TOUS les relevés)
station_ranking = silver.groupBy("station_id", "name", "lat", "lon", "capacity").agg(
    F.count("*").alias("nb_snapshots"),
    F.round(F.avg(F.when(F.col("is_operational"), (F.col("nb_bikes") == 0).cast("int"))), 3)
    .alias("pct_time_empty"),
    F.round(F.avg(F.when(F.col("is_operational"), (F.col("nb_free_docks") == 0).cast("int"))), 3)
    .alias("pct_time_full"),
    F.round(F.avg((~F.col("is_operational")).cast("int")), 3).alias("pct_time_out_of_service"),
)
save(station_ranking.coalesce(1), "station_ranking")

# 4) Santé du réseau et complétude des données, jour par jour
network_daily = (
    silver.withColumn("local_date", F.to_date(F.from_utc_timestamp("snapshot_at", LOCAL_TIMEZONE)))
    .groupBy("local_date")
    .agg(
        F.countDistinct("snapshot_at").alias("nb_cycles"),
        F.round(F.countDistinct("snapshot_at") / CYCLES_PER_DAY, 3).alias("data_coverage"),
        F.countDistinct(F.when(F.col("is_operational"), F.col("station_id"))).alias("nb_stations_operational"),
        F.countDistinct(F.when(~F.col("is_operational"), F.col("station_id"))).alias("nb_stations_out_of_service"),
        F.sum("nb_bikes").alias("sum_bikes"),
    )
    .withColumn("avg_bikes_in_network", F.round(F.col("sum_bikes") / F.col("nb_cycles")).cast("int"))
    .drop("sum_bikes")
    .orderBy("local_date")
)
save(network_daily.coalesce(1), "network_daily")

print("Gold écrit : station_hourly, hourly_profile, station_ranking, network_daily")
spark.stop()
