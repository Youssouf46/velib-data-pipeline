"""Batch Bronze → Silver : relevés de stations nettoyés, typés et enrichis."""
import json
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType,
)

STATION_INFORMATION_URL = (
    "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json"
)
BRONZE_PATH = "/opt/spark/data/bronze/station_status"
SILVER_STATUS_PATH = "/opt/spark/data/silver/station_status"
SILVER_INFO_PATH = "/opt/spark/data/silver/station_information"

# Schéma appliqué à la LECTURE (schema-on-read) : Bronze reste brut
STATUS_SCHEMA = StructType([
    StructField("station_id", LongType()),
    StructField("stationCode", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_bikes_available_types", ArrayType(MapType(StringType(), IntegerType()))),
    StructField("num_docks_available", IntegerType()),
    StructField("is_installed", IntegerType()),
    StructField("is_renting", IntegerType()),
    StructField("is_returning", IntegerType()),
    StructField("last_reported", LongType()),
    StructField("ingestion_time", LongType()),
])

INFO_SCHEMA = StructType([
    StructField("station_id", LongType()),
    StructField("stationCode", StringType()),
    StructField("name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("capacity", IntegerType()),
])


def fetch_station_information(spark):
    """Télécharge le référentiel des stations (nom, GPS, capacité) et le renvoie en DataFrame."""
    request = urllib.request.Request(STATION_INFORMATION_URL, headers={"User-Agent": "velib-data-pipeline"})
    with urllib.request.urlopen(request, timeout=30) as response:
        stations = json.load(response)["data"]["stations"]
    rows = [{field.name: station.get(field.name) for field in INFO_SCHEMA.fields} for station in stations]
    return spark.createDataFrame(rows, INFO_SCHEMA).withColumnRenamed("stationCode", "station_code")


def bikes_of_type(bike_type):
    """Somme d'un type de vélo dans [{"mechanical": 5}, {"ebike": 1}]."""
    return F.aggregate(
        "num_bikes_available_types",
        F.lit(0),
        lambda total, bike_map: total + F.coalesce(bike_map[bike_type], F.lit(0)),
    )


spark = SparkSession.builder.appName("silver-station-status").getOrCreate()

# 1) Référentiel des stations → table Silver dédiée
station_information = fetch_station_information(spark)
station_information.write.format("delta").mode("overwrite").save(SILVER_INFO_PATH)

# 2) Bronze → parsing du JSON brut
bronze = spark.read.format("delta").load(BRONZE_PATH)
parsed = (
    bronze.select(F.from_json("raw_json", STATUS_SCHEMA).alias("s"), "partition", "offset")
    .select("s.*", "partition", "offset")
)

# Le détail mécanique/électrique est parfois vide ([{}, {}]) : on le saura grâce à ce test
has_bike_breakdown = F.exists("num_bikes_available_types", lambda bike_map: F.size(bike_map) > 0)

# 3) Nettoyage, typage et enrichissement
silver = (
    parsed
    .dropDuplicates(["station_id", "ingestion_time"])          # doublons TECHNIQUES uniquement
    .select(
        "station_id",
        F.col("stationCode").alias("station_code"),
        F.col("num_bikes_available").alias("nb_bikes"),
        F.when(has_bike_breakdown, bikes_of_type("mechanical")).alias("nb_mechanical"),  # NULL si inconnu
        F.when(has_bike_breakdown, bikes_of_type("ebike")).alias("nb_ebike"),
        F.col("num_docks_available").alias("nb_free_docks"),
        (F.col("is_installed") == 1).alias("is_installed"),
        (F.col("is_renting") == 1).alias("is_renting"),
        (F.col("is_returning") == 1).alias("is_returning"),
        ((F.col("is_installed") == 1) & (F.col("is_renting") == 1) & (F.col("is_returning") == 1))
        .alias("is_operational"),
        F.timestamp_seconds("last_reported").alias("last_reported_at"),
        F.timestamp_seconds("ingestion_time").alias("snapshot_at"),
        F.to_date(F.timestamp_seconds("ingestion_time")).alias("snapshot_date"),
        "partition",                                           # traçabilité vers Kafka / Bronze
        "offset",
    )
    .join(
        F.broadcast(station_information.select("station_id", "name", "lat", "lon", "capacity")),
        "station_id",
        "left",
    )
)

# 4) Écriture : recalcul complet, partitionné par jour
silver.write.format("delta").mode("overwrite").partitionBy("snapshot_date").save(SILVER_STATUS_PATH)

print(f"Silver écrit : {spark.read.format('delta').load(SILVER_STATUS_PATH).count()} relevés")
spark.stop()
