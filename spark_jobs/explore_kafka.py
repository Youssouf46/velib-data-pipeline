"""Exploration (batch) du topic Kafka velib-station-status."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, MapType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "velib-station-status"

# Schéma du JSON envoyé par le producer : on choisit les champs utiles et leurs types
STATION_SCHEMA = StructType([
    StructField("station_id", LongType()),
    StructField("stationCode", StringType()),
    StructField("num_bikes_available", IntegerType()),
    StructField("num_bikes_available_types", ArrayType(MapType(StringType(), IntegerType()))),
    StructField("num_docks_available", IntegerType()),
    StructField("is_installed", IntegerType()),
    StructField("is_renting", IntegerType()),
    StructField("is_returning", IntegerType()),
    StructField("last_reported", LongType()),
    StructField("api_last_updated", LongType()),
    StructField("ingestion_time", LongType()),
])


def title(text):
    print(f"\n===== {text} =====")


spark = SparkSession.builder.appName("explore-kafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Lecture BATCH de tout le topic : du premier offset au dernier
raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

title("Schéma brut renvoyé par Kafka")
raw.printSchema()

# 2) Décodage : octets → texte → JSON → colonnes
parsed = (
    raw.select(
        F.col("key").cast("string").alias("key"),
        F.from_json(F.col("value").cast("string"), STATION_SCHEMA).alias("data"),
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    )
    .select("key", "data.*", "partition", "offset", "kafka_timestamp")
    .cache()  # réutilisé par plusieurs actions → on ne relit Kafka qu'une fois
)

title("Schéma après parsing")
parsed.printSchema()

title("Aperçu")
parsed.select(
    "station_id", "num_bikes_available", "num_bikes_available_types",
    "num_docks_available", "last_reported", "ingestion_time",
).show(5, truncate=False)

title("Volume")
total = parsed.count()
print(f"Messages : {total}")
parsed.groupBy("partition").count().orderBy("partition").show()

title("Stations et cycles d'ingestion")
parsed.agg(
    F.countDistinct("station_id").alias("stations"),
    F.countDistinct("ingestion_time").alias("cycles"),
    F.from_unixtime(F.min("ingestion_time")).alias("premier_cycle_utc"),
    F.from_unixtime(F.max("ingestion_time")).alias("dernier_cycle_utc"),
).show(truncate=False)

title("Doublons : même station + même last_reported")
nouvelles = parsed.select("station_id", "last_reported").distinct().count()
print(f"Mesures réellement nouvelles : {nouvelles} / {total} ({100 * nouvelles / total:.1f} %)")

title("État des stations au dernier cycle")
dernier_cycle = parsed.agg(F.max("ingestion_time")).first()[0]
(
    parsed.filter(F.col("ingestion_time") == dernier_cycle)
    .groupBy("is_installed", "is_renting", "is_returning")
    .count()
    .show()
)

spark.stop()
