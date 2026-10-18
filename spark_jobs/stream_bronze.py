"""Streaming Kafka → Bronze (Delta Lake) : copie brute et continue du topic."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "velib-station-status"
BRONZE_PATH = "/opt/spark/data/bronze/station_status"
CHECKPOINT_PATH = "/opt/spark/data/checkpoints/bronze_station_status"

spark = SparkSession.builder.appName("bronze-station-status").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Source : le topic Kafka, lu en continu
kafka_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")   # utilisé au 1er lancement seulement, ensuite c'est le checkpoint qui décide
    .option("maxOffsetsPerTrigger", 300000)  # au plus 300 000 messages par micro-batch
    .load()
)

# 2) Bronze : le message brut + ses métadonnées Kafka, sans transformation métier
bronze = kafka_stream.select(
    F.col("key").cast("string").alias("station_key"),
    F.col("value").cast("string").alias("raw_json"),
    "topic",
    "partition",
    "offset",
    F.col("timestamp").alias("kafka_timestamp"),
    F.current_timestamp().alias("bronze_ingested_at"),
    F.to_date("timestamp").alias("ingestion_date"),
)

# 3) Écriture continue en Delta Lake
query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .partitionBy("ingestion_date")
    .trigger(processingTime="1 minute")
    .start(BRONZE_PATH)
)

# 4) Suivi : afficher chaque micro-batch terminé
last_batch_id = -1
while query.isActive:
    progress = query.lastProgress
    if progress and progress["batchId"] != last_batch_id:
        last_batch_id = progress["batchId"]
        print(f"micro-batch {last_batch_id} : {progress['numInputRows']} messages écrits", flush=True)
    query.awaitTermination(10)
