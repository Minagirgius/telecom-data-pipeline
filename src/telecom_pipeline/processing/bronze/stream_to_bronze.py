from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("Telecom-Bronze-Network-Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "network_events")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 1000)
    .load()
)

bronze_df = df.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_json"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/opt/telecom/data/processed/bronze")
    .option(
        "checkpointLocation",
        "/opt/telecom/data/checkpoints/bronze_network_events"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()