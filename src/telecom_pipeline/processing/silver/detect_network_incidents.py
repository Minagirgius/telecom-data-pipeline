
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE = "/opt/telecom/data"

SILVER_PATH = (
    f"{BASE}/processed/silver/network_events_streaming_v2"
)

INCIDENTS_PATH = (
    f"{BASE}/processed/incidents/network_incidents_streaming"
)

CHECKPOINT_PATH = (
    f"{BASE}/checkpoints/network_incidents_streaming"
)

spark = (
    SparkSession.builder
    .appName("Telecom-Incident-Detection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read the validated Silver data.
silver_schema = spark.read.parquet(SILVER_PATH).schema

events = (
    spark.readStream
    .schema(silver_schema)
    .format("parquet")
    .load(SILVER_PATH)
)

# Identify network problems using metrics.
detected = (
    events
    .withColumn(
        "detected_incident",
        (
            (F.col("network_status") == "DOWN")
            | (F.col("signal_strength_dbm") < -105)
            | (F.col("call_drop_rate") > 5)
            | (F.col("latency_ms") > 150)
            | (F.col("packet_loss_pct") > 3)
        )
    )
    .filter(F.col("detected_incident"))
    .withColumn(
        "incident_severity",
        F.when(
            F.col("network_status") == "DOWN",
            "CRITICAL"
        )
        .when(
            (F.col("packet_loss_pct") > 10)
            | (F.col("call_drop_rate") > 10),
            "HIGH"
        )
        .otherwise("MEDIUM")
    )
    .withColumn(
        "detection_time",
        F.current_timestamp()
    )
)

query = (
    detected.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", INCIDENTS_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print(
    "Telecom Incident Detection Streaming started.",
    flush=True
)

query.awaitTermination()
