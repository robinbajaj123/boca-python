import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, struct, to_json, concat

# ─────────────────────────────────────────────────────────────
# 1. Required Spark config for localhost
# ─────────────────────────────────────────────────────────────
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# ─────────────────────────────────────────────────────────────
# 2. Create Spark Session
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("KafkaStreamProducer") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────
# 3. Generate synthetic stream
# ─────────────────────────────────────────────────────────────
raw_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 2) \
    .load()

# ─────────────────────────────────────────────────────────────
# 4. Enrich with fake fields
# ─────────────────────────────────────────────────────────────
kafka_ready_df = raw_df.withColumn("user_id", concat(lit("user_"), col("value").cast("string"))) \
    .withColumn("item", lit("book")) \
    .withColumn("amount", (col("value") % 300).cast("double") + 50.0)

# ─────────────────────────────────────────────────────────────
# 5. Prepare Kafka-compatible key/value
# ─────────────────────────────────────────────────────────────
kafka_ready_df = kafka_ready_df \
    .withColumn("key", col("user_id").cast("string")) \
    .withColumn("value", to_json(struct("user_id", "item", "amount"))) \
    .select("key", "value")

# ─────────────────────────────────────────────────────────────
# 6. Stream directly to Kafka (no micro-batching needed)
# ─────────────────────────────────────────────────────────────
query = kafka_ready_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "orders") \
    .option("checkpointLocation", "/tmp/spark-kafka-checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
