import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType
# ─────────────────────────────────────────────────────────────
# 1. Create Spark Session
# ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("KafkaStreamConsumer") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.port", "4040") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# ─────────────────────────────────────────────────────────────
# 2. Define the expected schema of the JSON messages
# ─────────────────────────────────────────────────────────────
schema = StructType() \
    .add("user_id", StringType()) \
    .add("item", StringType()) \
    .add("amount", DoubleType()) \
    .add("ts", StringType())  # Use TimestampType if needed

# ─────────────────────────────────────────────────────────────
# 3. Read Kafka stream
# ─────────────────────────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# ─────────────────────────────────────────────────────────────
# 4. Convert Kafka value from binary -> string -> JSON
# ─────────────────────────────────────────────────────────────
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .withColumn("data", from_json(col("json"), schema)) \
    .select("data.*")

# ─────────────────────────────────────────────────────────────
# 5. Optional: Filter messages with amount > 100
# ─────────────────────────────────────────────────────────────
df_filtered = df_parsed.filter(col("amount") > 100)

# ─────────────────────────────────────────────────────────────
# 6. Write stream to console
# ─────────────────────────────────────────────────────────────
query = df_filtered.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
