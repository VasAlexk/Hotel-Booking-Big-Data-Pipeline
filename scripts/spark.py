# Databricks notebook source
# === Εγκατάσταση βιβλιοθηκών ===
%pip install kafka-python



# COMMAND ----------

spark.conf.set("spark.mongodb.write.connection.uri", 
    "mongodb+srv://<USERNAME>:<PASSWORD>@cluster0.n6928z1.mongodb.net/?retryWrites=true&w=majority")

# COMMAND ----------

# === 2. Imports για Spark ===
from pyspark.sql.types import *
from pyspark.sql.functions import *

# === 3. Schema για τα Kafka JSON ===
schema = StructType([
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("booking_id", StringType()),
    StructField("check_in_date", StringType()),
    StructField("check_out_date", StringType()),
    StructField("hotel_id", StringType()),
    StructField("item_id", StringType()),
    StructField("location", StringType()),
    StructField("num_guests", StringType()),
    StructField("page_url", StringType()),
    StructField("payment_method", StringType()),
    StructField("price", StringType()),
    StructField("room_type", StringType()),
    StructField("total_price", DoubleType())
])



# COMMAND ----------

# === Kafka Input from NEW topic ===
raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "150.140.142.67:9094")
    .option("subscribe", "SDMD-1097464-final")  # νέο topic
    .load()
)

# COMMAND ----------

# === 5. JSON parsing ===
parsed_df = (
    raw_kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", to_timestamp("timestamp"))
)

# COMMAND ----------

# === 6. Κανονικοποίηση τύπων events ===
parsed_df = parsed_df.withColumn(
    "event_type",
    when(col("event_type") == "search_hotels", "search")
    .when(col("event_type") == "complete_booking", "booking")  
    .otherwise(col("event_type"))
)


# COMMAND ----------

# === 7. Watermark για aggregation ===
parsed_df = parsed_df.withWatermark("timestamp", "30 minutes")


# COMMAND ----------

# === 8. Aggregation ===
from pyspark.sql.functions import coalesce, lit
agg_df = (
    parsed_df
    .filter(col("event_type").isin("search", "booking"))
    .filter(col("location").isNotNull())
    .groupBy(window("timestamp", "50 minutes"), col("location"))
    .agg(
        count(when(col("event_type") == "search", True)).alias("search_volume"),
        count(when(col("event_type") == "booking", True)).alias("bookings_volume"),
        sum(when(col("event_type") == "booking", col("total_price"))).alias("sales_volume")
    )
    .select(
        col("window.start").alias("time"),
        col("location").alias("destination_name"),
        col("search_volume"),
        col("bookings_volume"),
        col("sales_volume")
    )
    .select(
        "time",
        "destination_name",
        coalesce("search_volume", lit(0)).alias("search_volume"),
        coalesce("bookings_volume", lit(0)).alias("bookings_volume"),
        coalesce("sales_volume", lit(0.0)).alias("sales_volume")
    )
)


# COMMAND ----------


agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 minutes") \
    .start()

# COMMAND ----------

# === 10. Write functions προς MongoDB
def write_raw_to_mongo(batch_df, batch_id):
    print(f"RAW batch {batch_id}")
    batch_df.write.format("mongodb") \
        .option("database", "bigdata") \
        .option("collection", "raw_events") \
        .mode("append") \
        .save()

def write_agg_to_mongo(batch_df, batch_id):
    print(f"AGGREGATED batch {batch_id}")
    batch_df.write.format("mongodb") \
        .option("database", "bigdata") \
        .option("collection", "city_stats") \
        .mode("append") \
        .save()


# COMMAND ----------

# === 11. Write Streams προς MongoDB με trigger κάθε 10 λεπτά ===

# RAW stream
parsed_df.writeStream \
    .foreachBatch(write_raw_to_mongo) \
    .outputMode("append") \
    .trigger(processingTime="10 minutes") \
    .option("checkpointLocation", "/dbfs/checkpoints/raw") \
    .start()

# AGGREGATED stream
agg_df.writeStream \
    .foreachBatch(write_agg_to_mongo) \
    .outputMode("update") \
    .trigger(processingTime="10 minutes") \
    .option("checkpointLocation", "/dbfs/checkpoints/agg") \
    .start()
