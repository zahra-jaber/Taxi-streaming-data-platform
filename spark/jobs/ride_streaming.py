from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    count,
    sum,
    avg,
    to_date
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ==========================================================
# 1. SPARK SESSION
# ==========================================================

spark = (
    SparkSession.builder
    .appName("TaxiRideStreaming")

    # -----------------------------
    # Cassandra connection
    # -----------------------------
    .config(
        "spark.cassandra.connection.host",
        "cassandra"
    )
    .config(
        "spark.cassandra.connection.port",
        "9042"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================================
# 2. SCHEMA KAFKA EVENT
# ==========================================================

ride_schema = StructType([

    StructField(
        "ride_id",
        StringType(),
        True
    ),

    StructField(
        "driver_id",
        StringType(),
        True
    ),

    StructField(
        "customer_id",
        StringType(),
        True
    ),

    StructField(
        "city",
        StringType(),
        True
    ),

    StructField(
        "vehicle_type",
        StringType(),
        True
    ),

    StructField(
        "distance_km",
        DoubleType(),
        True
    ),

    StructField(
        "fare_mad",
        DoubleType(),
        True
    ),

    StructField(
        "status",
        StringType(),
        True
    ),

    StructField(
        "timestamp",
        StringType(),
        True
    )
])


# ==========================================================
# 3. BRONZE
# Kafka -> Raw JSON
# ==========================================================

kafka_df = (
    spark.readStream
    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )

    .option(
        "subscribe",
        "rides"
    )

    .option(
        "startingOffsets",
        "latest"
    )

    .load()
)


bronze = (
    kafka_df
    .select(
        col("value")
        .cast("string")
        .alias("raw_json")
    )
)


# ==========================================================
# 4. PARSE JSON
# ==========================================================

parsed = (
    bronze

    .select(
        from_json(
            col("raw_json"),
            ride_schema
        ).alias("data")
    )

    .select("data.*")
)


# ==========================================================
# 5. SILVER
# Clean + Validate + Deduplicate
# ==========================================================

silver = (

    parsed

    # Convert Kafka timestamp string
    # into Spark timestamp
    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("timestamp")
        )
    )

    # Timestamp must exist
    .filter(
        col("event_timestamp").isNotNull()
    )

    # Ride ID must exist
    .filter(
        col("ride_id").isNotNull()
    )

    # Valid distance
    .filter(
        col("distance_km") > 0
    )

    # Valid fare
    .filter(
        col("fare_mad") > 0
    )

    # Only completed rides
    .filter(
        col("status") == "completed"
    )

    # Watermark for streaming deduplication
    .withWatermark(
        "event_timestamp",
        "10 minutes"
    )

    # Remove duplicated rides
    .dropDuplicates(
        ["ride_id"]
    )
)


# ==========================================================
# 6. SILVER -> CASSANDRA
# rides_by_id
# ==========================================================

def write_silver_to_cassandra(batch_df, batch_id):

    print(
        f"\n===== SILVER BATCH {batch_id} ====="
    )

    if batch_df.isEmpty():
        print("Silver batch empty.")
        return

    silver_output = (
        batch_df

        .select(
            "ride_id",
            "driver_id",
            "customer_id",
            "city",
            "vehicle_type",
            "distance_km",
            "fare_mad",
            "status",
            "event_timestamp"
        )
    )

    (
        silver_output
        .write
        .format(
            "org.apache.spark.sql.cassandra"
        )
        .mode("append")
        .options(
            keyspace="taxi_streaming",
            table="rides_by_id"
        )
        .save()
    )

    print(
        f"Silver batch {batch_id} written to Cassandra."
    )


silver_query = (
    silver

    .writeStream

    .foreachBatch(
        write_silver_to_cassandra
    )

    .outputMode("append")

    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/silver_cassandra"
    )

    .start()
)


# ==========================================================
# 7. GOLD
# 5-MINUTE CITY AGGREGATION
# ==========================================================

gold_city = (

    silver

    .groupBy(

        window(
            col("event_timestamp"),
            "5 minutes"
        ),

        col("city")
    )

    .agg(

        count("*")
        .alias("total_rides"),

        sum("fare_mad")
        .alias("total_revenue"),

        avg("fare_mad")
        .alias("average_fare")
    )
    .withColumn("total_rides", col("total_rides").cast("int"))
)


# ==========================================================
# 8. GOLD -> CASSANDRA
# gold_city_metrics
# ==========================================================

def write_gold_to_cassandra(batch_df, batch_id):

    print(
        f"\n===== GOLD BATCH {batch_id} ====="
    )

    if batch_df.isEmpty():
        print("Gold batch empty.")
        return

    gold_output = (

        batch_df

        # Extract window information
        .withColumn(
            "window_start",
            col("window.start")
        )

        .withColumn(
            "window_end",
            col("window.end")
        )

        # Date used for Cassandra partition
        .withColumn(
            "event_date",
            to_date(
                col("window_start")
            )
        )

        .select(
            "city",
            "event_date",
            "window_start",
            "window_end",
            "total_rides",
            "total_revenue",
            "average_fare"
        )
    )

    (
        gold_output

        .write

        .format(
            "org.apache.spark.sql.cassandra"
        )

        .mode("append")

        .options(
            keyspace="taxi_streaming",
            table="gold_city_metrics"
        )

        .save()
    )

    print(
        f"Gold batch {batch_id} written to Cassandra."
    )


gold_query = (

    gold_city

    .writeStream

    .foreachBatch(
        write_gold_to_cassandra
    )

    .outputMode("append")

    .option(
        "checkpointLocation",
        "/opt/spark/checkpoints/gold_cassandra"
    )

    .start()
)


# ==========================================================
# 9. WAIT FOR STREAMING
# ==========================================================

print(
    "\n=============================================="
)

print(
    "Taxi Ride Streaming Pipeline Started"
)

print(
    "Kafka -> Bronze -> Silver -> Cassandra"
)

print(
    "Silver -> Gold -> Cassandra"
)

print(
    "==============================================\n"
)


spark.streams.awaitAnyTermination()