from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    year,
    month,
    dayofmonth
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


# ============================================================
# 1. Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("RealTimeFlightPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# 2. Read flight data from Kafka
# ============================================================

flight_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )
    .option(
        "subscribe",
        "flights"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# ============================================================
# 3. Convert Kafka value from bytes to string
# ============================================================

flight_data = flight_stream.selectExpr(
    "CAST(value AS STRING) AS flight_json"
)


# ============================================================
# 4. Define flight JSON schema
# ============================================================

flight_schema = StructType([

    StructField(
        "icao24",
        StringType(),
        True
    ),

    StructField(
        "callsign",
        StringType(),
        True
    ),

    StructField(
        "country",
        StringType(),
        True
    ),

    StructField(
        "longitude",
        DoubleType(),
        True
    ),

    StructField(
        "latitude",
        DoubleType(),
        True
    ),

    StructField(
        "altitude",
        DoubleType(),
        True
    ),

    StructField(
        "velocity",
        DoubleType(),
        True
    )
])


# ============================================================
# 5. Parse JSON
# ============================================================

parsed_flights = flight_data.select(

    from_json(
        col("flight_json"),
        flight_schema
    ).alias("data")

)


# ============================================================
# 6. Extract flight columns
# ============================================================

flights = parsed_flights.select(
    "data.*"
)


# ============================================================
# 7. Add ONE processing timestamp
#
# Same timestamp will be used by:
#
# Spark
#    ↓
# Elasticsearch
#    ↓
# S3
#
# ============================================================

flights = flights.withColumn(
    "processed_at",
    current_timestamp()
)


# ============================================================
# 8. Write each Spark batch to Elasticsearch
# ============================================================

def write_to_elasticsearch(batch_df, batch_id):

    # --------------------------------------------------------
    # Check empty batch
    # --------------------------------------------------------

    if batch_df.isEmpty():

        print(
            f"Elasticsearch | "
            f"Batch {batch_id} | "
            f"No records"
        )

        return


    # ========================================================
    # Function executed for EACH Spark partition
    # ========================================================

    def write_partition(rows):

        # ----------------------------------------------------
        # Create Elasticsearch connection
        # ----------------------------------------------------

        es = Elasticsearch(
            "http://localhost:9200"
        )

        actions = []

        partition_records = 0


        # ====================================================
        # Read rows belonging to this partition
        # ====================================================

        for row in rows:

            document = row.asDict()

            partition_records += 1


            # ------------------------------------------------
            # Create geo_point location
            # ------------------------------------------------

            if (
                document.get("latitude") is not None
                and document.get("longitude") is not None
            ):

                document["location"] = {

                    "lat": document["latitude"],

                    "lon": document["longitude"]

                }


            # ------------------------------------------------
            # Skip records without aircraft ID
            # ------------------------------------------------

            if document.get("icao24") is None:

                continue


            # ------------------------------------------------
            # Create Elasticsearch document
            #
            # Same aircraft
            #      ↓
            # Same _id
            #      ↓
            # Latest state replaces old state
            #
            # ------------------------------------------------

            actions.append({

                "_index": "flights",

                "_id": document["icao24"],

                "_source": document

            })


        # ====================================================
        # Bulk write this partition
        # ====================================================

        if actions:

            try:

                success, errors = bulk(

                    es,

                    actions,

                    raise_on_error=False

                )

                print(

                    f"Elasticsearch | "
                    f"Batch {batch_id} | "
                    f"Partition Records: {partition_records} | "
                    f"Indexed: {success} | "
                    f"Failed: {len(errors)}"

                )

            except Exception as e:

                print(

                    f"Elasticsearch | "
                    f"Batch {batch_id} | "
                    f"Bulk write error: {e}"

                )

        else:

            print(

                f"Elasticsearch | "
                f"Batch {batch_id} | "
                f"No valid records in partition"

            )


        # ----------------------------------------------------
        # Close Elasticsearch connection
        # ----------------------------------------------------

        es.close()


    # ========================================================
    # Execute write_partition for every Spark partition
    # ========================================================

    batch_df.foreachPartition(
        write_partition
    )


# ============================================================
# 9. Write each Spark batch to S3
# ============================================================

def write_to_s3(batch_df, batch_id):

    # --------------------------------------------------------
    # Check empty batch
    # --------------------------------------------------------

    if batch_df.isEmpty():

        print(
            f"S3 | "
            f"Batch {batch_id} | "
            f"No records"
        )

        return


    # ========================================================
    # Create date partition columns
    # ========================================================

    historical_df = (

        batch_df

        .withColumn(
            "year",
            year(col("processed_at"))
        )

        .withColumn(
            "month",
            month(col("processed_at"))
        )

        .withColumn(
            "day",
            dayofmonth(col("processed_at"))
        )

    )


    # ========================================================
    # Write historical data to S3
    # ========================================================

    (
        historical_df

        .write

        .mode("append")

        .partitionBy(
            "year",
            "month",
            "day"
        )

        .parquet(
            "s3a://chetan-realtime-flight-data-2026/flights/"
        )
    )


    print(

        f"S3 | "
        f"Batch {batch_id} | "
        f"Historical data written to S3"

    )


# ============================================================
# 10. Start Elasticsearch Streaming Query
# ============================================================

elasticsearch_query = (

    flights

    .writeStream

    .foreachBatch(
        write_to_elasticsearch
    )

    .outputMode(
        "append"
    )

    .option(
        "checkpointLocation",
        "checkpoints/elasticsearch"
    )

    .trigger(
        processingTime="5 seconds"
    )

    .start()

)


# ============================================================
# 11. Start S3 Streaming Query
# ============================================================

s3_query = (

    flights

    .writeStream

    .foreachBatch(
        write_to_s3
    )

    .outputMode(
        "append"
    )

    .option(
        "checkpointLocation",
        "checkpoints/s3_partitioned"
    )

    .trigger(
        processingTime="30 seconds"
    )

    .start()

)


# ============================================================
# 12. Keep application running
# ============================================================

elasticsearch_query.awaitTermination()

s3_query.awaitTermination()