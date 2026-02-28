from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.view(
    name="trips_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
# validations are given , the errors would be logged
@dp.expect("valid_date", "year(business_date) >= 2020")
@dp.expect("valid_driver_rating", "driver_rating BETWEEN 1 AND 10")
@dp.expect("valid_passenger_rating", "passenger_rating BETWEEN 1 AND 10")
# you can check the documents for different types of expectations, 
# you can also create your own custom expectations using the @dp.expect decorator and defining a function that implements the validation logic.
# @dp.expect_or_drop() - it would drop the data that are not statisfying
# @dp.expect_or_fail() - it would fail the pipeline
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips") # only read the updated records as change data feed is enabled here
    df_silver = df_bronze.withColumn("passenger_type", F.lower("passenger_type"))
    # here we are changing the column names but we can do other transformations as per the business use case need

    df_silver = df_bronze.select(
        F.col("trip_id").alias("id"),
        F.col("date").cast("date").alias("business_date"),
        F.col("city_id").alias("city_id"),
        F.col("passenger_type").alias("passenger_category"),
        F.col("distance_travelled_km").alias("distance_kms"),
        F.col("fare_amount").alias("sales_amt"),
        F.col("passenger_rating").alias("passenger_rating"),
        F.col("driver_rating").alias("driver_rating"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp"),
    )

    df_silver = df_silver.withColumn( # previous ingest timestamp would be bronze timestamp , so we need to update it here 
        "silver_processed_timestamp", F.current_timestamp()
    )
    return df_silver


dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    # if for the key you find a record then update else insert
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_processed_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
