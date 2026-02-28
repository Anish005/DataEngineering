from pyspark import pipelines as dp
import pyspark.sql.functions as F

SOURCE_PATH = "s3://goodcabs/data-store/trips"


# https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema
# streaming table definition using the @dp.table decorator, which allows us to define a streaming table that will be continuously updated as new data arrives in the source path. The table is configured with various properties to optimize performance and handle schema changes effectively.
@dp.table(
    name="transportation.bronze.trips",
    comment="Streaming ingestion of raw orders data with Auto Loader",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)
def orders_bronze():
    df = (
        # because of this it would only process the new files that are added to the source path, and not reprocess the old files
        # we want to process each file in the new day
        #Auto Loader will automatically infer the schema of the data and handle any schema changes that may occur over time. 
        # This means that if new columns are added to the CSV files, 
        # Auto Loader will automatically detect and include them in the streaming DataFrame without requiring manual intervention.
        # This the feature we get in the databricks when we use the cloudFiles format, it will automatically infer the schema of the data and handle any schema changes that may occur over time.
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue") # if the data changes while streaming it would create a new rescue column 
        .option("cloudFiles.maxFilesPerTrigger", 100) # per trigger processing of the batch 
        .load(SOURCE_PATH)
    )

     # Rename the problematic column
    df = df.withColumnRenamed(
        "distance_travelled(km)",
        "distance_travelled_km"
    )
    # meta data features
    df = df.withColumn("file_name", F.col("_metadata.file_path")).withColumn("ingest_datetime", F.current_timestamp())

    return df
'''
This is a Delta Live Tables streaming pipeline that ingests CSV data from S3 using Auto Loader. It processes only new files, handles 
schema evolution using rescue mode, adds metadata columns for lineage, and writes the data into a Bronze Delta table with optimization 
and CDC enabled.”


'''