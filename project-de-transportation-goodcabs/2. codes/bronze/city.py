from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

# Configuration

@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City Raw Data Processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").option("mergeSchema", "true").option("columnNameOfCorruptRecord","_corrupt_record").load(SOURCE_PATH)

    df = df.withColumn("file_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    
    return df
'''
# city_bronze_ingest.py
# Imperative Spark code to ingest City CSV data from S3 into a Bronze Delta table

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# -------------------------------------------------------------------
# 1. Spark Session
# -------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("City Bronze Ingestion")
    .getOrCreate()
)

# -------------------------------------------------------------------
# 2. S3 Configuration (USE IAM ROLE in PROD – this is for secrets-based auth)
# -------------------------------------------------------------------
spark.conf.set(
    "fs.s3a.access.key",
    dbutils.secrets.get(scope="aws-creds", key="access-key")
)

spark.conf.set(
    "fs.s3a.secret.key",
    dbutils.secrets.get(scope="aws-creds", key="secret-key")
)

spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# -------------------------------------------------------------------
# 3. Configuration
# -------------------------------------------------------------------
SOURCE_PATH = "s3a://my-bucket/transportation/city/"
TARGET_PATH = "s3a://my-bucket/delta/transportation/bronze/city"
TARGET_TABLE = "transportation.bronze.city"

# -------------------------------------------------------------------
# 4. Read Raw CSV from S3 (Bronze Ingest)
# -------------------------------------------------------------------
raw_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .option("mergeSchema", "true")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .load(SOURCE_PATH)
)

# -------------------------------------------------------------------
# 5. Add Metadata Columns
# -------------------------------------------------------------------
bronze_df = (
    raw_df
    .withColumn("file_name", col("_metadata.file_path"))
    .withColumn("ingest_datetime", current_timestamp())
)

# -------------------------------------------------------------------
# 6. Write Bronze Delta Table
# -------------------------------------------------------------------
(
    bronze_df.write
    .format("delta")
    .mode("append")
    .option("delta.enableChangeDataFeed", "true")
    .option("delta.autoOptimize.optimizeWrite", "true")
    .option("delta.autoOptimize.autoCompact", "true")
    .save(TARGET_PATH)
)

# -------------------------------------------------------------------
# 7. Register Table in Metastore
# -------------------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
    USING DELTA
    LOCATION '{TARGET_PATH}'
""")

# -------------------------------------------------------------------
# 8. Set Table Properties (Bronze Metadata)
# -------------------------------------------------------------------
spark.sql(f"""
    ALTER TABLE {TARGET_TABLE}
    SET TBLPROPERTIES (
        quality = 'bronze',
        layer = 'bronze',
        source_format = 'csv'
    )
""")

# -------------------------------------------------------------------
# 9. Success Log
# -------------------------------------------------------------------
print("✅ City Bronze ingestion completed successfully.")



'''