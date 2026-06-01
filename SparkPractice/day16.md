## Number Of Units Per Nationality

Write a query that returns how many different apartment-type units (counted by distinct unit_id) 
are owned by people under 30, grouped by their nationality. Sort the results by the number of apartments in descending order.

### Table 1 - airbnb_hosts
age: bigint <br>
gender: text <br>
host_id: bigint <br>
nationality: text <br>

### Table 2 - airbnb_units
city:text <br>
country:text <br>
host_id:bigint <br>
n_bedrooms:bigint <br>
n_beds:bigint <br>
unit_id:text <br>
unit_type:text <br>

```python 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder \            # ✅ Fix 1 : no () after builder
    .appName("Airbnb Analysis") \
    .getOrCreate()                        # ✅ Fix 2 : removed empty config()

# ── Schema ────────────────────────────────────────────────────────────────
hosts_schema = T.StructType([
    T.StructField("host_id",     T.IntegerType(), True),
    T.StructField("nationality", T.StringType(),  True),
    T.StructField("age",         T.IntegerType(), True),
])

units_schema = T.StructType([
    T.StructField("unit_id",   T.IntegerType(), True),
    T.StructField("host_id",   T.IntegerType(), True),
    T.StructField("unit_type", T.StringType(),  True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
hosts_data = [
    (1, "American", 25),   # age < 30 ✅
    (2, "British",  28),   # age < 30 ✅
    (3, "Indian",   35),   # age >= 30 ❌
    (4, "American", 22),   # age < 30 ✅
    (5, "French",   29),   # age < 30 ✅
]

units_data = [
    (101, 1, "Apartment"),  # host 1 ✅
    (102, 1, "House"),      # not apartment ❌
    (103, 2, "Apartment"),  # host 2 ✅
    (104, 3, "Apartment"),  # host 3 age >= 30 ❌
    (105, 4, "Apartment"),  # host 4 ✅
    (106, 4, "Apartment"),  # host 4 ✅
    (107, 5, "Apartment"),  # host 5 ✅
]

df_hosts = spark.createDataFrame(hosts_data, schema=hosts_schema)
df_units = spark.createDataFrame(units_data, schema=units_schema)

df_hosts.show(truncate=False)
df_units.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

result_without_transform = df_hosts \
    .join(
        df_units,
        on  = "host_id",
        how = "inner"
    ) \
    .filter(
        (F.col("unit_type") == "Apartment") &  # ✅ Fix 4 : & not "and"
        (F.col("age") < 30)                    # ✅ Fix 3 : filter before groupBy
    ) \
    .groupBy("nationality") \                  # ✅ groupBy after filter
    .agg(
        F.countDistinct("unit_id")             # ✅ Fix 6,7 : countDistinct
         .alias("apartment_count")             #             not withColumn + col
    ) \
    .orderBy(
        F.col("apartment_count").desc()        # ✅ Fix 8 : .desc() inside orderBy
    )
    # ✅ Fix 9 : no dropDuplicates needed

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Join hosts with units ────────────────────────────────────────
def join_hosts_units(df):
    """Join airbnb_hosts with airbnb_units on host_id"""
    return df.join(
        df_units,
        on  = "host_id",
        how = "inner"
    )

# ── Step 2 : Filter apartments and young hosts ────────────────────────────
def filter_apartments_young_hosts(df):
    """
    Filter :
    unit_type = Apartment
    age < 30
    """
    return df.filter(
        (F.col("unit_type") == "Apartment") &  # ✅ & operator
        (F.col("age") < 30)                    # ✅ parentheses around each
    )

# ── Step 3 : Count distinct apartments per nationality ───────────────────
def count_apartments_by_nationality(df):
    """
    Group by nationality
    Count distinct unit_ids as apartment_count
    """
    return df \
        .groupBy("nationality") \
        .agg(
            F.countDistinct("unit_id")
             .alias("apartment_count")
        )

# ── Step 4 : Order by apartment_count desc ───────────────────────────────
def order_by_count_desc(df):
    """Order results by apartment count descending"""
    return df.orderBy(
        F.col("apartment_count").desc()        # ✅ .desc() inside orderBy
    )

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_hosts
    .transform(join_hosts_units)
    .transform(filter_apartments_young_hosts)
    .transform(count_apartments_by_nationality)
    .transform(order_by_count_desc)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)



```