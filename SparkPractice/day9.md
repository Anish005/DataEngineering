## Number of Violations

You are given a dataset of health inspections that includes details about violations. Each row represents an inspection, and if an inspection resulted in a violation, the violation_id column will contain a value.


Count the total number of violations that occurred at 'Roxanne Cafe' for each year, based on the inspection date. Output the year and the corresponding number of violations in ascending order of the year.

Table -> sf_restaurant_health_violations

```python 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import date

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("SF Restaurant Health").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
restaurant_schema = T.StructType([
    T.StructField("violation_id",     T.LongType(),   True),
    T.StructField("business_name",    T.StringType(), True),
    T.StructField("inspection_date",  T.DateType(),   True),
    T.StructField("inspection_score", T.DoubleType(), True),
    T.StructField("violation_desc",   T.StringType(), True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
restaurant_data = [
    (1,  "Roxanne Cafe", date(2014, 3, 15), 85.0,  "Food temp violation"),
    (2,  "Roxanne Cafe", date(2014, 6, 20), 78.0,  "Pest control issue"),
    (3,  "Roxanne Cafe", date(2015, 1, 10), 90.0,  "Improper storage"),
    (4,  "Roxanne Cafe", date(2015, 8, 25), None,  "No violation"),        # inspection_score is null — filtered out
    (5,  "Roxanne Cafe", date(2016, 4, 5),  88.0,  "Hygiene violation"),
    (6,  "Roxanne Cafe", date(2016, 9, 18), 72.0,  "Equipment issue"),
    (7,  "Roxanne Cafe", date(2016, 11, 3), 91.0,  "Minor cleanliness"),
    (8,  "Burger Place", date(2014, 5, 12), 80.0,  "Food temp violation"), # different business — filtered out
    (9,  "Burger Place", date(2015, 7, 22), 75.0,  "Pest control issue"),  # different business — filtered out
    (10, "Roxanne Cafe", date(2017, 2, 14), None,  "No violation"),        # inspection_score is null — filtered out
]

# ── Create DataFrame ──────────────────────────────────────────────────────
df_restaurant = spark.createDataFrame(restaurant_data, schema=restaurant_schema)

df_restaurant.printSchema()
df_restaurant.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

result_without_transform = df_restaurant \
    .filter(
        (F.col("business_name") == "Roxanne Cafe") &          # WHERE business_name = 'Roxanne Cafe'
        (F.col("inspection_score").isNotNull())               # AND inspection_score IS NOT NULL
    ) \
    .withColumn("year", F.year(F.col("inspection_date"))) \   # EXTRACT(year from date(inspection_date))
    .groupBy("year") \                                        # GROUP BY year
    .agg(
        F.count("violation_id").alias("total_violations")     # COUNT(violation_id)
    ) \
    .orderBy(F.col("year").asc())                             # ORDER BY year ASC

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Filter WHERE business_name and inspection_score ─────────────
def filter_roxanne_cafe(df):
    return df.filter(
        (F.col("business_name") == "Roxanne Cafe") &
        (F.col("inspection_score").isNotNull())
    )

# ── Step 2 : Extract year from inspection_date ───────────────────────────
def extract_inspection_year(df):
    return df.withColumn(
        "year", F.year(F.col("inspection_date"))
    )

# ── Step 3 : Count violations per year ───────────────────────────────────
def count_violations_by_year(df):
    return df \
        .groupBy("year") \
        .agg(
            F.count("violation_id").alias("total_violations")
        )

# ── Step 4 : Order by year asc ───────────────────────────────────────────
def order_by_year_asc(df):
    return df.orderBy(F.col("year").asc())

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_restaurant
    .transform(filter_roxanne_cafe)
    .transform(extract_inspection_year)
    .transform(count_violations_by_year)
    .transform(order_by_year_asc)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+----+----------------+
|year|total_violations|
+----+----------------+
|2014|2               |
|2015|1               |
|2016|3               |
+----+----------------+

── With Transform ──
+----+----------------+
|year|total_violations|
+----+----------------+
|2014|2               |
|2015|1               |
|2016|3               |
+----+----------------+


```