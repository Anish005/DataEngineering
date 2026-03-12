
## Workers by Department Since April

Find the number of workers by department who joined on or after April 1, 2014.


Output the department name along with the corresponding number of workers.


Sort the results based on the number of workers in descending order.




```python 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import date

# ── Spark Session ────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Worker Department").getOrCreate()

# ── Schema ───────────────────────────────────────────────────────────────
worker_schema = T.StructType([
    T.StructField("worker_id",    T.LongType(),   True),
    T.StructField("first_name",   T.StringType(), True),
    T.StructField("last_name",    T.StringType(), True),
    T.StructField("salary",       T.LongType(),   True),
    T.StructField("joining_date", T.DateType(),   True),
    T.StructField("department",   T.StringType(), True),
])

# ── Sample Data ──────────────────────────────────────────────────────────
worker_data = [
    (1,  "John",    "Smith",   50000, date(2013, 2, 15), "HR"),
    (2,  "Jane",    "Doe",     60000, date(2014, 4, 1),  "Finance"),
    (3,  "Bob",     "Brown",   55000, date(2014, 6, 10), "HR"),
    (4,  "Alice",   "Johnson", 70000, date(2015, 3, 20), "IT"),
    (5,  "Charlie", "Wilson",  65000, date(2013, 11, 5), "Finance"),
    (6,  "Diana",   "Taylor",  72000, date(2016, 1, 25), "IT"),
    (7,  "Eve",     "Martinez",58000, date(2014, 5, 18), "HR"),
    (8,  "Frank",   "Garcia",  68000, date(2017, 8, 30), "IT"),
    (9,  "Grace",   "Lee",     53000, date(2013, 12, 1), "Finance"),
    (10, "Henry",   "Walker",  75000, date(2015, 7, 14), "IT"),
]

# ── Create DataFrame ─────────────────────────────────────────────────────
df_worker = spark.createDataFrame(worker_data, schema=worker_schema)

df_worker.printSchema()
df_worker.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

result_without_transform = df_worker \
    .filter(F.col("joining_date") >= F.lit(date(2014, 4, 1))) \   # WHERE date(joining_date) >= "2014-04-01"
    .groupBy("department") \                                       # GROUP BY department
    .agg(
        F.count("worker_id").alias("num_workers")                  # COUNT(worker_id)
    ) \
    .orderBy(F.col("num_workers").desc())                          # ORDER BY num_workers DESC

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Filter WHERE joining_date >= "2014-04-01" ───────────────────
def filter_by_joining_date(df):
    return df.filter(
        F.col("joining_date") >= F.lit(date(2014, 4, 1))
    )

# ── Step 2 : Count workers per department ────────────────────────────────
def count_workers_by_department(df):
    return df \
        .groupBy("department") \
        .agg(
            F.count("worker_id").alias("num_workers")
        )

# ── Step 3 : Order by num_workers desc ───────────────────────────────────
def order_by_num_workers_desc(df):
    return df.orderBy(F.col("num_workers").desc())

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_worker
    .transform(filter_by_joining_date)
    .transform(count_workers_by_department)
    .transform(order_by_num_workers_desc)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+----------+-----------+
|department|num_workers|
+----------+-----------+
|IT        |4          |
|HR        |2          |
|Finance   |1          |
+----------+-----------+

── With Transform ──
+----------+-----------+
|department|num_workers|
+----------+-----------+
|IT        |4          |
|HR        |2          |
|Finance   |1          |
+----------+-----------+






```