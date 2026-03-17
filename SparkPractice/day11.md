## New Products
Calculate the net change in the number of products launched by companies in 2020 compared to 2019. Your output should include the company names and the net difference.
(Net difference = Number of products launched in 2020 - The number launched in 2019.)

Table  - car_launches

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Car Launches Net Difference").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
car_schema = T.StructType([
    T.StructField("product_name", T.StringType(),  True),
    T.StructField("year",         T.IntegerType(), True),
    T.StructField("company_name", T.StringType(),  True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
car_data = [
    ("Model A", 2019, "Toyota"),
    ("Model B", 2019, "Honda"),
    ("Model C", 2019, "Ford"),
    ("Model D", 2019, "Toyota"),
    ("Model E", 2020, "Toyota"),
    ("Model F", 2020, "Honda"),
    ("Model G", 2020, "Honda"),
    ("Model H", 2020, "Ford"),
    ("Model I", 2020, "BMW"),
    ("Model J", 2021, "Toyota"),   # ── filtered out by isin(2019, 2020)
]

# ── Create DataFrame ──────────────────────────────────────────────────────
df_car = spark.createDataFrame(car_data, schema=car_schema)

df_car.printSchema()
df_car.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

result_without_transform = df_car \
    .filter(F.col("year").isin(2019, 2020)) \               # WHERE year IN (2019, 2020)
    .groupBy("company_name") \                              # GROUP BY company_name
    .agg(
        (
            F.sum(F.when(F.col("year") == 2020, 1).otherwise(0)) -   # SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END)
            F.sum(F.when(F.col("year") == 2019, 1).otherwise(0))     # SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END)
        ).alias("net_difference")
    ) \
    .orderBy(F.col("company_name").asc())                   # ORDER BY company_name

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Filter WHERE year IN (2019, 2020) ────────────────────────────
def filter_relevant_years(df):
    return df.filter(F.col("year").isin(2019, 2020))

# ── Step 2 : Conditional SUM aggregation per company ─────────────────────
def calc_net_difference(df):
    return df \
        .groupBy("company_name") \
        .agg(
            (
                F.sum(F.when(F.col("year") == 2020, 1).otherwise(0)) -
                F.sum(F.when(F.col("year") == 2019, 1).otherwise(0))
            ).alias("net_difference")
        )

# ── Step 3 : Order by company_name asc ───────────────────────────────────
def order_by_company(df):
    return df.orderBy(F.col("company_name").asc())

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_car
    .transform(filter_relevant_years)
    .transform(calc_net_difference)
    .transform(order_by_company)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+------------+--------------+
|company_name|net_difference|
+------------+--------------+
|BMW         |1             |
|Ford        |1             |
|Honda       |1             |
|Toyota      |-1            |
+------------+--------------+

── With Transform ──
+------------+--------------+
|company_name|net_difference|
+------------+--------------+
|BMW         |1             |
|Ford        |1             |
|Honda       |1             |
|Toyota      |-1            |
+------------+--------------+
```