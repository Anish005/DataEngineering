## Finding User Purchases

Identify returning active users by finding users who made a second purchase within 1 to 7 days after their first purchase. Ignore same-day purchases. Output a list of these user_ids.

### Table - amazon_transactions

id:bigint <br>
user_id:bigint <br>
item:text <br>
created_at:date <br>
revenue:bigint <br>

```python 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import datetime

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Amazon Repeat Buyers").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
transactions_schema = T.StructType([
    T.StructField("user_id",    T.LongType(),      True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("amount",     T.DoubleType(),    True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
transactions_data = [
    (1, datetime(2024, 1, 1),  100.0),
    (1, datetime(2024, 1, 5),  200.0),   # 2nd purchase — 4 days after 1st ✅
    (1, datetime(2024, 1, 20), 150.0),   # 3rd purchase — ignored
    (2, datetime(2024, 1, 1),  300.0),
    (2, datetime(2024, 1, 3),  250.0),   # 2nd purchase — 2 days after 1st ✅
    (2, datetime(2024, 1, 9),  180.0),   # 3rd purchase — ignored
    (3, datetime(2024, 1, 1),  400.0),
    (3, datetime(2024, 1, 10), 350.0),   # 2nd purchase — 9 days after 1st ❌
    (4, datetime(2024, 1, 1),  500.0),   # only one purchase → filtered out
    (5, datetime(2024, 1, 1),  500.0),
    (5, datetime(2024, 1, 1),  300.0),   # same day — deduped by groupBy
    (5, datetime(2024, 1, 6),  200.0),   # 2nd purchase — 5 days after 1st ✅
]

# ── Create DataFrame ──────────────────────────────────────────────────────
df_transactions = spark.createDataFrame(transactions_data, schema=transactions_schema)

df_transactions.printSchema()
df_transactions.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : daily — one row per user per day ──────────────────────────────
daily = df_transactions \
    .groupBy(
        "user_id",
        F.to_date("created_at").alias("purchase_date")        # GROUP BY over DISTINCT
    ) \
    .agg(F.lit(1).alias("_dummy")) \
    .drop("_dummy")

# ── CTE 2 : ranked — ROW_NUMBER per user ordered by date ─────────────────
window_spec = W.partitionBy("user_id").orderBy("purchase_date")

ranked = daily \
    .withColumn("rn", F.row_number().over(window_spec))       # ROW_NUMBER()

# ── CTE 3 : first_two — pivot rn=1 and rn=2 into columns ─────────────────
first_two = ranked \
    .filter(F.col("rn") <= 2) \                               # WHERE rn <= 2
    .groupBy("user_id") \
    .agg(
        F.max(
            F.when(F.col("rn") == 1, F.col("purchase_date"))
        ).alias("first_date"),                                # rn = 1 → first purchase
        F.max(
            F.when(F.col("rn") == 2, F.col("purchase_date"))
        ).alias("second_date")                               # rn = 2 → second purchase
    )

# ── Final : Filter users whose 2nd purchase was within 7 days of 1st ─────
result_without_transform = first_two \
    .filter(
        F.col("second_date").isNotNull() &                    # has a second purchase
        F.datediff(
            F.col("second_date"),
            F.col("first_date")
        ).between(1, 7)                                       # within 1 to 7 days
    ) \
    .select("user_id") \
    .orderBy("user_id")

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : One row per user per purchase day ─────────────────────────────
def get_daily_purchases(df):
    return df \
        .groupBy(
            "user_id",
            F.to_date("created_at").alias("purchase_date")
        ) \
        .agg(F.lit(1).alias("_dummy")) \
        .drop("_dummy")

# ── CTE 2 : Rank purchases per user by date ───────────────────────────────
def add_row_number(df):
    window_spec = W.partitionBy("user_id").orderBy("purchase_date")
    return df.withColumn("rn", F.row_number().over(window_spec))

# ── CTE 3 : Pivot first and second purchase dates into columns ────────────
def pivot_first_two(df):
    return df \
        .filter(F.col("rn") <= 2) \
        .groupBy("user_id") \
        .agg(
            F.max(
                F.when(F.col("rn") == 1, F.col("purchase_date"))
            ).alias("first_date"),
            F.max(
                F.when(F.col("rn") == 2, F.col("purchase_date"))
            ).alias("second_date")
        )

# ── Final : Filter 2nd purchase within 7 days of 1st ─────────────────────
def filter_within_7_days(df):
    return df \
        .filter(
            F.col("second_date").isNotNull() &
            F.datediff(
                F.col("second_date"),
                F.col("first_date")
            ).between(1, 7)
        ) \
        .select("user_id") \
        .orderBy("user_id")

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_transactions
    .transform(get_daily_purchases)
    .transform(add_row_number)
    .transform(pivot_first_two)
    .transform(filter_within_7_days)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+-------+
|user_id|
+-------+
|1      |   -- 1st: Jan 1 → 2nd: Jan 5  = 4 days ✅
|2      |   -- 1st: Jan 1 → 2nd: Jan 3  = 2 days ✅
|5      |   -- 1st: Jan 1 → 2nd: Jan 6  = 5 days ✅
+-------+
-- user 3 excluded : Jan 1 → Jan 10 = 9 days ❌
-- user 4 excluded : only one purchase ❌

── With Transform ──
+-------+
|user_id|
+-------+
|1      |
|2      |
|5      |
+-------+

```