## Finding Purchases

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
spark = SparkSession.builder.appName("Returning Active Users").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
transactions_schema = T.StructType([
    T.StructField("user_id",    T.LongType(),      True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("amount",     T.DoubleType(),    True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
transactions_data = [
    (1, datetime(2024, 1, 1),  100.0),
    (1, datetime(2024, 1, 5),  200.0),   # Jan01→Jan05 =  4 days ✅
    (1, datetime(2024, 1, 20), 150.0),   # Jan05→Jan20 = 15 days ❌
    (2, datetime(2024, 1, 1),  300.0),
    (2, datetime(2024, 1, 3),  250.0),   # Jan01→Jan03 =  2 days ✅
    (2, datetime(2024, 1, 9),  180.0),   # Jan03→Jan09 =  6 days ✅
    (3, datetime(2024, 1, 1),  400.0),
    (3, datetime(2024, 1, 10), 350.0),   # Jan01→Jan10 =  9 days ❌
    (4, datetime(2024, 1, 1),  500.0),   # single purchase ❌
    (5, datetime(2024, 1, 1),  500.0),
    (5, datetime(2024, 1, 1),  300.0),   # same day duplicate — datediff=0 filtered
    (5, datetime(2024, 1, 6),  200.0),   # Jan01→Jan06 =  5 days ✅
    (6, datetime(2024, 1, 1),  100.0),
    (6, datetime(2024, 1, 15), 200.0),   # Jan01→Jan15 = 14 days ❌
    (6, datetime(2024, 1, 18), 300.0),   # Jan15→Jan18 =  3 days ✅
]

df_transactions = spark.createDataFrame(transactions_data, schema=transactions_schema)

df_transactions.printSchema()
df_transactions.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# 🔵 APPROACH 1 : LEAD()
# ══════════════════════════════════════════════════════════════════════════
#
# Logic : For each row look FORWARD to the NEXT purchase date
#         if next purchase is within 1-7 days → returning user
#
# user_id | tx_date    | next_date  | datediff
# 1       | 2024-01-01 | 2024-01-05 | 4  ✅
# 1       | 2024-01-05 | 2024-01-20 | 15 ❌
# 1       | 2024-01-20 | NULL       | filtered
# ══════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────
# ✅ LEAD — WAY 1 : Without df.transform()
# ──────────────────────────────────────────────────────────────────────────
print("═" * 60)
print("🔵 APPROACH 1 : LEAD — Without Transform")
print("═" * 60)

window_lead = W.partitionBy("user_id").orderBy("tx_date")

result_lead_without_transform = df_transactions \
    .select(
        "user_id",
        F.to_date("created_at").alias("tx_date")           # project early
    ) \
    .dropDuplicates(["user_id", "tx_date"]) \              # deduplicate same day
    .withColumn(
        "next_date",
        F.lead("tx_date").over(window_lead)                # LEAD → next purchase
    ) \
    .filter(
        F.col("next_date").isNotNull() &                   # has next purchase
        F.datediff(
            F.col("next_date"),
            F.col("tx_date")
        ).between(1, 7)                                    # within 1 to 7 days
    ) \
    .select("user_id") \
    .dropDuplicates()                                      # one row per user
    .orderBy("user_id")

result_lead_without_transform.show(truncate=False)


# ──────────────────────────────────────────────────────────────────────────
# ✅ LEAD — WAY 2 : With df.transform()
# ──────────────────────────────────────────────────────────────────────────
print("═" * 60)
print("🔵 APPROACH 1 : LEAD — With Transform")
print("═" * 60)

def project_and_dedup(df):
    """Select only needed columns and remove same day duplicates"""
    return df \
        .select(
            "user_id",
            F.to_date("created_at").alias("tx_date")
        ) \
        .dropDuplicates(["user_id", "tx_date"])

def add_lead_next_date(df):
    """Add next purchase date using LEAD window function"""
    window_spec = W.partitionBy("user_id").orderBy("tx_date")
    return df.withColumn(
        "next_date",
        F.lead("tx_date").over(window_spec)
    )

def filter_lead_returning_users(df):
    """Filter users with next purchase within 1-7 days"""
    return df \
        .filter(
            F.col("next_date").isNotNull() &
            F.datediff(
                F.col("next_date"),
                F.col("tx_date")
            ).between(1, 7)
        ) \
        .select("user_id") \
        .dropDuplicates() \
        .orderBy("user_id")

result_lead_with_transform = (
    df_transactions
    .transform(project_and_dedup)
    .transform(add_lead_next_date)
    .transform(filter_lead_returning_users)
)

result_lead_with_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# 🟢 APPROACH 2 : LAG()
# ══════════════════════════════════════════════════════════════════════════
#
# Logic : For each row look BACKWARD to the PREVIOUS purchase date
#         if current purchase is within 1-7 days of previous → returning user
#         DATEDIFF > 0 naturally handles same day without explicit dedup
#
# user_id | tx_date    | prev_tx_date | datediff
# 1       | 2024-01-01 | NULL         | filtered by IS NOT NULL
# 1       | 2024-01-01 | 2024-01-01   | 0  → filtered by > 0 (same day)
# 1       | 2024-01-05 | 2024-01-01   | 4  ✅
# 1       | 2024-01-20 | 2024-01-05   | 15 ❌
# ══════════════════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────────────────
# ✅ LAG — WAY 1 : Without df.transform()
# ──────────────────────────────────────────────────────────────────────────
print("═" * 60)
print("🟢 APPROACH 2 : LAG — Without Transform")
print("═" * 60)

window_lag = W.partitionBy("user_id").orderBy("tx_date")

result_lag_without_transform = df_transactions \
    .select(
        "user_id",
        F.to_date("created_at").alias("tx_date")           # project early
    ) \
    .withColumn(
        "prev_tx_date",
        F.lag("tx_date").over(window_lag)                  # LAG → previous purchase
    ) \
    .filter(
        F.col("prev_tx_date").isNotNull() &                # has previous purchase
        (F.datediff(
            F.col("tx_date"),
            F.col("prev_tx_date")
        ) > 0) &                                           # exclude same day
        (F.datediff(
            F.col("tx_date"),
            F.col("prev_tx_date")
        ) <= 7)                                            # within 7 days
    ) \
    .select("user_id") \
    .dropDuplicates() \                                    # one row per user
    .orderBy("user_id")

result_lag_without_transform.show(truncate=False)


# ──────────────────────────────────────────────────────────────────────────
# ✅ LAG — WAY 2 : With df.transform()
# ──────────────────────────────────────────────────────────────────────────
print("═" * 60)
print("🟢 APPROACH 2 : LAG — With Transform")
print("═" * 60)

def project_only(df):
    """Select only needed columns — no dedup needed for LAG approach"""
    return df.select(
        "user_id",
        F.to_date("created_at").alias("tx_date")           # project early
    )

def add_lag_prev_date(df):
    """Add previous purchase date using LAG window function"""
    window_spec = W.partitionBy("user_id").orderBy("tx_date")
    return df.withColumn(
        "prev_tx_date",
        F.lag("tx_date").over(window_spec)
    )

def filter_lag_returning_users(df):
    """Filter users whose current purchase is 1-7 days after previous"""
    diff = F.datediff(F.col("tx_date"), F.col("prev_tx_date"))
    return df \
        .filter(
            F.col("prev_tx_date").isNotNull() &            # has previous purchase
            (diff > 0) &                                   # exclude same day
            (diff <= 7)                                    # within 7 days
        ) \
        .select("user_id") \
        .dropDuplicates() \
        .orderBy("user_id")

result_lag_with_transform = (
    df_transactions
    .transform(project_only)
    .transform(add_lag_prev_date)
    .transform(filter_lag_returning_users)
)

result_lag_with_transform.show(truncate=False)

```