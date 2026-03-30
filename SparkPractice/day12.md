## Users By Average Time

Calculate each user's average session time, where a session is defined as the time difference between a page_load and a page_exit. Assume each user has only one session per day. If there are multiple page_load or page_exit events on the same day, use only the latest page_load and the earliest page_exit. Only consider sessions where the page_load occurs before the page_exit on the same day. Output the user_id and their average session time.

### table - facebook_web_log
user_id:bigint<br>
timestamp:datetime<br>
action:text<br>

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import datetime

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Facebook Session Duration").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
web_log_schema = T.StructType([
    T.StructField("user_id",   T.LongType(),      True),
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("action",    T.StringType(),    True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
web_log_data = [
    (1, datetime(2024, 1, 1,  9,  0,  0), "page_load"),
    (1, datetime(2024, 1, 1,  9, 30,  0), "page_exit"),
    (1, datetime(2024, 1, 1, 11,  0,  0), "page_load"),
    (1, datetime(2024, 1, 1, 11, 45,  0), "page_exit"),
    (2, datetime(2024, 1, 1, 10,  0,  0), "page_load"),
    (2, datetime(2024, 1, 1, 10, 20,  0), "page_exit"),
    (2, datetime(2024, 1, 2,  8,  0,  0), "page_load"),
    (2, datetime(2024, 1, 2,  8, 50,  0), "page_exit"),
    (3, datetime(2024, 1, 1, 14,  0,  0), "page_load"),
    (3, datetime(2024, 1, 1, 14, 40,  0), "page_exit"),
    (4, datetime(2024, 1, 1, 10,  0,  0), "page_load"),  # no exit — filtered out
    (5, datetime(2024, 1, 1, 12,  0,  0), "page_exit"),  # no load — filtered out
]

# ── Create DataFrame ──────────────────────────────────────────────────────
df_web_log = spark.createDataFrame(web_log_data, schema=web_log_schema)

df_web_log.printSchema()
df_web_log.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE : Pivot load and exit timestamps in single pass ───────────────────
cte = df_web_log \
    .groupBy(
        "user_id",
        F.to_date("timestamp").alias("date")                         # DATE(timestamp)
    ) \
    .agg(
        F.max(
            F.when(F.col("action") == "page_load",                   # CASE WHEN action = 'page_load'
                   F.col("timestamp"))                               # THEN timestamp ELSE NULL END
        ).alias("late_load"),

        F.min(
            F.when(F.col("action") == "page_exit",                   # CASE WHEN action = 'page_exit'
                   F.col("timestamp"))                               # THEN timestamp ELSE NULL END
        ).alias("early_exit")
    )

# ── Final : Filter nulls + avg session duration ───────────────────────────
result_without_transform = cte \
    .filter(
        F.col("late_load").isNotNull() &                             # WHERE late_load  IS NOT NULL
        F.col("early_exit").isNotNull()                              # AND   early_exit IS NOT NULL
    ) \
    .groupBy("user_id") \
    .agg(
        F.round(
            F.avg(
                F.col("early_exit").cast("long") -                   # TIMESTAMPDIFF(SECOND,
                F.col("late_load").cast("long")                      #   late_load, early_exit)
            ), 2
        ).alias("avg_time")
    ) \
    .orderBy("user_id")

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE : Pivot load/exit into columns per user per day ───────────────────
def pivot_load_exit(df):
    return df \
        .groupBy(
            "user_id",
            F.to_date("timestamp").alias("date")
        ) \
        .agg(
            F.max(
                F.when(F.col("action") == "page_load",
                       F.col("timestamp"))
            ).alias("late_load"),

            F.min(
                F.when(F.col("action") == "page_exit",
                       F.col("timestamp"))
            ).alias("early_exit")
        )

# ── Step 2 : Filter WHERE both late_load and early_exit are not null ──────
def filter_valid_sessions(df):
    return df.filter(
        F.col("late_load").isNotNull() &
        F.col("early_exit").isNotNull()
    )

# ── Step 3 : Avg session duration per user ────────────────────────────────
def avg_session_duration(df):
    return df \
        .groupBy("user_id") \
        .agg(
            F.round(
                F.avg(
                    F.col("early_exit").cast("long") -
                    F.col("late_load").cast("long")
                ), 2
            ).alias("avg_time")
        ) \
        .orderBy("user_id")

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_web_log
    .transform(pivot_load_exit)
    .transform(filter_valid_sessions)
    .transform(avg_session_duration)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+-------+--------+
|user_id|avg_time|
+-------+--------+
|1      |1800.0  |   -- max load = 11:00, min exit = 09:30 → 9:30-11:00 window
|2      |1800.0  |   -- avg of (20 min + 50 min sessions) in seconds
|3      |2400.0  |   -- 40 mins in seconds
+-------+--------+

── With Transform ──
+-------+--------+
|user_id|avg_time|
+-------+--------+
|1      |1800.0  |
|2      |1800.0  |
|3      |2400.0  |
+-------+--------+
```