## Acceptance rate by Date
Calculate the friend acceptance rate for each date when friend requests were sent. A request is sent if action = sent and accepted if action = accepted. If a request is not accepted, there is no record of it being accepted in the table.


The output will only include dates where requests were sent and at least one of them was accepted (acceptance can occur on any date after the request is sent).

### Table - fb_friend_requests
user_id_sender:text <br>
user_id_receiver:text <br>
date:date <br>
action:text <br>

```python 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import date

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Friend Request Rate").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
requests_schema = T.StructType([
    T.StructField("user_id_sender",   T.StringType(), True),
    T.StructField("user_id_receiver", T.StringType(), True),
    T.StructField("date",             T.DateType(),   True),
    T.StructField("action",           T.StringType(), True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
requests_data = [
    ("A", "B", date(2024, 1, 1), "sent"),
    ("A", "C", date(2024, 1, 1), "sent"),
    ("A", "D", date(2024, 1, 1), "sent"),
    ("A", "B", date(2024, 1, 1), "accepted"),       # accepted
    ("A", "C", date(2024, 1, 1), "accepted"),       # accepted
    ("B", "C", date(2024, 1, 2), "sent"),
    ("B", "D", date(2024, 1, 2), "sent"),
    ("B", "C", date(2024, 1, 2), "accepted"),       # accepted
    ("C", "D", date(2024, 1, 3), "sent"),           # no acceptance
]

# ── Create DataFrame ──────────────────────────────────────────────────────
df_requests = spark.createDataFrame(requests_data, schema=requests_schema)

df_requests.printSchema()
df_requests.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : sent ──────────────────────────────────────────────────────────
sent = df_requests.filter(F.col("action") == "sent")

# ── CTE 2 : accept ────────────────────────────────────────────────────────
accept = df_requests.filter(F.col("action") == "accepted")

# ── Final : Left join + acceptance rate ───────────────────────────────────
result_without_transform = sent \
    .join(
        accept,
        on  = [
            sent["user_id_sender"]   == accept["user_id_sender"],    # ✅ sender matches
            sent["user_id_receiver"] == accept["user_id_receiver"]   # ✅ receiver matches
        ],
        how = "left"                                                  # ✅ LEFT JOIN
    ) \
    .groupBy(sent["date"]) \                                          # GROUP BY s.date
    .agg(
        F.round(
            F.count(accept["user_id_receiver"]) /                    # accepted count
            F.count(sent["user_id_sender"]),                         # sent count
            2
        ).alias("rate")
    ) \
    .orderBy("date")

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : Filter sent requests ─────────────────────────────────────────
def filter_sent(df):
    return df.filter(F.col("action") == "sent")

# ── CTE 2 : Filter accepted requests ─────────────────────────────────────
def filter_accept(df):
    return df.filter(F.col("action") == "accepted")

# ── Final : Left join sent ↔ accept + calculate rate ─────────────────────
def calc_acceptance_rate(df):
    df_accept = df_requests.transform(filter_accept)
    return df \
        .join(
            df_accept,
            on  = [
                df["user_id_sender"]   == df_accept["user_id_sender"],
                df["user_id_receiver"] == df_accept["user_id_receiver"]
            ],
            how = "left"
        ) \
        .groupBy(df["date"]) \
        .agg(
            F.round(
                F.count(df_accept["user_id_receiver"]) /
                F.count(df["user_id_sender"]),
                2
            ).alias("rate")
        ) \
        .orderBy("date")

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_requests
    .transform(filter_sent)
    .transform(calc_acceptance_rate)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+----------+----+
|date      |rate|
+----------+----+
|2024-01-01|0.67|   -- 2 accepted / 3 sent
|2024-01-02|0.5 |   -- 1 accepted / 2 sent
|2024-01-03|0.0 |   -- 0 accepted / 1 sent
+----------+----+

── With Transform ──
+----------+----+
|date      |rate|
+----------+----+
|2024-01-01|0.67|
|2024-01-02|0.5 |
|2024-01-03|0.0 |
+----------+----+

```