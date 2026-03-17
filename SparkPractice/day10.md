## Highest Cost Orders

Find the customers with the highest daily total order cost between 2019-02-01 and 2019-05-01. If a customer had more than one order on a certain day, sum the order costs on a daily basis. Output each customer's first name, total cost of their items, and the date. If multiple customers tie for the highest daily total on the same date, return all of them.


For simplicity, you can assume that every first name in the dataset is unique.

Table - (customer's, orders)



```python 

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T
from datetime import date

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Customer Daily Spending").getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
orders_schema = T.StructType([
    T.StructField("id",               T.LongType(),   True),
    T.StructField("cust_id",          T.LongType(),   True),
    T.StructField("order_date",       T.DateType(),   True),
    T.StructField("total_order_cost", T.DoubleType(), True),
])

customers_schema = T.StructType([
    T.StructField("id",         T.LongType(),   True),
    T.StructField("first_name", T.StringType(), True),
    T.StructField("last_name",  T.StringType(), True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
orders_data = [
    (1,  1, date(2019, 2, 5),  150.0),
    (2,  2, date(2019, 2, 5),  200.0),
    (3,  3, date(2019, 2, 5),  180.0),
    (4,  1, date(2019, 2, 5),   50.0),   # same customer same day — will sum
    (5,  2, date(2019, 3, 10), 300.0),
    (6,  3, date(2019, 3, 10), 250.0),
    (7,  4, date(2019, 3, 10), 320.0),
    (8,  1, date(2019, 4, 20), 400.0),
    (9,  4, date(2019, 4, 20), 380.0),
    (10, 2, date(2019, 4, 20), 210.0),
    (11, 5, date(2019, 6, 1),  500.0),   # outside date range — filtered out
]

customers_data = [
    (1, "John",    "Smith"),
    (2, "Jane",    "Doe"),
    (3, "Bob",     "Brown"),
    (4, "Alice",   "Johnson"),
    (5, "Charlie", "Wilson"),
]

# ── Create DataFrames ─────────────────────────────────────────────────────
df_orders    = spark.createDataFrame(orders_data,    schema=orders_schema)
df_customers = spark.createDataFrame(customers_data, schema=customers_schema)

df_orders.printSchema()
df_customers.printSchema()


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : total_daily_spending ──────────────────────────────────────────
total_daily_spending = df_orders \
    .filter(
        (F.col("order_date") >= F.lit(date(2019, 2, 1))) &    # BETWEEN '2019-02-01'
        (F.col("order_date") <= F.lit(date(2019, 5, 1)))      # AND     '2019-05-01'
    ) \
    .join(
        df_customers,
        df_orders["cust_id"] == df_customers["id"],            # JOIN ON o.cust_id = c.id
        how="inner"
    ) \
    .groupBy("first_name", "last_name", "order_date") \        # GROUP BY first_name, last_name, order_date
    .agg(
        F.sum("total_order_cost").alias("total_daily_cost")    # SUM(total_order_cost)
    )

# ── CTE 2 : customer_spend_rnk ────────────────────────────────────────────
window_spec = W.partitionBy("order_date") \                    # PARTITION BY order_date
               .orderBy(F.col("total_daily_cost").desc())      # ORDER BY total_daily_cost DESC

customer_spend_rnk = total_daily_spending \
    .withColumn(
        "rnk", F.dense_rank().over(window_spec)                # DENSE_RANK()
    )

# ── Final SELECT ──────────────────────────────────────────────────────────
result_without_transform = customer_spend_rnk \
    .filter(F.col("rnk") == 1) \                               # WHERE rnk = 1
    .select("first_name", "order_date", "total_daily_cost") \  # SELECT columns
    .orderBy(F.col("order_date").asc())                        # ORDER BY order_date

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE 1 : Filter date range ─────────────────────────────────────────────
def filter_date_range(df):
    return df.filter(
        (F.col("order_date") >= F.lit(date(2019, 2, 1))) &
        (F.col("order_date") <= F.lit(date(2019, 5, 1)))
    )

# ── CTE 1 : Join orders with customers ───────────────────────────────────
def join_orders_customers(df):
    return df.join(
        df_customers,
        df["cust_id"] == df_customers["id"],
        how="inner"
    )

# ── CTE 1 : Group and sum total daily cost ────────────────────────────────
def total_daily_spending_agg(df):
    return df \
        .groupBy("first_name", "last_name", "order_date") \
        .agg(
            F.sum("total_order_cost").alias("total_daily_cost")
        )

# ── CTE 2 : Add dense_rank window function ────────────────────────────────
def add_dense_rank(df):
    window_spec = W.partitionBy("order_date") \
                   .orderBy(F.col("total_daily_cost").desc())
    return df.withColumn("rnk", F.dense_rank().over(window_spec))

# ── Final : Filter rnk = 1 ────────────────────────────────────────────────
def filter_top_rank(df):
    return df.filter(F.col("rnk") == 1)

# ── Final : Select and order ──────────────────────────────────────────────
def select_final_columns(df):
    return df \
        .select("first_name", "order_date", "total_daily_cost") \
        .orderBy(F.col("order_date").asc())

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_orders
    .transform(filter_date_range)
    .transform(join_orders_customers)
    .transform(total_daily_spending_agg)
    .transform(add_dense_rank)
    .transform(filter_top_rank)
    .transform(select_final_columns)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output
```
── Without Transform ──
+----------+----------+---------------+
|first_name|order_date|total_daily_cost|
+----------+----------+---------------+
|Jane      |2019-02-05|200.0          |
|Alice     |2019-03-10|320.0          |
|John      |2019-04-20|400.0          |
+----------+----------+---------------+

── With Transform ──
+----------+----------+---------------+
|first_name|order_date|total_daily_cost|
+----------+----------+---------------+
|Jane      |2019-02-05|200.0          |
|Alice     |2019-03-10|320.0          |
|John      |2019-04-20|400.0          |
+----------+----------+---------------+


```

### Window function reference

```python 
# ── Define Window Spec ────────────────────────────────────────────────────
window_spec = W.partitionBy("order_date") \
               .orderBy(F.col("total_daily_cost").desc())

# ── Ranking Functions ─────────────────────────────────────────────────────
F.rank().over(window_spec)          # gaps in rank  : 1,1,3,4
F.dense_rank().over(window_spec)    # no gaps       : 1,1,2,3  ← used here
F.row_number().over(window_spec)    # always unique : 1,2,3,4

# ── Analytical Functions ──────────────────────────────────────────────────
F.lag("total_daily_cost",  1).over(window_spec)   # previous row value
F.lead("total_daily_cost", 1).over(window_spec)   # next row value
F.sum("total_daily_cost").over(window_spec)        # running total
F.avg("total_daily_cost").over(window_spec)        # running average

```