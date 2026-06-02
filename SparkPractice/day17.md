## Find the number of inspections for each risk category by inspection type

Find the number of inspections that resulted in each risk category per each inspection type.
Consider the records with no risk category value belongs to a separate category.
Output the result along with the corresponding inspection type and the corresponding total number of inspections per that type. The output should be pivoted, meaning that each risk category + total number should be a separate column.
Order the result based on the number of inspections per inspection type in descending order.

## Table - sf_restaurant_health_violations
business_address:text <br>
business_city:text <br>
business_id:bigint <br>
business_latitude:double <br>
business_location:text <br>
business_longitude:double <br>
business_name:text <br>
business_phone_number:double <br>
business_postal_code:double <br>
business_state:text <br>
inspection_date:date <br>
inspection_id:text <br>
inspection_score:double <br>
inspection_type:text <br>
risk_category:text <br>
violation_description:text <br>
violation_id:text <br>

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T

# ── Spark Session ─────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SF Restaurant Health Violations") \
    .getOrCreate()

# ── Schema ────────────────────────────────────────────────────────────────
violations_schema = T.StructType([
    T.StructField("business_id",     T.IntegerType(), True),
    T.StructField("business_name",   T.StringType(),  True),
    T.StructField("inspection_type", T.StringType(),  True),
    T.StructField("risk_category",   T.StringType(),  True),
    T.StructField("violation_desc",  T.StringType(),  True),
    T.StructField("inspection_date", T.DateType(),    True),
])

# ── Sample Data ───────────────────────────────────────────────────────────
violations_data = [
    (1, "Pizza Palace",   "Routine",   None,            "No violation",    None),
    (2, "Burger Barn",    "Routine",   "Low Risk",      "Minor issue",     None),
    (3, "Sushi Spot",     "Routine",   "Moderate Risk", "Medium issue",    None),
    (4, "Taco Town",      "Routine",   "High Risk",     "Major issue",     None),
    (5, "Curry House",    "Routine",   "Low Risk",      "Minor issue",     None),
    (6, "Noodle Nook",    "Routine",   "High Risk",     "Major issue",     None),
    (7, "Salad Stop",     "Complaint", None,            "No violation",    None),
    (8, "Wrap World",     "Complaint", "Low Risk",      "Minor issue",     None),
    (9, "Dim Sum Den",    "Complaint", "Moderate Risk", "Medium issue",    None),
    (10,"Fish Fry",       "Complaint", "High Risk",     "Major issue",     None),
    (11,"Steak House",    "Complaint", "Low Risk",      "Minor issue",     None),
    (12,"Vegan Vibes",    "Followup",  None,            "No violation",    None),
    (13,"BBQ Barn",       "Followup",  "Low Risk",      "Minor issue",     None),
    (14,"Pasta Place",    "Followup",  "Moderate Risk", "Medium issue",    None),
    (15,"Donut Den",      "Routine",   None,            "No violation",    None),
    (16,"Coffee Corner",  "Routine",   "Moderate Risk", "Medium issue",    None),
    (17,"Waffle World",   "Complaint", "Moderate Risk", "Medium issue",    None),
    (18,"Pita Palace",    "Followup",  "High Risk",     "Major issue",     None),
]

df_violations = spark.createDataFrame(
    violations_data,
    schema=violations_schema
)

df_violations.printSchema()
df_violations.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── CTE : conditional aggregation per inspection type ────────────────────
cte = df_violations \
    .groupBy("inspection_type") \
    .agg(
        # ✅ NULL check — isNull() equivalent of risk_category IS NULL
        F.sum(
            F.when(F.col("risk_category").isNull(), 1)
             .otherwise(0)
        ).alias("no_risk_results"),

        # ✅ Low Risk count
        F.sum(
            F.when(F.col("risk_category") == "Low Risk", 1)
             .otherwise(0)
        ).alias("low_risk_results"),

        # ✅ Moderate Risk count
        F.sum(
            F.when(F.col("risk_category") == "Moderate Risk", 1)
             .otherwise(0)
        ).alias("medium_risk_results"),

        # ✅ High Risk count
        F.sum(
            F.when(F.col("risk_category") == "High Risk", 1)
             .otherwise(0)
        ).alias("high_risk_results")
    )

print("── CTE Output ──")
cte.show(truncate=False)
# +---------------+--------------+----------------+-------------------+-----------------+
# |inspection_type|no_risk_results|low_risk_results|medium_risk_results|high_risk_results|
# +---------------+--------------+----------------+-------------------+-----------------+
# |Routine        |2             |2               |2                  |2                |
# |Complaint      |1             |2               |2                  |1                |
# |Followup       |1             |1               |1                  |1                |
# +---------------+--------------+----------------+-------------------+-----------------+

# ── Final : add total_inspections + order by desc ─────────────────────────
result_without_transform = cte \
    .withColumn(
        "total_inspections",
        F.col("no_risk_results")   +              # ✅ sum all risk categories
        F.col("low_risk_results")  +
        F.col("medium_risk_results") +
        F.col("high_risk_results")
    ) \
    .orderBy(F.col("total_inspections").desc())   # ✅ ORDER BY total DESC

print("── Without Transform — Final Result ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Conditional aggregation per inspection type ─────────────────
def aggregate_risk_categories(df):
    """
    Group by inspection_type
    Count each risk category using conditional SUM
    Equivalent to SQL CASE WHEN inside SUM
    """
    return df \
        .groupBy("inspection_type") \
        .agg(
            F.sum(
                F.when(F.col("risk_category").isNull(), 1)
                 .otherwise(0)
            ).alias("no_risk_results"),            # ✅ NULL → no risk

            F.sum(
                F.when(F.col("risk_category") == "Low Risk", 1)
                 .otherwise(0)
            ).alias("low_risk_results"),           # ✅ Low Risk count

            F.sum(
                F.when(F.col("risk_category") == "Moderate Risk", 1)
                 .otherwise(0)
            ).alias("medium_risk_results"),        # ✅ Moderate Risk count

            F.sum(
                F.when(F.col("risk_category") == "High Risk", 1)
                 .otherwise(0)
            ).alias("high_risk_results")           # ✅ High Risk count
        )

# ── Step 2 : Add total inspections column ────────────────────────────────
def add_total_inspections(df):
    """
    Add total_inspections column
    Sum of all risk category counts
    Equivalent to SQL column addition in SELECT
    """
    return df.withColumn(
        "total_inspections",
        F.col("no_risk_results")    +
        F.col("low_risk_results")   +
        F.col("medium_risk_results") +
        F.col("high_risk_results")
    )

# ── Step 3 : Order by total_inspections desc ──────────────────────────────
def order_by_total_desc(df):
    """Order results by total_inspections descending"""
    return df.orderBy(
        F.col("total_inspections").desc()
    )

# ── Chain with .transform() ───────────────────────────────────────────────
result_with_transform = (
    df_violations
    .transform(aggregate_risk_categories)
    .transform(add_total_inspections)
    .transform(order_by_total_desc)
)

print("── With Transform — Final Result ──")
result_with_transform.show(truncate=False)

```