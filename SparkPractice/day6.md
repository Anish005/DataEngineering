## Popularity of Hack

Meta/Facebook has developed a new programing language called Hack.To measure the popularity of Hack they ran a survey with their employees. The survey included data on previous programing familiarity as well as the number of years of experience, age, gender and most importantly satisfaction with Hack. Due to an error location data was not collected, but your supervisor demands a report showing average popularity of Hack by office location. Luckily the user IDs of employees completing the surveys were stored.
Based on the above, find the average popularity of the Hack per office location.
Output the location along with the average popularity.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window as W
from pyspark.sql import types as T

# ── Sample Data ─────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("Facebook Survey").getOrCreate()

# Facebook Employees
employees_data = [
    (1,  "New York"),
    (2,  "San Francisco"),
    (3,  "New York"),
    (4,  "Chicago"),
    (5,  "San Francisco"),
    (6,  "Chicago"),
]

employees_schema = T.StructType([
    T.StructField("id",       T.LongType(),   True),
    T.StructField("location", T.StringType(), True),
])

# Facebook Hack Survey
survey_data = [
    (1,  85),
    (2,  90),
    (3,  78),
    (4,  88),
    (5,  95),
    (6,  70),
]

survey_schema = T.StructType([
    T.StructField("employee_id", T.LongType(),    True),
    T.StructField("popularity",  T.IntegerType(), True),
])

# ── Create DataFrames ────────────────────────────────────────────────────
df_employees = spark.createDataFrame(employees_data, schema=employees_schema)
df_survey    = spark.createDataFrame(survey_data,    schema=survey_schema)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 1 — Without df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Join ────────────────────────────────────────────────────────
df_joined = df_employees.join(
    df_survey,
    df_employees["id"] == df_survey["employee_id"],
    how="inner"
)

# ── Step 2 : Aggregate ───────────────────────────────────────────────────
result_without_transform = df_joined \
    .groupBy("location") \
    .agg(
        F.round(F.avg("popularity"), 2).alias("avg_popularity")
    ) \
    .orderBy("location")

print("── Without Transform ──")
result_without_transform.show(truncate=False)


# ══════════════════════════════════════════════════════════════════════════
# ✅ WAY 2 — With df.transform()
# ══════════════════════════════════════════════════════════════════════════

# ── Step 1 : Join Function ───────────────────────────────────────────────
def join_employee_survey(df):
    return df.join(
        df_survey,
        df["id"] == df_survey["employee_id"],
        how="inner"
    )

# ── Step 2 : Aggregate Function ──────────────────────────────────────────
def avg_popularity_by_location(df):
    return df.groupBy("location") \
             .agg(
                 F.round(F.avg("popularity"), 2).alias("avg_popularity")
             ) \
             .orderBy("location")

# ── Chain with .transform() ──────────────────────────────────────────────
result_with_transform = (
    df_employees
    .transform(join_employee_survey)
    .transform(avg_popularity_by_location)
)

print("── With Transform ──")
result_with_transform.show(truncate=False)
```

---

## 📤 Expected Output — Both Ways Produce the Same Result
```
── Without Transform ──
+-------------+--------------+
|location     |avg_popularity|
+-------------+--------------+
|Chicago      |79.0          |
|New York     |81.5          |
|San Francisco|92.5          |
+-------------+--------------+

── With Transform ──
+-------------+--------------+
|location     |avg_popularity|
+-------------+--------------+
|Chicago      |79.0          |
|New York     |81.5          |
|San Francisco|92.5          |
+-------------+--------------+



```