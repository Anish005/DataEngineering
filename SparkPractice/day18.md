# Reviews of Categories

Calculate number of reviews for every business category. Output the category along with the total number of reviews. Order by total reviews in descending order.

## Table -  yelp_reviews

address:text<br>
business_id:text<br>
categories:text<br>
city:text<br>
is_open:bigint<br>
latitude:double<br>
longitude:double<br>
name:text<br>
neighborhood:text<br>
postal_code:text<br>
review_count:bigint<br>
stars:double<br>
state:text<br>

```python
from pyspark.sql import functions as F

df_result = (
    df
    .withColumn(
        "category",
        F.explode(
            F.split(F.col("categories"), ";")
        )
    )
    .groupBy("category")
    .agg(
        F.sum("review_count").alias("review_cnt")
    )
    .orderBy(F.col("review_cnt").desc())
)

df_result.show()
```