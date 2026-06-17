## Top Businesses With Most Reviews
Find the top 5 businesses with most reviews. Assume that each row has a unique business_id such that the total reviews for each business is listed on each row. Output the business name along with the total number of reviews and order your results by the total reviews in descending order.


If there are ties in review counts, businesses with the same number of reviews receive the same rank, and subsequent ranks are skipped accordingly (e.g., if two businesses tie for rank 4, the next business receives rank 6, skipping rank 5).

### Table : yelp_business

address:text <br>
business_id:text <br>
categories:text <br>
city:text <br>
is_open:bigint <br>
latitude:double <br>
longitude:double <br>
name:text <br>
neighborhood:text <br>
postal_code:text <br>
review_count:bigint <br>
stars:double <br>
state:text <br>

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

window_spec =  Window.orderBy(F.col("review_count").desc())
df_cte = df.withColumn("rnk" , F.rank().over(window_spec))

df_res =  df_cte.filter(F.col("rnk") <= 5).select("name", "review_count").orderBy(F.col("review_count").desc())

display(df_res)
```