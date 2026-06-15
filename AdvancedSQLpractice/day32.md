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

```sql
WITH cte AS (
SELECT category,
          review_count
   FROM yelp_business,
        json_table(concat('["', replace(categories, ';', '","'), '"]'), '$[*]' columns(category varchar(255) path '$')) AS jt
)
        
SELECT category,
      sum(review_count) AS review_cnt
FROM cte
GROUP BY category
ORDER BY review_cnt DESC

```