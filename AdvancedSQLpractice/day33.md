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

```sql
WITH ranked_businesses AS (
    SELECT name, 
           review_count, 
           RANK() OVER (ORDER BY review_count DESC) AS rank_value
    FROM yelp_business
)
SELECT name, 
       review_count
FROM ranked_businesses
WHERE rank_value <= 5
ORDER BY review_count DESC;
```