## Call Center

You are given two DataFrames, calls_df and customers_df, which contain information about calls made by customers of a telecommunications company and information about the customers, respectively.

Write a function that returns the number of distinct customers who made calls on each date, along with the total duration of calls made on each date.

```sql
SELECT 
    c.date,
    COUNT(DISTINCT cu.cust_id) AS num_customers,
    SUM(CAST(c.duration AS INT)) AS total_duration
FROM cc_calls c
LEFT JOIN cc_customer cu
    ON c.cust_id = cu.cust_id
GROUP BY c.date
ORDER BY c.date


```