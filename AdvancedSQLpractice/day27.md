## Finding  Purchases

Identify returning active users by finding users who made a second purchase within 1 to 7 days after their first purchase. Ignore same-day purchases. Output a list of these user_ids.

### Table - amazon_transactions

id:bigint <br>
user_id:bigint <br>
item:text <br>
created_at:date <br>
revenue:bigint <br>

```sql
----  USING LEAD()
WITH consecutive AS (                          -- ✅ single CTE instead of two
    SELECT
        user_id,
        DATE(created_at)        AS purchase_date,
        LEAD(DATE(created_at)) OVER (          -- ✅ LEAD directly on base table
            PARTITION BY user_id               --    no need for daily CTE at all
            ORDER BY DATE(created_at)
        )                       AS next_date
    FROM amazon_transactions
    GROUP BY user_id, DATE(created_at)         -- ✅ GROUP BY over DISTINCT
)
SELECT user_id                                 -- ✅ no DISTINCT needed here
FROM consecutive
WHERE next_date IS NOT NULL
  AND DATEDIFF(next_date, purchase_date) BETWEEN 1 AND 7
GROUP BY user_id                               -- ✅ GROUP BY over SELECT DISTINCT
ORDER BY user_id;

--------------------------------------------------------------
--USING LAG()
WITH ordered_tx AS
    (SELECT user_id,
            DATE(created_at) AS tx_date,
            LAG(DATE(created_at)) OVER (PARTITION BY user_id
                                        ORDER BY created_at) AS prev_tx_date
     FROM amazon_transactions)
SELECT DISTINCT user_id
FROM ordered_tx
WHERE prev_tx_date IS NOT NULL
    AND DATEDIFF(tx_date, prev_tx_date) > 0
    AND DATEDIFF(tx_date, prev_tx_date) <= 7;
```