## Finding User Purchases

Identify returning active users by finding users who made a second purchase within 1 to 7 days after their first purchase. Ignore same-day purchases. Output a list of these user_ids.

### Table - amazon_transactions

id:bigint <br>
user_id:bigint <br>
item:text <br>
created_at:date <br>
revenue:bigint <br>

```sql 
WITH daily AS (
    SELECT
        user_id,
        DATE(created_at) AS purchase_date
    FROM amazon_transactions
    GROUP BY user_id, DATE(created_at)       -- ✅ GROUP BY instead of DISTINCT
),
ranked AS (
    SELECT
        user_id,
        purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY purchase_date
        ) AS rn
    FROM daily
),
first_two AS (
    SELECT
        user_id,
        MAX(CASE WHEN rn = 1 THEN purchase_date END) AS first_date,
        MAX(CASE WHEN rn = 2 THEN purchase_date END) AS second_date
    FROM ranked
    WHERE rn <= 2                            -- ✅ only first two purchases
    GROUP BY user_id
)
SELECT user_id
FROM first_two
WHERE second_date IS NOT NULL
  AND DATEDIFF(second_date, first_date) BETWEEN 1 AND 7
ORDER BY user_id;

```