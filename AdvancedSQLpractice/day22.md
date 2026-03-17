## Highest Cost Orders

Find the customers with the highest daily total order cost between 2019-02-01 and 2019-05-01. If a customer had more than one order on a certain day, sum the order costs on a daily basis. Output each customer's first name, total cost of their items, and the date. If multiple customers tie for the highest daily total on the same date, return all of them.


For simplicity, you can assume that every first name in the dataset is unique.

Table - (customer's, orders)

```sql 
WITH total_daily_spending AS (
    SELECT 
        c.first_name,
        c.last_name,
        o.order_date,
        SUM(o.total_order_cost) AS total_daily_cost
    FROM orders o
    JOIN customers c 
        ON o.cust_id = c.id
    WHERE o.order_date BETWEEN '2019-02-01' AND '2019-05-01'
    GROUP BY c.first_name, c.last_name, o.order_date
),
customer_spend_rnk AS (
    SELECT 
        first_name,
        last_name,
        order_date,
        total_daily_cost,
        DENSE_RANK() OVER (
            PARTITION BY order_date 
            ORDER BY total_daily_cost DESC
        ) AS rnk
    FROM total_daily_spending
)
SELECT 
    first_name,
    order_date,
    total_daily_cost
FROM customer_spend_rnk
WHERE rnk = 1
ORDER BY order_date;

```