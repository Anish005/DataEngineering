## New Products
Calculate the net change in the number of products launched by companies in 2020 compared to 2019. Your output should include the company names and the net difference.
(Net difference = Number of products launched in 2020 - The number launched in 2019.)

Table  - car_launches

```sql
-- first intuition
WITH num_products_2019 AS (
    SELECT 
        COUNT(product_name) AS num_products_2019, 
        company_name                                    
    FROM car_launches
    WHERE year = 2019    
    GROUP BY company_name
),                                   
num_products_2020 AS (              
    SELECT 
        COUNT(product_name) AS num_products_2020,  
        company_name                              
    FROM car_launches
    WHERE year = 2020
    GROUP BY company_name
)
SELECT 
    (num_products_2020 - num_products_2019) AS net_difference,
    num_products_2019.company_name 
FROM num_products_2019  
INNER JOIN num_products_2020
    ON num_products_2019.company_name = num_products_2020.company_name;

-- optimized version of the above query

WITH launches AS (
    SELECT company_name,
           year
    FROM car_launches
)

SELECT company_name,
       COUNT(CASE WHEN year = 2020 THEN 1 END) - COUNT(CASE WHEN year = 2019 THEN 1 END) AS total_launch
FROM launches
GROUP BY company_name;

--MOST OPTIMIZED VERSION
SELECT 
    company_name,
    SUM(CASE WHEN year = 2020 THEN 1 ELSE 0 END) -
    SUM(CASE WHEN year = 2019 THEN 1 ELSE 0 END) AS net_difference
FROM car_launches             
WHERE year IN (2019, 2020)    
GROUP BY company_name         
ORDER BY company_name;

```