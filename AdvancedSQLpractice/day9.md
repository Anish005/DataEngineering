## Calculate the highest salary differences

Calculates the difference between the highest salaries in the marketing and engineering departments. Output just the absolute difference in salaries.

Two tables - 
db_employee(id, first_name , last_name ,department_id ,salary)
db_department(id, department_name)

```sql
-- BRUTE FORCE
WITH max_marketing_salary AS (
    SELECT MAX(salary) AS salary1
    FROM db_employee d
    JOIN db_dept dp 
      ON d.department_id = dp.id
    WHERE dp.department = 'marketing'
),
max_engineering_salary AS (
    SELECT MAX(salary) AS salary2
    FROM db_employee d
    JOIN db_dept dp 
      ON d.department_id = dp.id
    WHERE dp.department = 'engineering'
)
SELECT ABS(salary1 - salary2) AS salary_difference
FROM max_marketing_salary, max_engineering_salary;

```

```sql
-- OPTIMIZED APPROACH
-- use CASE
SELECT ABS(
    MAX(CASE WHEN dp.department = 'marketing' THEN salary END) -
    MAX(CASE WHEN dp.department = 'engineering' THEN salary END)
) AS salary_difference
FROM db_employee d
JOIN db_dept dp
  ON d.department_id = dp.id;
-- no cte and twice scanning
```