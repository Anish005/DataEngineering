## Employees With the Same Salary

Find employees who earn the same salary.
Output the worker id along with the first name and the salary in descending order.

### Table : worker

department:text <br>
first_name:text <br>
joining_date:date <br>
last_name:text <br>
salary:bigint <br>
worker_id:bigint <br>
```sql

SELECT
    w1.worker_id AS worker_id,
    w1.first_name AS first_name,
    w1.salary AS salary
  FROM worker AS w1
  JOIN worker AS w2 ON w1.salary = w2.salary
 WHERE w1.worker_id <> w2.worker_id;
```