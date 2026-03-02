## Worker with highest salaries

Management wants to analyze only employees with official job titles. Find the job titles of the employees with the highest salary. If multiple employees have the same highest salary, include all their job titles.

### Two tables
worker(department, first_name ,joining_date ,last_name, salary, worker_id) <>
title(worker_ref_id, worker_title ,affected_from)

```sql
with max_salary_title as(
    select t.worker_title , 
        dense_rank() over(order by w.salary desc) rnk
    from worker w join title t on w.worker_id = t.worker_ref_id
    where t.worker_title is not null
)
select distinct worker_title from max_salary_title
where rnk = 1;


```
