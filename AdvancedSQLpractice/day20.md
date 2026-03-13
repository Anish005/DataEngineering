## April Admin Employees

Find the number of employees working in the Admin department that joined in April or later, in any year.

Table -- worker

```sql 
select count(worker_id) as n_admins from worker where extract(month from date(joining_date)) >= 4 and department = 'Admin';


```