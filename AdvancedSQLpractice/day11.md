## Finding updated records
We have a table with employees and their salaries, however, some of the records are old and contain outdated salary information. Find the current salary of each employee assuming that salaries increase each year. Output their id, first name, last name, department ID, and current salary. Order your list by employee ID in ascending order.

### Table
ms_employee_salary(department_id, first_name, id, last_name ,salary)

```sql
with salaryRNK AS(
select  id, first_name, last_name, department_id, salary,
        DENSE_RANK() over(partition by id order by salary desc) rnk
from ms_employee_salary
)

select id, first_name, last_name, department_id, salary
from salaryRNK
where rnk=1

```