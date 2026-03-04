## Average Salaries

Compare each employee's salary with the average salary of the corresponding department.
Output the department, first name, and salary of employees along with the average salary of that department.

### Table

employee(<br>
address:text<br>
age:bigint<br>
bonus:bigint<br>
city:text<br>
department:text<br>
email:text<br>
employee_title:text<br>
first_name:text<br>
id:bigint<br>
last_name:text<br>
manager_id:bigint<br>
salary:bigint<br>
sex:text<br>
target:bigint<br>
)

```sql 
with dept_avg as (
    select department, avg(salary) as avg_salary from employee
    group by department
)
select e.department, e.first_name , e.salary, d.avg_salary from employee e
join dept_avg d on e.department = d.department


```