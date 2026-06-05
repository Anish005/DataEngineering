## Schema for Questions
```sql
-- create table
CREATE TABLE employees (
 id INT PRIMARY KEY,
 name VARCHAR(100),
 department VARCHAR(100),
 manager_id INT,
 salary DECIMAL(10,2),
 hire_date DATE
);

-- insert the records 
INSERT INTO employees (id, name, department, manager_id, salary, hire_date)
VALUES
(1, 'Alice', 'HR', NULL, 70000, '2015-06-23'),
(2, 'Bob', 'IT', 1, 90000, '2016-09-17'),
(3, 'Charlie', 'Finance', 1, 80000, '2017-02-01'),
(4, 'David', 'IT', 2, 75000, '2018-07-11'),
(5, 'Eve', 'Finance', 3, 72000, '2019-04-30');

-- q1 : retrive employees and their managers recursively

with employeeHierarchy as (
    select id,name ,manager_id ,1 as level from employees
    where manager_id is null
    union all
    select e.id, e.name ,e.manager, eh.level+1 from employeeHierarchy eh on e.manager_id=eh.id

)
select * from employeeHierarchy order by level;

--q2 calculate the number of organisational hierarchy
with Hierarchy as (
    select id,name ,manager_id ,1 as level from employees
    where manager_id is null
    union all
    select e.id, e.name ,e.manager, eh.level+1 from Hierarchy eh on e.manager_id=eh.id
)
select max(level) as num_level from Hierarchy;

--q3: calculate cummulative salary within each department

with cummulative_salary as (
    select sum(salary) over(partition by department) as running_salary from employees
)
select * from cummulative_salary;

```
