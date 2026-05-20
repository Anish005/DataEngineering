## Comparing Salary

Write a query that compares each employee's salary to their manager's and the average department salary (excluding the manager's salary). Display the department, employee ID, employee's salary, manager's salary, and department average salary. Order by department, then by employee salary (highest to lowest).



𝐒𝐜𝐡𝐞𝐦𝐚 𝐚𝐧𝐝 𝐃𝐚𝐭𝐚𝐬𝐞𝐭:
CREATE TABLE employee_o (id INT PRIMARY KEY,first_name VARCHAR(50),last_name VARCHAR(50),age INT,gender VARCHAR(10),employee_title VARCHAR(50),department VARCHAR(50),salary INT,manager_id INT);

INSERT INTO employee_o (id, first_name, last_name, age, gender, employee_title, department, salary, manager_id) VALUES(1, 'Alice', 'Smith', 45, 'F', 'Manager', 'HR', 9000, 1),(2, 'Bob', 'Johnson', 34, 'M', 'Assistant', 'HR', 4500, 1),(3, 'Charlie', 'Williams', 28, 'M', 'Coordinator', 'HR', 4800, 1),(4, 'Diana', 'Brown', 32, 'F', 'Manager', 'IT', 12000, 4),(5, 'Eve', 'Jones', 27, 'F', 'Analyst', 'IT', 7000, 4),(6, 'Frank', 'Garcia', 29, 'M', 'Developer', 'IT', 7500, 4),(7, 'Grace', 'Miller', 30, 'F', 'Manager', 'Finance', 10000, 7),(8, 'Hank', 'Davis', 26, 'M', 'Analyst', 'Finance', 6200, 7),(9, 'Ivy', 'Martinez', 31, 'F', 'Clerk', 'Finance', 5900, 7),(10, 'John', 'Lopez', 36, 'M', 'Manager', 'Marketing', 11000, 10),(11, 'Kim', 'Gonzales', 29, 'F', 'Specialist', 'Marketing', 6800, 10),(12, 'Leo', 'Wilson', 27, 'M', 'Coordinator', 'Marketing', 6600, 10);


```sql 
WITH manager_salary AS (
    SELECT 
        id, 
        salary,  
        department 
    FROM employee_o 
    WHERE id = manager_id               -- ✅ clever self-reference filter
),
departmental_salary AS (
    SELECT 
        AVG(salary) AS department_salary,  
        department 
    FROM employee_o 
    WHERE employee_title <> 'Manager'
    GROUP BY department
)
SELECT 
    e.department,
    e.id  AS employee_id,  
    e.salary AS employee_salary,

    CASE 
        WHEN m.salary IS NULL THEN 'No Manager'
        ELSE CAST(m.salary AS VARCHAR)
    END AS manager_salary,

    d.department_salary  AS avg_dept_salary

FROM employee_o e 
LEFT JOIN manager_salary  m ON e.manager_id = m.id        
JOIN departmental_salary d ON e.department = d.department
WHERE e.employee_title <> 'Manager'                          
ORDER BY e.department ASC, e.salary DESC;


```
