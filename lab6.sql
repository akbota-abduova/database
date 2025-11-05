DROP TABLE IF EXISTS employees;
CREATE TABLE IF NOT EXISTS employees(
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    dept_id INT,
    salary DECIMAL(10,2)
);
DROP TABLE IF EXISTS departments;
CREATE TABLE IF NOT EXISTS departments(
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50),
    location VARCHAR(50)
);
DROP TABLE IF EXISTS projects;
CREATE TABLE IF NOT EXISTS projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(50),
    dept_id INT,
    budget decimal(10, 2)
);

-- Insert data into employees
 INSERT INTO employees (emp_id, emp_name, dept_id, salary)
VALUES
 (1, 'John Smith', 101, 50000),
 (2, 'Jane Doe', 102, 60000),
 (3, 'Mike Johnson', 101, 55000),
 (4, 'Sarah Williams', 103, 65000),
 (5, 'Tom Brown', NULL, 45000);-- Insert data into departments
 INSERT INTO departments (dept_id, dept_name, location) VALUES
 (101, 'IT', 'Building A'),
 (102, 'HR', 'Building B'),
 (103, 'Finance', 'Building C'),
 (104, 'Marketing', 'Building D');-- Insert data into projects
 INSERT INTO projects (project_id, project_name, dept_id,
budget) VALUES
 (1, 'Website Redesign', 101, 100000),
 (2, 'Employee Training', 102, 50000),
 (3, 'Budget Analysis', 103, 75000),
 (4, 'Cloud Migration', 101, 150000),
 (5, 'AI Research', NULL, 200000);

--part 2.1
SELECT e.emp_name, d.dept_name
FROM employees e
CROSS JOIN departments d;
-- Answer: 5*4 = 20


-- part 2.2
SELECT e.emp_name, d.dept_name
FROM employees e, departments d;

SELECT e.emp_name, d.dept_name
FROM employees e
INNER JOIN departments d ON TRUE;

-- part 2.3
SELECT  e.emp_name, p.project_name
FROM employees e CROSS JOIN projects p
ORDER BY e.emp_name, p.project_name;
-- N*M = 5*5 = 25

-- part 3.1
SELECT e.emp_name, d.dept_name, d.location
FROM employees e
INNER JOIN departments d
ON e.dept_id = d.dept_id;
-- return 4 rows where dept_id in employees equal to dept_id in departments
-- Tom Brown not included, because his dept_id = to NULL

-- 3.2
SELECT e.emp_name, d.dept_name, d.location
FROM employees e INNER JOIN departments d USING (dept_id);
-- ON we can use with difference colum's names, but USING only with equal names. end when we select all table with ON we take 2 colum with equal variables, but with USING only one.

--3.3
SELECT emp_name, dept_name, location
FROM employees
NATURAL INNER JOIN departments;

--3.4
SELECT employees.emp_name, departments.dept_name, projects.project_name
FROM employees
INNER JOIN departments USING  (dept_id)
INNER JOIN projects USING (dept_id);

SELECT employees.emp_name, departments.dept_name, projects.project_name
FROM employees
INNER JOIN departments ON employees.dept_id = departments.dept_id
INNER JOIN projects ON projects.dept_id = departments.dept_id;

SELECT employees.emp_name, departments.dept_name, projects.project_name
FROM employees
NATURAL INNER JOIN departments
NATURAL INNER JOIN projects;


-- part 4.1
SELECT
    e.emp_name,
    e.dept_id AS emp_dept,
    d.dept_id AS dept_dept,
    d.dept_name
FROM employees e LEFT JOIN departments d ON e.dept_id = d.dept_id;
-- all id will be equal to NULL, because in LEFT JOIN result depends  from first left table;

-- 4.2
SELECT
    e.emp_name,
    e.dept_id AS emp_dept,
    d.dept_id AS dept_dept,
    d.dept_name
FROM employees e LEFT JOIN departments d USING (dept_id);

--4.3
SELECT e.emp_name, d.dept_name
FROM employees e LEFT JOIN  departments d USING (dept_id) WHERE d.dept_id IS NULL;

--4.4
SELECT
    d.dept_name,
    COUNT(e.emp_id) AS employee_count
FROM departments d LEFT JOIN employees e USING (dept_id)
GROUP BY d.dept_id, d.dept_name
ORDER BY employee_count DESC;

-- part 5.1
SELECT e.emp_name, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id;

--5.2
SELECT e.emp_name, d.dept_name
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id;

--5.3
SELECT d.dept_name, d.location
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id
WHERE e.emp_id IS NULL;

--part 6.1
SELECT
    e.emp_name,
    e.dept_id AS emp_dept,
    d.dept_id AS dept_dept,
    d.dept_name
FROM employees e
FULL JOIN departments d
    ON e.dept_id = d.dept_id;
-- employees have NULL value on the left side, and departments on tj=he right side

--6.2
SELECT
    d.dept_name,
    p.project_name,
    p.budget
FROM departments d
FULL JOIN projects p
    ON d.dept_id = p.dept_id;

--6.3
SELECT
    CASE
        WHEN e.emp_id IS NULL THEN 'Department without
employees'
        WHEN d.dept_id IS NULL THEN 'Employee without
department'
        ELSE 'Matched'
    END AS record_status,
    e.emp_name,
    d.dept_name
 FROM employees e
 FULL JOIN departments d ON e.dept_id = d.dept_id
 WHERE e.emp_id IS NULL OR d.dept_id IS NULL;

--part 7.1
SELECT e.emp_name, d.dept_name, e.salary
FROM employees e
LEFT JOIN departments d
    ON e.dept_id = d.dept_id
   AND d.location = 'Building A';

--7.2
SELECT e.emp_name, d.dept_name, e.salary
FROM employees e
LEFT JOIN departments d
  ON e.dept_id = d.dept_id
WHERE d.location = 'Building A';
-- Query 1 (ON clause): Applies the filter BEFORE the join, so all employees are included, but only departments in Building A are matched.
-- Query 2 (WHERE clause): Applies the filter AFTER the join, so employees are excluded if their department is not in Building A.

--7.3
SELECT e.emp_name, d.dept_name, e.salary
FROM employees e
INNER JOIN departments d
  ON e.dept_id = d.dept_id
 AND d.location = 'Building A';

SELECT e.emp_name, d.dept_name, e.salary
FROM employees e
INNER JOIN departments d
  ON e.dept_id = d.dept_id
WHERE d.location = 'Building A';
--No difference.
-- Because INNER JOIN only keeps rows that match in both tables,
-- and the filter (d.location = 'Building A') applies to those same matched rows,
-- regardless of whether it’s placed in the ON clause or the WHERE clause

-- part 8.1
SELECT
    d.dept_name,
    e.emp_name,
    e.salary,
    p.project_name,
    p.budget
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id
LEFT JOIN projects p ON d.dept_id = p.dept_id
ORDER BY d.dept_name, e.emp_name;

--8.2
ALTER TABLE employees
ADD COLUMN manager_id INT;

UPDATE employees SET manager_id = 3 WHERE emp_id = 1;  -- John Smith → Mike Johnson
UPDATE employees SET manager_id = 3 WHERE emp_id = 2;  -- Jane Doe → Mike Johnson
UPDATE employees SET manager_id = NULL WHERE emp_id = 3; -- Mike Johnson сам менеджер
UPDATE employees SET manager_id = 3 WHERE emp_id = 4;  -- Sarah Williams → Mike Johnson
UPDATE employees SET manager_id = 3 WHERE emp_id = 5;  -- Tom Brown → Mike Johnson

SELECT
    e.emp_name AS employee,
    m.emp_name AS manager
FROM employees e
LEFT JOIN employees m
    ON e.manager_id = m.emp_id;

--8.3
SELECT
    d.dept_name,
    AVG(e.salary) AS avg_salary
FROM departments d
INNER JOIN employees e
    ON d.dept_id = e.dept_id
GROUP BY d.dept_id, d.dept_name
HAVING AVG(e.salary) > 50000;


-- ANSWERS TO QUESTIONS
-- 1) What is the difference between INNER JOIN and LEFT JOIN?
--INNER JOIN показывает только совпадающие строки из обеих таблиц. Если нет совпадения — строка не попадает в результат.
--LEFT JOIN показывает все строки из левой таблицы, даже если совпадения нет (в этом случае правая часть заполняется NULL).

-- 2) When would you use CROSS JOIN in a practical scenario?
-- CROSS JOIN создаёт все возможные комбинации строк из двух таблиц

-- 3) Explain why the position of a filter condition (ON vs WHERE) matters for outer joins but not for inner joins.
-- При INNER JOIN — без разницы, где фильтр (ON или WHERE), потому что соединяются только совпавшие строки.
-- При OUTER JOIN (LEFT/RIGHT) — есть разница:
-- Фильтр в ON применяется во время соединения → строки без совпадений остаются, просто поля = NULL.
-- Фильтр в WHERE применяется после соединения → строки с NULL удаляются (в итоге JOIN становится как INNER).

-- 4) What is the result of:
-- SELECT COUNT(*) FROM table1 CROSS JOIN table2 if table1 has 5 rows and table2 has 10 rows?
-- 5 * 10 = 50

-- 5) How does NATURAL JOIN determine which columns to join on?
-- NATURAL JOIN автоматически соединяет таблицы по всем столбцам с одинаковыми именами
-- (например, dept_id в обеих таблицах). Он не требует писать ON или USING — SQL сам находит совпадающие колонки.

-- 6)What are the potential risks of using NATURAL JOIN?
-- Неочевидность — если добавить новую колонку с тем же именем, SQL может начать соединять по ней случайно.
-- Ошибки — легко получить неправильные данные, если одинаковые названия колонок имеют разный смысл.
-- Проблемы с совместимостью — сложно читать и отлаживать код.
-- Поэтому NATURAL JOIN используют редко, лучше явно писать ON или USING.

-- 7) Convert this LEFT JOIN to a RIGHT JOIN:
-- Результат будет тем же, потому что RIGHT JOIN — зеркальная версия LEFT JOIN.

-- 8) When should you use FULL OUTER JOIN instead of other join types?
-- Используют FULL OUTER JOIN, когда нужно увидеть все строки из обеих таблиц,даже если они не совпадают;
-- найти несвязанные данные (например, сотрудников без отдела и отделы без сотрудников);
-- объединить результаты LEFT и RIGHT JOIN.

-- Additional Challenges
--1
SELECT e.emp_name, e.dept_id AS emp_dept, d.dept_id AS dept_dept, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id

UNION

SELECT e.emp_name, e.dept_id AS emp_dept, d.dept_id AS dept_dept, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id;

--2
SELECT e.emp_name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.dept_id
WHERE d.dept_id IN (
    SELECT dept_id
    FROM projects
    WHERE dept_id IS NOT NULL
    GROUP BY dept_id
    HAVING COUNT(project_id) > 1
);

--3
SELECT
    e.emp_name AS employee,
    m.emp_name AS manager,
    mm.emp_name AS top_manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id
LEFT JOIN employees mm ON m.manager_id = mm.emp_id;

--4
SELECT
    e1.emp_name AS employee1,
    e2.emp_name AS employee2,
    d.dept_name
FROM employees e1
INNER JOIN employees e2
    ON e1.dept_id = e2.dept_id
   AND e1.emp_id < e2.emp_id
INNER JOIN departments d
    ON e1.dept_id = d.dept_id;