-- ************************************************************
-- LABORATORY WORK #3 - DML Operations (Advanced)
-- ************************************************************

-- ОЧИСТКА ДЛЯ ГАРАНТИРОВАННОГО ЗАПУСКА В POSTGRESQL
DROP SCHEMA public CASCADE;
CREATE SCHEMA public;

-- Task A
CREATE TABLE IF NOT EXISTS employees (
    emp_id     SERIAL PRIMARY KEY,
	first_name VARCHAR(100),
	last_name  VARCHAR(100),
	department VARCHAR(100),
	salary     INTEGER,
	hire_date  DATE,
	status     VARCHAR(50) DEFAULT 'Active'
);

CREATE TABLE IF NOT EXISTS departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(100),
    budget INTEGER,
    manager_id INTEGER
);

CREATE TABLE IF NOT EXISTS projects (
    project_id    SERIAL PRIMARY KEY,
    project_name  VARCHAR(150),
    dept_id       INTEGER,
    start_date    DATE,
    end_date      DATE,
    budget        INTEGER,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- ДОБАВЛЕНИЕ НАЧАЛЬНЫХ ДАННЫХ ДЛЯ ТЕСТИРОВАНИЯ
INSERT INTO departments (dept_name, budget, manager_id)
VALUES
    ('IT', 200000, 1),
    ('HR', 150000, 2),
    ('Finance', 300000, 1);

INSERT INTO employees (first_name, last_name, department, salary, hire_date, status)
VALUES 
('Alex', 'Smith', 'IT', 65000, '2019-10-15', 'Active'),
('Maria', 'Jones', 'HR', 78000, '2019-05-20', 'Active'),
('David', 'Lee', 'Sales', 58000, '2021-01-10', 'Active');

INSERT INTO projects (project_name, dept_id, start_date, end_date, budget)
VALUES
('System Upgrade', 1, '2022-06-01', '2022-12-31', 80000),
('Recruitment Drive', 2, '2023-01-01', '2023-05-15', 30000);

-- Task B
INSERT INTO employees (first_name, last_name, department)
VALUES ('Doremifa', 'Solasi', 'MUSIC');

INSERT INTO employees (first_name, last_name, department, salary, status)
VALUES ('Data', 'Bases', 'IT', DEFAULT, DEFAULT);

INSERT INTO employees (first_name, last_name, department, hire_date, salary)
VALUES ('Mikky', 'Mouse', 'Sales', CURRENT_DATE, 50000 * 1.1);

DROP TABLE IF EXISTS temp_employees;
CREATE TEMP TABLE temp_employees AS
SELECT *
FROM employees
WHERE department = 'IT';

-- Part C
UPDATE employees
SET salary = salary * 1.10;

UPDATE employees
SET status = 'Senior'
WHERE salary > 60000
  AND hire_date < '2020-01-01';

UPDATE employees
SET department = CASE
    WHEN salary > 80000 THEN 'Management'
    WHEN salary BETWEEN 50000 AND 80000 THEN 'Senior'
    ELSE 'Junior'
END;

UPDATE employees
SET department = DEFAULT
WHERE status = 'Inactive';

UPDATE departments d
SET budget = (
  SELECT AVG(e.salary) * 1.20
  FROM employees e
  WHERE e.department = d.dept_name
);

UPDATE employees
SET salary = salary * 1.15,
    status = 'Promoted'
WHERE department = 'Sales';

-- Part D
INSERT INTO employees (first_name, last_name, status, salary, hire_date)
VALUES ('Zoe', 'Term', 'Terminated', 50000, '2021-01-01'),
       ('Young', 'New', NULL, 35000, '2024-01-01');

DELETE FROM employees
WHERE status = 'Terminated';

DELETE FROM employees
WHERE salary < 40000
  AND hire_date > '2023-01-01'
  AND department IS NULL;

DELETE FROM departments d
WHERE d.dept_name NOT IN (
  SELECT DISTINCT department
  FROM employees
  WHERE department IS NOT NULL
);

DELETE FROM projects
WHERE end_date < '2023-01-01'
RETURNING *;

-- Part E
INSERT INTO employees (first_name, last_name, department, salary)
VALUES ('Kto', 'To', NULL, NULL);

UPDATE employees
SET department = 'Unassigned'
WHERE department IS NULL;

DELETE FROM employees
WHERE salary IS NULL
   OR department IS NULL;

-- Part F
INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES ('Return', 'Test', 'QA', 45000, CURRENT_DATE)
RETURNING emp_id, (first_name || ' ' || last_name) AS full_name;

UPDATE employees
SET salary = salary + 5000
WHERE department = 'IT'
RETURNING emp_id,
          salary - 5000 AS old_salary,
          salary AS new_salary;

DELETE FROM employees
WHERE hire_date < '2020-01-01'
RETURNING *;

-- Part G
INSERT INTO employees (first_name, last_name, department, salary, hire_date)
SELECT 'Unique', 'Person', 'R&D', 52000, CURRENT_DATE
WHERE NOT EXISTS (
  SELECT 1
  FROM employees
  WHERE first_name = 'Unique' AND last_name = 'Person'
);

UPDATE employees e
SET salary = salary * CASE
  WHEN (
    SELECT d.budget
    FROM departments d
    WHERE d.dept_name = e.department
  ) > 100000 THEN 1.10
  ELSE 1.05
END;

INSERT INTO employees (first_name, last_name, department, salary, hire_date)
VALUES
  ('I','for','depa',40000,CURRENT_DATE),
  ('dont','create','depa',42000,CURRENT_DATE),
  ('have','new','depa',44000,CURRENT_DATE),
  ('a','names','depa',46000,CURRENT_DATE),
  ('fantasy','all time','depa',48000,CURRENT_DATE);

UPDATE employees
SET salary = salary * 1.10
WHERE department = 'depa';

CREATE TABLE IF NOT EXISTS employee_archive AS
SELECT * FROM employees WHERE 1=0;

WITH moved_rows AS (
    DELETE FROM employees
    WHERE status = 'Inactive'
    RETURNING *
)
INSERT INTO employee_archive
SELECT * FROM moved_rows;

UPDATE projects p
SET end_date = COALESCE(end_date, CURRENT_DATE) + INTERVAL '30 days'
WHERE p.budget > 50000
  AND (
    SELECT COUNT(*)
    FROM employees e
    WHERE e.department = (
      SELECT d.dept_name
      FROM departments d
      WHERE d.dept_id = p.dept_id
    )
  ) > 3;

-- ФИНАЛЬНАЯ ПРОВЕРКА
SELECT * FROM employees;
SELECT * FROM departments;
SELECT * FROM projects;
SELECT * FROM employee_archive;