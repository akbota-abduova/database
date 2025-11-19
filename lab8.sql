-- PART 2. BASIC INDEXES
-- EX 2.1
CREATE INDEX emp_salary_idx ON employees(salary);

SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'employees';

-- ANSWER: indexes = 2 (PRIMARY KEY + emp_salary_idx)

-- EX 2.2
CREATE INDEX emp_dept_idx ON employees(dept_id);

SELECT * FROM employees WHERE dept_id = 101;
-- ANSWER: FK columns often used in JOIN/WHERE → indexing speeds up queries

-- EX 2.3
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
ORDER BY tablename, indexname;
-- ANSWER: automatically created indexes = primary keys

-- PART 3. MULTICOLUMN INDEXES
-- EX 3.1
CREATE INDEX emp_dept_salary_idx ON employees(dept_id, salary);

SELECT emp_name, salary
FROM employees
WHERE dept_id = 101 AND salary > 52000;
-- ANSWER: Not useful if filtering ONLY salary (because dept_id is first)

-- EX 3.2
CREATE INDEX emp_salary_dept_idx ON employees(salary, dept_id);

SELECT * FROM employees WHERE dept_id = 102 AND salary > 50000;
SELECT * FROM employees WHERE salary > 50000 AND dept_id = 102;
-- ANSWER: Yes, order matters. Index works best by the first column.

-- PART 4. UNIQUE INDEXES
-- EX 4.1
ALTER TABLE employees ADD COLUMN email VARCHAR(100);

UPDATE employees SET email='john.smith@company.com' WHERE emp_id=1;
UPDATE employees SET email='jane.doe@company.com' WHERE emp_id=2;
UPDATE employees SET email='mike.johnson@company.com' WHERE emp_id=3;
UPDATE employees SET email='sarah.williams@company.com' WHERE emp_id=4;
UPDATE employees SET email='tom.brown@company.com' WHERE emp_id=5;

CREATE UNIQUE INDEX emp_email_unique_idx ON employees(email);

--INSERT INTO employees (emp_id, emp_name, dept_id, salary, email)
--VALUES (6, 'New Employee', 101, 55000, 'john.smith@company.com');
-- ANSWER: ERROR: duplicate key value violates unique constraint

-- EX 4.2
ALTER TABLE employees ADD COLUMN phone VARCHAR(20) UNIQUE;

SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename='employees' AND indexname LIKE '%phone%';

-- ANSWER: PostgreSQL creates B-TREE index automatically.

-- PART 5. ORDER BY INDEXES
-- EX 5.1
CREATE INDEX emp_salary_desc_idx ON employees(salary DESC);

SELECT emp_name, salary
FROM employees
ORDER BY salary DESC;
-- ANSWER: Index avoids sorting, speeds up ORDER BY DESC

-- EX 5.2
CREATE INDEX proj_budget_nulls_first_idx ON projects(budget NULLS FIRST);

SELECT project_name, budget
FROM projects
ORDER BY budget NULLS FIRST;

-- PART 6. EXPRESSION INDEXES
-- EX 6.1
CREATE INDEX emp_name_lower_idx ON employees(LOWER(emp_name));

SELECT * FROM employees WHERE LOWER(emp_name) = 'john smith';
-- ANSWER: Without this, PostgreSQL must scan all rows (Seq Scan)

-- EX 6.2
ALTER TABLE employees ADD COLUMN hire_date DATE;

UPDATE employees SET hire_date='2020-01-15' WHERE emp_id=1;
UPDATE employees SET hire_date='2019-06-20' WHERE emp_id=2;
UPDATE employees SET hire_date='2021-03-10' WHERE emp_id=3;
UPDATE employees SET hire_date='2020-11-05' WHERE emp_id=4;
UPDATE employees SET hire_date='2018-08-25' WHERE emp_id=5;

CREATE INDEX emp_hire_year_idx ON employees(EXTRACT(YEAR FROM hire_date));

SELECT emp_name, hire_date
FROM employees
WHERE EXTRACT(YEAR FROM hire_date)=2020;

-- PART 7. MANAGING INDEXES
-- EX 7.1
ALTER INDEX emp_salary_idx RENAME TO employees_salary_index;

SELECT indexname FROM pg_indexes WHERE tablename='employees';

-- EX 7.2
DROP INDEX emp_salary_dept_idx;
-- ANSWER: Drop index if unused, duplicate, or slows writes

-- EX 7.3
REINDEX INDEX employees_salary_index;
-- ANSWER: REINDEX fixes bloat after many INSERT/UPDATE/DELETE

-- PART 8. PRACTICAL SCENARIOS
-- EX 8.1
CREATE INDEX emp_salary_filter_idx ON employees(salary)
WHERE salary > 50000;

-- EX 8.2
CREATE INDEX proj_high_budget_idx ON projects(budget)
WHERE budget > 80000;

SELECT project_name, budget
FROM projects
WHERE budget > 80000;
-- ANSWER: Partial index = smaller, faster, only for needed rows

-- EX 8.3
EXPLAIN SELECT * FROM employees WHERE salary > 52000;
-- ANSWER: If Index Scan → index used. Seq Scan → index NOT used.

-- PART 9. INDEX TYPES
-- EX 9.1
CREATE INDEX dept_name_hash_idx ON departments USING HASH (dept_name);

SELECT * FROM departments WHERE dept_name='IT';
-- ANSWER: Use HASH only for equality =, not for ranges

-- EX 9.2
CREATE INDEX proj_name_btree_idx ON projects(project_name);
CREATE INDEX proj_name_hash_idx ON projects USING HASH (project_name);

SELECT * FROM projects WHERE project_name='Website Redesign';
SELECT * FROM projects WHERE project_name > 'Database';
-- ANSWER: Range queries work only on B-tree

-- PART 10. CLEANUP AND BEST PRACTICES
-- EX 10.1
SELECT schemaname, tablename, indexname,
       pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size
FROM pg_indexes
WHERE schemaname='public'
ORDER BY tablename, indexname;
-- ANSWER: Largest index = on columns with biggest data or long strings

-- EX 10.2
DROP INDEX IF EXISTS proj_name_hash_idx;

-- EX 10.3
CREATE VIEW index_documentation AS
SELECT tablename, indexname, indexdef,
       'Improves salary-based queries' AS purpose
FROM pg_indexes
WHERE schemaname='public' AND indexname LIKE '%salary%';

SELECT * FROM index_documentation;

-- 1. ANSWER: Default index type = B-tree
-- 2. ANSWER: Use index for WHERE, JOIN, ORDER BY
-- 3. ANSWER: Do NOT index small tables or rarely-used columns
-- 4. ANSWER: INSERT/UPDATE/DELETE slow down because index updates
-- 5. ANSWER: Use EXPLAIN or EXPLAIN ANALYZE