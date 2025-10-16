DROP SCHEMA IF EXISTS lab_constraints CASCADE;
CREATE SCHEMA lab_constraints;
SET search_path TO lab_constraints;

-- Part 1: CHECK Constraints

-- Task 1.1: Basic CHECK (employees)
DROP TABLE IF EXISTS employees CASCADE;
CREATE TABLE employees (
    employee_id   INTEGER PRIMARY KEY,
    first_name    TEXT,
    last_name     TEXT,
    age           INTEGER     CHECK (age BETWEEN 18 AND 65),
    salary        NUMERIC(12,2) CHECK (salary > 0)
);
/* Constraints:
   - age must be between 18 and 65 inclusive
   - salary must be > 0
*/

-- Valid data
INSERT INTO employees VALUES
(1,'Aruzhan','Kaly',25,250000.00),
(2,'Timur','Bek',40,450000.00);

-- Invalid attempts (uncomment one by one to test)
-- INSERT INTO employees VALUES (3,'Ali','TooYoung',17,100000.00);
--   -- ERROR:  new row for relation "employees" violates check constraint "employees_age_check"
-- INSERT INTO employees VALUES (4,'Aika','ZeroSalary',30,0);
--   -- ERROR:  violates check constraint "employees_salary_check"

-- Task 1.2: Named CHECK (products_catalog)
DROP TABLE IF EXISTS products_catalog CASCADE;
CREATE TABLE products_catalog (
    product_id     INTEGER PRIMARY KEY,
    product_name   TEXT NOT NULL,
    regular_price  NUMERIC(12,2),
    discount_price NUMERIC(12,2),
    CONSTRAINT valid_discount CHECK (
        regular_price > 0
        AND discount_price > 0
        AND discount_price < regular_price
    )
);

-- Valid data
INSERT INTO products_catalog VALUES
(10,'Notebook', 1500.00,1200.00),
(11,'Headphones', 800.00, 699.99);

-- Invalid attempts
-- INSERT INTO products_catalog VALUES (12,'BadZero', 0, 0.01);
--   -- ERROR: violates "valid_discount" (regular_price > 0)
-- INSERT INTO products_catalog VALUES (13,'BadDisc', 100, 100);
--   -- ERROR: violates "valid_discount" (discount_price < regular_price)

-- Task 1.3: Multiple column CHECK (bookings)
DROP TABLE IF EXISTS bookings CASCADE;
CREATE TABLE bookings (
    booking_id     INTEGER PRIMARY KEY,
    check_in_date  DATE,
    check_out_date DATE,
    num_guests     INTEGER CHECK (num_guests BETWEEN 1 AND 10),
    CONSTRAINT chk_dates CHECK (check_out_date > check_in_date)
);

-- Valid data
INSERT INTO bookings VALUES
(100, DATE '2025-10-15', DATE '2025-10-20', 2),
(101, DATE '2025-11-01', DATE '2025-11-05', 4);

-- Invalid attempts
-- INSERT INTO bookings VALUES (102, DATE '2025-10-20', DATE '2025-10-19', 2);
--   -- ERROR: violates "chk_dates" (check_out_date must be after check_in_date)
-- INSERT INTO bookings VALUES (103, DATE '2025-12-01', DATE '2025-12-10', 0);
--   -- ERROR: violates "bookings_num_guests_check" (1..10)

-- Part 2: NOT NULL Constraints

-- Task 2.1: customers (NOT NULL)
DROP TABLE IF EXISTS customers CASCADE;
CREATE TABLE customers (
    customer_id       INTEGER     NOT NULL,
    email             TEXT        NOT NULL,
    phone             TEXT,                  -- nullable
    registration_date DATE        NOT NULL,
    PRIMARY KEY (customer_id)
);

-- Task 2.2: inventory (NOT NULL + CHECK)
DROP TABLE IF EXISTS inventory CASCADE;
CREATE TABLE inventory (
    item_id      INTEGER     NOT NULL,
    item_name    TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL CHECK (quantity >= 0),
    unit_price   NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    last_updated TIMESTAMP   NOT NULL,
    PRIMARY KEY (item_id)
);

-- Task 2.3: Testing NOT NULL
-- Valid
INSERT INTO customers VALUES
(1,'aika@example.com','+77010000001', DATE '2025-10-01'),
(2,'dias@example.com',NULL,           DATE '2025-10-02');

INSERT INTO inventory VALUES
(1,'SSD 1TB',10,39999.99, now()),
(2,'USB-C Cable',200,1999.00, now());

-- Invalid attempts (NOT NULL / CHECK)
-- INSERT INTO customers (customer_id, email, registration_date) VALUES (3, NULL, DATE '2025-10-03');
--   -- ERROR: null value in column "email" violates not-null constraint
-- INSERT INTO inventory VALUES (3,'Mouse',NULL,5999.00, now());
--   -- ERROR: null value in column "quantity" violates not-null constraint
-- INSERT INTO inventory VALUES (4,'BadPrice',5,0, now());
--   -- ERROR: violates check constraint (unit_price > 0)

-- Part 3: UNIQUE Constraints

-- Task 3.1 + 3.3: users with named UNIQUE constraints
DROP TABLE IF EXISTS users CASCADE;
CREATE TABLE users (
    user_id    INTEGER PRIMARY KEY,
    username   TEXT NOT NULL,
    email      TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT unique_username UNIQUE (username),
    CONSTRAINT unique_email    UNIQUE (email)
);

-- Valid
INSERT INTO users (user_id, username, email) VALUES
(1,'kbtushnik','kbt@email.kz'),
(2,'timur','timur@email.kz');

-- Invalid (UNIQUE)
-- INSERT INTO users (user_id, username, email) VALUES (3,'kbtushnik','other@email.kz');
--   -- ERROR: duplicate key value violates unique constraint "unique_username"
-- INSERT INTO users (user_id, username, email) VALUES (4,'other','timur@email.kz');
--   -- ERROR: duplicate key value violates unique constraint "unique_email"

-- Task 3.2: Multi-Column UNIQUE (course_enrollments)
DROP TABLE IF EXISTS course_enrollments CASCADE;
CREATE TABLE course_enrollments (
    enrollment_id INTEGER PRIMARY KEY,
    student_id    INTEGER NOT NULL,
    course_code   TEXT    NOT NULL,
    semester      TEXT    NOT NULL,
    CONSTRAINT uniq_student_course_sem UNIQUE (student_id, course_code, semester)
);

-- Valid
INSERT INTO course_enrollments VALUES
(1,1001,'CS101','Fall-2025'),
(2,1001,'CS102','Fall-2025'),
(3,1002,'CS101','Fall-2025');

-- Invalid duplicate of (student_id, course_code, semester)
-- INSERT INTO course_enrollments VALUES (4,1001,'CS101','Fall-2025');
--   -- ERROR: duplicate key value violates unique constraint "uniq_student_course_sem"

-- Part 4: PRIMARY KEY Constraints

-- Task 4.1: departments (single PK) + tests
DROP TABLE IF EXISTS departments CASCADE;
CREATE TABLE departments (
    dept_id   INTEGER PRIMARY KEY,
    dept_name TEXT NOT NULL,
    location  TEXT
);

INSERT INTO departments VALUES
(10,'Engineering','Almaty'),
(20,'HR','Astana'),
(30,'Finance','Shymkent');

-- Invalid attempts
-- INSERT INTO departments VALUES (10,'Duplicate','Oral');
--   -- ERROR: duplicate key value violates unique constraint "departments_pkey"
-- INSERT INTO departments VALUES (NULL,'NullPK','Atyrau');
--   -- ERROR: null value in column "dept_id" violates not-null constraint

-- Task 4.2: student_courses (composite PK)
DROP TABLE IF EXISTS student_courses CASCADE;
CREATE TABLE student_courses (
    student_id      INTEGER NOT NULL,
    course_id       INTEGER NOT NULL,
    enrollment_date DATE,
    grade           TEXT,
    PRIMARY KEY (student_id, course_id)
);

-- Valid
INSERT INTO student_courses VALUES
(2001, 501, DATE '2025-09-01', 'A'),
(2001, 502, DATE '2025-09-01', 'B'),
(2002, 501, DATE '2025-09-01', 'A');

-- Invalid composite duplicate
-- INSERT INTO student_courses VALUES (2001, 501, DATE '2025-09-10', 'A');
--   -- ERROR: duplicate key value violates unique constraint "student_courses_pkey"

-- Task 4.3: Comparison (document in comments)
-- UNIQUE vs PRIMARY KEY:
-- * Both enforce uniqueness.
-- * PRIMARY KEY also implies NOT NULL and identifies the row; table can have only one PRIMARY KEY.
-- * A table may have multiple UNIQUE constraints, but only one PRIMARY KEY.
-- When to use single vs composite PK:
-- * Single-column PK when one natural/key column identifies rows (e.g., dept_id).
-- * Composite PK when the combination uniquely identifies rows (e.g., student_id+course_id).
-- Why only one PK but many UNIQUE:
-- * The PK is the table’s canonical identifier; databases allow only one such identifier.
-- * UNIQUE just enforces additional uniqueness rules on other columns/combos.

-- Part 5: FOREIGN KEY Constraints
-- Task 5.1: employees_dept referencing departments
DROP TABLE IF EXISTS employees_dept CASCADE;
CREATE TABLE employees_dept (
    emp_id    INTEGER PRIMARY KEY,
    emp_name  TEXT NOT NULL,
    dept_id   INTEGER REFERENCES departments(dept_id),
    hire_date DATE
);

-- Valid (dept_id exists)
INSERT INTO employees_dept VALUES
(10001,'Aibek',10,DATE '2025-10-10'),
(10002,'Dana',20,DATE '2025-10-12');

-- Invalid FK (dept_id 99 does not exist)
-- INSERT INTO employees_dept VALUES (10003,'NonExistDept',99,DATE '2025-10-13');
--   -- ERROR: insert or update on table "employees_dept" violates foreign key constraint

-- Task 5.2: Library schema with multiple FKs
DROP TABLE IF EXISTS books CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS publishers CASCADE;

CREATE TABLE authors (
    author_id   INTEGER PRIMARY KEY,
    author_name TEXT NOT NULL,
    country     TEXT
);

CREATE TABLE publishers (
    publisher_id   INTEGER PRIMARY KEY,
    publisher_name TEXT NOT NULL,
    city           TEXT
);

CREATE TABLE books (
    book_id         INTEGER PRIMARY KEY,
    title           TEXT NOT NULL,
    author_id       INTEGER REFERENCES authors(author_id),
    publisher_id    INTEGER REFERENCES publishers(publisher_id),
    publication_year INTEGER,
    isbn            TEXT UNIQUE
);

-- Sample data
INSERT INTO authors VALUES
(1,'Fyodor Dostoevsky','Russia'),
(2,'Abay Kunanbayuly','Kazakhstan'),
(3,'Jane Austen','United Kingdom');

INSERT INTO publishers VALUES
(1,'Penguin','London'),
(2,'Atamura','Almaty');

INSERT INTO books VALUES
(100,'Crime and Punishment',1,1,1866,'9780140449136'),
(101,'Abay Zholy',2,2,1942,'9786018011607'),
(102,'Pride and Prejudice',3,1,1813,'9780141199078');

-- Task 5.3: ON DELETE options demo
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products_fk CASCADE;
DROP TABLE IF EXISTS categories CASCADE;

CREATE TABLE categories (
    category_id   INTEGER PRIMARY KEY,
    category_name TEXT NOT NULL
);

CREATE TABLE products_fk (
    product_id   INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category_id  INTEGER REFERENCES categories(category_id) ON DELETE RESTRICT
    -- RESTRICT: cannot delete a category if products reference it
);

CREATE TABLE orders (
    order_id   INTEGER PRIMARY KEY,
    order_date DATE NOT NULL
);

CREATE TABLE order_items (
    item_id    INTEGER PRIMARY KEY,
    order_id   INTEGER REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products_fk(product_id),
    quantity   INTEGER CHECK (quantity > 0)
    -- CASCADE: deleting an order deletes its order_items
);

-- Seed data
INSERT INTO categories VALUES (1,'Electronics'),(2,'Books');
INSERT INTO products_fk VALUES (1001,'Laptop',1),(1002,'Novel',2);

INSERT INTO orders VALUES (9001, DATE '2025-10-10');
INSERT INTO order_items VALUES (1,9001,1001,1),(2,9001,1002,2);

-- Scenario 1: Try to delete a category that has products (should fail)
-- DELETE FROM categories WHERE category_id = 1;
--   -- EXPECT: ERROR (RESTRICT) because products_fk rows reference category 1

-- Scenario 2: Delete an order and see items auto-deleted
-- SELECT COUNT(*) AS before_items FROM order_items WHERE order_id=9001;
-- DELETE FROM orders WHERE order_id=9001;            -- CASCADE to order_items
-- SELECT COUNT(*) AS after_items  FROM order_items WHERE order_id=9001;
--   -- EXPECT: before_items=2, after_items=0

-- Part 6: Practical Application – E-commerce schema

/* Requirements recap:
   Tables:
     customers (PK, email UNIQUE)
     products  (PK, price>=0, stock_quantity>=0)
     orders    (PK, FK to customers, status in allowed set)
     order_details (PK, FK to orders & products, quantity>0, unit_price>0, CASCADE on order delete)
*/

DROP TABLE IF EXISTS order_details CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- Recreate customers for the e-commerce domain (separate from Part 2)
CREATE TABLE customers (
    customer_id       INTEGER PRIMARY KEY,
    name              TEXT    NOT NULL,
    email             TEXT    NOT NULL UNIQUE,
    phone             TEXT,
    registration_date DATE    NOT NULL
);

CREATE TABLE products (
    product_id     INTEGER PRIMARY KEY,
    name           TEXT    NOT NULL,
    description    TEXT,
    price          NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0)
);

CREATE TABLE orders (
    order_id     INTEGER PRIMARY KEY,
    customer_id  INTEGER NOT NULL REFERENCES customers(customer_id) ON DELETE RESTRICT,
    order_date   DATE    NOT NULL,
    total_amount NUMERIC(14,2) NOT NULL CHECK (total_amount >= 0),
    status       TEXT    NOT NULL CHECK (status IN ('pending','processing','shipped','delivered','cancelled'))
);

CREATE TABLE order_details (
    order_detail_id INTEGER PRIMARY KEY,
    order_id        INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id      INTEGER NOT NULL REFERENCES products(product_id),
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price > 0)
    -- (Optional) You could add a CHECK to ensure unit_price equals products.price at time of order,
    -- but typically we store the snapshot price here.
);

-- Sample data (≥5 per table)

-- customers (5)
INSERT INTO customers VALUES
(1,'Aruzhan Kaldayeva','aruzhan@example.com','+77010000001', DATE '2025-09-15'),
(2,'Timur Serik','timur@example.com','+77010000002', DATE '2025-09-20'),
(3,'Dana Ali','dana@example.com',NULL,               DATE '2025-10-01'),
(4,'Askar Nurtas','askar@example.com','+77010000004',DATE '2025-10-05'),
(5,'Amina Kuat','amina@example.com','+77010000005',  DATE '2025-10-10');

-- products (5)
INSERT INTO products VALUES
(101,'Phone X','128GB, 6.1"', 299999.00, 25),
(102,'Phone Case','Silicone',  4999.00,   200),
(103,'Charger 30W','Fast',     9999.00,   150),
(104,'Laptop Z','16GB/512GB',  799999.00, 10),
(105,'Headset Pro','BT 5.3',   59999.00,  50);

-- orders (5)
INSERT INTO orders VALUES
(5001,1,DATE '2025-10-12', 314, 'pending'),     -- total_amount will be updated below
(5002,2,DATE '2025-10-12', 314, 'processing'),
(5003,3,DATE '2025-10-13', 314, 'shipped'),
(5004,4,DATE '2025-10-13', 314, 'delivered'),
(5005,5,DATE '2025-10-14', 314, 'pending');

-- order_details (≥5; multiple lines per order)
INSERT INTO order_details VALUES
(90001,5001,101,1,299999.00),
(90002,5001,102,1,   4999.00),
(90003,5002,104,1,799999.00),
(90004,5002,103,2,   9999.00),
(90005,5003,105,1,  59999.00),
(90006,5003,103,1,   9999.00),
(90007,5004,101,1,299999.00),
(90008,5004,103,1,   9999.00),
(90009,5005,102,2,   4999.00),
(90010,5005,105,1,  59999.00);

-- Recompute total_amounts (demo sum of details)
UPDATE orders o
SET total_amount = s.sum_amount
FROM (
  SELECT order_id, SUM(quantity * unit_price)::NUMERIC(14,2) AS sum_amount
  FROM order_details
  GROUP BY order_id
) s
WHERE o.order_id = s.order_id;

-- Quick check:
-- SELECT order_id, total_amount FROM orders ORDER BY order_id;

-- Constraint tests for e-commerce (uncomment to see errors):

-- 1) Duplicate email (UNIQUE)
-- INSERT INTO customers VALUES (6,'Dup Email','aruzhan@example.com',NULL, DATE '2025-10-14');
--   -- ERROR: duplicate key value violates unique constraint "customers_email_key"

-- 2) Invalid status
-- INSERT INTO orders VALUES (6000,1,DATE '2025-10-14',100,'on_hold');
--   -- ERROR: violates check constraint (status IN ...)

-- 3) Negative price / stock
-- INSERT INTO products VALUES (200,'Bad Price','x',-1,10);
--   -- ERROR: violates check (price >= 0)
-- INSERT INTO products VALUES (201,'Bad Stock','x',1000,-5);
--   -- ERROR: violates check (stock_quantity >= 0)

-- 4) Quantity not positive
-- INSERT INTO order_details VALUES (91000,5001,101,0,299999.00);
--   -- ERROR: violates check (quantity > 0)

-- 5) Foreign key delete behavior:
--    Delete an order -> corresponding order_details must be removed (CASCADE)
-- SELECT COUNT(*) AS before_items FROM order_details WHERE order_id=5001;
-- DELETE FROM orders WHERE order_id=5001;     -- CASCADE
-- SELECT COUNT(*) AS after_items FROM order_details WHERE order_id=5001;

--    Try deleting a customer referenced by orders (RESTRICT -> should fail)
-- DELETE FROM customers WHERE customer_id=2;
--   -- EXPECT: ERROR due to FK ON DELETE RESTRICT from orders -> customers

-- Optional stock check example query (not a constraint):
-- SELECT p.product_id, p.name, p.stock_quantity,
--        COALESCE(SUM(od.quantity),0) AS total_ordered
-- FROM products p
-- LEFT JOIN order_details od ON od.product_id = p.product_id
-- GROUP BY p.product_id, p.name, p.stock_quantity
-- ORDER BY p.product_id;