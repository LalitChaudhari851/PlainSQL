-- ═══════════════════════════════════════════════════════════
-- PlainSQL Enterprise — Database Schema & Seed Data
-- 6 tables: departments, employees, customers, products, sales, query_audit_log
-- ~100+ rows of realistic business data for evaluation
-- ═══════════════════════════════════════════════════════════

CREATE DATABASE IF NOT EXISTS chatbot;
USE chatbot;

-- ── Read-only user for the application ──────────────────
CREATE USER IF NOT EXISTS 'bot_user'@'%' IDENTIFIED BY 'YourSecurePassword123!';
GRANT SELECT ON chatbot.* TO 'bot_user'@'%';
FLUSH PRIVILEGES;


-- ═══════════════════════════════════════════════════════════
-- Table 1: departments
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS departments (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    budget DECIMAL(15,2) DEFAULT 0,
    location VARCHAR(100)
);

INSERT INTO departments (name, budget, location) VALUES
('Engineering', 2500000.00, 'San Francisco'),
('Sales', 1800000.00, 'New York'),
('Marketing', 1200000.00, 'New York'),
('Human Resources', 800000.00, 'Chicago'),
('Finance', 950000.00, 'Chicago'),
('Operations', 1100000.00, 'San Francisco'),
('Product', 1600000.00, 'San Francisco'),
('Customer Support', 700000.00, 'Austin');


-- ═══════════════════════════════════════════════════════════
-- Table 2: employees
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    department_id INT,
    role VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

INSERT INTO employees (name, department_id, role, salary, hire_date) VALUES
-- Engineering (dept 1)
('Alice Chen', 1, 'Senior Engineer', 145000.00, '2021-03-15'),
('Bob Martinez', 1, 'Staff Engineer', 175000.00, '2019-06-20'),
('Carol Johnson', 1, 'Engineer', 105000.00, '2023-01-10'),
('David Kim', 1, 'Engineer', 110000.00, '2022-09-05'),
('Eve Patel', 1, 'Engineering Manager', 165000.00, '2020-02-14'),
-- Sales (dept 2)
('Frank Wilson', 2, 'Sales Director', 140000.00, '2020-07-01'),
('Grace Lee', 2, 'Account Executive', 95000.00, '2022-04-18'),
('Henry Brown', 2, 'Account Executive', 92000.00, '2023-02-28'),
('Irene Davis', 2, 'Sales Rep', 72000.00, '2023-11-15'),
('Jack Thompson', 2, 'Sales Rep', 68000.00, '2024-01-20'),
-- Marketing (dept 3)
('Karen White', 3, 'Marketing Director', 135000.00, '2021-01-05'),
('Leo Garcia', 3, 'Content Manager', 85000.00, '2022-06-12'),
('Maria Rodriguez', 3, 'Marketing Analyst', 78000.00, '2023-03-22'),
-- HR (dept 4)
('Nathan Clark', 4, 'HR Director', 125000.00, '2020-10-01'),
('Olivia Scott', 4, 'Recruiter', 72000.00, '2023-05-08'),
-- Finance (dept 5)
('Peter Adams', 5, 'Finance Director', 150000.00, '2019-11-01'),
('Quinn Hall', 5, 'Financial Analyst', 88000.00, '2022-08-15'),
('Rachel Young', 5, 'Accountant', 75000.00, '2023-07-01'),
-- Operations (dept 6)
('Samuel Wright', 6, 'Operations Manager', 115000.00, '2021-04-10'),
('Tina Lopez', 6, 'Operations Analyst', 82000.00, '2023-09-14'),
-- Product (dept 7)
('Uma Sharma', 7, 'Product Manager', 140000.00, '2021-08-20'),
('Victor Nguyen', 7, 'Product Designer', 110000.00, '2022-01-15'),
('Wendy Park', 7, 'UX Researcher', 95000.00, '2023-04-01'),
-- Customer Support (dept 8)
('Xavier Jones', 8, 'Support Manager', 90000.00, '2022-03-10'),
('Yara Mitchell', 8, 'Support Specialist', 58000.00, '2024-02-05'),
('Zane Carter', 8, 'Support Specialist', 56000.00, '2025-01-15');


-- ═══════════════════════════════════════════════════════════
-- Table 3: customers
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    company VARCHAR(150),
    region VARCHAR(50),
    join_date DATE
);

INSERT INTO customers (name, company, region, join_date) VALUES
('Acme Corp', 'Acme Corporation', 'North America', '2022-01-15'),
('Globex Inc', 'Globex International', 'North America', '2022-03-20'),
('Initech', 'Initech Solutions', 'North America', '2023-01-10'),
('Umbrella Co', 'Umbrella Corporation', 'Europe', '2022-06-05'),
('Wayne Enterprises', 'Wayne Enterprises', 'North America', '2021-11-18'),
('Stark Industries', 'Stark Industries LLC', 'North America', '2023-04-22'),
('Oscorp', 'Oscorp Technologies', 'North America', '2023-07-30'),
('Cyberdyne', 'Cyberdyne Systems', 'Asia Pacific', '2022-09-14'),
('Soylent Corp', 'Soylent Corporation', 'Europe', '2023-02-28'),
('Tyrell Corp', 'Tyrell Corporation', 'Asia Pacific', '2022-12-01'),
('Massive Dynamic', 'Massive Dynamic Inc', 'Europe', '2023-08-15'),
('Wonka Industries', 'Wonka Industries', 'Europe', '2024-01-05'),
('Hooli', 'Hooli Inc', 'North America', '2024-03-10'),
('Pied Piper', 'Pied Piper LLC', 'North America', '2024-06-20'),
('Dunder Mifflin', 'Dunder Mifflin Inc', 'North America', '2025-01-08'),
('Weyland Corp', 'Weyland Corporation', 'Asia Pacific', '2023-10-25'),
('LexCorp', 'LexCorp International', 'Europe', '2024-05-12'),
('Aperture Science', 'Aperture Science LLC', 'North America', '2025-02-14'),
('MomCorp', 'MomCorp Industries', 'Asia Pacific', '2024-09-01'),
('Abstergo', 'Abstergo Industries', 'Europe', '2025-03-20');


-- ═══════════════════════════════════════════════════════════
-- Table 4: products
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_quantity INT DEFAULT 0
);

INSERT INTO products (name, category, price, stock_quantity) VALUES
('Enterprise Analytics Suite', 'Software', 2499.99, 999),
('Data Pipeline Pro', 'Software', 1299.99, 999),
('Cloud Monitoring Tool', 'Software', 899.99, 999),
('API Gateway License', 'Software', 599.99, 999),
('ML Model Server', 'Software', 3499.99, 999),
('Smart Dashboard Display', 'Hardware', 449.00, 120),
('IoT Sensor Kit', 'Hardware', 189.00, 350),
('Edge Computing Module', 'Hardware', 799.00, 85),
('Rack Server Unit', 'Hardware', 4500.00, 25),
('Network Switch Pro', 'Hardware', 1200.00, 60),
('24/7 Premium Support', 'Services', 5000.00, 999),
('Implementation Package', 'Services', 15000.00, 999),
('Training Workshop (5-day)', 'Services', 3500.00, 999),
('Security Audit Package', 'Services', 8000.00, 999),
('Data Migration Service', 'Services', 12000.00, 999),
('Wireless Keyboard', 'Hardware', 79.99, 500),
('USB-C Hub', 'Hardware', 64.99, 800),
('Noise-Canceling Headset', 'Hardware', 199.99, 200),
('Ergonomic Mouse', 'Hardware', 89.99, 400),
('Portable SSD 2TB', 'Hardware', 149.99, 150),
('Basic Support Plan', 'Services', 1200.00, 999),
('Custom Integration', 'Services', 25000.00, 999),
('Compliance Report Tool', 'Software', 749.99, 999),
('Log Aggregator', 'Software', 499.99, 999),
('Backup Solution Pro', 'Software', 999.99, 999);


-- ═══════════════════════════════════════════════════════════
-- Table 5: sales
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    customer_id INT,
    product_id INT,
    quantity INT DEFAULT 1,
    total_amount DECIMAL(12,2),
    sale_date DATE,
    FOREIGN KEY (employee_id) REFERENCES employees(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

INSERT INTO sales (employee_id, customer_id, product_id, quantity, total_amount, sale_date) VALUES
-- 2023 Q1
(6, 1, 1, 2, 4999.98, '2023-01-18'),
(7, 2, 3, 5, 4499.95, '2023-01-25'),
(6, 5, 12, 1, 15000.00, '2023-02-10'),
(8, 4, 11, 1, 5000.00, '2023-02-14'),
(7, 3, 2, 3, 3899.97, '2023-03-05'),
(9, 1, 14, 1, 8000.00, '2023-03-20'),
-- 2023 Q2
(6, 6, 1, 1, 2499.99, '2023-04-12'),
(7, 2, 5, 1, 3499.99, '2023-04-28'),
(8, 8, 7, 10, 1890.00, '2023-05-15'),
(6, 5, 15, 1, 12000.00, '2023-05-22'),
(9, 4, 13, 2, 7000.00, '2023-06-08'),
(10, 9, 4, 5, 2999.95, '2023-06-18'),
-- 2023 Q3
(7, 10, 1, 1, 2499.99, '2023-07-10'),
(6, 1, 12, 1, 15000.00, '2023-07-25'),
(8, 3, 6, 4, 1796.00, '2023-08-05'),
(7, 11, 2, 2, 2599.98, '2023-08-20'),
(6, 6, 14, 1, 8000.00, '2023-09-10'),
(9, 8, 10, 2, 2400.00, '2023-09-28'),
-- 2023 Q4
(6, 12, 5, 2, 6999.98, '2023-10-05'),
(10, 2, 11, 1, 5000.00, '2023-10-18'),
(7, 5, 1, 3, 7499.97, '2023-11-02'),
(8, 9, 9, 1, 4500.00, '2023-11-15'),
(6, 4, 15, 1, 12000.00, '2023-12-01'),
(9, 13, 3, 3, 2699.97, '2023-12-20'),
-- 2024 Q1
(6, 1, 22, 1, 25000.00, '2024-01-15'),
(7, 14, 1, 1, 2499.99, '2024-01-28'),
(8, 3, 13, 1, 3500.00, '2024-02-10'),
(6, 5, 5, 2, 6999.98, '2024-02-22'),
(10, 15, 2, 1, 1299.99, '2024-03-05'),
(7, 6, 11, 1, 5000.00, '2024-03-18'),
-- 2024 Q2
(6, 2, 12, 1, 15000.00, '2024-04-05'),
(8, 16, 1, 1, 2499.99, '2024-04-20'),
(7, 10, 14, 1, 8000.00, '2024-05-08'),
(9, 4, 7, 15, 2835.00, '2024-05-22'),
(6, 17, 5, 1, 3499.99, '2024-06-10'),
(10, 11, 13, 2, 7000.00, '2024-06-25'),
-- 2024 Q3
(7, 1, 15, 1, 12000.00, '2024-07-12'),
(6, 18, 1, 2, 4999.98, '2024-07-28'),
(8, 8, 22, 1, 25000.00, '2024-08-15'),
(6, 3, 6, 6, 2694.00, '2024-08-30'),
(9, 19, 4, 4, 2399.96, '2024-09-10'),
(7, 12, 2, 1, 1299.99, '2024-09-25'),
-- 2024 Q4
(6, 5, 11, 1, 5000.00, '2024-10-08'),
(10, 14, 9, 1, 4500.00, '2024-10-22'),
(7, 2, 5, 1, 3499.99, '2024-11-05'),
(8, 20, 12, 1, 15000.00, '2024-11-18'),
(6, 16, 1, 1, 2499.99, '2024-12-02'),
(9, 9, 14, 1, 8000.00, '2024-12-15'),
-- 2025 Q1
(6, 1, 5, 3, 10499.97, '2025-01-10'),
(7, 15, 22, 1, 25000.00, '2025-01-25'),
(8, 4, 1, 1, 2499.99, '2025-02-08'),
(6, 18, 15, 1, 12000.00, '2025-02-20'),
(10, 13, 11, 1, 5000.00, '2025-03-05'),
(7, 6, 2, 2, 2599.98, '2025-03-18'),
-- 2025 Q2
(6, 2, 12, 1, 15000.00, '2025-04-02'),
(8, 20, 5, 1, 3499.99, '2025-04-15');


-- ═══════════════════════════════════════════════════════════
-- Table 6: query_audit_log (observability & compliance)
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS query_audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(50),
    tenant_id VARCHAR(50) DEFAULT 'default',
    query_text TEXT NOT NULL,
    generated_sql TEXT,
    intent VARCHAR(30),
    execution_time_ms FLOAT,
    row_count INT DEFAULT 0,
    status ENUM('success', 'blocked', 'error') NOT NULL,
    error_message TEXT,
    trace_id VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_audit_user (user_id),
    INDEX idx_audit_tenant (tenant_id),
    INDEX idx_audit_created (created_at),
    INDEX idx_audit_status (status)
);

-- Grant INSERT on audit log to the bot user (write-only for logging)
GRANT INSERT ON chatbot.query_audit_log TO 'bot_user'@'%';
FLUSH PRIVILEGES;