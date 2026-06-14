-- PlainSQL Enterprise Demo Dataset
-- Domain: B2B SaaS revenue, product usage, support, sales pipeline, and AI observability.
-- Scale target: 18 tables and 27K+ generated rows for realistic text-to-SQL demos.

CREATE DATABASE IF NOT EXISTS chatbot;
USE chatbot;

-- CREATE USER IF NOT EXISTS 'bot_user'@'%' IDENTIFIED BY 'YourSecurePassword123!';
-- GRANT SELECT ON chatbot.* TO 'bot_user'@'%';

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS ticket_events;
DROP TABLE IF EXISTS support_tickets;
DROP TABLE IF EXISTS product_usage_daily;
DROP TABLE IF EXISTS query_audit_log;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS invoices;
DROP TABLE IF EXISTS subscriptions;
DROP TABLE IF EXISTS opportunities;
DROP TABLE IF EXISTS workspace_users;
DROP TABLE IF EXISTS workspaces;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS accounts;
DROP TABLE IF EXISTS feature_catalog;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS plans;
DROP TABLE IF EXISTS incidents;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE departments (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    cost_center VARCHAR(20) NOT NULL,
    region VARCHAR(40) NOT NULL,
    annual_budget DECIMAL(14,2) NOT NULL
);

CREATE TABLE employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    department_id INT NOT NULL,
    manager_id INT NULL,
    full_name VARCHAR(120) NOT NULL,
    role VARCHAR(80) NOT NULL,
    location VARCHAR(80) NOT NULL,
    hire_date DATE NOT NULL,
    base_salary DECIMAL(12,2) NOT NULL,
    quota_arr DECIMAL(14,2) DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
);

CREATE TABLE plans (
    plan_id INT AUTO_INCREMENT PRIMARY KEY,
    plan_name VARCHAR(80) NOT NULL,
    billing_model ENUM('seat_based','usage_based','hybrid') NOT NULL,
    monthly_base_price DECIMAL(12,2) NOT NULL,
    included_seats INT NOT NULL,
    support_tier ENUM('standard','premium','enterprise') NOT NULL
);

CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(120) NOT NULL,
    product_family VARCHAR(80) NOT NULL,
    launch_date DATE NOT NULL,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE feature_catalog (
    feature_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    feature_name VARCHAR(120) NOT NULL,
    feature_category VARCHAR(80) NOT NULL,
    risk_level ENUM('low','medium','high') DEFAULT 'low',
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE accounts (
    account_id INT AUTO_INCREMENT PRIMARY KEY,
    account_name VARCHAR(160) NOT NULL,
    industry VARCHAR(80) NOT NULL,
    segment ENUM('startup','mid_market','enterprise','strategic') NOT NULL,
    region VARCHAR(60) NOT NULL,
    country VARCHAR(60) NOT NULL,
    employee_count INT NOT NULL,
    arr_band ENUM('under_25k','25k_100k','100k_500k','500k_plus') NOT NULL,
    health_score INT NOT NULL,
    churn_risk ENUM('low','medium','high') NOT NULL,
    created_at DATE NOT NULL
);

CREATE TABLE contacts (
    contact_id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    full_name VARCHAR(140) NOT NULL,
    title VARCHAR(120) NOT NULL,
    department VARCHAR(80) NOT NULL,
    email VARCHAR(180) NOT NULL,
    is_executive_sponsor BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE workspaces (
    workspace_id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    workspace_name VARCHAR(160) NOT NULL,
    environment ENUM('production','staging','sandbox') NOT NULL,
    cloud_region VARCHAR(50) NOT NULL,
    created_at DATE NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE workspace_users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    workspace_id INT NOT NULL,
    account_id INT NOT NULL,
    email VARCHAR(180) NOT NULL,
    role ENUM('admin','analyst','viewer','engineer') NOT NULL,
    last_active_at DATETIME NOT NULL,
    is_service_account BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE subscriptions (
    subscription_id INT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    plan_id INT NOT NULL,
    owner_employee_id INT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NULL,
    status ENUM('active','past_due','cancelled','trialing') NOT NULL,
    seats_purchased INT NOT NULL,
    contracted_arr DECIMAL(14,2) NOT NULL,
    discount_pct DECIMAL(5,2) DEFAULT 0,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (plan_id) REFERENCES plans(plan_id),
    FOREIGN KEY (owner_employee_id) REFERENCES employees(employee_id)
);

CREATE TABLE invoices (
    invoice_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    subscription_id INT NOT NULL,
    account_id INT NOT NULL,
    invoice_date DATE NOT NULL,
    due_date DATE NOT NULL,
    status ENUM('paid','open','void','uncollectible') NOT NULL,
    subtotal DECIMAL(14,2) NOT NULL,
    tax DECIMAL(14,2) NOT NULL,
    total DECIMAL(14,2) NOT NULL,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(subscription_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    INDEX idx_invoices_date (invoice_date),
    INDEX idx_invoices_status (status)
);

CREATE TABLE payments (
    payment_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    invoice_id BIGINT NOT NULL,
    account_id INT NOT NULL,
    paid_at DATETIME NOT NULL,
    payment_method ENUM('ach','wire','card','credits') NOT NULL,
    amount DECIMAL(14,2) NOT NULL,
    payment_status ENUM('succeeded','failed','refunded') NOT NULL,
    FOREIGN KEY (invoice_id) REFERENCES invoices(invoice_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
);

CREATE TABLE opportunities (
    opportunity_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    owner_employee_id INT NOT NULL,
    opportunity_name VARCHAR(180) NOT NULL,
    stage ENUM('prospecting','qualification','technical_validation','proposal','negotiation','closed_won','closed_lost') NOT NULL,
    source VARCHAR(80) NOT NULL,
    forecast_category ENUM('pipeline','best_case','commit','closed') NOT NULL,
    expected_arr DECIMAL(14,2) NOT NULL,
    probability_pct INT NOT NULL,
    created_date DATE NOT NULL,
    close_date DATE NOT NULL,
    last_activity_date DATE NOT NULL,
    loss_reason VARCHAR(120) NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (owner_employee_id) REFERENCES employees(employee_id),
    INDEX idx_opps_stage (stage),
    INDEX idx_opps_close_date (close_date)
);

CREATE TABLE product_usage_daily (
    usage_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    workspace_id INT NOT NULL,
    account_id INT NOT NULL,
    feature_id INT NOT NULL,
    usage_date DATE NOT NULL,
    active_users INT NOT NULL,
    query_count INT NOT NULL,
    successful_query_count INT NOT NULL,
    failed_query_count INT NOT NULL,
    p95_latency_ms INT NOT NULL,
    compute_seconds INT NOT NULL,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (feature_id) REFERENCES feature_catalog(feature_id),
    INDEX idx_usage_date (usage_date),
    INDEX idx_usage_account_date (account_id, usage_date)
);

CREATE TABLE support_tickets (
    ticket_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id INT NOT NULL,
    workspace_id INT NULL,
    contact_id INT NULL,
    assigned_employee_id INT NULL,
    priority ENUM('P1','P2','P3','P4') NOT NULL,
    status ENUM('open','pending_customer','resolved','closed') NOT NULL,
    category VARCHAR(80) NOT NULL,
    subject VARCHAR(220) NOT NULL,
    created_at DATETIME NOT NULL,
    first_response_minutes INT NOT NULL,
    resolution_minutes INT NULL,
    csat_score INT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(account_id),
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
    FOREIGN KEY (contact_id) REFERENCES contacts(contact_id),
    FOREIGN KEY (assigned_employee_id) REFERENCES employees(employee_id),
    INDEX idx_ticket_status_priority (status, priority)
);

CREATE TABLE ticket_events (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticket_id BIGINT NOT NULL,
    event_time DATETIME NOT NULL,
    actor_type ENUM('customer','agent','system') NOT NULL,
    event_type ENUM('created','comment','status_change','escalated','sla_breach','resolved') NOT NULL,
    note VARCHAR(300) NOT NULL,
    FOREIGN KEY (ticket_id) REFERENCES support_tickets(ticket_id)
);

CREATE TABLE incidents (
    incident_id INT AUTO_INCREMENT PRIMARY KEY,
    started_at DATETIME NOT NULL,
    ended_at DATETIME NULL,
    severity ENUM('sev1','sev2','sev3') NOT NULL,
    affected_product VARCHAR(120) NOT NULL,
    customer_impact VARCHAR(260) NOT NULL,
    root_cause VARCHAR(180) NULL
);

CREATE TABLE query_audit_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(80),
    tenant_id VARCHAR(80) DEFAULT 'demo',
    workspace_id INT NULL,
    query_text TEXT NOT NULL,
    generated_sql TEXT,
    intent VARCHAR(40),
    route ENUM('fast_path','rag_sql','clarification','blocked') NOT NULL,
    retrieval_hits INT DEFAULT 0,
    execution_time_ms FLOAT,
    row_count INT DEFAULT 0,
    status ENUM('success','blocked','error') NOT NULL,
    error_message TEXT,
    trace_id VARCHAR(40),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id),
    INDEX idx_audit_created (created_at),
    INDEX idx_audit_status (status),
    INDEX idx_audit_route (route)
);

INSERT INTO departments (name, cost_center, region, annual_budget) VALUES
('Engineering','ENG-100','North America',7200000),
('Product','PRD-200','North America',3400000),
('Sales','SAL-300','North America',6100000),
('Customer Success','CS-400','Global',3900000),
('Support','SUP-500','Global',2600000),
('Marketing','MKT-600','North America',2100000),
('Finance','FIN-700','North America',1700000),
('People','PPL-800','North America',1200000),
('Security','SEC-900','Global',2500000),
('Data','DAT-950','Global',3100000);

INSERT INTO plans (plan_name, billing_model, monthly_base_price, included_seats, support_tier) VALUES
('Starter','seat_based',799,10,'standard'),
('Growth','hybrid',2499,35,'premium'),
('Enterprise','hybrid',8999,150,'enterprise'),
('Strategic','usage_based',18000,400,'enterprise');

INSERT INTO products (product_name, product_family, launch_date) VALUES
('PlainSQL Copilot','AI Analytics','2023-01-12'),
('Schema Intelligence','Governance','2023-04-03'),
('SQL Guardrails','Security','2023-07-21'),
('Observability Console','Operations','2024-02-10');

INSERT INTO feature_catalog (product_id, feature_name, feature_category, risk_level) VALUES
(1,'Natural language query','Querying','medium'),
(1,'Auto visualization','Insights','low'),
(1,'Follow-up suggestions','Insights','low'),
(2,'Hybrid schema retrieval','Retrieval','medium'),
(2,'Join path ranking','Retrieval','high'),
(3,'Read-only validation','Safety','high'),
(3,'PII policy detection','Safety','high'),
(3,'SQL lint and explain','Safety','medium'),
(4,'Pipeline trace','Observability','low'),
(4,'Latency and audit exports','Observability','medium');

DELIMITER //

CREATE PROCEDURE seed_employees()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 80 DO
    INSERT INTO employees (department_id, manager_id, full_name, role, location, hire_date, base_salary, quota_arr, active)
    VALUES (
      1 + MOD(i, 10),
      IF(i <= 10, NULL, 1 + MOD(i, 10)),
      CONCAT('Employee ', LPAD(i, 3, '0')),
      ELT(1 + MOD(i, 8), 'Account Executive','Solutions Engineer','Data Engineer','Product Manager','Support Engineer','CS Manager','Security Analyst','Finance Analyst'),
      ELT(1 + MOD(i, 6), 'San Francisco','New York','Austin','Chicago','London','Bengaluru'),
      DATE_ADD('2020-01-01', INTERVAL MOD(i * 37, 2100) DAY),
      72000 + MOD(i * 7919, 128000),
      IF(MOD(i, 4) = 0, 0, 300000 + MOD(i * 17000, 1400000)),
      MOD(i, 17) <> 0
    );
    SET i = i + 1;
  END WHILE;
END//

CREATE PROCEDURE seed_accounts()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 120 DO
    INSERT INTO accounts (account_name, industry, segment, region, country, employee_count, arr_band, health_score, churn_risk, created_at)
    VALUES (
      CONCAT(ELT(1 + MOD(i, 10), 'Northstar','Apex','Nimbus','Cobalt','Vertex','Helio','Summit','Quantive','Brightlane','Redwood'), ' ', ELT(1 + MOD(i, 8), 'Analytics','Health','Banking','Retail','Logistics','Cloud','Media','Robotics'), ' ', i),
      ELT(1 + MOD(i, 8), 'SaaS','Fintech','Healthcare','Retail','Manufacturing','Media','Logistics','Education'),
      ELT(1 + MOD(i, 4), 'startup','mid_market','enterprise','strategic'),
      ELT(1 + MOD(i, 5), 'North America','Europe','Asia Pacific','Latin America','Middle East'),
      ELT(1 + MOD(i, 8), 'United States','Canada','United Kingdom','Germany','India','Singapore','Brazil','UAE'),
      40 + MOD(i * 83, 22000),
      ELT(1 + MOD(i, 4), 'under_25k','25k_100k','100k_500k','500k_plus'),
      35 + MOD(i * 13, 65),
      ELT(1 + MOD(i * 7, 3), 'low','medium','high'),
      DATE_ADD('2022-01-01', INTERVAL MOD(i * 19, 1100) DAY)
    );
    SET i = i + 1;
  END WHILE;
END//

CREATE PROCEDURE seed_contacts_workspaces_users()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 360 DO
    INSERT INTO contacts (account_id, full_name, title, department, email, is_executive_sponsor)
    VALUES (1 + MOD(i, 120), CONCAT('Contact ', LPAD(i, 4, '0')), ELT(1 + MOD(i, 7), 'VP Data','Director Analytics','RevOps Lead','Support Manager','CFO','Head of Product','Data Platform Lead'), ELT(1 + MOD(i, 5), 'Data','Finance','Revenue','Support','Product'), CONCAT('contact', i, '@example-customer.com'), MOD(i, 9) = 0);
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 180 DO
    INSERT INTO workspaces (account_id, workspace_name, environment, cloud_region, created_at)
    VALUES (1 + MOD(i, 120), CONCAT('workspace-', LPAD(i, 3, '0')), ELT(1 + MOD(i, 3), 'production','staging','sandbox'), ELT(1 + MOD(i, 5), 'us-east-1','us-west-2','eu-west-1','ap-south-1','ap-southeast-1'), DATE_ADD('2022-01-01', INTERVAL MOD(i * 11, 1200) DAY));
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 1800 DO
    INSERT INTO workspace_users (workspace_id, account_id, email, role, last_active_at, is_service_account)
    VALUES (1 + MOD(i, 180), 1 + MOD(i, 120), CONCAT('user', i, '@example-customer.com'), ELT(1 + MOD(i, 4), 'admin','analyst','viewer','engineer'), DATE_ADD('2026-01-01', INTERVAL MOD(i * 31, 130) DAY), MOD(i, 23) = 0);
    SET i = i + 1;
  END WHILE;
END//

CREATE PROCEDURE seed_subscriptions_revenue()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 360 DO
    INSERT INTO subscriptions (account_id, plan_id, owner_employee_id, start_date, end_date, status, seats_purchased, contracted_arr, discount_pct)
    VALUES (
      1 + MOD(i, 120),
      1 + MOD(i, 4),
      1 + MOD(i, 80),
      DATE_ADD('2022-01-01', INTERVAL MOD(i * 17, 1100) DAY),
      IF(MOD(i, 19) = 0, DATE_ADD('2025-01-01', INTERVAL MOD(i * 13, 420) DAY), NULL),
      ELT(1 + MOD(i * 5, 4), 'active','past_due','cancelled','trialing'),
      8 + MOD(i * 9, 420),
      12000 + MOD(i * 18731, 980000),
      MOD(i * 3, 25)
    );
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 4320 DO
    INSERT INTO invoices (subscription_id, account_id, invoice_date, due_date, status, subtotal, tax, total)
    VALUES (
      1 + MOD(i, 360),
      1 + MOD(i, 120),
      DATE_ADD('2023-01-01', INTERVAL MOD(i * 7, 1220) DAY),
      DATE_ADD(DATE_ADD('2023-01-01', INTERVAL MOD(i * 7, 1220) DAY), INTERVAL 30 DAY),
      ELT(1 + MOD(i * 11, 4), 'paid','open','void','uncollectible'),
      900 + MOD(i * 137, 85000),
      ROUND((900 + MOD(i * 137, 85000)) * 0.0825, 2),
      ROUND((900 + MOD(i * 137, 85000)) * 1.0825, 2)
    );
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 4100 DO
    INSERT INTO payments (invoice_id, account_id, paid_at, payment_method, amount, payment_status)
    VALUES (1 + MOD(i, 4320), 1 + MOD(i, 120), DATE_ADD('2023-01-02', INTERVAL MOD(i * 9, 1220) DAY), ELT(1 + MOD(i, 4), 'ach','wire','card','credits'), 900 + MOD(i * 137, 85000), ELT(1 + MOD(i * 13, 3), 'succeeded','failed','refunded'));
    SET i = i + 1;
  END WHILE;
END//

CREATE PROCEDURE seed_opportunities_usage_support()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 850 DO
    INSERT INTO opportunities (account_id, owner_employee_id, opportunity_name, stage, source, forecast_category, expected_arr, probability_pct, created_date, close_date, last_activity_date, loss_reason)
    VALUES (1 + MOD(i, 120), 1 + MOD(i, 80), CONCAT('Expansion opportunity ', i), ELT(1 + MOD(i, 7), 'prospecting','qualification','technical_validation','proposal','negotiation','closed_won','closed_lost'), ELT(1 + MOD(i, 5), 'inbound','partner','outbound','product_qualified','event'), ELT(1 + MOD(i, 4), 'pipeline','best_case','commit','closed'), 15000 + MOD(i * 23000, 1200000), 10 + MOD(i * 7, 90), DATE_ADD('2023-01-01', INTERVAL MOD(i * 5, 1160) DAY), DATE_ADD('2023-02-01', INTERVAL MOD(i * 7, 1160) DAY), DATE_ADD('2023-02-01', INTERVAL MOD(i * 6, 1160) DAY), IF(MOD(i, 7) = 0, ELT(1 + MOD(i, 4), 'budget freeze','no champion','security review failed','competitor selected'), NULL));
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 12000 DO
    INSERT INTO product_usage_daily (workspace_id, account_id, feature_id, usage_date, active_users, query_count, successful_query_count, failed_query_count, p95_latency_ms, compute_seconds)
    VALUES (1 + MOD(i, 180), 1 + MOD(i, 120), 1 + MOD(i, 10), DATE_ADD('2024-01-01', INTERVAL MOD(i, 865) DAY), 1 + MOD(i * 17, 240), 5 + MOD(i * 41, 1800), 5 + MOD(i * 37, 1700), MOD(i * 11, 90), 120 + MOD(i * 29, 4200), 40 + MOD(i * 97, 90000));
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 1600 DO
    INSERT INTO support_tickets (account_id, workspace_id, contact_id, assigned_employee_id, priority, status, category, subject, created_at, first_response_minutes, resolution_minutes, csat_score)
    VALUES (1 + MOD(i, 120), 1 + MOD(i, 180), 1 + MOD(i, 360), 1 + MOD(i, 80), ELT(1 + MOD(i * 5, 4), 'P1','P2','P3','P4'), ELT(1 + MOD(i * 7, 4), 'open','pending_customer','resolved','closed'), ELT(1 + MOD(i, 7), 'SQL generation','Authentication','Latency','Billing','Schema sync','Visualization','Data quality'), CONCAT('Customer reported ', ELT(1 + MOD(i, 6), 'incorrect join path','slow response','missing column','permission issue','chart mismatch','invoice discrepancy'), ' #', i), DATE_ADD('2024-01-01', INTERVAL MOD(i * 13, 850) DAY), 2 + MOD(i * 9, 420), IF(MOD(i, 5) = 0, NULL, 30 + MOD(i * 47, 9600)), IF(MOD(i, 4) = 0, NULL, 1 + MOD(i, 5)));
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 3200 DO
    INSERT INTO ticket_events (ticket_id, event_time, actor_type, event_type, note)
    VALUES (1 + MOD(i, 1600), DATE_ADD('2024-01-01', INTERVAL MOD(i * 11, 860) DAY), ELT(1 + MOD(i, 3), 'customer','agent','system'), ELT(1 + MOD(i, 6), 'created','comment','status_change','escalated','sla_breach','resolved'), CONCAT('Lifecycle event ', i, ' captured for support analytics'));
    SET i = i + 1;
  END WHILE;
END//

CREATE PROCEDURE seed_observability()
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= 90 DO
    INSERT INTO incidents (started_at, ended_at, severity, affected_product, customer_impact, root_cause)
    VALUES (DATE_ADD('2024-01-01', INTERVAL MOD(i * 23, 780) DAY), DATE_ADD(DATE_ADD('2024-01-01', INTERVAL MOD(i * 23, 780) DAY), INTERVAL 6 HOUR), ELT(1 + MOD(i, 3), 'sev1','sev2','sev3'), ELT(1 + MOD(i, 4), 'PlainSQL Copilot','Schema Intelligence','SQL Guardrails','Observability Console'), ELT(1 + MOD(i, 4), 'Elevated latency','Partial outage','Delayed schema indexing','Increased failed queries'), ELT(1 + MOD(i, 5), 'upstream provider error','cache saturation','migration regression','regional network loss','bad deploy'));
    SET i = i + 1;
  END WHILE;

  SET i = 1;
  WHILE i <= 2500 DO
    INSERT INTO query_audit_log (user_id, tenant_id, workspace_id, query_text, generated_sql, intent, route, retrieval_hits, execution_time_ms, row_count, status, error_message, trace_id, created_at)
    VALUES (CONCAT('user_', 1 + MOD(i, 1800)), CONCAT('acct_', 1 + MOD(i, 120)), 1 + MOD(i, 180), ELT(1 + MOD(i, 6), 'show ARR by segment','rank workspaces by failures','open P1 tickets by account','pipeline by stage','usage by feature','late invoices by region'), 'SELECT ...', ELT(1 + MOD(i, 5), 'aggregate','trend','diagnostic','lookup','comparison'), ELT(1 + MOD(i, 4), 'fast_path','rag_sql','clarification','blocked'), MOD(i * 7, 14), 80 + MOD(i * 31, 6800), MOD(i * 17, 500), ELT(1 + MOD(i * 19, 3), 'success','blocked','error'), IF(MOD(i, 11) = 0, 'Validation blocked non-read query', NULL), CONCAT('trace_', LPAD(i, 6, '0')), DATE_ADD('2024-01-01', INTERVAL MOD(i * 5, 860) DAY));
    SET i = i + 1;
  END WHILE;
END//

DELIMITER ;

CALL seed_employees();
CALL seed_accounts();
CALL seed_contacts_workspaces_users();
CALL seed_subscriptions_revenue();
CALL seed_opportunities_usage_support();
CALL seed_observability();

DROP PROCEDURE seed_employees;
DROP PROCEDURE seed_accounts;
DROP PROCEDURE seed_contacts_workspaces_users;
DROP PROCEDURE seed_subscriptions_revenue;
DROP PROCEDURE seed_opportunities_usage_support;
DROP PROCEDURE seed_observability;

GRANT INSERT ON chatbot.query_audit_log TO 'bot_user'@'%';
FLUSH PRIVILEGES;
