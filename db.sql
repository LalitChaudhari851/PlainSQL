create database chatbot;
use chatbot;

CREATE USER 'bot_user'@'%' IDENTIFIED BY 'YourSecurePassword123!';
GRANT SELECT ON chatbot.* TO 'bot_user'@'%';
FLUSH PRIVILEGES;


USE chatbot;

CREATE TABLE employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE
);

CREATE TABLE sales (
    sale_id INT AUTO_INCREMENT PRIMARY KEY,
    employee_id INT,
    amount DECIMAL(10,2),
    sale_date DATE,
    FOREIGN KEY (employee_id) REFERENCES employees(id)
);

-- Insert a little dummy data
INSERT INTO employees (name, department, salary, hire_date) VALUES 
('Alice', 'Sales', 70000, '2023-01-15'),
('Bob', 'Engineering', 90000, '2022-05-20');

INSERT INTO sales (employee_id, amount, sale_date) VALUES 
(1, 500.00, '2023-06-01'),
(1, 1200.50, '2023-06-03');