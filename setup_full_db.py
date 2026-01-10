import pymysql
import random
from faker import Faker
from dotenv import load_dotenv
import os
from urllib.parse import urlparse, unquote  # <--- Added 'unquote' here

# --- CONFIGURATION ---
NUM_EMPLOYEES = 50
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 20
NUM_SALES = 1000

# Load credentials
load_dotenv()
db_uri = os.getenv("DB_URI")

# Parse URI
parsed = urlparse(db_uri)
username = parsed.username
# FIX: 'unquote' converts 'Lalit%40851' back to 'Lalit@851'
password = unquote(parsed.password) 
host = parsed.hostname
port = parsed.port
# FIX: Handle cases where path is empty or just slash
dbname = parsed.path[1:] if parsed.path else "chatbot"

print(f"--- 🏭 INITIALIZING BUSINESS SIMULATOR for DB: {dbname} ---")

try:
    # Connect without selecting a DB first (to create it if missing)
    conn = pymysql.connect(host=host, user=username, password=password, port=port)
    cursor = conn.cursor()

    # 1. CREATE DATABASE & TABLES
    print("1. Rebuilding Schema...")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    cursor.execute(f"USE {dbname}")

    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in ["sales", "employees", "products", "customers", "departments"]:
        cursor.execute(f"DROP TABLE IF EXISTS {t}")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    queries = [
        "CREATE TABLE departments (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) UNIQUE, budget DECIMAL(15, 2), location VARCHAR(100))",
        "CREATE TABLE employees (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), department_id INT, role VARCHAR(50), salary DECIMAL(10, 2), hire_date DATE, FOREIGN KEY (department_id) REFERENCES departments(id))",
        "CREATE TABLE products (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), category VARCHAR(50), price DECIMAL(10, 2), stock_quantity INT)",
        "CREATE TABLE customers (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100), company VARCHAR(100), region VARCHAR(50), join_date DATE)",
        "CREATE TABLE sales (id INT AUTO_INCREMENT PRIMARY KEY, employee_id INT, customer_id INT, product_id INT, quantity INT, total_amount DECIMAL(10, 2), sale_date DATE, FOREIGN KEY (employee_id) REFERENCES employees(id), FOREIGN KEY (customer_id) REFERENCES customers(id), FOREIGN KEY (product_id) REFERENCES products(id))"
    ]

    for q in queries:
        cursor.execute(q)

    # 2. GENERATE DATA
    fake = Faker()
    print("2. Manufacturing Data...")

    # Departments
    depts = ["Sales", "Engineering", "HR", "Marketing", "Executive"]
    dept_ids = []
    for d in depts:
        cursor.execute("INSERT INTO departments (name, budget, location) VALUES (%s, %s, %s)", (d, random.randint(50000, 1000000), fake.city()))
        dept_ids.append(cursor.lastrowid)

    # Employees
    emp_ids = []
    roles = ["Manager", "Associate", "Analyst", "Director", "Intern"]
    for _ in range(NUM_EMPLOYEES):
        cursor.execute("INSERT INTO employees (name, email, department_id, role, salary, hire_date) VALUES (%s, %s, %s, %s, %s, %s)", 
                       (fake.name(), fake.email(), random.choice(dept_ids), random.choice(roles), random.randint(40000, 150000), fake.date_between(start_date='-5y', end_date='today')))
        emp_ids.append(cursor.lastrowid)

    # Products
    prod_ids = []
    for _ in range(NUM_PRODUCTS):
        cursor.execute("INSERT INTO products (name, category, price, stock_quantity) VALUES (%s, %s, %s, %s)", 
                       (fake.bs().title(), random.choice(["Software", "Hardware", "Service"]), round(random.uniform(50, 5000), 2), random.randint(0, 500)))
        prod_ids.append(cursor.lastrowid)

    # Customers
    cust_ids = []
    for _ in range(NUM_CUSTOMERS):
        cursor.execute("INSERT INTO customers (name, company, region, join_date) VALUES (%s, %s, %s, %s)", 
                       (fake.name(), fake.company(), random.choice(["North America", "Europe", "Asia", "South America"]), fake.date_between(start_date='-3y', end_date='today')))
        cust_ids.append(cursor.lastrowid)

    # Sales
    print(f"   -> Generating {NUM_SALES} sales transactions...")
    for _ in range(NUM_SALES):
        prod = random.choice(prod_ids)
        cursor.execute("SELECT price FROM products WHERE id=%s", (prod,))
        price = cursor.fetchone()[0]
        qty = random.randint(1, 10)
        cursor.execute("INSERT INTO sales (employee_id, customer_id, product_id, quantity, total_amount, sale_date) VALUES (%s, %s, %s, %s, %s, %s)", 
                       (random.choice(emp_ids), random.choice(cust_ids), prod, qty, price * qty, fake.date_between(start_date='-1y', end_date='today')))

    conn.commit()
    conn.close()
    print("✅ DONE! Database is populated.")

except Exception as e:
    print(f"\n❌ CRITICAL ERROR: {e}")
    print("Double check your password in .env!")