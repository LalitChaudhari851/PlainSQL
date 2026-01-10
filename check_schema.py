import os
from dotenv import load_dotenv
from src.db_connector import Database

# Load your actual database connection
db = Database()

print("\n--- 🔍 REAL DATABASE SCHEMA ---")
try:
    # Get all tables
    tables = db.get_tables()
    for table in tables:
        print(f"\n📂 TABLE: {table}")
        # Get columns for this table
        columns = db.get_table_schema(table)
        for col in columns:
            print(f"   - {col}")
    print("\n-------------------------------")
except Exception as e:
    print(f"❌ Error reading schema: {e}")