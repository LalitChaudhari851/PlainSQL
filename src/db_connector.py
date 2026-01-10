import pymysql
import os
from dotenv import load_dotenv
from urllib.parse import urlparse, unquote

# ✅ FIX 1: Class renamed to 'Database' (matches api_server.py)
class Database:
    def __init__(self):
        load_dotenv()
        db_uri = os.getenv("DB_URI")
        
        if not db_uri:
            raise ValueError("❌ DB_URI is missing from .env file")

        parsed = urlparse(db_uri)
        self.host = parsed.hostname
        self.port = parsed.port or 3306
        self.user = parsed.username
        self.password = unquote(parsed.password)
        self.db_name = parsed.path[1:]

    def get_connection(self):
        return pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.db_name,
            port=self.port,
            cursorclass=pymysql.cursors.DictCursor
        )

    # ✅ FIX 2: Method renamed to 'run_query' (matches api_server.py)
    def run_query(self, query):
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            return [f"Error: {e}"]
        finally:
            conn.close()

    def get_tables(self):
        """Returns a list of all table names."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                return [list(row.values())[0] for row in cursor.fetchall()]
        except Exception as e:
            return []
        finally:
            conn.close()

    def get_table_schema(self, table_name):
        """Returns column details for a specific table."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DESCRIBE {table_name}")
                columns = []
                for row in cursor.fetchall():
                    columns.append(f"{row['Field']} ({row['Type']})")
                return columns
        except Exception:
            return []
        finally:
            conn.close()

    # ✅ FIX 3: Added 'get_schema()' (no args) for the RAG system
    def get_schema(self):
        """Generates a full text schema of the database for the AI."""
        tables = self.get_tables()
        schema_text = ""
        
        for table in tables:
            columns = self.get_table_schema(table)
            schema_text += f"Table: {table}\nColumns:\n"
            for col in columns:
                schema_text += f"  - {col}\n"
            schema_text += "\n"
            
        return schema_text