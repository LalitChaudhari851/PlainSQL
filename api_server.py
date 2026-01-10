import sys
import os
import uvicorn
import re

# 🚨 FORCE PYTHON TO FIND THE 'src' FOLDER
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

# Import your custom modules
from src.db_connector import Database
from src.rag_manager import RAGSystem 
from src.sql_generator import SQLGenerator

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ChatRequest(BaseModel):
    question: str
    history: Optional[List[dict]] = [] 

# --- HELPER: CLEAN AI OUTPUT ---
def clean_sql(sql_text: str) -> str:
    """Removes markdown, 'sql' tags, and extra whitespace."""
    if not sql_text:
        return ""
    # Remove markdown code blocks (```sql ... ```)
    cleaned = re.sub(r"```sql|```", "", sql_text, flags=re.IGNORECASE).strip()
    # Remove trailing semicolons for consistency (optional, depending on DB driver)
    cleaned = cleaned.rstrip(';') 
    return cleaned

print("--- 🚀 SYSTEM STARTUP SEQUENCE ---")
try:
    print("   ...Connecting to Database")
    db = Database()
    print("   ✅ Database Connection: SUCCESS")
    
    print("   ...Initializing RAG System")
    rag = RAGSystem(db)
    print("   ✅ RAG System: ONLINE")
    
    print("   ...Loading AI Model")
    generator = SQLGenerator()
    print("   ✅ AI Model: LOADED")
    
except Exception as e:
    print(f"   ❌ CRITICAL STARTUP ERROR: {e}")

@app.post("/chat")
def chat_endpoint(request: ChatRequest):
    print(f"\n📨 NEW REQUEST: {request.question}") 
    
    try:
        # 1. Get Context
        context = rag.get_relevant_schema(request.question)
        
        # 2. Generate SQL
        raw_sql, explanation, friendly_msg = generator.generate_sql(request.question, context, request.history)
        
        # 3. Clean and Validate SQL
        # If the generator returned an error string directly (Error 1 fix)
        if "Error:" in raw_sql or "Invalid Query" in raw_sql:
            return {
                "answer": [],
                "sql": raw_sql,
                "message": "I couldn't generate a safe query for that request. Try asking for specific data like 'Show me users' or 'List orders'.",
                "follow_ups": ["Show top 10 rows from users", "Count total orders"]
            }

        cleaned_sql = clean_sql(raw_sql)
        print(f"   🧹 Cleaned SQL: {cleaned_sql}")

        # Safety check: Ensure it's a SELECT
        if not cleaned_sql.upper().startswith("SELECT"):
             return {
                "answer": [],
                "sql": cleaned_sql,
                "message": "Security Alert: I can only perform READ (SELECT) operations.",
                "follow_ups": []
            }

        # 4. Run Query (Error 2 fix)
        try:
            results = db.run_query(cleaned_sql)
        except Exception as db_err:
            # Catch MySQL Syntax errors specifically
            print(f"   ⚠️ DB Error: {db_err}")
            return {
                "answer": [f"Error: {str(db_err)}"], # This puts the error in a safe list
                "sql": cleaned_sql,
                "message": "There was a syntax error in the generated SQL. I have displayed the error above.",
                "follow_ups": []
            }
        
        # 5. Generate Follow-ups
        follow_ups = generator.generate_followup_questions(request.question, cleaned_sql)
        
        return {
            "answer": results,
            "sql": cleaned_sql,
            "explanation": explanation,
            "message": friendly_msg, 
            "follow_ups": follow_ups
        }

    except Exception as e:
        print(f"❌ General Processing Error: {e}")
        return {
            "answer": [],
            "sql": "-- System Error",
            "message": f"Critical Error: {str(e)}",
            "follow_ups": []
        }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)