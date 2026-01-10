import os
import re
import json
from huggingface_hub import InferenceClient
from dotenv import load_dotenv

class SQLGenerator:
    def __init__(self, api_key=None):
        load_dotenv()
        self.api_token = os.getenv("HUGGINGFACEHUB_API_TOKEN")
        self.repo_id = "Qwen/Qwen2.5-Coder-32B-Instruct"
        self.client = InferenceClient(token=self.api_token, timeout=25.0)

    def generate_followup_questions(self, question, sql_query):
        return ["Visualize this result", "Export as CSV", "Compare with last year"]

    def generate_sql(self, question, context, history=None):
        if history is None: history = []

        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "GRANT"]
        if any(word in question.upper() for word in forbidden):
             return "SELECT 'Error: Blocked by Safety Layer' as status", "Safety Alert", "I cannot execute commands that modify data."

        history_text = ""
        if history:
            history_text = "PREVIOUS CONVERSATION:\n" + "\n".join([f"User: {h['user']}\nSQL: {h['sql']}" for h in history[-2:]])

        system_prompt = f"""You are an elite SQL Expert.
Schema:
{context}

{history_text}

Rules:
1. Output JSON: {{ "sql": "SELECT ...", "message": "Friendly text", "explanation": "Brief summary" }}
2. Query MUST be Read-Only (SELECT).
3. Do not include markdown formatting like ```json.
"""
        messages = [{"role": "system", "content": system_prompt}, {"role": "user", "content": question}]

        try:
            print(f"   ⚡ Generating SQL...")
            response = self.client.chat_completion(messages=messages, model=self.repo_id, max_tokens=1024, temperature=0.1)
            raw_text = response.choices[0].message.content
            
            sql_query = ""
            message = "Here is the data."
            explanation = "Query generated successfully."

            try:
                clean_json = re.sub(r"```json|```", "", raw_text).strip()
                data = json.loads(clean_json)
                sql_query = data.get("sql", "")
                message = data.get("message", message)
                explanation = data.get("explanation", explanation)
            except:
                match = re.search(r"(SELECT[\s\S]+?;)", raw_text, re.IGNORECASE)
                if match: sql_query = match.group(1)

            sql_query = sql_query.strip().replace("\n", " ")
            if sql_query and not sql_query.endswith(";"): sql_query += ";"
            
            # ✅ FIX: Strip comments and whitespace before validation
            clean_check = re.sub(r"/\*.*?\*/|--.*?\n", "", sql_query, flags=re.DOTALL).strip().upper()

            # ✅ FIX: Allow SELECT or WITH clauses
            if not clean_check.startswith("SELECT") and not clean_check.startswith("WITH"):
                print(f"   ⚠️ Invalid SQL Blocked: {sql_query}")
                return "SELECT 'Error: Invalid Query Type (Non-SELECT)' as status", "Safety Error", "I can only perform read-only operations."

            return sql_query, explanation, message

        except Exception as e:
            print(f"   ❌ Model Error: {e}")
            safe_e = str(e).replace("'", "").replace('"', "")
            return f"SELECT 'Error: {safe_e}' as status", "System Error", "An unexpected error occurred."