import streamlit as st
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.getcwd())

from src.rag_manager import RAGManager
from src.sql_generator import SQLGenerator
from src.db_connector import DatabaseConnector

# --- 1. CONFIGURATION ---
st.set_page_config(
    page_title="NexusAI | Enterprise Data",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #0F1117; }
    #MainMenu, footer, header { visibility: hidden; }
    
    .stChatMessage { background-color: transparent !important; border: none !important; }
    
    div[data-testid="stChatMessage"]:nth-child(odd) { flex-direction: row-reverse; }
    div[data-testid="stChatMessage"]:nth-child(odd) .stMarkdown {
        background-color: #2B2D31; color: #E0E0E0;
        border-radius: 18px 18px 4px 18px; padding: 12px 20px;
        text-align: right; margin-left: auto;
    }
    
    div[data-testid="stChatMessage"]:nth-child(even) .stMarkdown {
        background-color: transparent; color: #F0F0F0; padding-left: 10px;
    }
    
    .stChatInput { position: fixed; bottom: 30px; width: 70% !important; left: 50%; transform: translateX(-50%); z-index: 1000; }
    .stTextInput > div > div > input { background-color: #1E2128; color: white; border-radius: 24px; border: 1px solid #363B47; }
    
    div[data-testid="stDataFrame"] { background-color: #161920; border-radius: 10px; padding: 10px; border: 1px solid #30363D; }
    section[data-testid="stSidebar"] { background-color: #0E1015; border-right: 1px solid #222; }
</style>
""", unsafe_allow_html=True)

# --- 2. INITIALIZATION ---
@st.cache_resource
def get_core():
    load_dotenv()
    key = os.getenv("GEMINI_API_KEY")
    return RAGManager(), SQLGenerator(key), DatabaseConnector()

try:
    rag, sql_gen, db = get_core()
except Exception as e:
    st.error(f"System Offline: {e}")
    st.stop()

# --- 3. SIDEBAR ---
with st.sidebar:
    st.markdown("## 🧠 NexusAI")
    st.caption("Enterprise SQL Agent v2.0")
    st.divider()
    
    if db:
        st.success("🟢 Database Connected")
    
    st.markdown("### 📚 Quick Prompts")
    prompts = [
        "Top 5 employees by salary",
        "Total sales revenue by Region",
        "Show me products with low stock",
        "Which department spends the most?"
    ]
    
    for p in prompts:
        if st.button(p, use_container_width=True):
            st.session_state.last_prompt = p
            
    if st.button("🗑️ Clear Context", type="primary", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

# --- 4. MAIN INTERFACE ---
if "messages" not in st.session_state:
    st.session_state.messages = []

if not st.session_state.messages:
    st.markdown("""
    <div style="text-align: center; margin-top: 100px;">
        <h1 style="font-size: 3rem; background: -webkit-linear-gradient(#eee, #333); -webkit-background-clip: text; -webkit-text-fill-color: transparent;">
            What can I help you analyze?
        </h1>
        <p style="color: #666;">Connect to your database and ask questions in plain English.</p>
    </div>
    """, unsafe_allow_html=True)

for msg in st.session_state.messages:
    with st.chat_message(msg["role"], avatar="👤" if msg["role"] == "user" else "✨"):
        st.markdown(msg["content"])
        
        if "data" in msg:
            # ✅ FIX: Switched to clean dataframe display
            st.dataframe(msg["data"], hide_index=True)
        if "chart" in msg:
            st.bar_chart(msg["chart"])
        if "sql" in msg:
            with st.expander("🛠️ View Query Logic"):
                st.code(msg["sql"], language="sql")

# Handle Input
user_input = st.chat_input("Ask anything...")

if "last_prompt" in st.session_state and st.session_state.last_prompt:
    user_input = st.session_state.last_prompt
    st.session_state.last_prompt = None

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user", avatar="👤"):
        st.markdown(user_input)

    with st.chat_message("assistant", avatar="✨"):
        status_box = st.empty()
        status_box.markdown("`⚡ analyzing...`")
        
        try:
            tables = rag.get_relevant_tables(user_input)
            context = "\n".join(tables)
            
            sql = sql_gen.generate_sql(user_input, context)
            
            results = db.execute_sql(sql)
            status_box.empty()
            
            if not results:
                response = "No data found matching that request."
                st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response, "sql": sql})
            else:
                df = pd.DataFrame(results)
                df_clean = df.reset_index(drop=True)
                
                response = f"Found **{len(df)}** records."
                st.markdown(response)
                # ✅ FIX: Updated dataframe display
                st.dataframe(df_clean, hide_index=True)
                
                chart_data = None
                numeric_cols = df_clean.select_dtypes(include=['number']).columns
                
                if not numeric_cols.empty and len(df_clean) > 1:
                    try:
                        non_numeric = df_clean.select_dtypes(exclude=['number']).columns
                        st.markdown("##### 📊 Trends")
                        if not non_numeric.empty:
                            x_axis = non_numeric[0]
                            y_axis = numeric_cols[0]
                            chart_data = df_clean.set_index(x_axis)[y_axis]
                            st.bar_chart(chart_data, color="#7B61FF")
                        else:
                            chart_data = df_clean[numeric_cols[0]]
                            st.bar_chart(chart_data, color="#7B61FF")
                    except Exception:
                        pass

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response,
                    "data": df_clean,
                    "chart": chart_data,
                    "sql": sql
                })

        except Exception as e:
            status_box.empty()
            st.error(f"Error: {e}")