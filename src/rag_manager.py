import chromadb
from src.db_connector import Database

class RAGSystem:
    def __init__(self, db_instance=None):
        # ✅ FIX: Accept the DB connection passed from the server
        if db_instance:
            self.db = db_instance
        else:
            self.db = Database()
        
        # Initialize ChromaDB
        print("   ...Connecting to ChromaDB")
        self.client = chromadb.PersistentClient(path="./chroma_db")
        self.collection = self.client.get_or_create_collection(name="schema_knowledge")
        
        # Refresh memory
        self._index_schema()

    def _index_schema(self):
        """Reads the database structure and saves it to ChromaDB."""
        try:
            tables = self.db.get_tables()
            
            if self.collection.count() > 0:
                existing_ids = self.collection.get()['ids']
                if existing_ids:
                    self.collection.delete(ids=existing_ids)

            for table in tables:
                columns = self.db.get_table_schema(table)
                col_list = []
                for col in columns:
                    if isinstance(col, dict):
                        col_list.append(f"{col['name']} ({col['type']})")
                    else:
                        col_list.append(str(col))
                
                schema_text = f"Table: {table}\nColumns: {', '.join(col_list)}"
                self.collection.add(
                    documents=[schema_text],
                    metadatas=[{"table": table}],
                    ids=[table]
                )
            print(f"   ✅ RAG System: Indexed {len(tables)} tables.")
            
        except Exception as e:
            print(f"   ⚠️ RAG Indexing Warning: {e}")

    def get_relevant_schema(self, question):
        try:
            results = self.collection.query(query_texts=[question], n_results=3)
            if results['documents']:
                return "\n\n".join(results['documents'][0])
            return ""
        except Exception:
            return self._get_full_schema_fallback()

    def _get_full_schema_fallback(self):
        tables = self.db.get_tables()
        schema = []
        for table in tables:
            cols = self.db.get_table_schema(table)
            col_list = [c['name'] if isinstance(c, dict) else str(c) for c in cols]
            schema.append(f"Table: {table}\nColumns: {', '.join(col_list)}")
        return "\n\n".join(schema)