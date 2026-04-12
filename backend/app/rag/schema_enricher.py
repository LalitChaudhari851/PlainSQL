"""
Schema Enricher — Creates rich schema documents for RAG indexing.
Includes column descriptions, sample values, foreign keys, and row counts.
"""

import structlog

logger = structlog.get_logger()


class SchemaEnricher:
    """
    Enriches raw DB schema with metadata for better RAG retrieval.
    Produces rich text documents that ChromaDB can search against.
    """

    def __init__(self, db_pool):
        self.db = db_pool

    def enrich_all_tables(self) -> list[dict]:
        """
        Enrich all tables in the database.
        Returns list of {table_name, document, metadata} dicts.
        """
        tables = self.db.get_tables()
        enriched = []

        for table in tables:
            try:
                doc = self._enrich_table(table)
                enriched.append(doc)
            except Exception as e:
                logger.warning("table_enrichment_failed", table=table, error=str(e))
                # Fallback: basic schema
                columns = self.db.get_table_schema(table)
                col_list = ", ".join([f"{c['name']} ({c['type']})" for c in columns])
                enriched.append({
                    "table_name": table,
                    "document": f"Table: {table}\nColumns: {col_list}",
                    "metadata": {"table": table, "enriched": False},
                })

        logger.info("schema_enrichment_complete", tables_enriched=len(enriched))
        return enriched

    def _enrich_table(self, table_name: str) -> dict:
        """Create a rich text document for a single table."""
        columns = self.db.get_table_schema(table_name)
        row_count = self.db.get_row_count(table_name)
        fks = self.db.get_foreign_keys(table_name)

        # Build document
        doc = f"Table: {table_name}\n"
        doc += f"Row Count: ~{row_count}\n"
        doc += f"Description: Contains {table_name.replace('_', ' ')} data\n"
        doc += "Columns:\n"

        column_names = []
        for col in columns:
            col_name = col["name"]
            col_type = col["type"]
            column_names.append(col_name)

            doc += f"  - {col_name} ({col_type})"

            # Add key info
            if col["key"] == "PRI":
                doc += " [PRIMARY KEY]"
            elif col["key"] == "MUL":
                doc += " [FOREIGN KEY]"
            elif col["key"] == "UNI":
                doc += " [UNIQUE]"

            # Add sample values for non-key columns
            if col["key"] != "PRI":
                samples = self.db.get_sample_values(table_name, col_name, limit=3)
                if samples:
                    sample_strs = [str(s)[:50] for s in samples]  # Truncate long values
                    doc += f" | Examples: {', '.join(sample_strs)}"

            doc += "\n"

        # Add relationships
        if fks:
            doc += "Relationships:\n"
            for fk in fks:
                doc += f"  - {fk['COLUMN_NAME']} → {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}\n"

        # Add searchable aliases
        doc += f"\nSearchable terms: {table_name} {' '.join(column_names)}"

        metadata = {
            "table": table_name,
            "columns": ",".join(column_names),
            "row_count": row_count,
            "has_fk": len(fks) > 0,
            "enriched": True,
        }

        return {
            "table_name": table_name,
            "document": doc,
            "metadata": metadata,
        }
