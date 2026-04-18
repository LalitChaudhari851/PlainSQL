"""
Auto Insights Generator — Statistical analysis and pattern detection on query results.
Generates human-readable insights without LLM (pure statistical analysis).
"""

import structlog
from collections import Counter
from typing import Any

logger = structlog.get_logger()


class InsightsGenerator:
    """Generates statistical insights from query result data."""

    def generate(self, results: list[dict], query: str = "") -> list[str]:
        """
        Analyze query results and generate human-readable insights.
        Uses statistical methods — no LLM needed.
        """
        if not results:
            return ["No data available for analysis."]

        insights = []

        # ── Basic stats ──────────────────────────────────
        row_count = len(results)
        col_count = len(results[0]) if results else 0
        insights.append(f"📊 Dataset: **{row_count}** records across **{col_count}** columns")

        # ── Classify columns ─────────────────────────────
        numeric_cols = []
        text_cols = []
        date_cols = []
        columns = list(results[0].keys()) if results else []

        for col in columns:
            sample = results[0].get(col)
            col_lower = col.lower()

            if any(d in col_lower for d in ["date", "time", "created", "updated"]):
                date_cols.append(col)
            elif isinstance(sample, (int, float)):
                numeric_cols.append(col)
            else:
                try:
                    if sample is not None:
                        float(sample)
                        numeric_cols.append(col)
                    else:
                        text_cols.append(col)
                except (ValueError, TypeError):
                    text_cols.append(col)

        # ── Numeric column analysis ──────────────────────
        for col in numeric_cols[:4]:
            values = self._extract_numeric_values(results, col)
            if not values:
                continue

            avg = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            total = sum(values)

            label = col.replace("_", " ").title()
            insights.append(
                f"**{label}**: Total {total:,.2f} | "
                f"Avg {avg:,.2f} | Range [{min_val:,.2f} — {max_val:,.2f}]"
            )

            # Outlier detection (IQR method)
            outliers = self._detect_outliers(values)
            if outliers:
                insights.append(
                    f"⚠️ **{len(outliers)} outliers** detected in {label} "
                    f"(values: {', '.join(f'{v:,.2f}' for v in outliers[:3])})"
                )

            # Concentration analysis
            if len(values) > 1:
                top_val = max(values)
                top_pct = (top_val / total * 100) if total > 0 else 0
                if top_pct > 30:
                    insights.append(f"🎯 Top value in **{label}** accounts for {top_pct:.1f}% of total")

        # ── Text column analysis ─────────────────────────
        for col in text_cols[:2]:
            str_values = [str(row.get(col, "")) for row in results if row.get(col)]
            if not str_values:
                continue

            counter = Counter(str_values)
            unique_count = len(counter)
            label = col.replace("_", " ").title()

            if unique_count == row_count:
                insights.append(f"🔑 **{label}** has all unique values")
            elif unique_count <= 10:
                top_items = counter.most_common(3)
                distribution = ", ".join(f"'{k}' ({v})" for k, v in top_items)
                insights.append(f"🏷️ **{label}** distribution: {distribution}")

        # ── Trend detection ──────────────────────────────
        if date_cols and numeric_cols and len(results) >= 3:
            insights.append("📈 Time-series data detected — trend analysis available")

        return insights

    def _extract_numeric_values(self, results: list[dict], col: str) -> list[float]:
        """Extract numeric values from a column, skipping nulls."""
        values = []
        for row in results:
            v = row.get(col)
            if v is not None:
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    continue
        return values

    def _detect_outliers(self, values: list[float]) -> list[float]:
        """Detect outliers using the IQR method."""
        if len(values) < 4:
            return []

        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[int(n * 0.25)]
        q3 = sorted_vals[int(n * 0.75)]
        iqr = q3 - q1

        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        return [v for v in values if v < lower or v > upper]
