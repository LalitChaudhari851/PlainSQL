"""
Visualization Agent — Generates chart configs and auto-insights from query results.
Last agent in the pipeline. Determines optimal chart type and generates follow-ups.
"""

import structlog
from collections import Counter

from app.agents.state import AgentState

logger = structlog.get_logger()

# Chart type selection thresholds
MAX_PIE_CATEGORIES = 8
MIN_LINE_POINTS = 3


def visualization_node(state: AgentState) -> dict:
    """
    Analyze query results and generate visualization config + insights.
    """
    results = state.get("query_results", [])
    columns = state.get("column_names", [])
    user_query = state.get("user_query", "")
    sql = state.get("sanitized_sql", "") or state.get("generated_sql", "")
    trace_id = state.get("trace_id", "unknown")

    logger.info("agent_started", agent="visualization", trace_id=trace_id)

    if not results:
        return {
            "chart_config": None,
            "chart_type": None,
            "insights": ["No data returned from the query."],
            "follow_up_questions": _generate_followups_empty(user_query),
        }

    # ── Classify columns ─────────────────────────────────
    numeric_cols = []
    text_cols = []
    date_cols = []

    for col in columns:
        sample_val = results[0].get(col)
        if sample_val is None:
            # Check other rows
            for row in results[:5]:
                if row.get(col) is not None:
                    sample_val = row[col]
                    break

        col_lower = col.lower()
        if any(d in col_lower for d in ["date", "time", "created", "updated", "day", "month", "year"]):
            date_cols.append(col)
        elif isinstance(sample_val, (int, float)):
            numeric_cols.append(col)
        else:
            # Try to parse as number
            try:
                if sample_val is not None:
                    float(sample_val)
                    numeric_cols.append(col)
                else:
                    text_cols.append(col)
            except (ValueError, TypeError):
                text_cols.append(col)

    # ── Determine chart type ─────────────────────────────
    chart_config = None
    chart_type = None
    row_count = len(results)

    if numeric_cols and (text_cols or date_cols):
        label_col = date_cols[0] if date_cols else text_cols[0]
        value_col = numeric_cols[0]

        labels = [str(row.get(label_col, "")) for row in results]
        values = []
        for row in results:
            v = row.get(value_col, 0)
            try:
                values.append(float(v) if v is not None else 0)
            except (ValueError, TypeError):
                values.append(0)

        # Choose chart type
        if date_cols and row_count >= MIN_LINE_POINTS:
            chart_type = "line"
        elif row_count <= MAX_PIE_CATEGORIES:
            chart_type = "doughnut"
        else:
            chart_type = "bar"

        colors = [
            "#38bdf8", "#a855f7", "#ec4899", "#22c55e", "#eab308",
            "#f97316", "#14b8a6", "#6366f1", "#f43f5e", "#84cc16",
        ]

        chart_config = {
            "type": chart_type,
            "data": {
                "labels": labels[:50],  # Cap at 50 labels for readability
                "datasets": [{
                    "label": value_col.replace("_", " ").title(),
                    "data": values[:50],
                    "backgroundColor": colors[:len(labels)],
                    "borderColor": "#1e293b",
                    "borderWidth": 2,
                }],
            },
            "options": {
                "responsive": True,
                "maintainAspectRatio": False,
                "plugins": {
                    "legend": {"position": "bottom", "labels": {"color": "#94A3B8"}},
                },
            },
        }

    # ── Generate insights ────────────────────────────────
    insights = _generate_insights(results, numeric_cols, text_cols, date_cols, row_count)

    # ── Generate follow-up questions ─────────────────────
    follow_ups = _generate_followups(user_query, columns, results)

    logger.info(
        "visualization_complete",
        chart_type=chart_type,
        insights_count=len(insights),
        followups_count=len(follow_ups),
    )

    return {
        "chart_config": chart_config,
        "chart_type": chart_type,
        "insights": insights,
        "follow_up_questions": follow_ups,
    }


def _generate_insights(results, numeric_cols, text_cols, date_cols, row_count) -> list[str]:
    """Generate statistical insights from query results."""
    insights = []
    insights.append(f"📊 **{row_count}** records returned")

    for col in numeric_cols[:3]:  # Top 3 numeric columns
        values = []
        for row in results:
            try:
                v = float(row.get(col, 0))
                values.append(v)
            except (ValueError, TypeError):
                continue

        if values:
            avg_val = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            total = sum(values)

            col_label = col.replace("_", " ").title()
            insights.append(f"**{col_label}**: avg {avg_val:,.2f} | min {min_val:,.2f} | max {max_val:,.2f}")

            if max_val > avg_val * 3 and len(values) > 2:
                insights.append(f"⚠️ Outlier detected in **{col_label}**: max ({max_val:,.2f}) is {max_val/avg_val:.1f}x the average")

            # Trend detection for date-sorted data
            if len(values) >= 3:
                first_half = sum(values[:len(values)//2])
                second_half = sum(values[len(values)//2:])
                if second_half > first_half * 1.2:
                    insights.append(f"📈 Upward trend detected in **{col_label}**")
                elif first_half > second_half * 1.2:
                    insights.append(f"📉 Downward trend detected in **{col_label}**")

    # Text column distribution
    for col in text_cols[:1]:
        values = [str(row.get(col, "")) for row in results]
        counter = Counter(values)
        if len(counter) > 1:
            top_val, top_count = counter.most_common(1)[0]
            pct = (top_count / len(values)) * 100
            if pct < 100:
                insights.append(f"🏷️ Most common **{col.replace('_', ' ')}**: '{top_val}' ({pct:.0f}%)")

    return insights


def _generate_followups(query: str, columns: list, results: list) -> list[str]:
    """Generate context-aware follow-up suggestions."""
    followups = []
    query_lower = query.lower()

    if "top" in query_lower or "best" in query_lower:
        followups.append("Show the bottom performers instead")
    
    if "salary" in query_lower or "amount" in query_lower or "revenue" in query_lower:
        followups.append("Show the distribution by department")
        followups.append("Compare with last year's data")

    if len(results) > 10:
        followups.append("Show only the top 5 results")

    if any(c.lower() in ["department", "region", "category"] for c in columns):
        followups.append("Break down by category")

    # Always offer these
    followups.extend([
        "Visualize this as a chart",
        "Export these results",
    ])

    return followups[:5]  # Cap at 5 suggestions


def _generate_followups_empty(query: str) -> list[str]:
    """Follow-up suggestions when no results are returned."""
    return [
        "Show all available data from this table",
        "List the tables in the database",
        "Try a broader search criteria",
    ]
