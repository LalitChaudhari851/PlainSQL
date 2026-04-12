"""
Anomaly Detector — Statistical anomaly detection on query results.
Uses IQR and Z-score methods to identify unusual data points.
"""

import math
import structlog

logger = structlog.get_logger()


class AnomalyDetector:
    """Detects statistical anomalies in query result data."""

    def detect(self, results: list[dict]) -> list[dict]:
        """
        Detect anomalies across all numeric columns in the results.
        Returns a list of anomaly descriptors.
        """
        if not results or len(results) < 4:
            return []

        anomalies = []
        columns = list(results[0].keys())

        for col in columns:
            values = []
            for row in results:
                try:
                    v = float(row.get(col, 0))
                    values.append(v)
                except (ValueError, TypeError):
                    continue

            if len(values) < 4:
                continue

            # ── IQR Method ───────────────────────────────
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            q1 = sorted_vals[int(n * 0.25)]
            q3 = sorted_vals[int(n * 0.75)]
            iqr = q3 - q1

            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            for i, row in enumerate(results):
                try:
                    v = float(row.get(col, 0))
                except (ValueError, TypeError):
                    continue

                if v < lower or v > upper:
                    anomalies.append({
                        "row_index": i,
                        "column": col,
                        "value": v,
                        "type": "above_upper" if v > upper else "below_lower",
                        "threshold": upper if v > upper else lower,
                        "method": "iqr",
                        "severity": "high" if (v > q3 + 3 * iqr or v < q1 - 3 * iqr) else "medium",
                        "description": (
                            f"{col} value {v:,.2f} is {'above' if v > upper else 'below'} "
                            f"the expected range [{lower:,.2f}, {upper:,.2f}]"
                        ),
                    })

            # ── Z-Score Method (for larger datasets) ─────
            if len(values) >= 10:
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / len(values)
                std = math.sqrt(variance) if variance > 0 else 0

                if std > 0:
                    for i, row in enumerate(results):
                        try:
                            v = float(row.get(col, 0))
                        except (ValueError, TypeError):
                            continue

                        z_score = abs((v - mean) / std)
                        if z_score > 3:
                            # Only add if not already caught by IQR
                            existing = any(
                                a["row_index"] == i and a["column"] == col
                                for a in anomalies
                            )
                            if not existing:
                                anomalies.append({
                                    "row_index": i,
                                    "column": col,
                                    "value": v,
                                    "type": "z_score_outlier",
                                    "z_score": round(z_score, 2),
                                    "method": "z_score",
                                    "severity": "high" if z_score > 4 else "medium",
                                    "description": (
                                        f"{col} value {v:,.2f} has z-score of {z_score:.2f} "
                                        f"(>{3} standard deviations from mean {mean:,.2f})"
                                    ),
                                })

        logger.info("anomaly_detection_complete", anomalies_found=len(anomalies))
        return anomalies
