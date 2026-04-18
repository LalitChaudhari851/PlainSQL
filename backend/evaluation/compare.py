"""
Evaluation Comparator — Compare two evaluation runs side-by-side.
Useful for measuring the impact of prompt changes, model swaps, or pipeline updates.
"""

import json
import sys
import os


def load_results(path: str) -> dict:
    """Load evaluation results from a JSON file."""
    with open(path, "r") as f:
        return json.load(f)


def compare(baseline: dict, candidate: dict) -> dict:
    """
    Compare two evaluation runs and produce a diff report.
    Returns structured comparison data.
    """
    b_results = {r["id"]: r for r in baseline.get("results", [])}
    c_results = {r["id"]: r for r in candidate.get("results", [])}

    all_ids = sorted(set(b_results.keys()) | set(c_results.keys()))

    comparisons = []
    regressions = []
    improvements = []

    for eval_id in all_ids:
        b = b_results.get(eval_id, {})
        c = c_results.get(eval_id, {})

        b_exec = b.get("execution_match", False)
        c_exec = c.get("execution_match", False)
        b_exact = b.get("exact_match", False)
        c_exact = c.get("exact_match", False)
        b_sim = b.get("structural_similarity", 0.0)
        c_sim = c.get("structural_similarity", 0.0)
        b_halluc = len(b.get("hallucinations", []))
        c_halluc = len(c.get("hallucinations", []))

        status = "unchanged"
        if c_exec and not b_exec:
            status = "improved"
            improvements.append(eval_id)
        elif b_exec and not c_exec:
            status = "regressed"
            regressions.append(eval_id)
        elif c_sim > b_sim + 0.1:
            status = "improved"
            improvements.append(eval_id)
        elif b_sim > c_sim + 0.1:
            status = "regressed"
            regressions.append(eval_id)

        comparisons.append({
            "id": eval_id,
            "question": b.get("question", c.get("question", "")),
            "status": status,
            "baseline": {
                "exact_match": b_exact,
                "execution_match": b_exec,
                "structural_similarity": b_sim,
                "hallucinations": b_halluc,
                "latency_ms": b.get("latency_ms", 0),
            },
            "candidate": {
                "exact_match": c_exact,
                "execution_match": c_exec,
                "structural_similarity": c_sim,
                "hallucinations": c_halluc,
                "latency_ms": c.get("latency_ms", 0),
            },
        })

    # Summary metrics
    summary = {
        "baseline": {
            "exact_match_rate": baseline.get("exact_match_rate", 0),
            "execution_accuracy": baseline.get("execution_accuracy", 0),
            "avg_similarity": baseline.get("avg_structural_similarity", 0),
            "total_hallucinations": baseline.get("total_hallucinations", 0),
            "avg_latency_ms": baseline.get("avg_latency_ms", 0),
        },
        "candidate": {
            "exact_match_rate": candidate.get("exact_match_rate", 0),
            "execution_accuracy": candidate.get("execution_accuracy", 0),
            "avg_similarity": candidate.get("avg_structural_similarity", 0),
            "total_hallucinations": candidate.get("total_hallucinations", 0),
            "avg_latency_ms": candidate.get("avg_latency_ms", 0),
        },
        "delta": {
            "exact_match_rate": round(
                candidate.get("exact_match_rate", 0) - baseline.get("exact_match_rate", 0), 1
            ),
            "execution_accuracy": round(
                candidate.get("execution_accuracy", 0) - baseline.get("execution_accuracy", 0), 1
            ),
            "avg_similarity": round(
                candidate.get("avg_structural_similarity", 0) - baseline.get("avg_structural_similarity", 0), 2
            ),
            "hallucination_delta": (
                candidate.get("total_hallucinations", 0) - baseline.get("total_hallucinations", 0)
            ),
            "latency_delta_ms": round(
                candidate.get("avg_latency_ms", 0) - baseline.get("avg_latency_ms", 0), 1
            ),
        },
        "improvements": len(improvements),
        "regressions": len(regressions),
        "unchanged": len(comparisons) - len(improvements) - len(regressions),
        "comparisons": comparisons,
    }

    return summary


def print_report(report: dict):
    """Print a human-readable comparison report."""
    print("\n" + "=" * 70)
    print("📊 EVALUATION COMPARISON REPORT")
    print("=" * 70)

    delta = report["delta"]

    print(f"\n{'Metric':<30} {'Baseline':>12} {'Candidate':>12} {'Delta':>10}")
    print("-" * 70)

    b = report["baseline"]
    c = report["candidate"]

    def arrow(val):
        if val > 0:
            return f"↑ +{val}"
        elif val < 0:
            return f"↓ {val}"
        return "  ="

    print(f"{'Exact Match Rate':<30} {b['exact_match_rate']:>11}% {c['exact_match_rate']:>11}% {arrow(delta['exact_match_rate']):>10}")
    print(f"{'Execution Accuracy':<30} {b['execution_accuracy']:>11}% {c['execution_accuracy']:>11}% {arrow(delta['execution_accuracy']):>10}")
    print(f"{'Avg Similarity':<30} {b['avg_similarity']:>12} {c['avg_similarity']:>12} {arrow(delta['avg_similarity']):>10}")
    print(f"{'Hallucinations':<30} {b['total_hallucinations']:>12} {c['total_hallucinations']:>12} {arrow(delta['hallucination_delta']):>10}")
    print(f"{'Avg Latency (ms)':<30} {b['avg_latency_ms']:>12} {c['avg_latency_ms']:>12} {arrow(delta['latency_delta_ms']):>10}")

    print(f"\n✅ Improvements: {report['improvements']}")
    print(f"❌ Regressions:  {report['regressions']}")
    print(f"➖ Unchanged:    {report['unchanged']}")

    # Print regressions detail
    regressions = [c for c in report["comparisons"] if c["status"] == "regressed"]
    if regressions:
        print(f"\n{'='*70}")
        print("⚠️  REGRESSIONS (queries that got worse)")
        print(f"{'='*70}")
        for r in regressions:
            print(f"\n  [{r['id']}] {r['question']}")
            print(f"    Baseline exec_match: {r['baseline']['execution_match']} → Candidate: {r['candidate']['execution_match']}")
            print(f"    Baseline similarity: {r['baseline']['structural_similarity']:.2f} → Candidate: {r['candidate']['structural_similarity']:.2f}")

    # Print improvements detail
    improvements = [c for c in report["comparisons"] if c["status"] == "improved"]
    if improvements:
        print(f"\n{'='*70}")
        print("🎉 IMPROVEMENTS (queries that got better)")
        print(f"{'='*70}")
        for r in improvements:
            print(f"\n  [{r['id']}] {r['question']}")
            print(f"    Baseline exec_match: {r['baseline']['execution_match']} → Candidate: {r['candidate']['execution_match']}")
            print(f"    Baseline similarity: {r['baseline']['structural_similarity']:.2f} → Candidate: {r['candidate']['structural_similarity']:.2f}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare.py <baseline.json> <candidate.json>")
        print("Example: python compare.py results/baseline_v1.json results/baseline_v2.json")
        sys.exit(1)

    baseline = load_results(sys.argv[1])
    candidate = load_results(sys.argv[2])
    report = compare(baseline, candidate)

    print_report(report)

    # Save report
    output_path = os.path.join(os.path.dirname(__file__), "results", "comparison_report.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n💾 Report saved to {output_path}")
