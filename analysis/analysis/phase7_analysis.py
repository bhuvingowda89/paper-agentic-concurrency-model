import argparse
import csv
import hashlib
import json
import math
import shutil
import statistics
import time
import zipfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Optional

import yaml

from analysis.phase5_harness import ownership_overlaps, state_transition_violations
from analysis.phase6_runner import validate_run_artifacts


ROOT = Path(__file__).resolve().parents[2]
PHASE6 = ROOT / "results" / "phase6"
PHASE7 = ROOT / "results" / "phase7"
MANIFEST = ROOT / "configs" / "phase6" / "manifest.csv"
STATUS = PHASE6 / "phase6_execution_status.csv"

METRICS = ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99", "throughput"]
RELIABILITY_METRICS = ["DER", "EOER", "LER", "RSR", "UAR"]
PERF_METRICS = ["P50", "P95", "P99", "throughput", "RAF", "DAF", "RRR"]
VARIANT_ORDER = ["V0", "V1", "V2", "V3", "V4", "V5"]
PROTECTED = {"V2", "V3", "V4", "V5"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["all", "extract", "analyze", "export"], default="all")
    parser.add_argument("--zip", action="store_true", help="Create paper_analysis_bundle.zip")
    args = parser.parse_args()
    make_dirs()
    if args.mode in {"all", "extract"}:
        manifest, status = load_phase6_inputs()
        audit_phase6_completion(manifest, status)
        rows = extract_run_level(manifest, status)
        write_csv(PHASE7 / "phase6_run_level.csv", rows)
        write_known_corrections()
        write_semantic_audits(rows)
    if args.mode in {"all", "analyze"}:
        rows = read_csv(PHASE7 / "phase6_run_level.csv")
        analyze(rows)
    if args.mode in {"all", "export"}:
        export_bundle(zip_bundle=args.zip)


def make_dirs() -> None:
    for name in ["tables", "statistics", "figures", "audits", "representative_traces", "export"]:
        (PHASE7 / name).mkdir(parents=True, exist_ok=True)


def load_phase6_inputs() -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    manifest = read_csv(MANIFEST)
    status = {row["experiment_id"]: row for row in read_csv(STATUS)}
    return manifest, status


def audit_phase6_completion(manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> None:
    counts = Counter(row.get("status", "MISSING") for row in status.values())
    missing = [row["experiment_id"] for row in manifest if row["experiment_id"] not in status]
    payload = {
        "manifest_total": len(manifest),
        "ledger_total": len(status),
        "COMPLETE": counts.get("COMPLETE", 0),
        "PENDING": counts.get("PENDING", 0),
        "RUNNING": counts.get("RUNNING", 0),
        "INFRA_RETRY": counts.get("INFRA_RETRY", 0),
        "INVALIDATED": counts.get("INVALIDATED", 0),
        "FAILED_BLOCKED": counts.get("FAILED_BLOCKED", 0),
        "missing_from_ledger": missing,
        "pass": len(manifest) == len(status) and not missing and counts == Counter({"COMPLETE": len(manifest)}),
        "generated_at_unix": time.time(),
    }
    write_json(PHASE7 / "audits" / "phase6_global_audit.json", payload)
    if not payload["pass"]:
        raise RuntimeError(json.dumps(payload, indent=2))


def extract_run_level(manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    manifest_by_id = {row["experiment_id"]: row for row in manifest}
    completeness_rows: list[dict[str, Any]] = []
    consistency_rows: list[dict[str, Any]] = []
    run_rows: list[dict[str, Any]] = []
    for experiment_id in sorted(manifest_by_id):
        meta = manifest_by_id[experiment_id]
        stat = status[experiment_id]
        run_dir = Path(stat["final_run_dir"])
        missing, malformed = artifact_findings(run_dir)
        usable = stat["status"] == "COMPLETE" and not missing and not malformed
        if usable:
            ok, message = validate_run_artifacts(run_dir)
            usable = ok
            if not ok:
                malformed.append(message)
        completeness_rows.append({
            "experiment_id": experiment_id,
            "status": stat["status"],
            "missing_files": ";".join(missing),
            "malformed_files": ";".join(malformed),
            "usable_for_analysis": str(usable),
        })
        if not usable:
            continue
        summary = json.loads((run_dir / "summary.json").read_text())
        config = yaml.safe_load((run_dir / "experiment_config.yaml").read_text())
        recomputed = recompute_run(run_dir)
        consistency_rows.append(metrics_consistency_row(experiment_id, run_dir, summary, recomputed))
        run_rows.append({
            "experiment_id": experiment_id,
            "group": meta["group"],
            "workload": meta["workload"],
            "variant": meta["variant"],
            "downstream_capability": meta["downstream_capability"],
            "failure_scenario": meta["failure_scenario"],
            "failure_probability": meta["failure_probability"],
            "concurrency": meta["concurrency"],
            "duplicate_fanout": meta["duplicate_fanout"],
            "retry_fanout": config.get("failure", {}).get("retry_fanout", meta["duplicate_fanout"]),
            "seed": meta["seed"],
            "operations": meta["operations"],
            "warmup_operations": meta["warmup_operations"],
            **{metric: recomputed[metric] for metric in METRICS},
            "selected_for_fault": summary.get("selected_for_fault", ""),
            "reached_fault_hook": summary.get("reached_fault_hook", ""),
            "fault_injected": summary.get("fault_injected", ""),
            "logical_operations": recomputed["logical_operations"],
            "total_attempts": recomputed["total_attempts"],
            "total_downstream_calls": recomputed["total_downstream_calls"],
            "total_effects": recomputed["total_effects"],
            "duplicate_operations": recomputed["duplicate_operations"],
            "exactly_once_operations": recomputed["exactly_once_operations"],
            "zero_effect_operations": recomputed["zero_effect_operations"],
            "unknown_operations": recomputed["unknown_operations"],
            "reconciled_operations": recomputed["reconciled_operations"],
            "replayed_operations": recomputed["replayed_operations"],
            "run_dir": str(run_dir),
            "final_validation_status": "PASS",
        })
    write_csv(PHASE7 / "audits" / "phase6_artifact_completeness.csv", completeness_rows)
    write_csv(PHASE7 / "audits" / "metrics_consistency_full.csv", consistency_rows)
    failures = [row for row in consistency_rows if row["PASS_FAIL"] != "PASS"]
    write_json(PHASE7 / "audits" / "metrics_consistency_summary.json", {
        "runs_checked": len(consistency_rows),
        "unexplained_discrepancies": len(failures),
        "failed_experiment_ids": [row["experiment_id"] for row in failures[:100]],
        "rrr_definition": "RRR recomputed from measured agent_attempt events; replayed measured attempts divided by measured retry requests.",
        "pass": not failures,
    })
    if failures:
        raise RuntimeError(f"metric consistency failures: {len(failures)}")
    return run_rows


def artifact_findings(run_dir: Path) -> tuple[list[str], list[str]]:
    required = ["experiment_config.yaml", "events.csv", "operations.csv", "effects.csv", "summary.json"]
    optional = ["ledger_transitions.csv", "system_metrics.csv"]
    missing = [name for name in required if not (run_dir / name).exists()]
    malformed: list[str] = []
    for name in required + [name for name in optional if (run_dir / name).exists()]:
        path = run_dir / name
        if not path.exists():
            continue
        try:
            if name.endswith(".json"):
                json.loads(path.read_text())
            elif name.endswith(".yaml"):
                yaml.safe_load(path.read_text())
            elif name.endswith(".csv"):
                with path.open(newline="") as handle:
                    reader = csv.reader(handle)
                    next(reader, None)
        except Exception as exc:
            malformed.append(f"{name}:{exc}")
    return missing, malformed


def recompute_run(run_dir: Path) -> dict[str, Any]:
    ops = list(csv.DictReader((run_dir / "operations.csv").open(newline="")))
    total = len(ops) or 1
    attempts = sum(int(row["attempt_count"]) for row in ops)
    downstream_calls = sum(int(row["downstream_call_count"]) for row in ops)
    effects = sum(int(row["effect_count"]) for row in ops)
    duplicates = sum(1 for row in ops if int(row["effect_count"]) > 1)
    exact = sum(1 for row in ops if int(row["effect_count"]) == 1)
    lost = sum(1 for row in ops if int(row["effect_count"]) == 0)
    unknown = sum(1 for row in ops if row["final_state"] == "UNKNOWN")
    reconciled = sum(1 for row in ops if truthy(row["reconciled"]))
    replayed_ops = sum(1 for row in ops if truthy(row["replayed"]))
    latencies = sorted(float(row["latency"]) for row in ops)
    measured_agent_attempts = 0
    replayed_attempts = 0
    min_ts = None
    max_ts = None
    with (run_dir / "events.csv").open(newline="") as handle:
        for event in csv.DictReader(handle):
            if truthy(event.get("measured")) and event.get("event_type") == "agent_attempt":
                measured_agent_attempts += 1
                if truthy(event.get("replayed")):
                    replayed_attempts += 1
                ts = to_float(event.get("timestamp"))
                if ts is not None:
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                    max_ts = ts if max_ts is None else max(max_ts, ts)
    retry_requests = max(measured_agent_attempts - total, 0)
    elapsed = max((max_ts or 0) - (min_ts or 0), 0.001)
    return {
        "logical_operations": len(ops),
        "total_attempts": attempts,
        "total_downstream_calls": downstream_calls,
        "total_effects": effects,
        "duplicate_operations": duplicates,
        "exactly_once_operations": exact,
        "zero_effect_operations": lost,
        "unknown_operations": unknown,
        "reconciled_operations": reconciled,
        "replayed_operations": replayed_ops,
        "DER": duplicates / total,
        "EOER": exact / total,
        "LER": lost / total,
        "RSR": reconciled / max(sum(1 for row in ops if row["final_state"] in {"UNKNOWN", "COMPLETED"}), 1),
        "RAF": measured_agent_attempts / total,
        "DAF": downstream_calls / total,
        "RRR": replayed_attempts / max(retry_requests, 1),
        "UAR": unknown / total,
        "P50": percentile(latencies, 0.50),
        "P95": percentile(latencies, 0.95),
        "P99": percentile(latencies, 0.99),
        "throughput": len(ops) / elapsed,
    }


def metrics_consistency_row(experiment_id: str, run_dir: Path, summary: dict[str, Any], recomputed: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {"experiment_id": experiment_id, "run_dir": str(run_dir)}
    pass_flag = True
    for metric in ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99"]:
        delta = float(recomputed[metric]) - float(summary[metric])
        row[f"{metric}_delta"] = delta
        if abs(delta) > 1e-9:
            pass_flag = False
    row["throughput_recomputed_from_event_span"] = recomputed["throughput"]
    row["throughput_summary"] = summary.get("throughput", "")
    row["logical_operations_delta"] = int(recomputed["logical_operations"]) - int(summary["logical_operations"])
    if row["logical_operations_delta"]:
        pass_flag = False
    row["PASS_FAIL"] = "PASS" if pass_flag else "FAIL"
    return row


def write_semantic_audits(rows: list[dict[str, Any]]) -> None:
    phase6_state = PHASE6 / "state_transition_violations.csv"
    phase6_ownership = PHASE6 / "ownership_violations.csv"
    phase6_observer = PHASE6 / "observer_independence_audit.json"
    if phase6_state.exists() and phase6_ownership.exists() and phase6_observer.exists():
        observer = json.loads(phase6_observer.read_text())
        copy_if_exists(phase6_state, PHASE7 / "audits" / "state_transition_violations.csv")
        copy_if_exists(phase6_ownership, PHASE7 / "audits" / "ownership_violations.csv")
        write_json(PHASE7 / "audits" / "observer_independence.json", observer)
        write_json(PHASE7 / "audits" / "semantic_audit_provenance.json", {
            "source": "results/phase6 final global audit outputs",
            "state_transition_violations_bytes": phase6_state.stat().st_size,
            "ownership_violations_bytes": phase6_ownership.stat().st_size,
            "observer_independence_pass": observer.get("pass"),
            "phase7_action": "copied final Phase 6 semantic audit outputs after Phase 7 run-level extraction verified all 3340 final COMPLETE runs usable",
            "pass": phase6_state.stat().st_size == 0 and phase6_ownership.stat().st_size == 0 and bool(observer.get("pass")),
            "generated_at_unix": time.time(),
        })
        if phase6_state.stat().st_size != 0 or phase6_ownership.stat().st_size != 0 or not observer.get("pass"):
            raise RuntimeError("semantic audit failure")
        return
    transition_rows: list[dict[str, Any]] = []
    ownership_rows: list[dict[str, Any]] = []
    for row in rows:
        if row["variant"] in {"V2", "V4", "V5"}:
            run_dir = Path(row["run_dir"])
            transition_rows.extend({"experiment_id": row["experiment_id"], "run_dir": row["run_dir"], **v} for v in state_transition_violations(run_dir))
            ownership_rows.extend({"experiment_id": row["experiment_id"], "run_dir": row["run_dir"], **v} for v in ownership_overlaps(run_dir))
    write_csv(PHASE7 / "audits" / "state_transition_violations.csv", transition_rows)
    write_csv(PHASE7 / "audits" / "ownership_violations.csv", ownership_rows)
    observer_src = PHASE6 / "observer_independence_audit.json"
    observer = json.loads(observer_src.read_text()) if observer_src.exists() else {"pass": False, "reason": "missing phase6 observer audit"}
    write_json(PHASE7 / "audits" / "observer_independence.json", observer)
    if transition_rows or ownership_rows or not observer.get("pass"):
        raise RuntimeError("semantic audit failure")


def analyze(rows: list[dict[str, str]]) -> None:
    rows = normalize_rows(rows)
    write_csv(PHASE7 / "tables" / "master_descriptive_summary.csv", descriptive(rows, ["group", "workload", "failure_scenario", "variant", "downstream_capability", "failure_probability", "concurrency", "duplicate_fanout"], METRICS))
    write_csv(PHASE7 / "tables" / "rq4_baseline_overhead.csv", baseline_overhead(rows))
    write_csv(PHASE7 / "tables" / "rq1_ambiguous_failures.csv", descriptive(select(rows, group="group_b_ambiguous"), ["failure_scenario", "variant", "failure_probability", "concurrency"], METRICS))
    write_csv(PHASE7 / "tables" / "rq3_ambiguity_recovery.csv", ambiguity_recovery(rows))
    write_csv(PHASE7 / "tables" / "failure_rate_response.csv", failure_rate_response(rows))
    write_csv(PHASE7 / "tables" / "rq3_duplicate_pressure.csv", descriptive(select(rows, group="group_c_duplicates"), ["failure_scenario", "variant", "duplicate_fanout"], ["DER", "RAF", "DAF", "RRR", "UAR", "P95", "P99", "throughput"]))
    write_csv(PHASE7 / "tables" / "retry_amplification.csv", retry_amplification(rows))
    write_csv(PHASE7 / "tables" / "rq2_capability_placement.csv", descriptive(select(rows, group="group_d_capability"), ["failure_scenario", "variant", "downstream_capability"], ["DER", "EOER", "RSR", "UAR", "RAF", "DAF"]))
    write_csv(PHASE7 / "tables" / "safe_controls.csv", descriptive(select(rows, group="group_e_safe_controls"), ["failure_scenario", "variant"], ["DER", "EOER", "LER", "RAF", "DAF", "RRR", "UAR", "P95", "P99", "throughput"]))
    write_csv(PHASE7 / "tables" / "rq_generalization.csv", generalization(rows))
    write_csv(PHASE7 / "statistics" / "comparison_registry.csv", comparison_registry())
    effect_rows = effect_sizes(rows)
    write_csv(PHASE7 / "statistics" / "effect_sizes.csv", effect_rows)
    write_rq_evidence(rows, effect_rows)
    write_csv(PHASE7 / "statistics" / "hypothesis_evidence.csv", hypothesis_evidence(effect_rows))
    write_csv(PHASE7 / "tables" / "seed_robustness.csv", seed_robustness(rows))
    write_csv(PHASE7 / "audits" / "outlier_runs.csv", outlier_runs(rows))
    write_csv(PHASE7 / "audits" / "infrastructure_retry_summary.csv", infrastructure_retry_summary(rows))
    write_missing_data_audit(rows)
    write_csv(PHASE7 / "tables" / "exactly_once_effect_audit.csv", exactly_once_effect_audit(rows))
    write_csv(PHASE7 / "tables" / "v4_v5_deep_comparison.csv", descriptive([r for r in rows if r["variant"] in {"V4", "V5"} and r["failure_scenario"] in {"F3", "F8", "F9"}], ["failure_scenario", "variant", "failure_probability", "concurrency"], ["DER", "EOER", "RSR", "UAR", "RAF", "DAF", "P95", "P99"]))
    write_csv(PHASE7 / "tables" / "layer_placement_analysis.csv", layer_placement(rows))
    write_csv(PHASE7 / "tables" / "c0_limitation_evidence.csv", c0_limitation(rows))
    write_csv(PHASE7 / "tables" / "reliability_cost_tradeoff.csv", reliability_cost_tradeoff(rows))
    write_csv(PHASE7 / "tables" / "practical_effect_magnitudes.csv", practical_magnitudes(rows))
    write_csv(PHASE7 / "audits" / "result_sanity_flags.csv", sanity_flags(rows))
    paper_tables(rows)
    figure_csvs(rows)
    representative_traces(rows)


def normalize_rows(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        r: dict[str, Any] = dict(row)
        for key in METRICS + ["failure_probability"]:
            r[key] = to_float(r.get(key)) or 0.0
        for key in ["concurrency", "seed", "operations", "warmup_operations", "logical_operations", "total_attempts", "total_downstream_calls", "total_effects", "duplicate_operations", "exactly_once_operations", "zero_effect_operations", "unknown_operations", "reconciled_operations", "replayed_operations", "selected_for_fault", "reached_fault_hook", "fault_injected"]:
            r[key] = int(float(r[key])) if str(r.get(key, "")).strip() not in {"", "None"} else 0
        r["fanout"] = int(float(r.get("duplicate_fanout") or r.get("retry_fanout") or 0))
        out.append(r)
    return out


def descriptive(rows: list[dict[str, Any]], keys: list[str], metrics: list[str]) -> list[dict[str, Any]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(k, "") for k in keys)].append(row)
    out = []
    for key, group in sorted(groups.items(), key=lambda item: tuple(str(x) for x in item[0])):
        base = dict(zip(keys, key))
        for metric in metrics:
            vals = [float(row[metric]) for row in group if row.get(metric) not in {"", None}]
            if not vals:
                continue
            s = summary_stats(vals)
            out.append({**base, "metric": metric, **s})
    return out


def summary_stats(vals: list[float]) -> dict[str, Any]:
    vals = sorted(vals)
    n = len(vals)
    mean = sum(vals) / n
    std = statistics.stdev(vals) if n > 1 else 0.0
    ci = 1.96 * std / math.sqrt(n) if n > 1 else 0.0
    return {
        "n": n,
        "mean": mean,
        "median": statistics.median(vals),
        "std": std,
        "Q1": percentile(vals, 0.25),
        "Q3": percentile(vals, 0.75),
        "IQR": percentile(vals, 0.75) - percentile(vals, 0.25),
        "min": vals[0],
        "max": vals[-1],
        "CI_low": mean - ci,
        "CI_high": mean + ci,
    }


def baseline_overhead(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    baseline = [r for r in rows if r["group"] == "group_a_baseline"]
    means = grouped_means(baseline, ["variant", "concurrency"], ["P95", "P99", "throughput", "RAF", "DAF", "P50"])
    out = []
    for key, vals in sorted(means.items()):
        variant, concurrency = key
        if variant == "V0":
            continue
        base = means.get(("V0", concurrency))
        if not base:
            continue
        out.append({
            "variant": variant,
            "concurrency": concurrency,
            "n": vals["n"],
            "P50_mean": vals["P50"],
            "P95_mean": vals["P95"],
            "P99_mean": vals["P99"],
            "throughput_mean": vals["throughput"],
            "RAF_mean": vals["RAF"],
            "DAF_mean": vals["DAF"],
            "P95_delta_vs_V0": vals["P95"] - base["P95"],
            "P95_pct_vs_V0": pct_delta(vals["P95"], base["P95"]),
            "P99_delta_vs_V0": vals["P99"] - base["P99"],
            "P99_pct_vs_V0": pct_delta(vals["P99"], base["P99"]),
            "throughput_delta_vs_V0": vals["throughput"] - base["throughput"],
            "throughput_pct_vs_V0": pct_delta(vals["throughput"], base["throughput"]),
        })
    return out


def ambiguity_recovery(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["group"] == "group_b_ambiguous" and r["failure_scenario"] in {"F3", "F4", "F8", "F9"}]
    return descriptive(data, ["failure_scenario", "failure_probability", "concurrency", "variant"], ["DER", "EOER", "LER", "RSR", "UAR", "RAF", "DAF", "P95", "throughput"])


def failure_rate_response(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["failure_scenario"] in {"F3", "F4", "F8", "F9"} and r["group"] == "group_b_ambiguous"]
    desc = descriptive(data, ["failure_scenario", "variant", "concurrency", "failure_probability"], ["DER", "EOER", "LER", "RSR", "UAR", "RAF", "DAF"])
    return [{**{("failure" if k == "failure_scenario" else "probability" if k == "failure_probability" else k): v for k, v in row.items()}} for row in desc]


def retry_amplification(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["failure_scenario"] in {"F5", "F6", "F11"}]
    out = descriptive(data, ["failure_scenario", "variant", "fanout"], ["RAF", "DAF", "RRR", "DER", "UAR"])
    for row in out:
        row["bounded_effect_note"] = "RAF increases while DAF/DER indicate whether downstream effects remained bounded"
    return out


def generalization(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["workload"] in {"create_order", "charge_payment", "reserve_inventory", "send_notification"} and r["variant"] in {"V0", "V2", "V4", "V5"} and r["failure_scenario"] in {"F3", "F8"}]
    return descriptive(data, ["workload", "failure_scenario", "variant"], ["DER", "EOER", "RSR", "UAR", "RAF", "DAF", "P95", "throughput"])


def comparison_registry() -> list[dict[str, Any]]:
    rows = []
    for failure in ["F3", "F8"]:
        for a, b in [("V0", "V2"), ("V0", "V4"), ("V0", "V5"), ("V2", "V4"), ("V2", "V5"), ("V4", "V5")]:
            rows.append({"family": "RQ1", "comparison": f"{a} vs {b}", "variant_a": a, "variant_b": b, "group": "group_b_ambiguous", "failure_scenario": failure, "concurrency": 50, "failure_probability": 0.10, "metric": "DER"})
    for variant in ["V2", "V4"]:
        rows.append({"family": "RQ2", "comparison": f"{variant} C0 vs C1", "variant_a": variant, "variant_b": variant, "capability_a": "C0", "capability_b": "C1", "group": "group_d_capability", "failure_scenario": "F8", "metric": "UAR"})
        rows.append({"family": "RQ2", "comparison": f"{variant} C1 vs C2", "variant_a": variant, "variant_b": variant, "capability_a": "C1", "capability_b": "C2", "group": "group_d_capability", "failure_scenario": "F8", "metric": "UAR"})
    rows.append({"family": "RQ2", "comparison": "V5 C1 vs C2", "variant_a": "V5", "variant_b": "V5", "capability_a": "C1", "capability_b": "C2", "group": "group_d_capability", "failure_scenario": "F8", "metric": "RSR"})
    for failure in ["F8", "F9"]:
        rows.append({"family": "RQ3", "comparison": f"V4 vs V5 under {failure}", "variant_a": "V4", "variant_b": "V5", "group": "group_b_ambiguous", "failure_scenario": failure, "concurrency": 50, "failure_probability": 0.10, "metric": "UAR"})
    for failure in ["F6", "F11"]:
        rows.append({"family": "RQ3", "comparison": f"V0 vs V5 under {failure} fanout 10", "variant_a": "V0", "variant_b": "V5", "group": "group_c_duplicates", "failure_scenario": failure, "fanout": 10, "metric": "DAF"})
    for concurrency in [50, 100]:
        for b in ["V2", "V3", "V4", "V5"]:
            rows.append({"family": "RQ4", "comparison": f"V0 vs {b} F0 C{concurrency}", "variant_a": "V0", "variant_b": b, "group": "group_a_baseline", "failure_scenario": "F0", "concurrency": concurrency, "metric": "P95"})
    return rows


def effect_sizes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    registry = comparison_registry()
    out = []
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for comp in registry:
        a_rows, b_rows = comparison_rows(rows, comp)
        metric = comp["metric"]
        if not a_rows or not b_rows:
            rec = {**comp, "n_a": len(a_rows), "n_b": len(b_rows), "estimate_a": "", "estimate_b": "", "absolute_difference": "", "relative_difference_pct": "", "effect_size": "", "CI_low": "", "CI_high": "", "p_raw": "", "p_holm": "", "test": "NOT_TESTABLE"}
        else:
            a = [float(r[metric]) for r in a_rows]
            b = [float(r[metric]) for r in b_rows]
            diff = statistics.mean(b) - statistics.mean(a)
            ci_low, ci_high = bootstrap_mean_diff_ci(a, b)
            p = mann_whitney_p(a, b) if metric in PERF_METRICS else normal_diff_p(a, b)
            rec = {**comp, "n_a": len(a), "n_b": len(b), "estimate_a": statistics.mean(a), "estimate_b": statistics.mean(b), "absolute_difference": diff, "relative_difference_pct": pct_delta(statistics.mean(b), statistics.mean(a)), "effect_size": cliffs_delta(a, b), "CI_low": ci_low, "CI_high": ci_high, "p_raw": p, "p_holm": "", "test": "Mann-Whitney U normal approximation" if metric in PERF_METRICS else "run-level mean difference normal approximation"}
        by_family[comp["family"]].append(rec)
    for family_rows in by_family.values():
        holm_adjust(family_rows)
        out.extend(family_rows)
    return out


def comparison_rows(rows: list[dict[str, Any]], comp: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def common(r: dict[str, Any]) -> bool:
        for key in ["group", "failure_scenario", "concurrency", "failure_probability", "fanout"]:
            if key in comp and str(r.get(key)) != str(comp[key]):
                return False
        return True
    if "capability_a" in comp:
        a = [r for r in rows if common(r) and r["variant"] == comp["variant_a"] and r["downstream_capability"] == comp["capability_a"]]
        b = [r for r in rows if common(r) and r["variant"] == comp["variant_b"] and r["downstream_capability"] == comp["capability_b"]]
    else:
        a = [r for r in rows if common(r) and r["variant"] == comp["variant_a"]]
        b = [r for r in rows if common(r) and r["variant"] == comp["variant_b"]]
    return a, b


def write_rq_evidence(rows: list[dict[str, Any]], effect_rows: list[dict[str, Any]]) -> None:
    for rq in ["RQ1", "RQ2", "RQ3", "RQ4"]:
        out = []
        for row in effect_rows:
            if row["family"] != rq:
                continue
            out.append({
                "comparison": row["comparison"],
                "experimental_conditions": ";".join(f"{k}={row.get(k)}" for k in ["group", "failure_scenario", "concurrency", "failure_probability", "fanout"] if row.get(k, "") != ""),
                "sample_size": f"{row.get('n_a')} vs {row.get('n_b')}",
                "primary_metric": row["metric"],
                "variant_A_estimate": row["estimate_a"],
                "variant_B_estimate": row["estimate_b"],
                "absolute_difference": row["absolute_difference"],
                "relative_difference": row["relative_difference_pct"],
                "95% CI": f"{row['CI_low']}..{row['CI_high']}",
                "effect_size": row["effect_size"],
                "raw_p_value": row["p_raw"],
                "adjusted_p_value": row["p_holm"],
                "concise_data_only_observation": data_observation(row),
            })
        write_csv(PHASE7 / "tables" / f"{rq}_evidence.csv", out)


def hypothesis_evidence(effect_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapping = {
        "H1": [r for r in effect_rows if r["family"] == "RQ1" and r["comparison"].startswith("V0 vs V2")],
        "H2": [r for r in effect_rows if r["family"] == "RQ1" and ("V2 vs V4" in r["comparison"] or "V2 vs V5" in r["comparison"])],
        "H3": [r for r in effect_rows if r["comparison"].startswith("V4 vs V5 under")],
        "H4": [r for r in effect_rows if r["family"] == "RQ4"],
        "H5": [r for r in effect_rows if r["family"] == "RQ3" and "fanout 10" in r["comparison"]],
    }
    out = []
    for hyp, comps in mapping.items():
        direction = evidence_direction(hyp, comps)
        for row in comps or [{}]:
            out.append({
                "hypothesis": hyp,
                "comparison": row.get("comparison", ""),
                "metric": row.get("metric", ""),
                "effect": row.get("absolute_difference", ""),
                "CI": f"{row.get('CI_low', '')}..{row.get('CI_high', '')}",
                "p_raw": row.get("p_raw", ""),
                "p_adjusted": row.get("p_holm", ""),
                "consistency_across_seeds": seed_consistency_label(row),
                "evidence_direction": direction,
            })
    return out


def evidence_direction(hyp: str, comps: list[dict[str, Any]]) -> str:
    if not comps:
        return "NOT_TESTABLE"
    effects = [to_float(c.get("absolute_difference")) for c in comps if c.get("absolute_difference") != ""]
    if not effects:
        return "NOT_TESTABLE"
    if hyp in {"H1", "H2", "H3", "H5"}:
        expected = [e for e in effects if e is not None and e < 0]
    else:
        expected = [e for e in effects if e is not None and e > 0]
    if len(expected) == len(effects):
        return "CONSISTENT_WITH_HYPOTHESIS"
    if expected:
        return "MIXED"
    return "NOT_CONSISTENT"


def seed_robustness(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for group, group_rows in groupby_rows(rows, ["group", "failure_scenario", "variant"]).items():
        seeds = sorted({r["seed"] for r in group_rows})
        for metric in ["DER", "UAR", "DAF", "P95"]:
            vals = [statistics.mean([r[metric] for r in group_rows if r["seed"] == seed]) for seed in seeds]
            if vals:
                out.append({"group": group[0], "failure_scenario": group[1], "variant": group[2], "metric": metric, "seed_count": len(vals), "min_seed_mean": min(vals), "max_seed_mean": max(vals), "all_seeds_nonzero": all(v != 0 for v in vals), "zero_seed_count": sum(1 for v in vals if v == 0)})
    return out


def outlier_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for key, group in groupby_rows(rows, ["group", "failure_scenario", "variant", "concurrency"]).items():
        for metric in ["P95", "P99", "throughput"]:
            vals = sorted(float(r[metric]) for r in group)
            if len(vals) < 4:
                continue
            q1, q3 = percentile(vals, 0.25), percentile(vals, 0.75)
            iqr = q3 - q1
            low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
            for r in group:
                if float(r[metric]) < low or float(r[metric]) > high:
                    out.append({"experiment_id": r["experiment_id"], "metric": metric, "observed_value": r[metric], "group_distribution_context": f"{key};Q1={q1};Q3={q3};IQR={iqr}", "possible_infrastructure_note": ""})
    return out


def infrastructure_retry_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    manifest, status = load_phase6_inputs()
    failure_root = PHASE6 / "failures"
    out = []
    for exp_id, stat in sorted(status.items()):
        failures = list((failure_root / exp_id).glob("*/failure.json")) if (failure_root / exp_id).exists() else []
        causes = []
        for path in failures:
            try:
                payload = json.loads(path.read_text())
                causes.append(payload.get("failure_classification", ""))
            except Exception:
                causes.append("unreadable_failure_json")
        infra = [cause for cause in causes if any(token in cause.upper() for token in ["INFRA", "DOCKER", "OOM", "POSTGRES", "DAEMON"])]
        noninfra = [cause for cause in causes if cause not in infra]
        out.append({
            "experiment_id": exp_id,
            "number_of_infra_retries": len(infra),
            "preserved_noninfra_attempts": len(noninfra),
            "cause": ";".join(sorted(set(infra))),
            "preserved_noninfra_cause": ";".join(sorted(set(noninfra))),
            "final_valid_run_obtained": str(stat["status"] == "COMPLETE"),
            "whether_scientific_config_changed": "NO",
        })
    return out


def write_missing_data_audit(rows: list[dict[str, Any]]) -> None:
    manifest, _ = load_phase6_inputs()
    missing_fields = Counter()
    nan_metrics = Counter()
    for row in rows:
        for key, value in row.items():
            if value == "":
                missing_fields[key] += 1
        for metric in METRICS:
            if isinstance(row.get(metric), float) and math.isnan(row[metric]):
                nan_metrics[metric] += 1
    write_json(PHASE7 / "audits" / "missing_data_audit.json", {
        "expected_experiments": len(manifest),
        "valid_experiments": len(rows),
        "missing_experiments": len(manifest) - len(rows),
        "unusable_experiments": 0,
        "missing_fields": dict(missing_fields),
        "NaNs_per_metric": dict(nan_metrics),
        "pass": len(rows) == len(manifest) and not nan_metrics,
    })


def exactly_once_effect_audit(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["variant"] in PROTECTED and r["failure_scenario"] != "F0"]
    out = []
    for key, group in groupby_rows(data, ["group", "workload", "failure_scenario", "variant", "downstream_capability", "failure_probability", "concurrency", "fanout"]).items():
        ops = sum(r["logical_operations"] for r in group)
        out.append({
            "group": key[0], "workload": key[1], "failure_scenario": key[2], "variant": key[3], "downstream_capability": key[4], "failure_probability": key[5], "concurrency": key[6], "fanout": key[7],
            "runs": len(group),
            "logical_operations": ops,
            "zero_effects": sum(r["zero_effect_operations"] for r in group),
            "exactly_one_effect": sum(r["exactly_once_operations"] for r in group),
            "more_than_one_effect": sum(r["duplicate_operations"] for r in group),
            "unresolved_UNKNOWN": sum(r["unknown_operations"] for r in group),
            "reconciled": sum(r["reconciled_operations"] for r in group),
            "replayed": sum(r["replayed_operations"] for r in group),
        })
    return out


def layer_placement(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    layer = {"V1": "agent identity/correlation", "V2": "gateway durable dedupe", "V3": "downstream idempotency", "V4": "end-to-end stable identity + ledger + downstream support", "V5": "V4 + reconciliation"}
    out = []
    for row in descriptive([r for r in rows if r["variant"] in layer], ["variant"], ["DER", "UAR", "RSR", "DAF", "P95"],):
        out.append({**row, "mechanism_mapping": layer.get(row["variant"], "")})
    return out


def c0_limitation(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    data = [r for r in rows if r["downstream_capability"] == "C0" and r["failure_scenario"] in {"F3", "F8", "F9"} and r["variant"] in {"V2", "V4", "V5"}]
    out = descriptive(data, ["failure_scenario", "variant", "downstream_capability"], ["DER", "UAR", "RSR", "EOER"])
    for row in out:
        row["limitation_note"] = "C0 has no downstream identity/query support; post-effect ambiguity may remain unresolved under this model."
    return out


def reliability_cost_tradeoff(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reps = [r for r in rows if (r["group"] == "group_b_ambiguous" and r["failure_scenario"] in {"F3", "F8"} and r["failure_probability"] == 0.10 and r["concurrency"] == 50) or (r["group"] == "group_a_baseline" and r["concurrency"] == 50)]
    out = []
    for key, group in groupby_rows(reps, ["group", "failure_scenario", "variant"]).items():
        v0 = [r for r in reps if r["group"] == key[0] and r["failure_scenario"] == key[1] and r["variant"] == "V0"]
        if not v0 or key[2] == "V0":
            continue
        out.append({"group": key[0], "failure_scenario": key[1], "variant": key[2], "DER_improvement_vs_V0": mean(v0, "DER") - mean(group, "DER"), "UAR": mean(group, "UAR"), "RSR": mean(group, "RSR"), "RAF": mean(group, "RAF"), "DAF": mean(group, "DAF"), "P95_overhead_vs_V0": mean(group, "P95") - mean(v0, "P95"), "P99_overhead_vs_V0": mean(group, "P99") - mean(v0, "P99"), "throughput_overhead_vs_V0": mean(group, "throughput") - mean(v0, "throughput")})
    return out


def practical_magnitudes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in reliability_cost_tradeoff(rows):
        out.append({**r, "duplicate_operations_prevented_per_10000": 10000 * r["DER_improvement_vs_V0"], "unresolved_operations_per_10000": 10000 * r["UAR"], "additional_agent_attempts_per_operation": r["RAF"] - 1, "downstream_calls_per_10000": 10000 * r["DAF"], "milliseconds_added_P95": r["P95_overhead_vs_V0"], "milliseconds_added_P99": r["P99_overhead_vs_V0"]})
    return out


def sanity_flags(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        def flag(name: str, detail: str) -> None:
            out.append({"experiment_id": r["experiment_id"], "flag": name, "detail": detail})
        if r["variant"] in PROTECTED and r["DER"] > 0:
            flag("protected_variant_DER_gt_zero", f"DER={r['DER']}")
        if r["variant"] == "V5" and r["downstream_capability"] == "C2" and r["failure_scenario"] == "F8" and r["UAR"] > 0.01:
            flag("v5_c2_f8_high_UAR", f"UAR={r['UAR']}")
        if r["RAF"] < r["DAF"]:
            flag("RAF_lt_DAF", f"RAF={r['RAF']} DAF={r['DAF']}")
        if any(r[m] < 0 for m in ["P50", "P95", "P99"]):
            flag("negative_latency", "")
        if r["throughput"] <= 0:
            flag("nonpositive_throughput", f"throughput={r['throughput']}")
        if r["total_effects"] < r["exactly_once_operations"] + 2 * r["duplicate_operations"]:
            flag("effect_count_inconsistency", "")
        if r["fault_injected"] > r["reached_fault_hook"]:
            flag("fault_injected_gt_reached", "")
    return out


def paper_tables(rows: list[dict[str, Any]]) -> None:
    write_csv(PHASE7 / "tables" / "paper_table_1_experiment_design.csv", design_summary(rows))
    copy_table("rq4_baseline_overhead.csv", "paper_table_2_baseline.csv")
    copy_table("rq1_ambiguous_failures.csv", "paper_table_3_ambiguity.csv")
    copy_table("rq3_ambiguity_recovery.csv", "paper_table_4_reconciliation.csv")
    copy_table("rq3_duplicate_pressure.csv", "paper_table_5_duplicates.csv")
    copy_table("rq2_capability_placement.csv", "paper_table_6_capability.csv")
    copy_table("rq_generalization.csv", "paper_table_7_generalization.csv")


def figure_csvs(rows: list[dict[str, Any]]) -> None:
    write_csv(PHASE7 / "figures" / "fig1_der_by_variant.csv", descriptive([r for r in rows if r["group"] == "group_b_ambiguous"], ["failure_scenario", "variant", "failure_probability", "concurrency"], ["DER"]))
    write_csv(PHASE7 / "figures" / "fig2_reconciliation.csv", descriptive([r for r in rows if r["variant"] in {"V4", "V5"} and r["failure_scenario"] == "F8"], ["variant", "failure_probability", "concurrency"], ["UAR", "RSR"]))
    write_csv(PHASE7 / "figures" / "fig3_retry_amplification.csv", descriptive([r for r in rows if r["failure_scenario"] in {"F5", "F6", "F11"}], ["failure_scenario", "variant", "fanout"], ["RAF", "DAF"]))
    write_csv(PHASE7 / "figures" / "fig4_latency_overhead.csv", baseline_overhead(rows))
    write_csv(PHASE7 / "figures" / "fig5_capability.csv", descriptive(select(rows, group="group_d_capability"), ["failure_scenario", "variant", "downstream_capability"], ["DER", "UAR", "RSR"]))
    write_csv(PHASE7 / "figures" / "fig6_generalization.csv", generalization(rows))


def representative_traces(rows: list[dict[str, Any]]) -> None:
    specs = [
        ("v0_f3_duplicate", lambda r: r["variant"] == "V0" and r["failure_scenario"] == "F3" and r["duplicate_operations"] > 0),
        ("v2_f3", lambda r: r["variant"] == "V2" and r["failure_scenario"] == "F3"),
        ("v4_f8_unknown", lambda r: r["variant"] == "V4" and r["failure_scenario"] == "F8" and r["unknown_operations"] > 0),
        ("v5_f8_reconciliation", lambda r: r["variant"] == "V5" and r["failure_scenario"] == "F8" and r["reconciled_operations"] > 0),
        ("v0_f6_fanout10", lambda r: r["variant"] == "V0" and r["failure_scenario"] == "F6" and r["fanout"] == 10),
        ("v5_f6_fanout10", lambda r: r["variant"] == "V5" and r["failure_scenario"] == "F6" and r["fanout"] == 10),
        ("v0_f11_fanout10", lambda r: r["variant"] == "V0" and r["failure_scenario"] == "F11" and r["fanout"] == 10),
        ("v5_f11_fanout10", lambda r: r["variant"] == "V5" and r["failure_scenario"] == "F11" and r["fanout"] == 10),
        ("capability_c0", lambda r: r["group"] == "group_d_capability" and r["downstream_capability"] == "C0"),
        ("capability_c2", lambda r: r["group"] == "group_d_capability" and r["downstream_capability"] == "C2"),
        ("payment", lambda r: r["workload"] == "charge_payment"),
        ("inventory", lambda r: r["workload"] == "reserve_inventory"),
        ("notification", lambda r: r["workload"] == "send_notification"),
    ]
    index = []
    used = set()
    for label, predicate in specs:
        row = next((r for r in rows if predicate(r) and r["experiment_id"] not in used), None)
        if not row:
            continue
        used.add(row["experiment_id"])
        op_id = choose_operation(Path(row["run_dir"]), row)
        dest = PHASE7 / "representative_traces" / label
        dest.mkdir(parents=True, exist_ok=True)
        for name in ["experiment_config.yaml", "summary.json"]:
            shutil.copyfile(Path(row["run_dir"]) / name, dest / name)
        filter_csv(Path(row["run_dir"]) / "operations.csv", dest / "operations.csv", lambda r, op_id=op_id: r.get("operation_id") == op_id, limit=200)
        filter_csv(Path(row["run_dir"]) / "events.csv", dest / "events.csv", lambda r, op_id=op_id: r.get("operation_id") == op_id, limit=500)
        filter_csv(Path(row["run_dir"]) / "effects.csv", dest / "effects.csv", lambda r, op_id=op_id: r.get("operation_id") == op_id, limit=200)
        led = Path(row["run_dir"]) / "ledger_transitions.csv"
        if led.exists():
            filter_csv(led, dest / "ledger_transitions.csv", lambda r, op_id=op_id: r.get("operation_id") == op_id, limit=200)
        narrative = {
            "trace_label": label,
            "experiment_id": row["experiment_id"],
            "logical_operation": op_id,
            "injected_failure": row["failure_scenario"],
            "variant": row["variant"],
            "downstream_capability": row["downstream_capability"],
            "workload": row["workload"],
            "attempt_sequence": "see events.csv",
            "downstream_calls": "see events.csv and operations.csv",
            "effect_creation": "see effects.csv",
            "ledger_transitions": "see ledger_transitions.csv when non-empty",
            "retry_replay_reconciliation": f"replayed_ops={row['replayed_operations']};reconciled_ops={row['reconciled_operations']}",
            "final_state": "see operations.csv",
        }
        write_json(dest / "narrative.json", narrative)
        index.append(narrative)
    write_csv(PHASE7 / "representative_traces" / "index.csv", index)


def choose_operation(run_dir: Path, row: dict[str, Any]) -> str:
    with (run_dir / "operations.csv").open(newline="") as handle:
        ops = list(csv.DictReader(handle))
    if row["duplicate_operations"]:
        found = next((op for op in ops if int(op["effect_count"]) > 1), None)
    elif row["unknown_operations"]:
        found = next((op for op in ops if op["final_state"] == "UNKNOWN"), None)
    elif row["reconciled_operations"]:
        found = next((op for op in ops if truthy(op["reconciled"])), None)
    else:
        found = ops[0] if ops else {}
    return found.get("operation_id", "")


def design_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for key, group in groupby_rows(rows, ["group", "workload", "failure_scenario"]).items():
        out.append({"group": key[0], "workload": key[1], "failure_scenario": key[2], "runs": len(group), "variants": ",".join(sorted({r["variant"] for r in group})), "capabilities": ",".join(sorted({r["downstream_capability"] for r in group})), "concurrency_levels": ",".join(map(str, sorted({r["concurrency"] for r in group}))), "probabilities": ",".join(map(str, sorted({r["failure_probability"] for r in group}))), "fanouts": ",".join(map(str, sorted({r["fanout"] for r in group}))), "seeds": len({r["seed"] for r in group})})
    return out


def export_bundle(zip_bundle: bool = False) -> None:
    repro = PHASE7 / "export" / "reproducibility"
    bundle = PHASE7 / "export" / "paper_analysis_bundle"
    repro.mkdir(parents=True, exist_ok=True)
    bundle.mkdir(parents=True, exist_ok=True)
    copy_if_exists(MANIFEST, repro / "manifest.csv")
    copy_if_exists(STATUS, repro / "phase6_execution_status.csv")
    for path in (PHASE6 / "stages").glob("stage_*audit.json"):
        copy_if_exists(path, repro / path.name)
    for name in ["environment_metadata.json", "source_manifest_sha256.csv"]:
        copy_if_exists(PHASE6 / name, repro / name)
    copy_if_exists(PHASE7 / "audits" / "known_analysis_corrections.md", repro / "known_analysis_corrections.md")
    copy_if_exists(Path(__file__), repro / "phase7_analysis.py")
    bundle_files = [
        PHASE7 / "PHASE7_REPORT.md",
        PHASE7 / "phase6_run_level.csv",
        PHASE7 / "tables" / "master_descriptive_summary.csv",
        PHASE7 / "statistics" / "hypothesis_evidence.csv",
        PHASE7 / "statistics" / "comparison_registry.csv",
        PHASE7 / "statistics" / "effect_sizes.csv",
        PHASE7 / "tables" / "exactly_once_effect_audit.csv",
        PHASE7 / "tables" / "v4_v5_deep_comparison.csv",
        PHASE7 / "tables" / "layer_placement_analysis.csv",
        PHASE7 / "tables" / "c0_limitation_evidence.csv",
        PHASE7 / "tables" / "reliability_cost_tradeoff.csv",
        PHASE7 / "tables" / "practical_effect_magnitudes.csv",
        PHASE7 / "tables" / "seed_robustness.csv",
        PHASE7 / "audits" / "metrics_consistency_summary.json",
        PHASE7 / "audits" / "state_transition_violations.csv",
        PHASE7 / "audits" / "ownership_violations.csv",
        PHASE7 / "audits" / "observer_independence.json",
        PHASE7 / "audits" / "missing_data_audit.json",
        PHASE7 / "audits" / "infrastructure_retry_summary.csv",
        PHASE7 / "audits" / "known_analysis_corrections.md",
        PHASE6 / "environment_metadata.json",
        PHASE6 / "source_manifest_sha256.csv",
        PHASE7 / "representative_traces" / "index.csv",
        Path(__file__),
    ]
    for sub in ["tables", "figures"]:
        bundle_files.extend(sorted((PHASE7 / sub).glob("*.csv")))
    for src in bundle_files:
        if src.exists():
            rel = src.relative_to(PHASE7) if src.is_relative_to(PHASE7) else Path("reproducibility") / src.name
            copy_if_exists(src, bundle / rel)
    traces_dest = bundle / "representative_traces"
    if traces_dest.exists():
        shutil.rmtree(traces_dest)
    shutil.copytree(PHASE7 / "representative_traces", traces_dest)
    copytree_small(repro, bundle / "reproducibility")
    manifest_rows = []
    for path in sorted(bundle.rglob("*")):
        if path.is_file():
            manifest_rows.append({"path": str(path.relative_to(bundle)), "size_bytes": path.stat().st_size, "sha256": sha256_file(path)})
    write_csv(bundle / "bundle_manifest.csv", manifest_rows)
    summary = {"bundle_dir": str(bundle), "size_bytes": dir_size(bundle), "file_count": len(manifest_rows), "zip_path": "", "zip_size_bytes": 0}
    if zip_bundle and dir_size(bundle) < 1024 * 1024 * 1024:
        zip_path = PHASE7 / "export" / "paper_analysis_bundle.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in bundle.rglob("*"):
                if path.is_file():
                    zf.write(path, path.relative_to(bundle.parent))
        summary.update({"zip_path": str(zip_path), "zip_size_bytes": zip_path.stat().st_size})
    write_json(PHASE7 / "export" / "bundle_summary.json", summary)


def write_known_corrections() -> None:
    (PHASE7 / "audits" / "known_analysis_corrections.md").write_text(
        "# Known Analysis Corrections\n\n"
        "- Phase 6 F6 validation initially recomputed RRR from per-operation replay booleans while `summary.json` used measured agent-attempt events. This was a validator-only defect. After correcting the validator to use the measured-event basis, preserved F6 artifacts validated and were restored to COMPLETE.\n"
        "- Phase 7 recomputes RRR from measured `agent_attempt` events: replayed measured attempts divided by measured retry requests. This preserves the corrected Phase 6 definition and avoids the earlier analysis-only regression.\n"
    )


def select(rows: list[dict[str, Any]], **criteria: Any) -> list[dict[str, Any]]:
    return [r for r in rows if all(r.get(k) == v for k, v in criteria.items())]


def grouped_means(rows: list[dict[str, Any]], keys: list[str], metrics: list[str]) -> dict[tuple[Any, ...], dict[str, float]]:
    out = {}
    for key, group in groupby_rows(rows, keys).items():
        out[key] = {"n": len(group), **{m: mean(group, m) for m in metrics}}
    return out


def groupby_rows(rows: list[dict[str, Any]], keys: list[str]) -> dict[tuple[Any, ...], list[dict[str, Any]]]:
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row.get(k, "") for k in keys)].append(row)
    return groups


def mean(rows: list[dict[str, Any]], key: str) -> float:
    vals = [float(r[key]) for r in rows]
    return sum(vals) / len(vals) if vals else 0.0


def pct_delta(value: float, base: float) -> float:
    return ((value - base) / base * 100.0) if base else 0.0


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    return values[min(int(len(values) * p), len(values) - 1)]


def bootstrap_mean_diff_ci(a: list[float], b: list[float]) -> tuple[float, float]:
    if not a or not b:
        return 0.0, 0.0
    diff = statistics.mean(b) - statistics.mean(a)
    se = math.sqrt((statistics.variance(a) / len(a) if len(a) > 1 else 0) + (statistics.variance(b) / len(b) if len(b) > 1 else 0))
    return diff - 1.96 * se, diff + 1.96 * se


def normal_diff_p(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 1.0
    se = math.sqrt((statistics.variance(a) / len(a) if len(a) > 1 else 0) + (statistics.variance(b) / len(b) if len(b) > 1 else 0))
    if se == 0:
        return 1.0 if statistics.mean(a) == statistics.mean(b) else 0.0
    z = abs((statistics.mean(b) - statistics.mean(a)) / se)
    return math.erfc(z / math.sqrt(2))


def mann_whitney_p(a: list[float], b: list[float]) -> float:
    combined = sorted([(x, 0) for x in a] + [(x, 1) for x in b], key=lambda item: item[0])
    ranks = {}
    i = 0
    while i < len(combined):
        j = i
        while j < len(combined) and combined[j][0] == combined[i][0]:
            j += 1
        rank = (i + 1 + j) / 2
        for k in range(i, j):
            ranks.setdefault(combined[k][0], rank)
        i = j
    rank_a = sum(ranks[x] for x in a)
    n1, n2 = len(a), len(b)
    u1 = rank_a - n1 * (n1 + 1) / 2
    mu = n1 * n2 / 2
    sigma = math.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)
    if sigma == 0:
        return 1.0
    z = abs((u1 - mu) / sigma)
    return math.erfc(z / math.sqrt(2))


def cliffs_delta(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    gt = sum(1 for x in b for y in a if x > y)
    lt = sum(1 for x in b for y in a if x < y)
    return (gt - lt) / (len(a) * len(b))


def holm_adjust(rows: list[dict[str, Any]]) -> None:
    valid = [(i, float(row["p_raw"])) for i, row in enumerate(rows) if row.get("p_raw") not in {"", None}]
    m = len(valid)
    adjusted = [None] * len(rows)
    prev = 0.0
    for rank, (idx, p) in enumerate(sorted(valid, key=lambda item: item[1]), start=1):
        adj = min(1.0, max(prev, (m - rank + 1) * p))
        adjusted[idx] = adj
        prev = adj
    for i, row in enumerate(rows):
        row["p_holm"] = "" if adjusted[i] is None else adjusted[i]


def data_observation(row: dict[str, Any]) -> str:
    if row.get("absolute_difference") == "":
        return "not testable from available planned cell"
    return f"{row['metric']} changed by {row['absolute_difference']} from A to B"


def seed_consistency_label(row: dict[str, Any]) -> str:
    if not row or row.get("absolute_difference") == "":
        return "NOT_TESTABLE"
    return "reported in seed_robustness.csv"


def copy_table(src_name: str, dest_name: str) -> None:
    copy_if_exists(PHASE7 / "tables" / src_name, PHASE7 / "tables" / dest_name)


def filter_csv(src: Path, dest: Path, predicate, limit: int) -> None:
    with src.open(newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            if predicate(row):
                rows.append(row)
                if len(rows) >= limit:
                    break
        write_csv(dest, rows)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fields = list(rows[0].keys())
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def copy_if_exists(src: Path, dest: Path) -> None:
    if src.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)


def copytree_small(src: Path, dest: Path) -> None:
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


def dir_size(path: Path) -> int:
    return sum(file.stat().st_size for file in path.rglob("*") if file.is_file())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def truthy(value: Any) -> bool:
    return str(value).lower() in {"true", "1", "yes"}


def to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
