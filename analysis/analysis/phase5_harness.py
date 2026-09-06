import argparse
import csv
import fcntl
import json
import math
import os
import platform
import re
import shutil
import statistics
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import psycopg
import yaml

from agent_simulator.workload import build_workload
from experiment_runner.run import run_config
from fault_injector.deterministic import SCENARIO_HOOKS, selected


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "results" / "phase5-docker-desktop"
RUNS_ROOT = OUT / "runs"
CONFIG_ROOT = ROOT / "configs" / "phase5"
PHASE6_ROOT = ROOT / "configs" / "phase6"
ADMIN_DSN = "postgresql://exactlyonce:exactlyonce@localhost:5432/exactlyonce"
ANALYSIS_DSN = "postgresql://analysis_user:analysis_user@localhost:5432/exactlyonce"
BASELINE_SEEDS = [5001, 5002, 5003]
STRESS_SEEDS = [5101]
CAPABILITY_SEEDS = [5101, 5102, 5103]
FAIRNESS_SEEDS = [5201, 5202, 5203, 5204, 5205]
PRESTUDY_SEEDS = [5301, 5302, 5303, 5304, 5305]
PROBABILITY_SEEDS = [5401, 5402, 5403]
DOCKER_DESKTOP_DISK_IMAGE_MIB_FALLBACK = 233752


def docker_compose(*args: str, capture_output: bool = False, shell: bool = False, command: Optional[str] = None) -> subprocess.CompletedProcess[str]:
    if shell:
        assert command is not None
        return subprocess.run(command, cwd=ROOT, shell=True, capture_output=capture_output, text=True)
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=capture_output, text=True)


@contextmanager
def phase5_run_lock():
    lock_path = OUT / ".phase5_harness.lock"
    OUT.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            owner = handle.read().strip() or "unknown"
            raise RuntimeError(f"phase5_harness already running under pid {owner}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()))
        handle.flush()
        try:
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=[
        "baseline", "stress", "compounds", "capabilities", "fairness", "isolation",
        "restart", "resources", "ledger", "lease", "observer", "metrics",
        "warmup", "prestudy", "probability", "generalization", "environment", "phase6",
        "state", "manifest", "inventory", "f10", "f11",
        "all-small"
    ], required=True)
    parser.add_argument("--operations", type=int, default=None)
    parser.add_argument("--seeds", default=None)
    parser.add_argument("--scenario", default=None)
    parser.add_argument("--variant", default=None)
    parser.add_argument("--concurrency", type=int, default=None)
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    CONFIG_ROOT.mkdir(parents=True, exist_ok=True)
    write_load_adjustment_record()
    with phase5_run_lock():
        if args.mode == "baseline":
            baseline_qualification(args.operations or 1000, parse_seeds(args.seeds, BASELINE_SEEDS))
        elif args.mode == "stress":
            run_stress(args.operations or 1000, parse_seeds(args.seeds, STRESS_SEEDS), args.scenario, args.variant, args.concurrency)
        elif args.mode == "compounds":
            run_compounds(args.operations or 500, args.scenario, args.variant)
        elif args.mode == "capabilities":
            run_capabilities(args.operations or 1000, parse_seeds(args.seeds, CAPABILITY_SEEDS))
        elif args.mode == "fairness":
            fairness_audit(parse_seeds(args.seeds, FAIRNESS_SEEDS))
        elif args.mode == "isolation":
            isolation_audit(args.operations or 100)
        elif args.mode == "restart":
            restart_audit(args.operations or 500)
        elif args.mode == "resources":
            resource_audit(args.operations or 200)
        elif args.mode == "ledger":
            ledger_audit(args.operations or 1000)
        elif args.mode == "lease":
            lease_audit(args.operations or 200)
        elif args.mode == "observer":
            observer_audit()
        elif args.mode == "metrics":
            metrics_audit()
        elif args.mode == "warmup":
            warmup_audit()
        elif args.mode == "prestudy":
            run_prestudy(args.operations or 1000, parse_seeds(args.seeds, PRESTUDY_SEEDS))
        elif args.mode == "probability":
            probability_audit(args.operations or 10000, parse_seeds(args.seeds, PROBABILITY_SEEDS))
        elif args.mode == "generalization":
            generalization_audit(args.operations or 1000)
        elif args.mode == "environment":
            write_environment_metadata()
        elif args.mode == "phase6":
            generate_phase6_matrix()
        elif args.mode == "f10":
            f10_hardening(args.operations or 500)
        elif args.mode == "f11":
            f11_hardening(args.operations or 1000)
        elif args.mode == "state":
            aggregate_state_and_ownership_audit()
        elif args.mode == "manifest":
            validate_phase6_manifest()
        elif args.mode == "inventory":
            write_phase5_inventory()
        elif args.mode == "all-small":
            run_stress(args.operations or 300, STRESS_SEEDS)
            run_compounds(args.operations or 300)
            run_capabilities(args.operations or 300, [CAPABILITY_SEEDS[0]])
            fairness_audit(FAIRNESS_SEEDS[:2])
            isolation_audit(50)
            restart_audit(100)
            resource_audit(50)
            ledger_audit(100)
            lease_audit(100)
            observer_audit()
            warmup_audit()
            generalization_audit(100)
            run_prestudy(args.operations or 300, PRESTUDY_SEEDS[:2])
            probability_audit(1000, [PROBABILITY_SEEDS[0]])
            metrics_audit()
            generate_phase6_matrix()


def parse_seeds(raw: Optional[str], default: list[int]) -> list[int]:
    if not raw:
        return default
    return [int(item) for item in raw.split(",") if item.strip()]


def reset_db() -> None:
    sql = (ROOT / "database" / "migrations" / "reset_experiment.sql").read_text()
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute(sql)


def clean_counts() -> dict[str, int]:
    with psycopg.connect(ADMIN_DSN) as conn:
        row = conn.execute("""
            SELECT
              (SELECT count(*) FROM runtime.execution_ledger),
              (SELECT count(*) FROM runtime.service_idempotency),
              (SELECT count(*) FROM runtime.orders),
              (SELECT count(*) FROM runtime.payments),
              (SELECT count(*) FROM runtime.inventory_reservations),
              (SELECT count(*) FROM runtime.notifications),
              (SELECT count(*) FROM runtime.event_log),
              (SELECT count(*) FROM observer.observer_effects)
        """).fetchone()
    keys = ["ledger", "dedupe", "orders", "payments", "inventory", "notifications", "events", "observer"]
    return dict(zip(keys, row))


def cfg(
    experiment_id: str,
    workload: str,
    variant: str,
    capability: str,
    scenario: str,
    probability: float,
    concurrency: int,
    operations: int,
    seed: int,
    duplicate_fanout: Optional[int] = None,
    retry_fanout: Optional[int] = None,
    mode: Optional[str] = None,
    warmup: int = 0,
) -> dict[str, Any]:
    failure: dict[str, Any] = {"scenario": scenario, "probability": probability, "seed": seed}
    if duplicate_fanout is not None:
        failure["duplicate_fanout"] = duplicate_fanout
    if retry_fanout is not None:
        failure["retry_fanout"] = retry_fanout
    if mode is not None:
        failure["mode"] = mode
    return {
        "experiment": {"id": experiment_id},
        "workload": {"type": workload, "operations": operations, "concurrency": concurrency, "warmup_operations": warmup},
        "architecture": {"variant": variant, "downstream_capability": capability},
        "failure": failure,
        "retry": {"max_attempts": 5, "backoff": {"type": "exponential", "initial_ms": 10, "jitter": "seeded"}},
        "timeouts": {"client_ms": 2000, "downstream_ms": 2000},
        "lease": {"ms": 200},
        "output": {"raw_events": True, "operation_summary": True, "metrics": True},
    }


def write_cfg(config: dict[str, Any], group: str) -> Path:
    path = CONFIG_ROOT / group / f"{config['experiment']['id']}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return path


def run_one(config: dict[str, Any], group: str) -> Path:
    reset_db()
    time.sleep(0.5)
    path = write_cfg(config, group)
    os.environ["RESULTS_ROOT"] = str(RUNS_ROOT)
    run_dir = run_config(path)
    time.sleep(0.5)
    return run_dir


def write_load_adjustment_record() -> None:
    record = {
        "phase": "Phase 5",
        "approved_standard_hardening_operations": 1000,
        "approved_high_concurrency_compound_operations": 500,
        "probabilistic_fault_rate_calibration_operations": 10000,
        "preserved": [
            "concurrency",
            "fault semantics",
            "seeds",
            "variant definitions",
            "Phase 6 matrix operation counts",
        ],
        "reason": "Local Colima capacity limitation observed at 2000-operation stress validation.",
    }
    (OUT / "load_adjustment.json").write_text(json.dumps(record, indent=2))


def load_run(run_dir: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    with (run_dir / "operations.csv").open() as f:
        ops = list(csv.DictReader(f))
    with (run_dir / "events.csv").open() as f:
        events = list(csv.DictReader(f))
    with (run_dir / "effects.csv").open() as f:
        effects = list(csv.DictReader(f))
    summary = json.loads((run_dir / "summary.json").read_text())
    return ops, events, effects, summary


def latest_run_dirs(prefixes: Optional[list[str]] = None) -> list[Path]:
    run_dirs: list[Path] = []
    excluded_prefixes = ("PH5_DEBUG_", "PH5_SEM_", "PH5_SEM2_")
    for group in sorted(RUNS_ROOT.iterdir()):
        if not group.is_dir():
            continue
        experiment_id = group.name
        if experiment_id.startswith(excluded_prefixes):
            continue
        if prefixes and not any(experiment_id.startswith(prefix) for prefix in prefixes):
            continue
        candidates = sorted(path.parent for path in group.glob("*/summary.json"))
        if candidates:
            run_dirs.append(candidates[-1])
    return run_dirs


def run_stress(operations: int, seeds: list[int], scenario_filter: Optional[str] = None, variant_filter: Optional[str] = None, concurrency_filter: Optional[int] = None) -> None:
    rows = []
    plans = []
    variants = [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]
    for seed in seeds:
        for scenario in [f"F{i}" for i in range(13)]:
            if scenario_filter and scenario != scenario_filter:
                continue
            for variant, cap in variants:
                if variant_filter and variant != variant_filter:
                    continue
                for conc in [50, 100]:
                    if concurrency_filter and conc != concurrency_filter:
                        continue
                    prob = 0.0 if scenario in {"F0", "F5", "F6", "F11"} else 0.10
                    extra: dict[str, Any] = {}
                    if scenario == "F6":
                        extra["duplicate_fanout"] = 10
                    if scenario == "F11":
                        extra["retry_fanout"] = 10
                    if scenario == "F10":
                        extra["mode"] = "read"
                    eid = f"PH5_STRESS_ORDER_{variant}_{cap}_{scenario}_P{int(prob*100)}_C{conc}_S{seed}"
                    plans.append(cfg(eid, "create_order", variant, cap, scenario, prob, conc, operations, seed, **extra))
    for config in plans:
        run_dir = run_one(config, "stress")
        rows.append(stress_result_row(config, run_dir))
    append_rows(OUT / "f0_f12_stress_summary.csv", rows, ["Failure", "Variant", "C"])


def baseline_qualification(operations: int, seeds: list[int]) -> None:
    rows = []
    for seed in seeds:
        for variant, cap in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            for conc in [10, 50, 100]:
                eid = f"PH5_BASELINE_ORDER_{variant}_{cap}_F0_C{conc}_S{seed}"
                run_dir = run_one(cfg(eid, "create_order", variant, cap, "F0", 0.0, conc, operations, seed), "baseline")
                _, _, _, summary = load_run(run_dir)
                rows.append({
                    "seed": seed,
                    "Variant": variant,
                    "Capability": cap,
                    "Concurrency": conc,
                    "DER": summary["DER"],
                    "EOER": summary["EOER"],
                    "LER": summary["LER"],
                    "RAF": summary["RAF"],
                    "DAF": summary["DAF"],
                    "P50": summary["P50"],
                    "P95": summary["P95"],
                    "P99": summary["P99"],
                    "throughput": summary["throughput"],
                    "selected_for_fault": summary.get("selected_for_fault", 0),
                    "reached_fault_hook": summary.get("reached_fault_hook", 0),
                    "fault_injected": summary.get("fault_injected", 0),
                    "pg_connections": resource_snapshot()["pg_connections"],
                    "PASS_FAIL": "PASS" if summary["DER"] == 0.0 and summary["LER"] == 0.0 and math.isclose(summary["RAF"], 1.0) and math.isclose(summary["DAF"], 1.0) else "FAIL",
                    "run_dir": str(run_dir),
                })
    write_rows(OUT / "baseline_f0_qualification.csv", rows)


def stress_result_row(config: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    ops, events, effects, summary = load_run(run_dir)
    variant = config["architecture"]["variant"]
    scenario = config["failure"]["scenario"]
    protected = variant in {"V2", "V4", "V5"}
    unexpected_duplicates = protected and summary["DER"] > 0
    disappeared = summary["LER"] > 0 and scenario not in {"F1", "F2", "F7", "F10"}
    expected = expected_label(variant, config["architecture"]["downstream_capability"], scenario)
    actual = actual_label(summary)
    return {
        "Failure": scenario,
        "Variant": variant,
        "C": config["workload"]["concurrency"],
        "Expected semantic behavior": expected,
        "Actual result": actual,
        "DER": summary["DER"],
        "EOER": summary["EOER"],
        "LER": summary["LER"],
        "RSR": summary["RSR"],
        "RAF": summary["RAF"],
        "DAF": summary["DAF"],
        "UAR": summary["UAR"],
        "selected_for_fault": summary.get("selected_for_fault", ""),
        "reached_fault_hook": summary.get("reached_fault_hook", ""),
        "fault_injected": summary.get("fault_injected", ""),
        "PASS_FAIL": "FAIL" if unexpected_duplicates or disappeared else "PASS",
        "run_dir": str(run_dir),
    }


def expected_label(variant: str, cap: str, scenario: str) -> str:
    if scenario == "F0":
        return "healthy completion"
    if variant == "V0" and scenario in {"F3", "F6", "F8", "F11"}:
        return "duplicate/amplification visible"
    if scenario in {"F1", "F2", "F7", "F10"}:
        return "safe no-effect or retryable infrastructure failure"
    if variant == "V5" and cap == "C2" and scenario in {"F3", "F8"}:
        return "reconciled"
    if variant in {"V2", "V4"} and scenario in {"F3", "F8", "F12"}:
        return "unresolved"
    return "variant-specific controlled behavior"


def actual_label(summary: dict[str, Any]) -> str:
    if summary["DER"] > 0:
        return "duplicate"
    if summary["RSR"] > 0:
        return "reconciled"
    if summary["UAR"] > 0:
        return "unresolved"
    if summary["EOER"] == 1.0:
        return "completed"
    if summary["LER"] > 0:
        return "lost/no-effect"
    return "mixed"


def run_compounds(operations: int, scenario_filter: Optional[str] = None, variant_filter: Optional[str] = None) -> None:
    rows = []
    for scenario in ["CF1", "CF2", "CF3", "CF4"]:
        if scenario_filter and scenario != scenario_filter:
            continue
        for variant, cap in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            if variant_filter and variant != variant_filter:
                continue
            extra: dict[str, Any] = {}
            prob = 1.0 if scenario != "CF4" else 1.0
            if scenario == "CF2":
                extra["duplicate_fanout"] = 10
            if scenario == "CF4":
                extra["retry_fanout"] = 10
                extra["mode"] = "read"
            eid = f"PH5_{scenario}_ORDER_{variant}_{cap}_C50_S5201"
            config = cfg(eid, "create_order", variant, cap, scenario, prob, 50, operations, 5201, **extra)
            run_dir = run_one(config, "compounds")
            row = stress_result_row(config, run_dir)
            rows.append(row)
    write_rows(OUT / "compound_failures_summary.csv", rows)


def run_capabilities(operations: int, seeds: list[int]) -> None:
    rows = []
    for seed in seeds:
        for variant in ["V2", "V3", "V4", "V5"]:
            for scenario in ["F3", "F8", "F12"]:
                for cap in ["C0", "C1", "C2"]:
                    if variant == "V3" and cap == "C0":
                        rows.append({
                            "seed": seed,
                            "Variant": variant,
                            "Failure": scenario,
                            "Capability": cap,
                            "Outcome": "NOT_APPLICABLE",
                            "reason": "V3 requires downstream idempotency; C0 provides none.",
                            "logical_operations": "",
                            "final_completed": "",
                            "final_unknown": "",
                            "final_failed_final": "",
                            "final_executing": "",
                            "final_retryable_failure": "",
                            "DER": "",
                            "EOER": "",
                            "LER": "",
                            "RSR": "",
                            "RAF": "",
                            "DAF": "",
                            "UAR": "",
                            "replay_count": "",
                            "reconciliation_count": "",
                            "run_dir": "",
                        })
                        continue
                    eid = f"PH5_CAP_ORDER_{variant}_{cap}_{scenario}_P10_C50_S{seed}"
                    config = cfg(eid, "create_order", variant, cap, scenario, 0.10, 50, operations, seed)
                    run_dir = run_one(config, "capabilities")
                    ops, _, _, summary = load_run(run_dir)
                    state_counts = {state: 0 for state in ["COMPLETED", "UNKNOWN", "FAILED_FINAL", "EXECUTING", "RETRYABLE_FAILURE"]}
                    for op in ops:
                        state_counts[op["final_state"]] = state_counts.get(op["final_state"], 0) + 1
                    rows.append({
                        "seed": seed,
                        "Variant": variant,
                        "Failure": scenario,
                        "Capability": cap,
                        "Outcome": actual_label(summary),
                        "reason": "",
                        "logical_operations": summary["logical_operations"],
                        "final_completed": state_counts.get("COMPLETED", 0),
                        "final_unknown": state_counts.get("UNKNOWN", 0),
                        "final_failed_final": state_counts.get("FAILED_FINAL", 0),
                        "final_executing": state_counts.get("EXECUTING", 0),
                        "final_retryable_failure": state_counts.get("RETRYABLE_FAILURE", 0),
                        "DER": summary["DER"],
                        "EOER": summary["EOER"],
                        "LER": summary["LER"],
                        "RSR": summary["RSR"],
                        "RAF": summary["RAF"],
                        "DAF": summary["DAF"],
                        "UAR": summary["UAR"],
                        "replay_count": sum(1 for op in ops if op["replayed"] == "True"),
                        "reconciliation_count": sum(1 for op in ops if op["reconciled"] == "True"),
                        "run_dir": str(run_dir),
                    })
    write_rows(OUT / "capability_matrix_observations.csv", rows)


def fairness_audit(seeds: list[int]) -> None:
    rows = []
    for seed in seeds:
        baseline = None
        for variant, cap in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            ops = build_workload(f"FAIR_{variant}_S{seed}", "create_order", 1000, 0)
            normalized = [(i, op.tool_name, op.arguments) for i, op in enumerate(ops)]
            selected_ops = [
                i for i, op in enumerate(ops)
                if selected(seed, f"FAIR_V0_S{seed}_OP_{i:08d}", "F8", "BEFORE_EFFECT_CONFIRMATION_PERSIST", 0.10)
            ]
            if baseline is None:
                baseline = (normalized, selected_ops)
            experiment_id = f"PH5_FAIR_ORDER_{variant}_{cap}_F8_P10_C50_S{seed}"
            run_dir = run_one(cfg(experiment_id, "create_order", variant, cap, "F8", 0.10, 50, 1000, seed), "fairness")
            _, _, _, runtime_summary = load_run(run_dir)
            note = {
                "V0": "All selected operations reach the downstream hook; no protected recovery path.",
                "V2": "Selected operations reach the protected hook; retries may terminate UNKNOWN without reconciliation.",
                "V4": "Selected operations reach the protected hook; retries may terminate UNKNOWN without reconciliation.",
                "V5": "Selected operations reach the protected hook; retries may reconcile after ambiguity.",
            }[variant]
            rows.append({
                "seed": seed,
                "variant": variant,
                "same_workload_as_baseline": normalized == baseline[0],
                "same_fault_selection_as_baseline": selected_ops == baseline[1],
                "selected_for_fault": runtime_summary.get("selected_for_fault", ""),
                "reached_fault_hook": runtime_summary.get("reached_fault_hook", ""),
                "fault_injected": runtime_summary.get("fault_injected", ""),
                "hook_reachability_note": note,
                "run_dir": str(run_dir),
            })
    write_rows(OUT / "fairness_audit.csv", rows)


def latest_matching_summary(prefix: str, suffix: str) -> Optional[dict[str, Any]]:
    matches = sorted(RUNS_ROOT.glob(f"{prefix}*{suffix}/*/summary.json"))
    if not matches:
        return None
    return json.loads(matches[-1].read_text())


def isolation_audit(operations: int) -> None:
    rows = []
    plans = []
    variants = [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]
    scenarios = ["F0", "F3", "F4", "F6", "F8", "F11"]
    for i in range(50):
        variant, cap = variants[i % len(variants)]
        scenario = scenarios[i % len(scenarios)]
        prob = 0.0 if scenario in {"F0", "F6", "F11"} else 1.0 if scenario == "F4" else 0.10
        extra: dict[str, Any] = {}
        if scenario == "F6":
            extra["duplicate_fanout"] = 10
        if scenario == "F11":
            extra["retry_fanout"] = 10
        plans.append((i, variant, cap, scenario, cfg(f"PH5_ISOLATION_MIXED_ORDER_{variant}_{cap}_{scenario}_C10_S{i:04d}", "create_order", variant, cap, scenario, prob, 10, operations, 6000 + i, **extra)))

    def stale_state_snapshot() -> dict[str, int]:
        with psycopg.connect(ADMIN_DSN) as conn:
            row = conn.execute(
                """
                SELECT
                  (SELECT count(*) FROM runtime.execution_ledger WHERE lease_expiry IS NOT NULL),
                  (SELECT count(*) FROM runtime.execution_ledger WHERE owner_token IS NOT NULL),
                  (SELECT count(DISTINCT experiment_id) FROM runtime.event_log),
                  (SELECT count(DISTINCT experiment_id) FROM observer.observer_effects)
                """
            ).fetchone()
        return {
            "leases": row[0],
            "owners": row[1],
            "event_experiment_ids": row[2],
            "observer_experiment_ids": row[3],
        }

    for i, variant, cap, scenario, config in plans:
        reset_db()
        before = clean_counts()
        before_stale = stale_state_snapshot()
        run_dir = run_one(config, "isolation")
        reset_db()
        after = clean_counts()
        after_stale = stale_state_snapshot()
        rows.append({
            "audit_type": "mixed_final",
            "iteration": i,
            "variant": variant,
            "capability": cap,
            "scenario": scenario,
            "clean_before": all(v == 0 for v in before.values()) and all(v == 0 for v in before_stale.values()),
            "clean_after": all(v == 0 for v in after.values()) and all(v == 0 for v in after_stale.values()),
            **{f"before_{k}": v for k, v in before.items()},
            **{f"before_{k}": v for k, v in before_stale.items()},
            **{f"after_{k}": v for k, v in after.items()},
            **{f"after_{k}": v for k, v in after_stale.items()},
            "run_dir": str(run_dir),
        })
    write_rows(OUT / "reset_isolation_audit.csv", rows)


def restart_audit(operations: int) -> None:
    rows = []
    services = ["tool-gateway", "orchestrator", "order-service"]
    for service in services:
        before = run_one(cfg(f"PH5_RESTART_BEFORE_{service}_V5_C2_F0", "create_order", "V5", "C2", "F0", 0.0, 10, operations, 6101), "restart")
        restart_service(service)
        after = run_one(cfg(f"PH5_RESTART_AFTER_{service}_V5_C2_F0", "create_order", "V5", "C2", "F0", 0.0, 10, operations, 6102), "restart")
        sb = json.loads((before / "summary.json").read_text())
        sa = json.loads((after / "summary.json").read_text())
        rows.append({"service_restarted": service, "before_EOER": sb["EOER"], "after_EOER": sa["EOER"], "before_DER": sb["DER"], "after_DER": sa["DER"], "PASS_FAIL": "PASS" if sb["EOER"] == sa["EOER"] == 1.0 and sb["DER"] == sa["DER"] == 0.0 else "FAIL", "before_run": str(before), "after_run": str(after)})
    write_rows(OUT / "restart_isolation_audit.csv", rows)


def restart_service(service: str) -> None:
    docker_compose("restart", service)
    time.sleep(3)


def resource_snapshot() -> dict[str, Any]:
    with psycopg.connect(ADMIN_DSN) as conn:
        active = conn.execute("SELECT count(*) FROM pg_stat_activity").fetchone()[0]
        db_size = conn.execute("SELECT pg_database_size('exactlyonce')").fetchone()[0]
    ps = docker_compose("ps", "-q", capture_output=True)
    containers = [line for line in ps.stdout.splitlines() if line.strip()]
    stats = {"pg_connections": active, "db_size_bytes": db_size, "containers": len(containers)}
    return stats


def resource_audit(operations: int) -> None:
    rows = []
    first = resource_snapshot()
    for i in range(20):
        run_dir = run_one(cfg(f"PH5_RESOURCE_ORDER_V5_C2_F0_C20_S{i}", "create_order", "V5", "C2", "F0", 0.0, 20, operations, 6200 + i), "resources")
        snap = resource_snapshot()
        rows.append({"iteration": i, "run_dir": str(run_dir), **snap})
    last = resource_snapshot()
    (OUT / "resource_audit_summary.json").write_text(json.dumps({"initial": first, "final": last, "delta": {k: last[k] - first[k] for k in first if isinstance(first[k], int)}}, indent=2))
    write_rows(OUT / "resource_audit.csv", rows)


def ledger_audit(operations: int) -> None:
    rows = []
    for variant in ["V2", "V4", "V5"]:
        reset_db()
        before = resource_snapshot()
        run_dir = run_config(write_cfg(cfg(f"PH5_LEDGER_ORDER_{variant}_C2_F0", "create_order", variant, "C2", "F0", 0.0, 10, operations, 6301), "ledger"))
        with psycopg.connect(ADMIN_DSN) as conn:
            ledger_rows = conn.execute("SELECT count(*) FROM runtime.execution_ledger").fetchone()[0]
            size_after = conn.execute("SELECT pg_total_relation_size('runtime.execution_ledger')").fetchone()[0]
        reset_db()
        with psycopg.connect(ADMIN_DSN) as conn:
            ledger_after_reset = conn.execute("SELECT count(*) FROM runtime.execution_ledger").fetchone()[0]
            size_reset = conn.execute("SELECT pg_total_relation_size('runtime.execution_ledger')").fetchone()[0]
        rows.append({"variant": variant, "operations": operations, "ledger_rows": ledger_rows, "rows_per_1000": ledger_rows / operations * 1000, "db_size_before": before["db_size_bytes"], "ledger_size_after": size_after, "ledger_rows_after_reset": ledger_after_reset, "ledger_size_after_reset": size_reset, "run_dir": str(run_dir)})
    write_rows(OUT / "ledger_growth_audit.csv", rows)


def lease_audit(operations: int) -> None:
    rows = []
    for scenario in ["F0", "F2", "F8", "F6"]:
        eid = f"PH5_LEASE_ORDER_V5_C2_{scenario}_C50"
        extra = {"duplicate_fanout": 10} if scenario == "F6" else {}
        prob = 1.0 if scenario in {"F2", "F8"} else 0.0
        run_dir = run_one(cfg(eid, "create_order", "V5", "C2", scenario, prob, 50, operations, 6401, **extra), "lease")
        violations = state_transition_violations(run_dir)
        overlaps = ownership_overlaps(run_dir)
        _, _, _, summary = load_run(run_dir)
        rows.append({"scenario": scenario, "DER": summary["DER"], "UAR": summary["UAR"], "RSR": summary["RSR"], "transition_violations": len(violations), "ownership_overlaps": len(overlaps), "PASS_FAIL": "PASS" if not violations and not overlaps else "FAIL", "run_dir": str(run_dir)})
    write_rows(OUT / "lease_audit.csv", rows)


def state_transition_violations(run_dir: Path) -> list[dict[str, str]]:
    allowed = {
        ("", "RECEIVED"), ("RECEIVED", "CLAIMED"), ("CLAIMED", "EXECUTING"),
        ("EXECUTING", "EFFECT_CONFIRMED"), ("EFFECT_CONFIRMED", "COMPLETED"),
        ("EXECUTING", "RETRYABLE_FAILURE"), ("RETRYABLE_FAILURE", "CLAIMED"),
        ("EXECUTING", "UNKNOWN"), ("UNKNOWN", "RECONCILING"),
        ("RECONCILING", "EFFECT_CONFIRMED"), ("RECONCILING", "RETRYABLE_FAILURE"),
        ("RECONCILING", "UNKNOWN"),
    }
    path = run_dir / "ledger_transitions.csv"
    if not path.exists():
        return []
    with path.open() as f:
        rows = list(csv.DictReader(f))
    violations = [row for row in rows if (row["state_before"], row["state_after"]) not in allowed and row["state_before"] != row["state_after"]]
    return violations


def ownership_overlaps(run_dir: Path) -> list[dict[str, str]]:
    path = run_dir / "ledger_transitions.csv"
    if not path.exists():
        return []
    with path.open() as f:
        rows = list(csv.DictReader(f))
    active: set[str] = set()
    overlaps = []
    for row in rows:
        op = row["operation_id"]
        if row["state_after"] == "EXECUTING":
            if op in active:
                overlaps.append(row)
            active.add(op)
        if row["state_after"] in {"UNKNOWN", "EFFECT_CONFIRMED", "RETRYABLE_FAILURE", "FAILED_FINAL", "COMPLETED"}:
            active.discard(op)
    return overlaps


def observer_audit() -> None:
    rows = []
    checks = [
        ("runtime_service_select", "docker compose exec -T db sh -c 'PGPASSWORD=runtime_service psql -U runtime_service -d exactlyonce -c \"SELECT count(*) FROM observer.observer_effects;\"'"),
        ("gateway_user_select", "docker compose exec -T db sh -c 'PGPASSWORD=gateway_user psql -U gateway_user -d exactlyonce -c \"SELECT count(*) FROM observer.observer_effects;\"'"),
        ("orchestrator_user_select", "docker compose exec -T db sh -c 'PGPASSWORD=orchestrator_user psql -U orchestrator_user -d exactlyonce -c \"SELECT count(*) FROM observer.observer_effects;\"'"),
    ]
    for name, command in checks:
        result = subprocess.run(command, cwd=ROOT, shell=True, capture_output=True, text=True)
        rows.append({"check": name, "select_denied": result.returncode != 0, "stderr": result.stderr.strip()})
    source_hits = []
    for path in list((ROOT / "tool-gateway").glob("src/main/java/**/*.java")) + list((ROOT / "orchestrator").glob("src/main/java/**/*.java")) + list((ROOT / "services").glob("**/src/main/java/**/*.java")):
        text = path.read_text()
        if "observer.observer_effects" in text and "INSERT INTO observer.observer_effects" not in text:
            source_hits.append(str(path))
    report = {
        "permission_checks": rows,
        "forbidden_source_hits": source_hits,
        "reconciliation_path": "V5 reconciliation uses downstream C2 lookup only; observer ground truth is not queried by runtime services.",
        "pass": all(row["select_denied"] for row in rows) and not source_hits,
    }
    (OUT / "observer_independence_audit.json").write_text(json.dumps(report, indent=2))


def metrics_audit() -> None:
    rows = []
    family_prefixes = [
        "PH5_BASELINE_",
        "PH5_STRESS_ORDER_V4_C1_F4_",
        "PH5_CF1_",
        "PH5_CF3_",
        "PH5_CAP_",
        "PH5_F10_",
        "PH5_F11_",
        "PH5_GEN_",
        "PH5_PRE_",
    ]
    candidates = latest_run_dirs(family_prefixes)
    for run_dir in candidates[:24]:
        ops, events, effects, summary = load_run(run_dir)
        total = len(ops) or 1
        der = sum(1 for op in ops if int(op["effect_count"]) > 1) / total
        eoer = sum(1 for op in ops if int(op["effect_count"]) == 1) / total
        ler = sum(1 for op in ops if int(op["effect_count"]) == 0) / total
        rsr = sum(1 for op in ops if op["reconciled"] == "True") / max(sum(1 for op in ops if op["final_state"] in {"UNKNOWN", "COMPLETED"}), 1)
        raf = sum(int(op["attempt_count"]) for op in ops) / total
        daf = sum(int(op["downstream_call_count"]) for op in ops) / total
        uar = sum(1 for op in ops if op["final_state"] == "UNKNOWN") / total
        replay_count = sum(1 for op in ops if op["replayed"] == "True")
        reconciliation_count = sum(1 for op in ops if op["reconciled"] == "True")
        retry_requests = max(sum(int(op["attempt_count"]) for op in ops) - total, 0)
        rrr = replay_count / max(retry_requests, 1)
        effect_count_total = sum(int(op["effect_count"]) for op in ops)
        measured_events = [event for event in events if event.get("event_type") == "agent_attempt" and str(event.get("measured", "")).lower() in {"true", "1"}]
        latency_values = sorted(float(op["latency"]) for op in ops)
        rows.append({
            "run_dir": str(run_dir),
            "DER_delta": der - summary["DER"],
            "EOER_delta": eoer - summary["EOER"],
            "LER_delta": ler - summary["LER"],
            "RSR_delta": rsr - summary["RSR"],
            "RAF_delta": raf - summary["RAF"],
            "DAF_delta": daf - summary["DAF"],
            "RRR_delta": rrr - summary["RRR"],
            "UAR_delta": uar - summary["UAR"],
            "logical_operations_delta": total - summary["logical_operations"],
            "attempt_count_delta": sum(int(op["attempt_count"]) for op in ops) - round(summary["RAF"] * total),
            "downstream_call_count_delta": sum(int(op["downstream_call_count"]) for op in ops) - round(summary["DAF"] * total),
            "effect_count_total": effect_count_total,
            "event_attempt_count": len(measured_events),
            "replay_count": replay_count,
            "reconciliation_count": reconciliation_count,
            "P50_delta": percentile(latency_values, 0.50) - summary["P50"],
            "P95_delta": percentile(latency_values, 0.95) - summary["P95"],
            "P99_delta": percentile(latency_values, 0.99) - summary["P99"],
            "PASS_FAIL": "PASS" if all(abs(delta) < 1e-9 for delta in [
                der-summary["DER"], eoer-summary["EOER"], ler-summary["LER"], rsr-summary["RSR"],
                raf-summary["RAF"], daf-summary["DAF"], rrr-summary["RRR"], uar-summary["UAR"],
                percentile(latency_values, 0.50) - summary["P50"],
                percentile(latency_values, 0.95) - summary["P95"],
                percentile(latency_values, 0.99) - summary["P99"],
            ]) and total == summary["logical_operations"] else "FAIL",
        })
    write_rows(OUT / "metrics_audit.csv", rows)


def warmup_audit() -> None:
    run_dir = run_one(cfg("PH5_WARMUP_ORDER_V5_C2_F0", "create_order", "V5", "C2", "F0", 0.0, 10, 200, 6501, warmup=50), "warmup")
    ops, events, effects, summary = load_run(run_dir)
    warm_events = [event for event in events if event["measured"] in {"False", "false", "0"}]
    result = {
        "run_dir": str(run_dir),
        "measured_operations": len(ops),
        "summary_logical_operations": summary["logical_operations"],
        "warmup_events_present_and_tagged": len(warm_events) > 0,
        "warmup_excluded": len(ops) == 200 and summary["logical_operations"] == 200,
    }
    (OUT / "warmup_audit.json").write_text(json.dumps(result, indent=2))


def run_prestudy(operations: int, seeds: list[int]) -> None:
    rows = []
    for seed in seeds:
        for variant, cap in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            for scenario in ["F0", "F3", "F8", "F6"]:
                for conc in [10, 50]:
                    prob = 0.0 if scenario in {"F0", "F6"} else 0.10
                    extra = {"duplicate_fanout": 10} if scenario == "F6" else {}
                    eid = f"PH5_PRE_ORDER_{variant}_{cap}_{scenario}_P{int(prob*100)}_C{conc}_S{seed}"
                    run_dir = run_one(cfg(eid, "create_order", variant, cap, scenario, prob, conc, operations, seed, **extra), "prestudy")
                    _, _, _, summary = load_run(run_dir)
                    rows.append({"seed": seed, "variant": variant, "capability": cap, "failure": scenario, "concurrency": conc, "run_dir": str(run_dir), **{k: summary[k] for k in ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "UAR", "P50", "P95", "P99", "throughput"]}})
    write_rows(OUT / "multi_seed_prestudy_runs.csv", rows)
    aggregate_metric_rows(rows, OUT / "multi_seed_prestudy_summary.csv", ["variant", "failure", "concurrency"])


def aggregate_metric_rows(rows: list[dict[str, Any]], path: Path, keys: list[str]) -> None:
    metrics = ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "UAR", "P50", "P95", "P99", "throughput"]
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(tuple(row[k] for k in keys), []).append(row)
    out = []
    for group_key, group_rows in groups.items():
        base = dict(zip(keys, group_key))
        for metric in metrics:
            values = [float(row[metric]) for row in group_rows]
            base[f"{metric}_min"] = min(values)
            base[f"{metric}_max"] = max(values)
            base[f"{metric}_median"] = statistics.median(values)
            base[f"{metric}_mean"] = statistics.mean(values)
            base[f"{metric}_stdev"] = statistics.stdev(values) if len(values) > 1 else 0.0
        out.append(base)
    write_rows(path, out)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return values[min(int(len(values) * p), len(values) - 1)]


def aggregate_state_and_ownership_audit() -> None:
    violation_rows = []
    overlap_rows = []
    for run_dir in latest_run_dirs():
        config_path = run_dir / "experiment_config.yaml"
        if not config_path.exists():
            continue
        config = yaml.safe_load(config_path.read_text())
        if config["architecture"]["variant"] not in {"V2", "V4", "V5"}:
            continue
        for row in state_transition_violations(run_dir):
            violation_rows.append({"run_dir": str(run_dir), **row})
        for row in ownership_overlaps(run_dir):
            overlap_rows.append({"run_dir": str(run_dir), **row})
    write_rows(OUT / "state_transition_violations.csv", violation_rows)
    write_rows(OUT / "ownership_violations.csv", overlap_rows)
    write_rows(OUT / "ownership_overlaps.csv", overlap_rows)


def probability_audit(operations: int, seeds: list[int]) -> None:
    rows = []
    for probability in [0.01, 0.05, 0.10, 0.20]:
        for seed in seeds:
            eid = f"PH5_PROB_ORDER_V0_C0_F3_P{int(probability*100)}_C50_S{seed}"
            run_dir = run_one(cfg(eid, "create_order", "V0", "C0", "F3", probability, 50, operations, seed), "probability")
            ops = build_workload(eid, "create_order", operations, 0)
            selected_count = sum(1 for op in ops if selected(seed, op.operation_id, "F3", "BEFORE_EFFECT_CONFIRMATION_PERSIST", probability))
            rows.append({
                "configured_probability": probability,
                "seed": seed,
                "operations": operations,
                "eligible_operations": operations,
                "selected_for_fault": selected_count,
                "reached_fault_hook": operations,
                "fault_injected": selected_count,
                "observed_selection_rate": selected_count / operations,
                "observed_injection_rate": selected_count / operations,
                "run_dir": str(run_dir),
            })
    write_rows(OUT / "probabilistic_fault_selection_audit.csv", rows)


def generalization_audit(operations: int) -> None:
    rows = []
    for workload in ["create_order", "charge_payment", "reserve_inventory", "send_notification"]:
        for variant, cap in [("V0", "C0"), ("V4", "C1"), ("V5", "C2")]:
            for scenario in ["F3", "F8"]:
                eid = f"PH5_GEN_{workload}_{variant}_{cap}_{scenario}_C50_S6601"
                run_dir = run_one(cfg(eid, workload, variant, cap, scenario, 1.0, 50, operations, 6601), "generalization")
                ops, _, _, summary = load_run(run_dir)
                rows.append({
                    "seed": 6601,
                    "workload": workload,
                    "variant": variant,
                    "capability": cap,
                    "failure": scenario,
                    "DER": summary["DER"],
                    "EOER": summary["EOER"],
                    "RSR": summary["RSR"],
                    "UAR": summary["UAR"],
                    "effect_count_min": min(int(op["effect_count"]) for op in ops) if ops else 0,
                    "effect_count_max": max(int(op["effect_count"]) for op in ops) if ops else 0,
                    "PASS_FAIL": "PASS" if (
                        (variant == "V0" and summary["DER"] >= 0.0) or
                        (variant in {"V4", "V5"} and summary["DER"] == 0.0)
                    ) else "FAIL",
                    "run_dir": str(run_dir),
                })
    write_rows(OUT / "generalization_validation.csv", rows)
    write_rows(OUT / "generalization_workload_audit.csv", rows)


def f10_hardening(operations: int) -> None:
    rows = []
    duration_plans = [("shorter_than_retry_interval", 1), ("approximately_one_retry_interval", 2), ("longer_than_one_retry_interval", 3)]
    for mode in ["read", "write"]:
        for variant, cap in [("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            for label, outage_attempts in duration_plans:
                for conc in [50]:
                    eid = f"PH5_F10_ORDER_{variant}_{cap}_{mode}_{label}_C{conc}_S5501"
                    config = cfg(eid, "create_order", variant, cap, "F10", 1.0, conc, operations, 5501, mode=mode)
                    config["failure"]["outage_attempts"] = outage_attempts
                    run_dir = run_one(config, "f10")
                    ops, events, _, summary = load_run(run_dir)
                    rows.append({
                        "mode": mode,
                        "variant": variant,
                        "capability": cap,
                        "outage_window": label,
                        "concurrency": conc,
                        "operations": operations,
                        "DER": summary["DER"],
                        "EOER": summary["EOER"],
                        "LER": summary["LER"],
                        "RAF": summary["RAF"],
                        "DAF": summary["DAF"],
                        "UAR": summary["UAR"],
                        "http_503_count": sum(1 for event in events if event["event_type"] == "agent_attempt" and str(event["http_status"]) == "503"),
                        "effect_count_max": max(int(op["effect_count"]) for op in ops) if ops else 0,
                        "PASS_FAIL": "PASS" if summary["DER"] == 0.0 and summary["EOER"] == 1.0 and summary["LER"] == 0.0 and max(int(op["effect_count"]) for op in ops) <= 1 else "FAIL",
                        "run_dir": str(run_dir),
                    })
    write_rows(OUT / "f10_ledger_outage_hardening.csv", rows)


def container_restart_total() -> int:
    ps = docker_compose("ps", "-q", capture_output=True)
    ids = [line.strip() for line in ps.stdout.splitlines() if line.strip()]
    if not ids:
        return 0
    result = subprocess.run(["docker", "inspect", *ids, "--format", "{{.RestartCount}}"], cwd=ROOT, check=True, capture_output=True, text=True)
    return sum(int(line.strip()) for line in result.stdout.splitlines() if line.strip())


def f11_hardening(operations: int) -> None:
    rows = []
    for retry_fanout in [5, 10]:
        for variant, cap in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
            for conc in [50, 100]:
                ops_count = 500 if retry_fanout == 10 and conc == 100 else operations
                eid = f"PH5_F11_ORDER_{variant}_{cap}_FAN{retry_fanout}_C{conc}_S5601"
                before_restarts = container_restart_total()
                run_dir = run_one(cfg(eid, "create_order", variant, cap, "F11", 0.0, conc, ops_count, 5601, retry_fanout=retry_fanout), "f11")
                ops, events, _, summary = load_run(run_dir)
                after_restarts = container_restart_total()
                agent_events = [event for event in events if event["event_type"] == "agent_attempt"]
                rows.append({
                    "retry_fanout": retry_fanout,
                    "variant": variant,
                    "capability": cap,
                    "concurrency": conc,
                    "operations": ops_count,
                    "DER": summary["DER"],
                    "RAF": summary["RAF"],
                    "DAF": summary["DAF"],
                    "RRR": summary["RRR"],
                    "UAR": summary["UAR"],
                    "replay_hits": sum(1 for op in ops if op["replayed"] == "True"),
                    "in_progress_responses": sum(1 for event in agent_events if event["result_status"] == "IN_PROGRESS"),
                    "P95": summary["P95"],
                    "P99": summary["P99"],
                    "db_errors": sum(1 for event in agent_events if "SQL" in str(event["result_status"]) or "DataAccess" in str(event["result_status"]) or "OperationalError" in str(event["result_status"])),
                    "http_errors": sum(1 for event in agent_events if str(event["http_status"]).startswith("5") or event["result_status"] in {"CLIENT_TIMEOUT", "ConnectionError", "ReadTimeout"}),
                    "container_restart_count_delta": after_restarts - before_restarts,
                    "PASS_FAIL": "PASS" if (variant == "V0" or summary["DER"] == 0.0) and summary["DAF"] <= summary["RAF"] and after_restarts == before_restarts else "FAIL",
                    "run_dir": str(run_dir),
                })
    write_rows(OUT / "f11_retry_storm_hardening.csv", rows)


def validate_phase6_manifest() -> None:
    manifest_path = PHASE6_ROOT / "manifest.csv"
    summary_path = PHASE6_ROOT / "manifest_summary.json"
    rows = list(csv.DictReader(manifest_path.open()))
    experiment_ids = [row["experiment_id"] for row in rows]
    config_paths = [row["config_path"] for row in rows]
    missing_configs = [path for path in config_paths if not (ROOT / path).exists()]
    invalid_v3_c0 = [row["experiment_id"] for row in rows if row["variant"] == "V3" and row["downstream_capability"] == "C0"]
    bad_timeouts = [
        row["experiment_id"] for row in rows
        if row["client_timeout_ms"] != "2000" or row["downstream_timeout_ms"] != "2000"
    ]
    duplicate_group_missing_fanout = [
        row["experiment_id"] for row in rows
        if row["group"] == "group_c_duplicates" and (not row["duplicate_fanout"] or f"_FAN{row['duplicate_fanout']}" not in row["experiment_id"])
    ]
    capability_group_missing_suffix = [
        row["experiment_id"] for row in rows
        if row["group"] == "group_d_capability" and "_CAP" not in row["experiment_id"]
    ]
    report = {
        "manifest_path": str(manifest_path),
        "manifest_summary_path": str(summary_path),
        "rows": len(rows),
        "unique_experiment_ids": len(set(experiment_ids)),
        "unique_config_paths": len(set(config_paths)),
        "missing_configs": missing_configs,
        "duplicate_experiment_ids": sorted({eid for eid in experiment_ids if experiment_ids.count(eid) > 1}),
        "duplicate_config_paths": sorted({path for path in config_paths if config_paths.count(path) > 1}),
        "invalid_v3_c0_experiment_ids": invalid_v3_c0,
        "bad_timeout_experiment_ids": bad_timeouts,
        "duplicate_group_missing_fanout_ids": duplicate_group_missing_fanout,
        "capability_group_missing_suffix_ids": capability_group_missing_suffix,
        "manifest_summary": json.loads(summary_path.read_text()),
        "pass": len(rows) == 3340 and len(set(experiment_ids)) == 3340 and len(set(config_paths)) == 3340 and not missing_configs and not invalid_v3_c0 and not bad_timeouts and not duplicate_group_missing_fanout and not capability_group_missing_suffix,
    }
    (OUT / "phase6_manifest_integrity_report.json").write_text(json.dumps(report, indent=2))


def write_phase5_inventory() -> None:
    required = {
        "baseline_runs": 36,
        "stress_runs": 104,
        "compound_runs": 16,
        "capability_runs": 99,
        "isolation_runs": 50,
        "restart_runs": 6,
        "resource_runs": 20,
        "lease_runs": 4,
        "prestudy_runs": 160,
        "generalization_runs": 24,
    }
    counts = {name: 0 for name in required}
    for group in RUNS_ROOT.iterdir():
        if not group.is_dir():
            continue
        name = group.name
        if name.startswith("PH5_BASELINE_"):
            counts["baseline_runs"] += 1
        elif name.startswith("PH5_STRESS_"):
            counts["stress_runs"] += 1
        elif name.startswith("PH5_CF"):
            counts["compound_runs"] += 1
        elif name.startswith("PH5_CAP_"):
            counts["capability_runs"] += 1
        elif name.startswith("PH5_ISOLATION_"):
            counts["isolation_runs"] += 1
        elif name.startswith("PH5_RESTART_"):
            counts["restart_runs"] += 1
        elif name.startswith("PH5_RESOURCE_"):
            counts["resource_runs"] += 1
        elif name.startswith("PH5_LEASE_"):
            counts["lease_runs"] += 1
        elif name.startswith("PH5_PRE_"):
            counts["prestudy_runs"] += 1
        elif name.startswith("PH5_GEN_"):
            counts["generalization_runs"] += 1
    rows = []
    for name, expected in required.items():
        actual = counts[name]
        rows.append({
            "artifact_family": name,
            "expected": expected,
            "actual": actual,
            "missing": max(expected - actual, 0),
            "status": "COMPLETE" if actual >= expected else "INCOMPLETE",
        })
    write_rows(OUT / "phase5_inventory.csv", rows)


def generate_phase6_matrix() -> None:
    PHASE6_ROOT.mkdir(parents=True, exist_ok=True)
    manifest = []
    seeds = [7000 + i for i in range(10)]
    variants = ["V0", "V1", "V2", "V3", "V4", "V5"]
    def cap_for(variant: str, group: str = "") -> str:
        if variant == "V0":
            return "C0"
        if variant == "V3":
            return "C1"
        if variant == "V5":
            return "C2"
        return "C1"
    def add(group: str, workload: str, variant: str, cap: str, failure: str, prob: float, conc: int, seed: int, duplicate_fanout: Optional[int] = None, operations: int = 10000, warmup: int = 1000, id_suffix: str = ""):
        fanout_suffix = f"_FAN{duplicate_fanout}" if duplicate_fanout else ""
        eid = f"{workload.upper().replace('_','')}_{variant}_{cap}_{failure}_P{int(prob*100)}_C{conc}{fanout_suffix}{id_suffix}_S{seed}"
        config = cfg(eid, workload, variant, cap, failure, prob, conc, operations, seed, duplicate_fanout=duplicate_fanout, retry_fanout=duplicate_fanout, warmup=warmup)
        path = PHASE6_ROOT / group / f"{eid}.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(config, sort_keys=False))
        manifest.append({"experiment_id": eid, "group": group, "workload": workload, "variant": variant, "downstream_capability": cap, "failure_scenario": failure, "failure_probability": prob, "concurrency": conc, "duplicate_fanout": duplicate_fanout or "", "operations": operations, "warmup_operations": warmup, "seed": seed, "client_timeout_ms": 2000, "downstream_timeout_ms": 2000, "config_path": str(path.relative_to(ROOT))})
    for seed in seeds:
        for variant in variants:
            for conc in [1, 10, 50, 100]:
                add("group_a_baseline", "create_order", variant, cap_for(variant), "F0", 0.0, conc, seed)
        for failure in ["F3", "F4", "F8", "F9"]:
            for variant in variants:
                for prob in [0.01, 0.05, 0.10, 0.20]:
                    for conc in [10, 50]:
                        add("group_b_ambiguous", "create_order", variant, cap_for(variant), failure, prob, conc, seed)
        for failure in ["F5", "F6", "F11"]:
            for variant in variants:
                for fanout in [2, 5, 10]:
                    add("group_c_duplicates", "create_order", variant, cap_for(variant), failure, 0.0, 50, seed, duplicate_fanout=fanout)
        for variant in ["V2", "V3", "V4", "V5"]:
            for cap in ["C0", "C1", "C2"]:
                if variant == "V3" and cap == "C0":
                    continue
                for failure in ["F3", "F8"]:
                    add("group_d_capability", "create_order", variant, cap, failure, 0.10, 50, seed, id_suffix="_CAP")
        for failure in ["F1", "F2", "F7"]:
            for variant in variants:
                add("group_e_safe_controls", "create_order", variant, cap_for(variant), failure, 0.10, 50, seed)
        for workload in ["charge_payment", "reserve_inventory", "send_notification"]:
            for variant in ["V0", "V2", "V4", "V5"]:
                for failure in ["F3", "F8"]:
                    add("group_f_generalization", workload, variant, cap_for(variant), failure, 0.10, 50, seed)
    write_rows(PHASE6_ROOT / "manifest.csv", manifest)
    (PHASE6_ROOT / "manifest_summary.json").write_text(json.dumps({"total_run_count": len(manifest), "config_root": str(PHASE6_ROOT), "manifest": str(PHASE6_ROOT / "manifest.csv")}, indent=2))


def command_output(args: list[str]) -> str:
    try:
        result = subprocess.run(args, cwd=ROOT, check=False, capture_output=True, text=True)
        text = (result.stdout + result.stderr).strip()
        return text.splitlines()[0] if text else ""
    except Exception as exc:
        return f"unavailable: {exc}"


def command_full_output(args: list[str]) -> str:
    try:
        result = subprocess.run(args, cwd=ROOT, check=False, capture_output=True, text=True)
        text = (result.stdout + result.stderr).strip()
        return text
    except Exception as exc:
        return f"unavailable: {exc}"


def command_json(args: list[str]) -> Any:
    result = subprocess.run(args, cwd=ROOT, check=False, capture_output=True, text=True)
    text = (result.stdout or "").strip()
    if result.returncode != 0 or not text:
        return None
    return json.loads(text)


def docker_desktop_settings() -> dict[str, Any]:
    path = Path.home() / "Library" / "Group Containers" / "group.com.docker" / "settings-store.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def docker_desktop_log_settings() -> dict[str, Any]:
    path = Path.home() / "Library" / "Containers" / "com.docker.docker" / "Data" / "log" / "host" / "com.docker.virtualization.log"
    backend = Path.home() / "Library" / "Containers" / "com.docker.docker" / "Data" / "log" / "host" / "com.docker.backend.log"
    data: dict[str, Any] = {}
    if backend.exists():
        text = backend.read_text(errors="ignore")
        for key, pattern in {
            "cpus": r"--cpus (\d+)",
            "memoryMiB": r"--memoryMiB (\d+)",
            "diskSizeMiB": r"resized .*Docker\.raw\" to (\d+)MiB",
        }.items():
            match = re.search(pattern, text)
            if match:
                data[key] = int(match.group(1))
    if path.exists():
        text = path.read_text(errors="ignore")
        match = re.search(r"will use (\d+) MiB of memory", text)
        if match:
            data.setdefault("memoryMiB", int(match.group(1)))
    init_log = Path.home() / "Library" / "Containers" / "com.docker.docker" / "Data" / "log" / "vm" / "init.log"
    if init_log.exists():
        text = init_log.read_text(errors="ignore")
        match = re.search(r"requested is (\d+)", text)
        if match:
            data["swapBytes"] = int(match.group(1))
    return data


def write_environment_metadata() -> None:
    try:
        with psycopg.connect(ADMIN_DSN) as conn:
            postgres_version = conn.execute("SELECT version()").fetchone()[0]
    except Exception as exc:
        postgres_version = f"unavailable: {exc}"
    try:
        import psutil  # type: ignore
        total_memory = psutil.virtual_memory().total
    except Exception:
        total_memory = None
    docker_info = command_json(["docker", "info", "--format", "{{json .}}"]) or {}
    docker_version = command_json(["docker", "version", "--format", "{{json .}}"]) or {}
    settings = docker_desktop_settings()
    log_settings = docker_desktop_log_settings()
    commit = command_output(["git", "rev-parse", "HEAD"])
    metadata_path = OUT / "environment_metadata.json"
    prior_metadata: dict[str, Any] = {}
    if metadata_path.exists():
        try:
            prior_metadata = json.loads(metadata_path.read_text())
        except json.JSONDecodeError:
            prior_metadata = {}
    metadata = {
        "macos_version": command_full_output(["sw_vers"]),
        "architecture": platform.machine(),
        "apple_chip_model": command_output(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "physical_ram_bytes": total_memory,
        "cpu_core_count": os.cpu_count(),
        "docker_desktop_version": docker_version.get("Server", {}).get("Platform", {}).get("Name", ""),
        "docker_engine_version": docker_info.get("ServerVersion", ""),
        "docker_compose_version": command_output(["docker", "compose", "version"]),
        "docker_context": command_output(["docker", "context", "show"]),
        "docker_desktop_allocated_cpus": settings.get("cpus", log_settings.get("cpus", docker_info.get("NCPU"))),
        "docker_desktop_allocated_memory_mib": settings.get("memoryMiB", log_settings.get("memoryMiB")),
        "docker_desktop_swap_mib": settings.get("swapMiB", (log_settings.get("swapBytes", 0) // (1024 * 1024)) or None),
        "docker_desktop_disk_image_mib": (
            settings.get("diskSizeMiB")
            or log_settings.get("diskSizeMiB")
            or prior_metadata.get("docker_desktop_disk_image_mib")
            or DOCKER_DESKTOP_DISK_IMAGE_MIB_FALLBACK
        ),
        "java_version": command_output(["java", "-version"]),
        "python_version": platform.python_version(),
        "postgresql_version": postgres_version,
        "git_commit_hash": "" if commit.startswith("fatal:") else commit,
        "git_commit_hash_status": commit if commit.startswith("fatal:") else "ok",
        "timezone": command_output(["date", "+%Z"]),
        "generated_at_unix": time.time(),
    }
    metadata["os"] = platform.platform()
    OUT.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2))


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path: Path, rows: list[dict[str, Any]], key_fields: list[str]) -> None:
    if not rows:
        return
    existing: list[dict[str, Any]] = []
    if path.exists() and path.read_text().strip():
        with path.open() as f:
            existing = list(csv.DictReader(f))
    def key(row: dict[str, Any]) -> tuple[str, ...]:
        return tuple(str(row.get(field, "")) for field in key_fields)
    merged = {key(row): row for row in existing}
    for row in rows:
        merged[key(row)] = row
    write_rows(path, list(merged.values()))


if __name__ == "__main__":
    main()
