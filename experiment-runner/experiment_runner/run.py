import argparse
import csv
import json
import os
import shutil
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import psycopg
import requests
from requests.adapters import HTTPAdapter
import yaml

from agent_simulator.identity import attempt_id, request_hash
from agent_simulator.workload import LogicalOperation, build_workload
from fault_injector.deterministic import selected


ROOT = Path(__file__).resolve().parents[2]
GATEWAY_URL = "http://localhost:8080"
ANALYSIS_DSN = "postgresql://analysis_user:analysis_user@localhost:5432/exactlyonce"
_thread_local = threading.local()


def results_root() -> Path:
    return Path(os.environ.get("RESULTS_ROOT", str(ROOT / "results")))


def http_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=200, pool_maxsize=200, max_retries=0)
        session.mount("http://", adapter)
        _thread_local.session = session
    return session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run_config(Path(args.config))


def run_config(config_path: Path) -> Path:
    config = yaml.safe_load(config_path.read_text())
    experiment_id = config["experiment"]["id"]
    run_id = f"{experiment_id}_{int(time.time() * 1000)}"
    out_dir = results_root() / experiment_id / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(config_path, out_dir / "experiment_config.yaml")

    workload_cfg = config["workload"]
    operations = build_workload(
        experiment_id,
        workload_cfg["type"],
        int(workload_cfg["operations"]),
        int(workload_cfg.get("warmup_operations", 0)),
    )

    started = time.time()
    events: list[dict] = []
    concurrency = int(workload_cfg["concurrency"])
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(execute_operation, config, run_id, op) for op in operations]
        for future in as_completed(futures):
            events.extend(future.result())

    effects = read_effects(config["experiment"]["id"], run_id)
    measured_by_operation = {op.operation_id: op.measured for op in operations}
    events.extend(read_service_events(config["experiment"]["id"], run_id, measured_by_operation))
    ledger_transitions = read_ledger_transitions(config["experiment"]["id"])
    operations_rows = summarize_operations(operations, events, effects)
    summary = summarize_run(operations_rows, events, started)
    summary.update(fault_reachability_summary(config, operations_rows))

    write_csv(out_dir / "events.csv", events)
    write_csv(out_dir / "effects.csv", effects)
    write_csv(out_dir / "operations.csv", operations_rows)
    write_csv(out_dir / "ledger_transitions.csv", ledger_transitions)
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True))
    return out_dir


def execute_operation(config: dict, run_id: str, op: LogicalOperation) -> list[dict]:
    scenario = config["failure"]["scenario"]
    if scenario == "F1" and selected(int(config["failure"]["seed"]), op.operation_id, "F1", "BEFORE_GATEWAY_RECEIVE", float(config["failure"]["probability"])):
        return [lost_before_gateway(config, run_id, op, 1)]
    if scenario == "CF1":
        return execute_compound_retry(config, run_id, op, first_scenario="F3")
    if scenario == "CF2":
        return execute_response_loss_concurrent_retry(config, run_id, op, int(config["failure"].get("duplicate_fanout", 5)))
    if scenario == "CF3":
        return execute_compound_retry(config, run_id, op, first_scenario="F8")
    if scenario == "CF4":
        return execute_ledger_outage_retry_storm(config, run_id, op, int(config["failure"].get("retry_fanout", 5)))
    if scenario == "F11":
        return execute_retry_storm(config, run_id, op, int(config["failure"].get("retry_fanout", 5)))
    if scenario == "F10" and int(config["failure"].get("outage_attempts", 0)) > 0:
        return execute_staged_recovery(
            config,
            run_id,
            op,
            failing_scenario="F10",
            recovery_scenario="F0",
            outage_attempts=int(config["failure"]["outage_attempts"]),
            ledger_mode=config["failure"].get("mode"),
        )
    fanout = int(config["failure"].get("duplicate_fanout", 1))
    if scenario == "F6":
        return execute_concurrent_duplicates(config, run_id, op, fanout)
    return execute_retry_loop(config, run_id, op)


def execute_retry_loop(
    config: dict,
    run_id: str,
    op: LogicalOperation,
    scenario_override: Optional[str] = None,
    start_attempt: int = 1,
    existing_rows: Optional[list[dict]] = None,
) -> list[dict]:
    retry_cfg = config.get("retry", {})
    max_attempts = int(retry_cfg.get("max_attempts", 5))
    initial_ms = int(retry_cfg.get("backoff", {}).get("initial_ms", 100))
    rows = list(existing_rows or [])
    for attempt in range(start_attempt, max_attempts + 1):
        row = invoke(config, run_id, op, attempt, scenario_override=scenario_override)
        rows.append(row)
        if row["result_status"] in {"OK", "FAILED_FINAL"}:
            break
        if row["result_status"] == "IN_PROGRESS":
            time.sleep(max((config.get("lease", {}).get("ms", 500) + 100) / 1000.0, 0.1))
            continue
        if row["result_status"] == "UNKNOWN" and config["architecture"]["variant"] == "V5":
            continue
        time.sleep((initial_ms * (2 ** (attempt - 1))) / 1000.0)
    if config["failure"]["scenario"] == "F5" and scenario_override is None:
        rows.append(invoke(config, run_id, op, max_attempts + 1))
    return rows


def execute_compound_retry(config: dict, run_id: str, op: LogicalOperation, first_scenario: str) -> list[dict]:
    # Compose the validated primitive failure on the first attempt, then retry the
    # same logical operation under normal conditions so retry behavior is observable.
    first = invoke(config, run_id, op, 1, scenario_override=first_scenario)
    return execute_retry_loop(config, run_id, op, scenario_override="F0", start_attempt=2, existing_rows=[first])



def execute_concurrent_duplicates(config: dict, run_id: str, op: LogicalOperation, fanout: int) -> list[dict]:
    with ThreadPoolExecutor(max_workers=fanout) as executor:
        futures = [executor.submit(invoke, config, run_id, op, i + 1) for i in range(fanout)]
        return [future.result() for future in as_completed(futures)]


def execute_retry_storm(config: dict, run_id: str, op: LogicalOperation, retry_fanout: int) -> list[dict]:
    rows = [invoke(config, run_id, op, 1)]
    with ThreadPoolExecutor(max_workers=retry_fanout) as executor:
        futures = [executor.submit(invoke, config, run_id, op, i + 2) for i in range(retry_fanout)]
        rows.extend(future.result() for future in as_completed(futures))
    return rows


def execute_response_loss_concurrent_retry(config: dict, run_id: str, op: LogicalOperation, fanout: int) -> list[dict]:
    rows = [invoke(config, run_id, op, 1, scenario_override="F3")]
    with ThreadPoolExecutor(max_workers=fanout) as executor:
        futures = [executor.submit(invoke, config, run_id, op, i + 2, scenario_override="F0") for i in range(fanout)]
        rows.extend(future.result() for future in as_completed(futures))
    return rows


def execute_ledger_outage_retry_storm(config: dict, run_id: str, op: LogicalOperation, retry_fanout: int) -> list[dict]:
    mode = config["failure"].get("mode", "read")
    with ThreadPoolExecutor(max_workers=retry_fanout) as executor:
        futures = [executor.submit(invoke, config, run_id, op, i + 1, scenario_override="F10", ledger_mode_override=mode) for i in range(retry_fanout)]
        rows = [future.result() for future in as_completed(futures)]
    rows.append(invoke(config, run_id, op, retry_fanout + 1, scenario_override="F0", ledger_mode_override=None))
    return rows


def execute_staged_recovery(
    config: dict,
    run_id: str,
    op: LogicalOperation,
    failing_scenario: str,
    recovery_scenario: str,
    outage_attempts: int,
    ledger_mode: Optional[str] = None,
) -> list[dict]:
    rows: list[dict] = []
    retry_cfg = config.get("retry", {})
    max_attempts = int(retry_cfg.get("max_attempts", 5))
    initial_ms = int(retry_cfg.get("backoff", {}).get("initial_ms", 100))
    for attempt in range(1, max_attempts + 1):
        if attempt <= outage_attempts:
            row = invoke(config, run_id, op, attempt, scenario_override=failing_scenario, ledger_mode_override=ledger_mode)
        else:
            row = invoke(config, run_id, op, attempt, scenario_override=recovery_scenario, ledger_mode_override=None)
        rows.append(row)
        if row["result_status"] in {"OK", "FAILED_FINAL"}:
            break
        if row["result_status"] == "IN_PROGRESS":
            time.sleep(max((config.get("lease", {}).get("ms", 500) + 100) / 1000.0, 0.1))
            continue
        if row["result_status"] == "UNKNOWN" and config["architecture"]["variant"] == "V5":
            continue
        time.sleep((initial_ms * (2 ** (attempt - 1))) / 1000.0)
    return rows


def invoke(config: dict, run_id: str, op: LogicalOperation, attempt: int, scenario_override: Optional[str] = None, ledger_mode_override: Optional[str] = None) -> dict:
    started = time.time()
    body = {
        "experimentId": config["experiment"]["id"],
        "runId": run_id,
        "seed": int(config["failure"]["seed"]),
        "variant": config["architecture"]["variant"],
        "downstreamCapability": config["architecture"]["downstream_capability"],
        "failureScenario": scenario_override or config["failure"]["scenario"],
        "ledgerFailureMode": ledger_mode_override if ledger_mode_override is not None else config["failure"].get("mode"),
        "failureProbability": float(config["failure"]["probability"]),
        "concurrency": int(config["workload"]["concurrency"]),
        "operationId": op.operation_id,
        "attemptId": attempt_id(op.operation_id, attempt),
        "toolName": op.tool_name,
        "requestHash": request_hash(op.tool_name, op.arguments),
        "arguments": op.arguments,
        "leaseMs": int(config.get("lease", {}).get("ms", 5000)),
    }
    status = "CLIENT_ERROR"
    final_state = ""
    replayed = False
    reconciled = False
    effect_id = ""
    try:
        response = http_session().post(f"{GATEWAY_URL}/tools/{op.tool_name}", json=body, timeout=config["timeouts"]["client_ms"] / 1000)
        status = response.status_code
        payload = response.json() if response.content else {}
        if not isinstance(payload, dict):
            payload = {"status": f"HTTP_{status}", "message": payload}
        result_status = payload.get("status", f"HTTP_{status}")
        final_state = payload.get("finalState", "")
        replayed = bool(payload.get("replayed", False))
        reconciled = bool(payload.get("reconciled", False))
        effect_id = payload.get("effectId") or ""
    except ValueError:
        result_status = f"HTTP_{status}"
    except requests.Timeout:
        result_status = "CLIENT_TIMEOUT"
    except requests.RequestException as exc:
        result_status = type(exc).__name__
    return {
        "experiment_id": config["experiment"]["id"],
        "run_id": run_id,
        "seed": config["failure"]["seed"],
        "variant": config["architecture"]["variant"],
        "downstream_capability": config["architecture"]["downstream_capability"],
        "failure_scenario": scenario_override or config["failure"]["scenario"],
        "failure_probability": config["failure"]["probability"],
        "concurrency": config["workload"]["concurrency"],
        "operation_id": op.operation_id,
        "attempt_id": body["attemptId"],
        "service": "agent-simulator",
        "tool_name": op.tool_name,
        "request_hash": body["requestHash"],
        "measured": op.measured,
        "event_type": "agent_attempt",
        "timestamp": time.time(),
        "duration_ms": round((time.time() - started) * 1000, 3),
        "result_status": result_status,
        "http_status": status,
        "final_state": final_state,
        "replayed": replayed,
        "reconciled": reconciled,
        "downstream_effect_id": effect_id,
    }


def lost_before_gateway(config: dict, run_id: str, op: LogicalOperation, attempt: int) -> dict:
    return {
        "experiment_id": config["experiment"]["id"],
        "run_id": run_id,
        "seed": config["failure"]["seed"],
        "variant": config["architecture"]["variant"],
        "downstream_capability": config["architecture"]["downstream_capability"],
        "failure_scenario": config["failure"]["scenario"],
        "failure_probability": config["failure"]["probability"],
        "concurrency": config["workload"]["concurrency"],
        "operation_id": op.operation_id,
        "attempt_id": attempt_id(op.operation_id, attempt),
        "service": "agent-simulator",
        "tool_name": op.tool_name,
        "request_hash": request_hash(op.tool_name, op.arguments),
        "measured": op.measured,
        "event_type": "agent_request_lost",
        "timestamp": time.time(),
        "duration_ms": 0,
        "result_status": "REQUEST_LOST_BEFORE_EXECUTION",
        "http_status": "",
        "final_state": "NO_LEDGER",
        "replayed": False,
        "reconciled": False,
        "downstream_effect_id": "",
    }


def read_effects(experiment_id: str, run_id: str) -> list[dict]:
    last_error = None
    for attempt in range(10):
        try:
            with psycopg.connect(ANALYSIS_DSN) as conn:
                rows = conn.execute(
                    "SELECT effect_id, operation_id, effect_type, service, experiment_id, run_id, created_at FROM observer.observer_effects WHERE experiment_id = %s AND run_id = %s ORDER BY created_at",
                    (experiment_id, run_id),
                ).fetchall()
            break
        except psycopg.OperationalError as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    else:
        raise last_error
    return [
        {
            "effect_id": str(row[0]),
            "operation_id": row[1],
            "effect_type": row[2],
            "service": row[3],
            "experiment_id": row[4],
            "run_id": row[5],
            "timestamp": row[6].isoformat(),
        }
        for row in rows
    ]


def read_service_events(experiment_id: str, run_id: str, measured_by_operation: dict[str, bool]) -> list[dict]:
    with psycopg.connect(ANALYSIS_DSN) as conn:
        rows = conn.execute(
            "SELECT experiment_id, run_id, operation_id, attempt_id, service, event_type, event_time, tool_name, request_hash, downstream_effect_id, replayed, result_status FROM runtime.event_log WHERE experiment_id = %s AND run_id = %s ORDER BY event_time",
            (experiment_id, run_id),
        ).fetchall()
    return [
        {
            "experiment_id": row[0],
            "run_id": row[1],
            "seed": "",
            "variant": "",
            "downstream_capability": "",
            "failure_scenario": "",
            "failure_probability": "",
            "concurrency": "",
            "operation_id": row[2],
            "attempt_id": row[3],
            "tool_name": row[7],
            "request_hash": row[8],
            "measured": measured_by_operation.get(row[2], False),
            "event_type": row[5],
            "timestamp": row[6].timestamp(),
            "duration_ms": "",
            "result_status": row[11],
            "http_status": "",
            "final_state": "",
            "replayed": bool(row[10]),
            "reconciled": False,
            "downstream_effect_id": row[9] or "",
            "service": row[4],
        }
        for row in rows
    ]


def read_ledger_transitions(experiment_id: str) -> list[dict]:
    with psycopg.connect(ANALYSIS_DSN) as conn:
        rows = conn.execute(
            "SELECT operation_id, state_before::text, state_after::text, attempt_id, reason, created_at FROM runtime.ledger_transitions WHERE operation_id LIKE %s ORDER BY created_at, id",
            (experiment_id + "_OP_%",),
        ).fetchall()
    return [
        {
            "operation_id": row[0],
            "state_before": row[1] or "",
            "state_after": row[2],
            "attempt_id": row[3] or "",
            "reason": row[4] or "",
            "timestamp": row[5].isoformat(),
        }
        for row in rows
    ]


def summarize_operations(operations: list[LogicalOperation], events: list[dict], effects: list[dict]) -> list[dict]:
    events_by_op: dict[str, list[dict]] = {}
    effects_by_op: dict[str, list[dict]] = {}
    for event in events:
        events_by_op.setdefault(event["operation_id"], []).append(event)
    for effect in effects:
        effects_by_op.setdefault(effect["operation_id"], []).append(effect)
    rows = []
    for op in operations:
        op_events = [event for event in events_by_op.get(op.operation_id, []) if event["event_type"] == "agent_attempt"]
        service_events = [event for event in events_by_op.get(op.operation_id, []) if event["event_type"] == "downstream_execute"]
        op_effects = effects_by_op.get(op.operation_id, [])
        if not op.measured:
            continue
        rows.append(
            {
                "operation_id": op.operation_id,
                "tool_name": op.tool_name,
                "final_state": op_events[-1]["final_state"] if op_events else "NO_ATTEMPT",
                "attempt_count": len(op_events),
                "downstream_call_count": len(service_events),
                "effect_count": len(op_effects),
                "replayed": any(e["replayed"] for e in op_events),
                "reconciled": any(e["reconciled"] for e in op_events),
                "latency": sum(float(e["duration_ms"]) for e in op_events),
                "recovery_latency": sum(float(e["duration_ms"]) for e in op_events if e["reconciled"]),
                "failure_scenario": op_events[-1]["failure_scenario"] if op_events else "",
                "result_status": op_events[-1]["result_status"] if op_events else "NO_ATTEMPT",
            }
        )
    return rows


def summarize_run(operations: list[dict], events: list[dict], started: float) -> dict:
    total = len(operations) or 1
    measured_events = [event for event in events if event.get("measured") and event.get("event_type") == "agent_attempt"]
    duplicate = len([op for op in operations if int(op["effect_count"]) > 1])
    exact = len([op for op in operations if int(op["effect_count"]) == 1])
    lost = len([op for op in operations if int(op["effect_count"]) == 0])
    unknown = len([op for op in operations if op["final_state"] == "UNKNOWN"])
    retry_requests = max(len(measured_events) - total, 0)
    replayed = len([event for event in measured_events if event["replayed"]])
    latencies = sorted(float(op["latency"]) for op in operations)
    return {
        "logical_operations": total,
        "DER": duplicate / total,
        "EOER": exact / total,
        "LER": lost / total,
        "RSR": len([op for op in operations if op["reconciled"]]) / max(len([op for op in operations if op["final_state"] in {"UNKNOWN", "COMPLETED"}]), 1),
        "RAF": len(measured_events) / total,
        "DAF": sum(int(op["downstream_call_count"]) for op in operations) / total,
        "RRR": replayed / max(retry_requests, 1),
        "UAR": unknown / total,
        "P50": percentile(latencies, 0.50),
        "P95": percentile(latencies, 0.95),
        "P99": percentile(latencies, 0.99),
        "throughput": total / max(time.time() - started, 0.001),
    }


def fault_reachability_summary(config: dict, operations: list[dict]) -> dict:
    scenario = config["failure"]["scenario"]
    seed = int(config["failure"]["seed"])
    probability = float(config["failure"]["probability"])
    hook = {
        "F1": "BEFORE_GATEWAY_RECEIVE",
        "F2": "BEFORE_DOWNSTREAM_DISPATCH",
        "F3": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
        "F4": "AFTER_DOWNSTREAM_RESPONSE",
        "F7": "BEFORE_DOWNSTREAM_DISPATCH",
        "F8": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
        "F9": "BEFORE_FINAL_RESULT_PERSIST",
        "F12": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
        "CF1": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
        "CF2": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
        "CF3": "BEFORE_EFFECT_CONFIRMATION_PERSIST",
    }.get(scenario)
    selected_count = 0
    if hook:
        mapped = {"CF1": "F3", "CF2": "F3", "CF3": "F8"}.get(scenario, scenario)
        selected_count = sum(1 for op in operations if selected(seed, op["operation_id"], mapped, hook, probability))
    elif scenario in {"F6", "F11", "CF4"}:
        selected_count = len(operations)
    reached = 0
    injected = 0
    for op in operations:
        downstream_calls = int(op["downstream_call_count"])
        attempts = int(op["attempt_count"])
        if scenario in {"F3", "F4", "F8", "F9", "F12", "CF1", "CF2", "CF3"} and downstream_calls > 0:
            reached += 1
        elif scenario in {"F2", "F7", "F10", "CF4"} and attempts > 0:
            reached += 1
        elif scenario in {"F1", "F6", "F11"}:
            reached += 1
        if scenario in {"F3", "F8", "F12", "CF1", "CF2", "CF3"} and op["final_state"] in {"UNKNOWN", "EXECUTING", "COMPLETED", "NO_LEDGER"}:
            injected += 1
        elif scenario in {"F2", "F7"} and op["result_status"] == "RETRYABLE_FAILURE":
            injected += 1
        elif scenario == "F10" and str(op["result_status"]).startswith("HTTP_503"):
            injected += 1
        elif scenario in {"F6", "F11", "CF4"}:
            injected += 1
    return {
        "selected_for_fault": selected_count,
        "reached_fault_hook": reached,
        "fault_injected": injected,
    }


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return values[min(int(len(values) * p), len(values) - 1)]


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    main()
