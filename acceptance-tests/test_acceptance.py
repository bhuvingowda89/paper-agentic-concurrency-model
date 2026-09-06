import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
import pytest
import requests
import yaml

from agent_simulator.identity import attempt_id, request_hash
from agent_simulator.workload import build_workload
from experiment_runner.run import run_config
from fault_injector.deterministic import selected


GATEWAY = "http://localhost:8080"
ADMIN_DSN = "postgresql://exactlyonce:exactlyonce@localhost:5432/exactlyonce"
ANALYSIS_DSN = "postgresql://analysis_user:analysis_user@localhost:5432/exactlyonce"
RUNTIME_DSN = "postgresql://runtime_service:runtime_service@localhost:5432/exactlyonce"


def require_env():
    try:
        requests.get(f"{GATEWAY}/health", timeout=1).raise_for_status()
    except Exception as exc:
        pytest.skip(f"testbed services are not running: {exc}")


def reset():
    with psycopg.connect(ADMIN_DSN, autocommit=True) as conn:
        conn.execute(open("database/migrations/reset_experiment.sql").read())


def invoke(op_id, args, variant="V5", capability="C2", scenario="F0", probability=0.0, attempt=1, experiment="TEST", ledger_mode=None, tool="create_order"):
    body = {
        "experimentId": experiment,
        "runId": "pytest",
        "seed": 42,
        "variant": variant,
        "downstreamCapability": capability,
        "failureScenario": scenario,
        "ledgerFailureMode": ledger_mode,
        "failureProbability": probability,
        "concurrency": 1,
        "operationId": op_id,
        "attemptId": attempt_id(op_id, attempt),
        "toolName": tool,
        "requestHash": request_hash(tool, args),
        "arguments": args,
        "leaseMs": 200,
    }
    return requests.post(f"{GATEWAY}/tools/{tool}", json=body, timeout=3)


def effect_count(op_id):
    with psycopg.connect(ANALYSIS_DSN) as conn:
        return conn.execute("SELECT count(*) FROM observer.observer_effects WHERE operation_id = %s", (op_id,)).fetchone()[0]


def ledger_state(op_id):
    with psycopg.connect(ADMIN_DSN) as conn:
        row = conn.execute("SELECT state::text FROM runtime.execution_ledger WHERE operation_id = %s", (op_id,)).fetchone()
        return row[0] if row else None


def order_args(quantity=1):
    return {"customer_id": "cust-a", "product_id": "sku-a", "quantity": quantity}


def test_t1_request_hash_conflict():
    require_env()
    reset()
    op_id = "T1"
    assert invoke(op_id, order_args(1)).status_code == 200
    response = invoke(op_id, order_args(2), attempt=2)
    assert response.status_code == 409
    assert effect_count(op_id) == 1


def test_t2_completed_replay():
    require_env()
    reset()
    op_id = "T2"
    first = invoke(op_id, order_args()).json()
    second = invoke(op_id, order_args(), attempt=2).json()
    assert second["replayed"] is True
    assert first["effectId"] == second["effectId"]
    assert effect_count(op_id) == 1


def test_t3_concurrent_duplicate_protection():
    require_env()
    reset()
    op_id = "T3"
    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(lambda i: invoke(op_id, order_args(), attempt=i), range(1, 11)))
    assert effect_count(op_id) == 1


def test_t4_v0_duplicate_baseline():
    require_env()
    reset()
    op_id = "T4"
    first = invoke(op_id, order_args(), variant="V0", capability="C0", scenario="F3", probability=1.0, attempt=1)
    assert first.status_code == 504
    second = invoke(op_id, order_args(), variant="V0", capability="C0", scenario="F0", probability=0.0, attempt=2)
    assert second.status_code == 200
    assert effect_count(op_id) > 1


def test_t5_v2_ambiguous_gateway_crash_c0():
    require_env()
    reset()
    op_id = "T5"
    first = invoke(op_id, order_args(), variant="V2", capability="C0", scenario="F8", probability=1.0)
    assert first.json()["finalState"] == "EXECUTING"
    time.sleep(0.25)
    invoke(op_id, order_args(), variant="V2", capability="C0", scenario="F0", probability=0.0, attempt=2)
    assert ledger_state(op_id) == "UNKNOWN"
    assert effect_count(op_id) == 1


def test_t6_v3_service_replay():
    require_env()
    reset()
    op_id = "T6"
    for i in range(1, 4):
        assert invoke(op_id, order_args(), variant="V3", capability="C1", attempt=i).status_code == 200
    assert effect_count(op_id) == 1


def test_t7_v5_crash_recovery():
    require_env()
    reset()
    op_id = "T7"
    first = invoke(op_id, order_args(), variant="V5", capability="C2", scenario="F8", probability=1.0)
    assert first.json()["finalState"] == "EXECUTING"
    assert ledger_state(op_id) == "EXECUTING"
    time.sleep(0.25)
    second = invoke(op_id, order_args(), variant="V5", capability="C2", scenario="F0", probability=0.0, attempt=2)
    payload = second.json()
    assert payload["finalState"] == "COMPLETED"
    assert payload["reconciled"] is True
    assert effect_count(op_id) == 1
    third = invoke(op_id, order_args(), variant="V5", capability="C2", scenario="F0", probability=0.0, attempt=3).json()
    assert third["replayed"] is True
    assert third["effectId"] == payload["effectId"]


def test_t8_c0_unresolved_ambiguity():
    require_env()
    reset()
    op_id = "T8"
    invoke(op_id, order_args(), variant="V5", capability="C0", scenario="F12", probability=1.0)
    second = invoke(op_id, order_args(), variant="V5", capability="C0", scenario="F0", probability=0.0, attempt=2)
    assert second.json()["finalState"] == "UNKNOWN"


def test_t11_v4_delayed_response_commits_before_retry():
    require_env()
    reset()
    op_id = "T11"
    with pytest.raises(requests.Timeout):
        invoke(op_id, order_args(), variant="V4", capability="C1", scenario="F4", probability=1.0)
    second = invoke(op_id, order_args(), variant="V4", capability="C1", scenario="F0", probability=0.0, attempt=2)
    payload = second.json()
    assert payload["finalState"] == "COMPLETED"
    assert payload["replayed"] is True
    assert effect_count(op_id) == 1


def test_t9_ground_truth_independence():
    require_env()
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        with psycopg.connect(RUNTIME_DSN) as conn:
            conn.execute("SELECT count(*) FROM observer.observer_effects").fetchone()


def test_t10_seed_reproducibility():
    ops_a = build_workload("REPRO", "create_order", 20, 0)
    ops_b = build_workload("REPRO", "create_order", 20, 0)
    assert ops_a == ops_b
    decisions_a = [selected(1004, op.operation_id, "F8", "BEFORE_EFFECT_CONFIRMATION_PERSIST", 0.10) for op in ops_a]
    decisions_b = [selected(1004, op.operation_id, "F8", "BEFORE_EFFECT_CONFIRMATION_PERSIST", 0.10) for op in ops_b]
    assert decisions_a == decisions_b


def test_t12_gateway_request_hash_accepts_charge_payment_arguments():
    require_env()
    reset()
    op_id = "T12"
    response = invoke(op_id, {"customer_id": "cust-a", "amount": "10.00"}, variant="V5", capability="C2", scenario="F0", probability=0.0, experiment="T12", tool="charge_payment")
    assert response.status_code == 200
    payload = response.json()
    assert payload["finalState"] == "COMPLETED"
    assert effect_count(op_id) == 1


def test_f10_temporary_ledger_unavailability_read_and_write():
    require_env()
    reset()
    read_op = "F10_READ"
    read_fail = invoke(read_op, order_args(), variant="V5", capability="C2", scenario="F10", probability=1.0, ledger_mode="read")
    assert read_fail.status_code == 503
    assert ledger_state(read_op) is None
    assert effect_count(read_op) == 0
    read_recover = invoke(read_op, order_args(), variant="V5", capability="C2", scenario="F0", probability=0.0, attempt=2)
    assert read_recover.status_code == 200
    assert effect_count(read_op) == 1

    write_op = "F10_WRITE"
    write_fail = invoke(write_op, order_args(), variant="V5", capability="C2", scenario="F10", probability=1.0, ledger_mode="write")
    assert write_fail.status_code == 503
    assert ledger_state(write_op) is None
    assert effect_count(write_op) == 0
    write_recover = invoke(write_op, order_args(), variant="V5", capability="C2", scenario="F0", probability=0.0, attempt=2)
    assert write_recover.status_code == 200
    assert effect_count(write_op) == 1


def test_f11_retry_storm_records_single_logical_operation(tmp_path):
    require_env()
    reset()
    config = {
        "experiment": {"id": "ORDER_V5_C2_F11_P0_C1_S9911"},
        "workload": {"type": "create_order", "operations": 1, "concurrency": 1, "warmup_operations": 0},
        "architecture": {"variant": "V5", "downstream_capability": "C2"},
        "failure": {"scenario": "F11", "probability": 0.0, "seed": 9911, "retry_fanout": 10},
        "retry": {"max_attempts": 5, "backoff": {"type": "exponential", "initial_ms": 10, "jitter": "seeded"}},
        "timeouts": {"client_ms": 2000, "downstream_ms": 2000},
        "output": {"raw_events": True, "operation_summary": True, "metrics": True},
    }
    config_path = tmp_path / "f11.yaml"
    config_path.write_text(yaml.safe_dump(config))
    out_dir = run_config(config_path)
    operations_csv = (out_dir / "operations.csv").read_text()
    assert "ORDER_V5_C2_F11_P0_C1_S9911_OP_00000000" in operations_csv
    assert ",11," in operations_csv


def run_harness_config(tmp_path, experiment_id, variant, capability, scenario, operations=20, concurrency=5, probability=1.0):
    config = {
        "experiment": {"id": experiment_id},
        "workload": {"type": "create_order", "operations": operations, "concurrency": concurrency, "warmup_operations": 0},
        "architecture": {"variant": variant, "downstream_capability": capability},
        "failure": {"scenario": scenario, "probability": probability, "seed": 9911},
        "retry": {"max_attempts": 5, "backoff": {"type": "exponential", "initial_ms": 10, "jitter": "seeded"}},
        "timeouts": {"client_ms": 2000, "downstream_ms": 2000},
        "lease": {"ms": 200},
        "output": {"raw_events": True, "operation_summary": True, "metrics": True},
    }
    config_path = tmp_path / f"{experiment_id}.yaml"
    config_path.write_text(yaml.safe_dump(config))
    return run_config(config_path)


def read_operations_and_summary(run_dir):
    import csv
    import json

    with (run_dir / "operations.csv").open() as handle:
        operations = list(csv.DictReader(handle))
    summary = json.loads((run_dir / "summary.json").read_text())
    return operations, summary


def test_cf1_compound_response_loss_forces_agent_retry(tmp_path):
    require_env()
    for variant, capability in [("V0", "C0"), ("V2", "C0"), ("V4", "C1"), ("V5", "C2")]:
        reset()
        run_dir = run_harness_config(tmp_path, f"CF1_{variant}", variant, capability, "CF1")
        operations, summary = read_operations_and_summary(run_dir)
        assert operations
        assert min(int(row["attempt_count"]) for row in operations) > 1
        assert summary["RAF"] > 1.0


def test_cf3_compound_reuses_f8_semantics_on_retry(tmp_path):
    require_env()

    reset()
    v4_dir = run_harness_config(tmp_path, "CF3_V4", "V4", "C1", "CF3")
    v4_ops, v4_summary = read_operations_and_summary(v4_dir)
    assert v4_ops
    assert min(int(row["attempt_count"]) for row in v4_ops) > 1
    assert all(int(row["effect_count"]) == 1 for row in v4_ops)
    assert all(row["final_state"] == "UNKNOWN" for row in v4_ops)
    assert v4_summary["UAR"] > 0.0
    assert v4_summary["RSR"] == 0.0
    assert all(row["reconciled"] == "False" for row in v4_ops)

    reset()
    v5_dir = run_harness_config(tmp_path, "CF3_V5", "V5", "C2", "CF3")
    v5_ops, v5_summary = read_operations_and_summary(v5_dir)
    assert v5_ops
    assert min(int(row["attempt_count"]) for row in v5_ops) > 1
    assert all(int(row["effect_count"]) == 1 for row in v5_ops)
    assert all(row["final_state"] == "COMPLETED" for row in v5_ops)
    assert any(row["reconciled"] == "True" for row in v5_ops)
    assert v5_summary["RSR"] > 0.0
    assert v5_summary["UAR"] == 0.0
