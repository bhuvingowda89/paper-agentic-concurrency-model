import argparse
import csv
import fcntl
import hashlib
import json
import os
import platform
import shutil
import subprocess
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
import yaml

from experiment_runner.run import run_config, summarize_run
from agent_simulator.workload import build_workload
from analysis.phase5_harness import (
    ADMIN_DSN,
    ROOT,
    clean_counts,
    command_full_output,
    command_json,
    command_output,
    docker_compose,
    docker_desktop_log_settings,
    docker_desktop_settings,
    ownership_overlaps,
    percentile,
    reset_db,
    state_transition_violations,
)


OUT = ROOT / "results" / "phase6"
RUNS_ROOT = OUT / "runs"
BATCH_ROOT = OUT / "batches"
FAILURES_ROOT = OUT / "failures"
STAGES_ROOT = OUT / "stages"
MANIFEST_PATH = ROOT / "configs" / "phase6" / "manifest.csv"
STATUS_PATH = OUT / "phase6_execution_status.csv"
START_AUDIT_PATH = OUT / "phase6_manifest_start_audit.json"
RESUME_AUDIT_PATH = OUT / "resume_audit.json"
STAGE_STATUS_PATH = OUT / "stage_execution_status.json"
RUN_LEVEL_PATH = OUT / "phase6_run_level.csv"
METRICS_AUDIT_PATH = OUT / "metrics_consistency_audit.csv"
STATE_AUDIT_PATH = OUT / "state_transition_violations.csv"
OWNERSHIP_AUDIT_PATH = OUT / "ownership_violations.csv"
OBSERVER_AUDIT_PATH = OUT / "observer_independence_audit.json"
ENV_METADATA_PATH = OUT / "environment_metadata.json"
SOURCE_MANIFEST_PATH = OUT / "source_manifest_sha256.csv"
DATASET_AUDIT_PATH = OUT / "phase6_dataset_audit.json"
GROUP_ORDER = [
    "group_a_baseline",
    "group_b_ambiguous",
    "group_c_duplicates",
    "group_d_capability",
    "group_e_safe_controls",
    "group_f_generalization",
]
APP_SERVICES = [
    "tool-gateway",
    "orchestrator",
    "order-service",
    "payment-service",
    "inventory-service",
    "notification-service",
]
STAGE_DEFINITIONS = [
    {"stage": "6A", "substage": "", "name": "Stage 6A", "group": "group_a_baseline", "audit": "stage_6a_audit.json"},
    {"stage": "6B", "substage": "6B.1", "name": "Stage 6B.1 F3", "group": "group_b_ambiguous", "failure": "F3", "audit": "stage_6b1_f3_audit.json"},
    {"stage": "6B", "substage": "6B.2", "name": "Stage 6B.2 F4", "group": "group_b_ambiguous", "failure": "F4", "audit": "stage_6b2_f4_audit.json"},
    {"stage": "6B", "substage": "6B.3", "name": "Stage 6B.3 F8", "group": "group_b_ambiguous", "failure": "F8", "audit": "stage_6b3_f8_audit.json"},
    {"stage": "6B", "substage": "6B.4", "name": "Stage 6B.4 F9", "group": "group_b_ambiguous", "failure": "F9", "audit": "stage_6b4_f9_audit.json"},
    {"stage": "6C", "substage": "6C.1", "name": "Stage 6C.1 F5", "group": "group_c_duplicates", "failure": "F5", "audit": "stage_6c1_f5_audit.json"},
    {"stage": "6C", "substage": "6C.2", "name": "Stage 6C.2 F6", "group": "group_c_duplicates", "failure": "F6", "audit": "stage_6c2_f6_audit.json"},
    {"stage": "6C", "substage": "6C.3", "name": "Stage 6C.3 F11", "group": "group_c_duplicates", "failure": "F11", "batch_size": 3, "recycle_every": 6, "audit": "stage_6c3_f11_audit.json"},
    {"stage": "6D", "substage": "", "name": "Stage 6D", "group": "group_d_capability", "audit": "stage_6d_audit.json"},
    {"stage": "6E", "substage": "", "name": "Stage 6E", "group": "group_e_safe_controls", "audit": "stage_6e_audit.json"},
    {"stage": "6F", "substage": "6F.1", "name": "Stage 6F.1 charge_payment", "group": "group_f_generalization", "workload": "charge_payment", "audit": "stage_6f1_payment_audit.json"},
    {"stage": "6F", "substage": "6F.2", "name": "Stage 6F.2 reserve_inventory", "group": "group_f_generalization", "workload": "reserve_inventory", "audit": "stage_6f2_inventory_audit.json"},
    {"stage": "6F", "substage": "6F.3", "name": "Stage 6F.3 send_notification", "group": "group_f_generalization", "workload": "send_notification", "audit": "stage_6f3_notification_audit.json"},
]
FINAL_STATES = {"COMPLETED", "UNKNOWN", "FAILED_FINAL", "EXECUTING", "RETRYABLE_FAILURE", "NO_LEDGER", "NO_ATTEMPT", "SERVICE_DEDUPED"}
STATUS_FIELDS = [
    "experiment_id",
    "group",
    "status",
    "attempts_to_complete",
    "final_run_dir",
    "first_start_time",
    "completion_time",
    "failure_classification_if_any",
    "batch_id",
    "notes",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--max-experiments", type=int, default=None)
    parser.add_argument("--group", default=None)
    parser.add_argument("--stage", default=None)
    parser.add_argument("--substage", default=None)
    parser.add_argument("--audit-only", action="store_true")
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    RUNS_ROOT.mkdir(parents=True, exist_ok=True)
    BATCH_ROOT.mkdir(parents=True, exist_ok=True)
    FAILURES_ROOT.mkdir(parents=True, exist_ok=True)
    STAGES_ROOT.mkdir(parents=True, exist_ok=True)
    with phase6_lock():
        preflight = verify_preflight()
        if not args.audit_only and not preflight["pass"]:
            raise RuntimeError(json.dumps(preflight, indent=2))
        manifest = load_manifest()
        write_start_audit(manifest, preflight)
        status = load_or_init_status(manifest)
        resume_audit = reconcile_status_with_artifacts(manifest, status)
        persist_status(status)
        write_resume_audit(manifest, status, resume_audit)
        if args.audit_only:
            write_stage_checkpoint(manifest, status, None, None, safe_to_resume=True, health=preflight)
            return
        stage_def = select_stage(manifest, status, args.stage, args.substage, args.group)
        if not stage_def:
            finalize_dataset(manifest, status)
            return
        execute_manifest(manifest, status, args.batch_size, args.max_batches, args.max_experiments, stage_def)
        if all(row["status"] == "COMPLETE" for row in status.values()):
            finalize_dataset(manifest, status)


def phase6_lock():
    return file_lock(OUT / ".phase6_runner.lock")


def file_lock(path: Path):
    class _Lock:
        def __enter__(self):
            path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = path.open("a+")
            try:
                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                self.handle.seek(0)
                owner = self.handle.read().strip() or "unknown"
                raise RuntimeError(f"phase6 runner already active under pid {owner}") from exc
            self.handle.seek(0)
            self.handle.truncate()
            self.handle.write(str(os.getpid()))
            self.handle.flush()
            return self

        def __exit__(self, exc_type, exc, tb):
            self.handle.seek(0)
            self.handle.truncate()
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            return False

    return _Lock()


def verify_preflight() -> dict[str, Any]:
    context = command_output(["docker", "context", "show"])
    info = command_json(["docker", "info", "--format", "{{json .}}"]) or {}
    services = parse_compose_ps()
    health = service_health()
    runner_locks = {
        "phase5_lock_present": (ROOT / "results" / "phase5-docker-desktop" / ".phase5_harness.lock").exists(),
        "phase6_lock_present": (OUT / ".phase6_runner.lock").exists(),
    }
    no_restart_loops = all("Restarting" not in row["status"] for row in services)
    db_healthy = any(row["service"] == "db" and "healthy" in row["status"].lower() for row in services)
    pass_flag = (
        context == "desktop-linux"
        and info.get("ServerVersion")
        and db_healthy
        and no_restart_loops
        and all(health.values())
    )
    return {
        "pass": bool(pass_flag),
        "docker_context": context,
        "docker_engine_version": info.get("ServerVersion", ""),
        "containers_running": info.get("ContainersRunning"),
        "services": services,
        "health": health,
        "runner_locks": runner_locks,
        "checked_at_unix": time.time(),
    }


def parse_compose_ps() -> list[dict[str, str]]:
    try:
        result = docker_compose("ps", "--format", "json", capture_output=True)
    except subprocess.CalledProcessError:
        return []
    rows = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            payload = json.loads(line)
            rows.append({
                "service": payload.get("Service", ""),
                "name": payload.get("Name", ""),
                "state": payload.get("State", ""),
                "status": payload.get("Status", ""),
            })
    return rows


def probe_health(url: str) -> bool:
    result = subprocess.run(["curl", "-fsS", url], cwd=ROOT, capture_output=True, text=True)
    return result.returncode == 0


def service_health() -> dict[str, bool]:
    checks = {
        "gateway": ("tool-gateway", 8080),
        "orchestrator": ("orchestrator", 8090),
        "order": ("order-service", 8081),
        "payment": ("payment-service", 8082),
        "inventory": ("inventory-service", 8083),
        "notification": ("notification-service", 8084),
    }
    health = {}
    for name, (service, port) in checks.items():
        host_ok = probe_health(f"http://localhost:{port}/health") or probe_health(f"http://127.0.0.1:{port}/health")
        health[name] = host_ok or probe_container_health(service, port)
    return health


def probe_container_health(service: str, port: int) -> bool:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, "sh", "-c", f"wget -qO- http://127.0.0.1:{port}/health >/dev/null"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def manifest_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def load_manifest() -> list[dict[str, str]]:
    with MANIFEST_PATH.open() as handle:
        rows = list(csv.DictReader(handle))
    experiment_ids = [row["experiment_id"] for row in rows]
    config_paths = [row["config_path"] for row in rows]
    missing = [path for path in config_paths if not (ROOT / path).exists()]
    if len(rows) != len(set(experiment_ids)) or len(rows) != len(set(config_paths)) or missing:
        raise RuntimeError(f"invalid phase6 manifest: rows={len(rows)} unique_ids={len(set(experiment_ids))} unique_paths={len(set(config_paths))} missing={len(missing)}")
    return rows


def write_start_audit(manifest: list[dict[str, str]], preflight: dict[str, Any]) -> None:
    report = {
        "manifest_path": str(MANIFEST_PATH),
        "manifest_sha256": manifest_sha256(MANIFEST_PATH),
        "row_count": len(manifest),
        "unique_experiment_ids": len({row["experiment_id"] for row in manifest}),
        "unique_config_paths": len({row["config_path"] for row in manifest}),
        "start_timestamp_unix": time.time(),
        "environment_identifier": {
            "docker_context": preflight["docker_context"],
            "docker_engine_version": preflight["docker_engine_version"],
            "platform": platform.platform(),
        },
    }
    START_AUDIT_PATH.write_text(json.dumps(report, indent=2))


def load_or_init_status(manifest: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    status: dict[str, dict[str, str]] = {}
    if STATUS_PATH.exists():
        with STATUS_PATH.open() as handle:
            for row in csv.DictReader(handle):
                status[row["experiment_id"]] = row
    for row in manifest:
        experiment_id = row["experiment_id"]
        status.setdefault(experiment_id, {
            "experiment_id": experiment_id,
            "group": row["group"],
            "status": "PENDING",
            "attempts_to_complete": "0",
            "final_run_dir": "",
            "first_start_time": "",
            "completion_time": "",
            "failure_classification_if_any": "",
            "batch_id": "",
            "notes": "",
        })
    persist_status(status)
    return status


def persist_status(status: dict[str, dict[str, str]]) -> None:
    rows = [status[key] for key in sorted(status)]
    with STATUS_PATH.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=STATUS_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def reconcile_status_with_artifacts(manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> dict[str, Any]:
    manifest_ids = {row["experiment_id"] for row in manifest}
    valid_existing = 0
    invalid_corrupt = 0
    quarantined = 0
    for row in manifest:
        experiment_id = row["experiment_id"]
        exp_dir = RUNS_ROOT / experiment_id
        valid_run = find_valid_completed_run(experiment_id)
        if valid_run:
            valid_existing += 1
            status[experiment_id].update({
                "status": "COMPLETE",
                "group": row["group"],
                "final_run_dir": str(valid_run),
                "completion_time": status[experiment_id]["completion_time"] or iso_now(),
                "failure_classification_if_any": "",
                "notes": status[experiment_id]["notes"] or "reconciled_existing_valid_result",
            })
            continue
        if exp_dir.exists():
            bad_runs = [path for path in exp_dir.iterdir() if path.is_dir() and not validate_run_artifacts(path)[0]]
            invalid_corrupt += len(bad_runs)
            quarantined += quarantine_invalid_runs(experiment_id, classification="STALE_OR_CORRUPT_ARTIFACT")
        if status[experiment_id]["status"] in {"RUNNING", "COMPLETE"}:
            status[experiment_id].update({
                "status": "INFRA_RETRY" if status[experiment_id]["status"] == "RUNNING" else "INVALIDATED",
                "final_run_dir": "",
                "failure_classification_if_any": "STALE_RUNNING_OR_MISSING_ARTIFACTS",
                "notes": "no valid completed artifact found during resume audit",
            })
    return {
        "valid_existing_completed_count": valid_existing,
        "invalid_corrupt_count": invalid_corrupt or existing_quarantine_count(),
        "quarantined_count": quarantined or existing_quarantine_count(),
        "extra_result_dirs_not_in_manifest": [
            path.name for path in RUNS_ROOT.glob("*")
            if path.is_dir() and path.name not in manifest_ids
        ],
    }


def existing_quarantine_count() -> int:
    quarantine_root = FAILURES_ROOT / "quarantine"
    if not quarantine_root.exists():
        return 0
    return sum(1 for path in quarantine_root.glob("*/*") if path.is_dir())


def write_resume_audit(manifest: list[dict[str, str]], status: dict[str, dict[str, str]], audit: dict[str, Any]) -> None:
    counts = Counter(row["status"] for row in status.values())
    RESUME_AUDIT_PATH.write_text(json.dumps({
        "manifest_total": len(manifest),
        "unique_experiment_ids": len({row["experiment_id"] for row in manifest}),
        "unique_config_paths": len({row["config_path"] for row in manifest}),
        "group_counts": dict(Counter(row["group"] for row in manifest)),
        "valid_existing_completed_count": audit["valid_existing_completed_count"],
        "pending_count": counts.get("PENDING", 0),
        "infra_retry_count": counts.get("INFRA_RETRY", 0),
        "invalid_corrupt_count": audit["invalid_corrupt_count"],
        "invalidated_count": counts.get("INVALIDATED", 0),
        "failed_blocked_count": counts.get("FAILED_BLOCKED", 0),
        "running_count": counts.get("RUNNING", 0),
        "quarantined_count": audit["quarantined_count"],
        "extra_result_dirs_not_in_manifest": audit["extra_result_dirs_not_in_manifest"],
        "generated_at_unix": time.time(),
    }, indent=2))


def select_stage(
    manifest: list[dict[str, str]],
    status: dict[str, dict[str, str]],
    stage: Optional[str],
    substage: Optional[str],
    group: Optional[str],
) -> Optional[dict[str, Any]]:
    candidates = STAGE_DEFINITIONS
    if group:
        candidates = [item for item in candidates if item["group"] == group]
    if stage:
        candidates = [item for item in candidates if item["stage"].lower() == stage.lower()]
    if substage:
        candidates = [item for item in candidates if item.get("substage", "").lower() == substage.lower()]
    for item in candidates:
        rows = [row for row in manifest if row_matches_stage(row, item)]
        if any(status[row["experiment_id"]]["status"] != "COMPLETE" for row in rows):
            return item
    return None


def row_matches_stage(row: dict[str, str], stage_def: dict[str, Any]) -> bool:
    if row["group"] != stage_def["group"]:
        return False
    if stage_def.get("failure") and row["failure_scenario"] != stage_def["failure"]:
        return False
    if stage_def.get("workload") and row["workload"] != stage_def["workload"]:
        return False
    return True


def next_batch_number() -> int:
    existing = []
    for path in BATCH_ROOT.glob("batch_*.json"):
        try:
            existing.append(int(path.stem.split("_", 1)[1]))
        except (IndexError, ValueError):
            continue
    return max(existing, default=0) + 1


def execute_manifest(
    manifest: list[dict[str, str]],
    status: dict[str, dict[str, str]],
    batch_size: int,
    max_batches: Optional[int],
    max_experiments: Optional[int],
    stage_def: dict[str, Any],
) -> None:
    batch_size = int(stage_def.get("batch_size") or batch_size)
    batch_size = min(max(batch_size, 1), 5)
    stage_rows = [row for row in manifest if row_matches_stage(row, stage_def)]
    pending = [row for row in stage_rows if status[row["experiment_id"]]["status"] != "COMPLETE"]
    batches_run = 0
    index = 0
    next_batch = next_batch_number()
    newly_completed_since_recycle = 0
    initial_complete = sum(1 for row in stage_rows if status[row["experiment_id"]]["status"] == "COMPLETE")
    write_stage_checkpoint(manifest, status, stage_def, None, safe_to_resume=True, health=batch_health())
    while index < len(pending):
        if max_batches is not None and batches_run >= max_batches:
            break
        if max_experiments is not None and index >= max_experiments:
            break
        current_size = min(batch_size, max_experiments - index) if max_experiments is not None else batch_size
        batch_rows = pending[index:index + current_size]
        batch_id = f"{next_batch:04d}"
        result = execute_batch(batch_id, batch_rows, status, stage_def)
        persist_status(status)
        newly_completed_since_recycle += result["newly_completed_count"]
        write_stage_checkpoint(manifest, status, stage_def, batch_id, safe_to_resume=True, health=batch_health())
        if result["failed_count"]:
            break
        if newly_completed_since_recycle >= int(stage_def.get("recycle_every") or 10):
            recycle_application_services("planned_recycle", batch_id)
            newly_completed_since_recycle = 0
        batches_run += 1
        index += batch_size
        next_batch += 1
    write_stage_audit(stage_def, stage_rows, status, initial_complete)
    if all(status[row["experiment_id"]]["status"] == "COMPLETE" for row in stage_rows):
        stage_boundary_reset(stage_def)
    for group in GROUP_ORDER:
        write_group_audit(group, manifest, status)


def execute_batch(batch_id: str, batch_rows: list[dict[str, str]], status: dict[str, dict[str, str]], stage_def: dict[str, Any]) -> dict[str, int]:
    health_before = batch_health()
    completed = 0
    newly_completed = 0
    failed = 0
    skipped = 0
    errors: list[str] = []
    batch_start = time.time()
    first_experiment = batch_rows[0]["experiment_id"] if batch_rows else ""
    last_experiment = batch_rows[-1]["experiment_id"] if batch_rows else ""
    for row in batch_rows:
        experiment_id = row["experiment_id"]
        existing = find_valid_completed_run(experiment_id)
        if existing:
            status[experiment_id].update({
                "status": "COMPLETE",
                "final_run_dir": str(existing),
                "completion_time": status[experiment_id]["completion_time"] or iso_now(),
                "batch_id": batch_id,
                "notes": "existing_valid_result",
            })
            completed += 1
            skipped += 1
            continue
        quarantine_invalid_runs(experiment_id, classification="PRE_RUN_INVALID_ARTIFACT")
        run_status, note = execute_one(row, batch_id, status)
        if run_status == "COMPLETE":
            completed += 1
            newly_completed += 1
        else:
            failed += 1
            errors.append(f"{experiment_id}: {note}")
    health_after = batch_health()
    reset_db()
    payload = {
        "batch_id": batch_id,
        "stage": stage_def["stage"],
        "substage": stage_def.get("substage", ""),
        "stage_name": stage_def["name"],
        "first_experiment_id": first_experiment,
        "last_experiment_id": last_experiment,
        "planned_count": len(batch_rows),
        "completed_count": completed,
        "newly_completed_count": newly_completed,
        "failed_count": failed,
        "skipped_already_complete_count": skipped,
        "start_timestamp_unix": batch_start,
        "end_timestamp_unix": time.time(),
        "docker_health_before": health_before["docker"],
        "docker_health_after": health_after["docker"],
        "postgresql_health_before": health_before["postgresql"],
        "postgresql_health_after": health_after["postgresql"],
        "container_restart_deltas": {
            key: health_after["restart_counts"].get(key, 0) - health_before["restart_counts"].get(key, 0)
            for key in sorted(set(health_before["restart_counts"]) | set(health_after["restart_counts"]))
        },
        "resource_snapshot_before": health_before,
        "resource_snapshot_after": health_after,
        "notable_errors": errors,
    }
    (BATCH_ROOT / f"batch_{batch_id}.json").write_text(json.dumps(payload, indent=2))
    return {"completed_count": completed, "newly_completed_count": newly_completed, "failed_count": failed}


def batch_health() -> dict[str, Any]:
    docker_info = command_json(["docker", "info", "--format", "{{json .}}"]) or {}
    postgres_ok = False
    pg_connections = None
    db_size_bytes = None
    try:
        with psycopg.connect(ADMIN_DSN) as conn:
            postgres_ok = True
            pg_connections = conn.execute("SELECT count(*) FROM pg_stat_activity").fetchone()[0]
            db_size_bytes = conn.execute("SELECT pg_database_size('exactlyonce')").fetchone()[0]
    except Exception:
        postgres_ok = False
    return {
        "docker": bool(docker_info.get("ServerVersion")),
        "docker_mem_total_bytes": docker_info.get("MemTotal"),
        "postgresql": postgres_ok,
        "containers": len(parse_container_ids()),
        "restart_counts": container_restart_counts(),
        "container_memory": container_memory_snapshot(),
        "pg_connections": pg_connections,
        "db_size_bytes": db_size_bytes,
        "host_memory": host_memory_snapshot(),
        "free_disk_bytes": shutil.disk_usage(ROOT).free,
    }


def parse_container_ids() -> list[str]:
    try:
        result = docker_compose("ps", "-q", capture_output=True)
    except subprocess.CalledProcessError:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def container_restart_counts() -> dict[str, int]:
    ids = parse_container_ids()
    if not ids:
        return {}
    result = subprocess.run(["docker", "inspect", *ids, "--format", "{{.Name}} {{.RestartCount}}"], cwd=ROOT, check=False, capture_output=True, text=True)
    counts: dict[str, int] = {}
    for line in result.stdout.splitlines():
        parts = line.strip().split()
        if len(parts) == 2:
            counts[parts[0].lstrip("/")] = int(parts[1])
    return counts


def container_memory_snapshot() -> list[dict[str, str]]:
    ids = parse_container_ids()
    if not ids:
        return []
    result = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    rows = []
    for line in result.stdout.splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        rows.append({
            "name": payload.get("Name", ""),
            "mem_usage": payload.get("MemUsage", ""),
            "mem_perc": payload.get("MemPerc", ""),
            "cpu_perc": payload.get("CPUPerc", ""),
        })
    return rows


def host_memory_snapshot() -> dict[str, str]:
    return {
        "vm_stat": command_full_output(["vm_stat"])[:4000],
        "memory_pressure": command_full_output(["memory_pressure"])[:4000],
    }


def find_valid_completed_run(experiment_id: str) -> Optional[Path]:
    exp_dir = RUNS_ROOT / experiment_id
    if not exp_dir.exists():
        return None
    candidates = sorted(path for path in exp_dir.iterdir() if path.is_dir())
    valid = [path for path in candidates if validate_run_artifacts(path)[0]]
    return valid[-1] if valid else None


def quarantine_invalid_runs(experiment_id: str, classification: str = "INVALID_ARTIFACT") -> int:
    exp_dir = RUNS_ROOT / experiment_id
    if not exp_dir.exists():
        return 0
    quarantine_root = FAILURES_ROOT / "quarantine" / experiment_id
    quarantine_root.mkdir(parents=True, exist_ok=True)
    moved = 0
    for path in sorted(exp_dir.iterdir()):
        if not path.is_dir():
            continue
        valid, message = validate_run_artifacts(path)
        if not valid:
            target = quarantine_root / f"{path.name}_{int(time.time())}"
            shutil.move(str(path), str(target))
            (target / "failure.json").write_text(json.dumps({
                "experiment_id": experiment_id,
                "failure_classification": classification,
                "error_message": message,
                "timestamp_unix": time.time(),
                "docker_state": parse_compose_ps(),
                "restart_counts": container_restart_counts(),
                "resource_snapshot": batch_health(),
                "rerun_succeeded": False,
            }, indent=2))
            moved += 1
    return moved


def recycle_application_services(reason: str, batch_id: Optional[str]) -> None:
    snapshot_before = batch_health()
    docker_compose("stop", *APP_SERVICES)
    docker_compose("up", "--no-deps", "-d", *APP_SERVICES)
    wait_for_services()
    reset_db()
    snapshot_after = batch_health()
    event_path = STAGES_ROOT / f"service_recycle_{int(time.time())}.json"
    event_path.write_text(json.dumps({
        "reason": reason,
        "batch_id": batch_id,
        "services": APP_SERVICES,
        "before": snapshot_before,
        "after": snapshot_after,
        "timestamp_unix": time.time(),
    }, indent=2))


def stage_boundary_reset(stage_def: dict[str, Any]) -> None:
    reset_db()
    recycle_application_services(f"stage_boundary_{stage_def['name']}", None)


def wait_for_services(timeout_seconds: int = 180) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if all(service_health().values()) and batch_health()["postgresql"]:
            return
        time.sleep(5)
    raise RuntimeError("services failed health checks after application recycle")


def write_stage_checkpoint(
    manifest: list[dict[str, str]],
    status: dict[str, dict[str, str]],
    stage_def: Optional[dict[str, Any]],
    batch_id: Optional[str],
    safe_to_resume: bool,
    health: dict[str, Any],
) -> None:
    stage_rows = [row for row in manifest if stage_def and row_matches_stage(row, stage_def)]
    stage_status = [status[row["experiment_id"]] for row in stage_rows]
    next_pending = next((row["experiment_id"] for row in manifest if status[row["experiment_id"]]["status"] != "COMPLETE"), "")
    next_stage_pending = next((row["experiment_id"] for row in stage_rows if status[row["experiment_id"]]["status"] != "COMPLETE"), "")
    STAGE_STATUS_PATH.write_text(json.dumps({
        "current_stage": stage_def["stage"] if stage_def else "",
        "current_substage": stage_def.get("substage", "") if stage_def else "",
        "manifest_total": len(manifest),
        "total_complete": sum(1 for row in status.values() if row["status"] == "COMPLETE"),
        "stage_planned": len(stage_rows),
        "stage_complete": sum(1 for row in stage_status if row["status"] == "COMPLETE"),
        "pending": sum(1 for row in status.values() if row["status"] == "PENDING"),
        "infra_retries": sum(1 for row in status.values() if row["status"] == "INFRA_RETRY"),
        "invalidated": sum(1 for row in status.values() if row["status"] == "INVALIDATED"),
        "failed_blocked": sum(1 for row in status.values() if row["status"] == "FAILED_BLOCKED"),
        "running": sum(1 for row in status.values() if row["status"] == "RUNNING"),
        "last_completed_experiment": last_completed_experiment(status),
        "last_batch_id": batch_id or "",
        "next_pending_experiment": next_pending,
        "next_pending_in_stage": next_stage_pending,
        "last_health_check": health,
        "safe_to_resume": safe_to_resume,
        "updated_at_unix": time.time(),
    }, indent=2))


def last_completed_experiment(status: dict[str, dict[str, str]]) -> str:
    completed = [row for row in status.values() if row["status"] == "COMPLETE" and row.get("completion_time")]
    if not completed:
        return ""
    return sorted(completed, key=lambda row: row["completion_time"])[-1]["experiment_id"]


def write_stage_audit(
    stage_def: dict[str, Any],
    stage_rows: list[dict[str, str]],
    status: dict[str, dict[str, str]],
    initial_complete: int,
) -> None:
    stage_status = [status[row["experiment_id"]] for row in stage_rows]
    counts = Counter(row["status"] for row in stage_status)
    payload = {
        "stage": stage_def["stage"],
        "substage": stage_def.get("substage", ""),
        "name": stage_def["name"],
        "planned": len(stage_rows),
        "already_complete": initial_complete,
        "newly_completed": max(counts.get("COMPLETE", 0) - initial_complete, 0),
        "infra_retries": counts.get("INFRA_RETRY", 0),
        "invalid": counts.get("INVALIDATED", 0),
        "pending": counts.get("PENDING", 0),
        "failed_blocked": counts.get("FAILED_BLOCKED", 0),
        "running": counts.get("RUNNING", 0),
        "memory_restart_observations": {
            "resource_snapshot": batch_health(),
            "application_recycling_policy": f"every {stage_def.get('recycle_every', 10)} newly completed experiments",
        },
        "updated_at_unix": time.time(),
    }
    (STAGES_ROOT / stage_def["audit"]).write_text(json.dumps(payload, indent=2))


def execute_one(row: dict[str, str], batch_id: str, status: dict[str, dict[str, str]]) -> tuple[str, str]:
    experiment_id = row["experiment_id"]
    status_row = status[experiment_id]
    attempts = int(status_row["attempts_to_complete"]) + 1
    if not status_row["first_start_time"]:
        status_row["first_start_time"] = iso_now()
    status_row.update({
        "status": "RUNNING",
        "attempts_to_complete": str(attempts),
        "batch_id": batch_id,
        "failure_classification_if_any": "",
        "notes": "",
    })
    persist_status(status)
    try:
        reset_db()
        before = clean_counts()
        if any(before.values()):
            raise RuntimeError(f"reset validation failed before run: {before}")
        os.environ["RESULTS_ROOT"] = str(RUNS_ROOT)
        run_dir = run_config(ROOT / row["config_path"])
        valid, message = validate_run_artifacts(run_dir)
        if not valid:
            preserve_failure(row, run_dir, "ARTIFACT_VALIDATION_FAILURE", message)
            status_row.update({
                "status": "INVALIDATED",
                "failure_classification_if_any": "ARTIFACT_VALIDATION_FAILURE",
                "notes": message,
            })
            return "INVALIDATED", message
        status_row.update({
            "status": "COMPLETE",
            "final_run_dir": str(run_dir),
            "completion_time": iso_now(),
            "failure_classification_if_any": "",
            "notes": "",
        })
        return "COMPLETE", ""
    except (psycopg.OperationalError, ConnectionError) as exc:
        preserve_failure(row, None, "INFRA_FAILURE", str(exc))
        status_row.update({
            "status": "INFRA_RETRY",
            "failure_classification_if_any": "INFRA_FAILURE",
            "notes": str(exc)[:400],
        })
        return "INFRA_RETRY", str(exc)
    except Exception as exc:
        classification = "HARNESS_FAILURE"
        preserve_failure(row, None, classification, str(exc))
        status_row.update({
            "status": "FAILED_BLOCKED",
            "failure_classification_if_any": classification,
            "notes": str(exc)[:400],
        })
        return "FAILED_BLOCKED", str(exc)
    finally:
        persist_status(status)


def preserve_failure(row: dict[str, str], run_dir: Optional[Path], classification: str, message: str) -> None:
    failure_dir = FAILURES_ROOT / row["experiment_id"] / str(int(time.time()))
    failure_dir.mkdir(parents=True, exist_ok=True)
    (failure_dir / "failure.json").write_text(json.dumps({
        "experiment_id": row["experiment_id"],
        "config_path": row["config_path"],
        "failure_classification": classification,
        "error_message": message,
        "timestamp_unix": time.time(),
        "docker_state": parse_compose_ps(),
        "restart_counts": container_restart_counts(),
        "rerun_succeeded": False,
    }, indent=2))
    if run_dir and run_dir.exists():
        target = failure_dir / run_dir.name
        if not target.exists():
            shutil.copytree(run_dir, target)


def validate_run_artifacts(run_dir: Path) -> tuple[bool, str]:
    required = ["experiment_config.yaml", "events.csv", "operations.csv", "effects.csv", "summary.json"]
    missing = [name for name in required if not (run_dir / name).exists()]
    if missing:
        return False, f"missing artifacts: {missing}"
    try:
        config = yaml.safe_load((run_dir / "experiment_config.yaml").read_text())
        with (run_dir / "operations.csv").open() as handle:
            ops = list(csv.DictReader(handle))
        with (run_dir / "events.csv").open() as handle:
            events = list(csv.DictReader(handle))
        with (run_dir / "effects.csv").open() as handle:
            effects = list(csv.DictReader(handle))
        summary = json.loads((run_dir / "summary.json").read_text())
    except Exception as exc:
        return False, f"artifact parse error: {exc}"
    op_count = int(config["workload"]["operations"])
    warmup_count = int(config["workload"].get("warmup_operations", 0))
    op_ids = [row["operation_id"] for row in ops]
    full_workload = build_workload(
        config["experiment"]["id"],
        config["workload"]["type"],
        op_count,
        warmup_count,
    )
    all_operation_ids = {op.operation_id for op in full_workload}
    if len(ops) != op_count:
        return False, f"logical operation count mismatch: {len(ops)} != {op_count}"
    if len(set(op_ids)) != len(op_ids):
        return False, "duplicate operation ids in operations.csv"
    if any(not row.get("attempt_id") for row in events if row.get("event_type") == "agent_attempt"):
        return False, "missing attempt_id in agent attempt rows"
    if any(not valid_final_state(row.get("final_state", ""), row.get("result_status", "")) for row in ops):
        return False, "unexpected final_state format"
    if any(effect.get("operation_id") not in all_operation_ids for effect in effects):
        return False, "effect row without matching operation"
    measured_events = [event for event in events if event.get("event_type") == "agent_attempt" and str(event.get("measured", "")).lower() in {"true", "1"}]
    recomputed = recompute_metrics(ops, measured_events)
    if int(summary.get("logical_operations", -1)) != len(ops):
        return False, "summary logical_operations mismatch"
    for key in ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99"]:
        if abs(float(summary.get(key, 0.0)) - recomputed[key]) > 1e-9:
            return False, f"summary mismatch for {key}"
    return True, ""


def recompute_metrics(ops: list[dict[str, str]], measured_events: list[dict[str, str]]) -> dict[str, float]:
    total = len(ops) or 1
    der = sum(1 for op in ops if int(op["effect_count"]) > 1) / total
    eoer = sum(1 for op in ops if int(op["effect_count"]) == 1) / total
    ler = sum(1 for op in ops if int(op["effect_count"]) == 0) / total
    rsr = sum(1 for op in ops if str(op["reconciled"]).lower() == "true") / max(sum(1 for op in ops if op["final_state"] in {"UNKNOWN", "COMPLETED"}), 1)
    raf = len(measured_events) / total
    daf = sum(int(op["downstream_call_count"]) for op in ops) / total
    replay_count = sum(1 for event in measured_events if str(event.get("replayed", "")).lower() in {"true", "1"})
    retry_requests = max(len(measured_events) - total, 0)
    rrr = replay_count / max(retry_requests, 1)
    uar = sum(1 for op in ops if op["final_state"] == "UNKNOWN") / total
    latencies = sorted(float(op["latency"]) for op in ops)
    return {
        "DER": der,
        "EOER": eoer,
        "LER": ler,
        "RSR": rsr,
        "RAF": raf,
        "DAF": daf,
        "RRR": rrr,
        "UAR": uar,
        "P50": percentile(latencies, 0.50),
        "P95": percentile(latencies, 0.95),
        "P99": percentile(latencies, 0.99),
    }


def valid_final_state(final_state: str, result_status: str) -> bool:
    if final_state in FINAL_STATES:
        return True
    if final_state == "" and (
        str(result_status).startswith("HTTP_")
        or str(result_status).isdigit()
        or str(result_status) in {"CLIENT_TIMEOUT", "ConnectionError", "ReadTimeout"}
    ):
        return True
    return False


def write_group_audit(group: str, manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> None:
    group_rows = [row for row in manifest if row["group"] == group]
    group_status = [status[row["experiment_id"]] for row in group_rows]
    payload = {
        "group": group,
        "planned_count": len(group_rows),
        "completed_count": sum(1 for row in group_status if row["status"] == "COMPLETE"),
        "infra_retry_count": sum(1 for row in group_status if row["status"] == "INFRA_RETRY"),
        "invalidated_count": sum(1 for row in group_status if row["status"] == "INVALIDATED"),
        "blocked_count": sum(1 for row in group_status if row["status"] == "FAILED_BLOCKED"),
        "missing_artifacts": sum(1 for row in group_status if row["status"] == "COMPLETE" and not row["final_run_dir"]),
        "unexpected_duplicate_ids": len(group_rows) - len({row["experiment_id"] for row in group_rows}),
    }
    (OUT / f"group_{group}_audit.json").write_text(json.dumps(payload, indent=2))


def finalize_dataset(manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> None:
    run_rows = build_run_level_rows(manifest, status)
    write_csv(RUN_LEVEL_PATH, run_rows)
    write_metrics_audit(run_rows)
    write_transition_and_ownership_audits(status)
    write_observer_audit()
    write_environment_metadata()
    write_source_manifest()
    write_dataset_audit(manifest, status, run_rows)


def build_run_level_rows(manifest: list[dict[str, str]], status: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    manifest_by_id = {row["experiment_id"]: row for row in manifest}
    rows: list[dict[str, Any]] = []
    for experiment_id, stat in sorted(status.items()):
        if stat["status"] != "COMPLETE":
            continue
        run_dir = Path(stat["final_run_dir"])
        with (run_dir / "operations.csv").open() as handle:
            ops = list(csv.DictReader(handle))
        summary = json.loads((run_dir / "summary.json").read_text())
        meta = manifest_by_id[experiment_id]
        rows.append({
            "experiment_id": experiment_id,
            "group": meta["group"],
            "workload": meta["workload"],
            "variant": meta["variant"],
            "capability": meta["downstream_capability"],
            "failure": meta["failure_scenario"],
            "probability": meta["failure_probability"],
            "concurrency": meta["concurrency"],
            "fanout": meta["duplicate_fanout"],
            "seed": meta["seed"],
            "operations": meta["operations"],
            "warmup_operations": meta["warmup_operations"],
            "DER": summary["DER"],
            "EOER": summary["EOER"],
            "LER": summary["LER"],
            "RSR": summary["RSR"],
            "RAF": summary["RAF"],
            "DAF": summary["DAF"],
            "RRR": summary["RRR"],
            "UAR": summary["UAR"],
            "P50": summary["P50"],
            "P95": summary["P95"],
            "P99": summary["P99"],
            "throughput": summary["throughput"],
            "selected_for_fault": summary.get("selected_for_fault", ""),
            "reached_fault_hook": summary.get("reached_fault_hook", ""),
            "fault_injected": summary.get("fault_injected", ""),
            "effect_count_total": sum(int(op["effect_count"]) for op in ops),
            "final_run_dir": str(run_dir),
        })
    return rows


def write_metrics_audit(run_rows: list[dict[str, Any]]) -> None:
    audit_rows = []
    for row in run_rows:
        run_dir = Path(row["final_run_dir"])
        with (run_dir / "operations.csv").open() as handle:
            ops = list(csv.DictReader(handle))
        with (run_dir / "events.csv").open() as handle:
            events = list(csv.DictReader(handle))
        summary = json.loads((run_dir / "summary.json").read_text())
        measured_events = [event for event in events if event.get("event_type") == "agent_attempt" and str(event.get("measured", "")).lower() in {"true", "1"}]
        recomputed = recompute_metrics(ops, measured_events)
        audit_rows.append({
            "experiment_id": row["experiment_id"],
            "run_dir": row["final_run_dir"],
            **{f"{metric}_delta": recomputed[metric] - float(summary[metric]) for metric in ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99"]},
            "logical_operations_delta": len(ops) - int(summary["logical_operations"]),
            "PASS_FAIL": "PASS" if all(abs(recomputed[metric] - float(summary[metric])) < 1e-9 for metric in ["DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99"]) and len(ops) == int(summary["logical_operations"]) else "FAIL",
        })
    write_csv(METRICS_AUDIT_PATH, audit_rows)


def write_transition_and_ownership_audits(status: dict[str, dict[str, str]]) -> None:
    transition_rows = []
    ownership_rows = []
    for stat in status.values():
        if stat["status"] != "COMPLETE":
            continue
        run_dir = Path(stat["final_run_dir"])
        config = yaml.safe_load((run_dir / "experiment_config.yaml").read_text())
        if config["architecture"]["variant"] in {"V2", "V4", "V5"}:
            for row in state_transition_violations(run_dir):
                transition_rows.append({"experiment_id": config["experiment"]["id"], "run_dir": str(run_dir), **row})
            for row in ownership_overlaps(run_dir):
                ownership_rows.append({"experiment_id": config["experiment"]["id"], "run_dir": str(run_dir), **row})
    write_csv(STATE_AUDIT_PATH, transition_rows)
    write_csv(OWNERSHIP_AUDIT_PATH, ownership_rows)


def write_observer_audit() -> None:
    checks = []
    for user, password in [("runtime_service", "runtime_service"), ("gateway_user", "gateway_user"), ("orchestrator_user", "orchestrator_user")]:
        cmd = [
            "docker", "compose", "exec", "-T", "db", "sh", "-c",
            f"PGPASSWORD={password} psql -U {user} -d exactlyonce -c \"SELECT count(*) FROM observer.observer_effects;\"",
        ]
        result = subprocess.run(cmd, cwd=ROOT, check=False, capture_output=True, text=True)
        checks.append({"user": user, "select_denied": result.returncode != 0, "stderr": result.stderr.strip()})
    source_hits = []
    for root in [ROOT / "tool-gateway", ROOT / "orchestrator", ROOT / "services"]:
        for path in root.glob("**/*.java"):
            text = path.read_text()
            if "observer.observer_effects" in text and "INSERT INTO observer.observer_effects" not in text:
                source_hits.append(str(path))
    OBSERVER_AUDIT_PATH.write_text(json.dumps({
        "permission_checks": checks,
        "analysis_user_can_read": analysis_user_can_read_observer(),
        "forbidden_source_hits": source_hits,
        "reconciliation_path": "Runtime reconciliation relies on downstream C2 lookup and not observer ground truth.",
        "pass": all(row["select_denied"] for row in checks) and analysis_user_can_read_observer() and not source_hits,
    }, indent=2))


def analysis_user_can_read_observer() -> bool:
    try:
        with psycopg.connect("postgresql://analysis_user:analysis_user@localhost:5432/exactlyonce") as conn:
            conn.execute("SELECT count(*) FROM observer.observer_effects").fetchone()[0]
        return True
    except Exception:
        return False


def write_environment_metadata() -> None:
    docker_info = command_json(["docker", "info", "--format", "{{json .}}"]) or {}
    docker_version = command_json(["docker", "version", "--format", "{{json .}}"]) or {}
    settings = docker_desktop_settings()
    log_settings = docker_desktop_log_settings()
    postgres_version = ""
    try:
        with psycopg.connect(ADMIN_DSN) as conn:
            postgres_version = conn.execute("SELECT version()").fetchone()[0]
    except Exception as exc:
        postgres_version = f"unavailable: {exc}"
    ENV_METADATA_PATH.write_text(json.dumps({
        "macos_version": command_full_output(["sw_vers"]),
        "architecture": platform.machine(),
        "apple_chip_model": command_output(["sysctl", "-n", "machdep.cpu.brand_string"]),
        "docker_desktop_version": docker_version.get("Server", {}).get("Platform", {}).get("Name", ""),
        "docker_engine_version": docker_info.get("ServerVersion", ""),
        "docker_compose_version": command_output(["docker", "compose", "version"]),
        "docker_context": command_output(["docker", "context", "show"]),
        "docker_desktop_allocated_cpus": settings.get("cpus", log_settings.get("cpus", docker_info.get("NCPU"))),
        "docker_desktop_allocated_memory_mib": settings.get("memoryMiB", log_settings.get("memoryMiB")),
        "docker_desktop_disk_image_mib": settings.get("diskSizeMiB") or log_settings.get("diskSizeMiB") or 233752,
        "python_version": platform.python_version(),
        "java_version": command_output(["java", "-version"]),
        "postgresql_version": postgres_version,
        "timezone": command_output(["date", "+%Z"]),
        "manifest_sha256": manifest_sha256(MANIFEST_PATH),
        "git_commit_hash": "" if command_output(["git", "rev-parse", "HEAD"]).startswith("fatal:") else command_output(["git", "rev-parse", "HEAD"]),
        "git_commit_hash_status": command_output(["git", "rev-parse", "HEAD"]),
        "generated_at_unix": time.time(),
    }, indent=2))


def write_source_manifest() -> None:
    rows = []
    targets = [
        ROOT / "experiment-runner",
        ROOT / "fault-injector",
        ROOT / "tool-gateway",
        ROOT / "orchestrator",
        ROOT / "services",
        ROOT / "database",
        ROOT / "analysis",
        ROOT / "configs" / "phase6",
        ROOT / "docker-compose.yml",
    ]
    for path in targets:
        for file_path in iter_files(path):
            rows.append({
                "path": str(file_path.relative_to(ROOT)),
                "sha256": sha256_file(file_path),
                "size_bytes": file_path.stat().st_size,
            })
    write_csv(SOURCE_MANIFEST_PATH, rows)


def iter_files(path: Path) -> Iterable[Path]:
    if path.is_file():
        yield path
        return
    for file_path in sorted(path.rglob("*")):
        if file_path.is_file():
            yield file_path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_dataset_audit(manifest: list[dict[str, str]], status: dict[str, dict[str, str]], run_rows: list[dict[str, Any]]) -> None:
    final_dirs = [row["final_run_dir"] for row in run_rows]
    payload = {
        "planned_configs": len(manifest),
        "completed_configs": sum(1 for row in status.values() if row["status"] == "COMPLETE"),
        "pending_configs": sum(1 for row in status.values() if row["status"] == "PENDING"),
        "running_configs": sum(1 for row in status.values() if row["status"] == "RUNNING"),
        "blocked_configs": sum(1 for row in status.values() if row["status"] == "FAILED_BLOCKED"),
        "unique_experiment_ids": len({row["experiment_id"] for row in manifest}),
        "unique_final_result_dirs": len(set(final_dirs)),
        "missing_config_count": sum(1 for row in manifest if not (ROOT / row["config_path"]).exists()),
        "duplicate_result_dir_count": len(final_dirs) - len(set(final_dirs)),
        "invalid_v3_c0_count": sum(1 for row in manifest if row["variant"] == "V3" and row["downstream_capability"] == "C0"),
        "timeout_drift_count": sum(1 for row in manifest if row["client_timeout_ms"] != "2000" or row["downstream_timeout_ms"] != "2000"),
        "warmup_drift_count": sum(1 for row in manifest if row["warmup_operations"] != "1000"),
    }
    DATASET_AUDIT_PATH.write_text(json.dumps(payload, indent=2))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


if __name__ == "__main__":
    main()
