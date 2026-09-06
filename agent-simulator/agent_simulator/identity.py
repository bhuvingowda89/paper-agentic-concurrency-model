import hashlib
import json
from typing import Any


def canonical_arguments(arguments: dict[str, Any]) -> str:
    return json.dumps(arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def request_hash(tool_name: str, arguments: dict[str, Any]) -> str:
    return hashlib.sha256((tool_name + canonical_arguments(arguments)).encode("utf-8")).hexdigest()


def operation_id(experiment_id: str, index: int) -> str:
    return f"{experiment_id}_OP_{index:08d}"


def attempt_id(operation_id_value: str, attempt: int) -> str:
    return f"{operation_id_value}_ATTEMPT_{attempt:02d}"

