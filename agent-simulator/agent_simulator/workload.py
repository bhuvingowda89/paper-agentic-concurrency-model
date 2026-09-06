from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LogicalOperation:
    index: int
    operation_id: str
    tool_name: str
    arguments: dict[str, Any]
    measured: bool


def build_workload(experiment_id: str, workload_type: str, operations: int, warmup: int) -> list[LogicalOperation]:
    from agent_simulator.identity import operation_id

    total = operations + warmup
    rows: list[LogicalOperation] = []
    for index in range(total):
        measured = index >= warmup
        op_id = operation_id(experiment_id, index)
        if workload_type == "create_order":
            args = {"customer_id": f"cust-{index % 100}", "product_id": f"sku-{index % 50}", "quantity": (index % 5) + 1}
            tool = "create_order"
        elif workload_type == "charge_payment":
            args = {"customer_id": f"cust-{index % 100}", "amount": f"{10 + (index % 90)}.00"}
            tool = "charge_payment"
        elif workload_type == "reserve_inventory":
            args = {"product_id": f"sku-{index % 50}", "quantity": (index % 5) + 1}
            tool = "reserve_inventory"
        elif workload_type == "send_notification":
            args = {"customer_id": f"cust-{index % 100}", "template": f"template-{index % 4}"}
            tool = "send_notification"
        else:
            raise ValueError(f"unknown workload type: {workload_type}")
        rows.append(LogicalOperation(index, op_id, tool, args, measured))
    return rows

