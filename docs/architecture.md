# Architecture

The testbed follows:

```text
Agent Simulator -> MCP-style Tool Gateway -> Orchestrator -> Downstream Service -> Business Effect
```

## Responsibilities

- Agent simulator: deterministic workload, stable `operation_id`, changing `attempt_id`, retries, duplicate fanout, and client timeouts.
- Tool gateway: HTTP/JSON MCP-style entry point, canonical request-hash validation, and forwarding to the orchestrator.
- Orchestrator: variant-specific behavior, execution ledger state transitions, leases, downstream dispatch, and V5 reconciliation.
- Downstream services: business effect creation for orders, payments, inventory reservations, and notifications.
- Observer: append-only ground-truth table populated by services after actual effects. Runtime components do not read it.

## Operation Identity

Every protected request carries `operation_id`, `attempt_id`, `tool_name`, `arguments`, and `request_hash`.

`request_hash = SHA256(tool_name + canonical_json(arguments))`.

The gateway rejects mismatched request hashes. Ledger-backed variants reject reuse of an existing `operation_id` with a different hash using `OPERATION_ID_CONFLICT`.

## Ledger State Machine

The ledger lives in `runtime.execution_ledger`. Legal transitions are enforced centrally by `runtime.is_legal_ledger_transition` and by orchestrator updates that require the expected prior state.

Normal path:

```text
RECEIVED -> CLAIMED -> EXECUTING -> EFFECT_CONFIRMED -> COMPLETED
```

Ambiguous path:

```text
EXECUTING -> UNKNOWN -> RECONCILING
```

V5 may resolve `RECONCILING` to `EFFECT_CONFIRMED` or `RETRYABLE_FAILURE`. C0/C1 cannot provide reliable lookup, so unresolved ambiguity remains `UNKNOWN`.

Expired `EXECUTING` leases are moved to `UNKNOWN`, never directly retried.

## Downstream Capability Modes

- C0: no idempotency mapping, every call may create a new effect.
- C1: effect creation and `operation_id` registration occur atomically in one service transaction.
- C2: C1 plus `/lookup/{operation_id}`.

## Variants

- V0: no stable identity protection; gateway/orchestrator ledger is bypassed and downstream C0 execution is used.
- V1: stable `operation_id` is propagated only for correlation.
- V2: durable gateway/orchestrator ledger with replay and duplicate suppression, without V5 reconciliation.
- V3: downstream service idempotency only, requiring C1/C2.
- V4: end-to-end identity, ledger, ownership, hash verification, downstream identity, and replay without reconciliation.
- V5: V4 plus explicit `UNKNOWN -> RECONCILING` recovery for C2.

