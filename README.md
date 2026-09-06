# End-to-End Exactly-Once Effects Testbed

Research testbed for evaluating idempotency and recovery mechanisms for LLM-style agent tool execution.

## Quick Start

```bash
make build
make up
make reset-experiment
make run-pilot
```

Run a single config:

```bash
make run CONFIG=configs/pilot/order_v5_c2_f8.yaml
```

Outputs are written under `results/<experiment_id>/<run_id>/`:

- `experiment_config.yaml`
- `events.csv`
- `operations.csv`
- `effects.csv`
- `summary.json`

## Components

- `agent-simulator`: deterministic Python agent and retry simulator.
- `tool-gateway`: MCP-style Spring Boot gateway.
- `orchestrator`: Spring Boot execution coordinator and reconciler.
- `services/*`: Spring Boot business services for orders, payments, inventory, and notifications.
- `fault-injector`: deterministic named failure hook utilities.
- `experiment-runner`: YAML-driven runner and raw output collector.
- `analysis`: aggregate metric generation.
- `database/migrations`: explicit PostgreSQL schema.

See `docs/runbook.md` for full commands.

