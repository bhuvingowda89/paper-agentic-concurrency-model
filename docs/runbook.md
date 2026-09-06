# Runbook

## Setup

Install Docker, Docker Compose, Python 3.11+, and Java 21 if running services outside containers.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Build and Start

```bash
make build
make up
```

Health endpoints:

- `http://localhost:8080/health`
- `http://localhost:8090/health`
- `http://localhost:8081/health`
- `http://localhost:8082/health`
- `http://localhost:8083/health`
- `http://localhost:8084/health`

## Reset State

```bash
make reset-experiment
```

This truncates ledger, business, idempotency, event, and observer tables. It does not delete `results/`.

## Run One Experiment

```bash
make run CONFIG=configs/pilot/order_v5_c2_f8.yaml
```

## Run Pilot

```bash
make run-pilot
```

Outputs are written under:

```text
results/<experiment_id>/<run_id>/
```

## Aggregate Results

```bash
PYTHONPATH=analysis python3 -m analysis.aggregate --results results --output results/aggregate.csv
```

## Troubleshooting

- If migrations fail after a partial start, run `make down`, remove the Compose volume if necessary, and rebuild.
- If the runner cannot import modules, use the Makefile targets so `PYTHONPATH` is set.
- If observer reads fail from runtime users, that is expected. Use `analysis_user` for post-run reads.

