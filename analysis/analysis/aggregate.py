import argparse
import csv
import json
from pathlib import Path


FIELDS = ["variant", "failure", "failure_rate", "concurrency", "seed", "DER", "EOER", "LER", "RSR", "RAF", "DAF", "RRR", "UAR", "P50", "P95", "P99", "throughput"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default="results")
    parser.add_argument("--output", default="results/aggregate.csv")
    args = parser.parse_args()
    rows = []
    for summary_path in Path(args.results).glob("*/*/summary.json"):
        summary = json.loads(summary_path.read_text())
        config_path = summary_path.parent / "experiment_config.yaml"
        text = config_path.read_text()
        rows.append({
            "variant": _extract(text, "variant:"),
            "failure": _extract(text, "scenario:"),
            "failure_rate": _extract(text, "probability:"),
            "concurrency": _extract(text, "concurrency:"),
            "seed": _extract(text, "seed:"),
            **{field: summary.get(field, "") for field in FIELDS if field not in {"variant", "failure", "failure_rate", "concurrency", "seed"}},
        })
    with Path(args.output).open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _extract(text: str, key: str) -> str:
    for line in text.splitlines():
        if line.strip().startswith(key):
            return line.split(":", 1)[1].strip()
    return ""


if __name__ == "__main__":
    main()

