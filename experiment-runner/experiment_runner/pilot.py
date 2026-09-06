import argparse
from pathlib import Path

from experiment_runner.run import run_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-dir", required=True)
    args = parser.parse_args()
    for config in sorted(Path(args.config_dir).glob("*.yaml")):
        out = run_config(config)
        print(out)


if __name__ == "__main__":
    main()

