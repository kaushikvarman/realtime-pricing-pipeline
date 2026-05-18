"""
  Idempotent Kafka topic creator.

  Reads topics.yaml and reconciles the broker so each declared topic exists with
  the desired partition count, cleanup policy, and retention.

  Usage:
      python scripts/create-topics.py              # apply
      python scripts/create-topics.py --dry-run    # show actions without applying

  Exit codes:
      0   all topics in desired state
      1   one or more topics failed to create
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

CONTAINER = "kafka"
BOOTSTRAP = "localhost:9092"


def docker_exec(cmd: list[str]) -> subprocess.CompletedProcess:
    """Run a CLI command inside the running kafka container."""
    return subprocess.run(
        ["docker", "exec", CONTAINER, *cmd],
        capture_output=True,
        text=True,
        check=True,
    )


def list_existing_topics() -> set[str]:
    result = docker_exec(["kafka-topics", "--bootstrap-server", BOOTSTRAP, "--list"])
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def create_topic(topic: dict, dry_run: bool) -> bool:
    name = topic["name"]
    cmd = [
        "kafka-topics",
        "--bootstrap-server",
        BOOTSTRAP,
        "--create",
        "--topic",
        name,
        "--partitions",
        str(topic["partitions"]),
        "--replication-factor",
        str(topic["replication"]),
        "--config",
        f"cleanup.policy={topic['cleanup_policy']}",
        "--config",
        f"retention.ms={topic['retention_ms']}",
    ]
    if dry_run:
        print(
            f"  [dry-run] would create: {name} "
            f"(partitions={topic['partitions']}, "
            f"cleanup={topic['cleanup_policy']}, "
            f"retention_ms={topic['retention_ms']})"
        )
        return True
    try:
        docker_exec(cmd)
        print(f"  + created: {name}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ! failed:  {name}\n    {e.stderr.strip()}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run", action="store_true", help="Print actions without contacting the broker."
    )
    parser.add_argument(
        "--config", default="topics.yaml", help="Path to topics.yaml (default: ./topics.yaml)"
    )
    args = parser.parse_args()

    spec = yaml.safe_load(Path(args.config).read_text())
    desired = spec["topics"]

    if args.dry_run:
        print("[dry-run mode — broker not contacted]")
        existing: set[str] = set()
    else:
        existing = list_existing_topics()
        print(f"Found {len(existing)} existing topic(s) on broker.")

    failed = 0
    for topic in desired:
        name = topic["name"]
        if name in existing:
            print(f"  = skip:    {name} (already exists)")
            continue
        if not create_topic(topic, args.dry_run):
            failed += 1

    print()
    print(f"Done. {len(desired)} declared, {failed} failed.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
