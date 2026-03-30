"""
Точка входа: python -m kafka_loader config.properties
"""
from __future__ import annotations

import sys
from pathlib import Path

from kafka_loader.config import AppConfig, ConfigError, parse_properties
from kafka_loader.loader import KafkaLoadGenerator, load_jsonl_lines


def main(argv: list) -> int:
    if len(argv) != 2:
        print(f"Usage: {argv[0]} <config.properties>", file=sys.stderr)
        return 1

    config_path = Path(argv[1])
    if not config_path.exists():
        print(f"Configuration error: Config file does not exist: {config_path}", file=sys.stderr)
        return 2

    props = parse_properties(config_path)
    config = AppConfig.from_properties(props)
    lines = load_jsonl_lines(config.file_path)

    generator = KafkaLoadGenerator(config, lines)
    generator.start()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(2)
