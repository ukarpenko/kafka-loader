from __future__ import annotations

import threading
from pathlib import Path
from typing import List

from .config import ConfigError


class AtomicCounter:

    def __init__(self) -> None:
        self._value = 0
        self._lock = threading.Lock()

    def increment(self, delta: int = 1) -> int:
        with self._lock:
            self._value += delta
            return self._value

    def get(self) -> int:
        with self._lock:
            return self._value


def load_jsonl_lines(file_path: str) -> List[str]:
    """Загружает непустые строки JSONL-файла в память."""
    lines = [
        line.strip()
        for line in Path(file_path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not lines:
        raise ConfigError(f"Input file is empty: {file_path}")
    return lines
