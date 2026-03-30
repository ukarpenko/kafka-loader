"""
Примитивы без зависимостей от Kafka.
"""
from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import List

from .config import ConfigError


class AtomicCounter:
    """Потокобезопасный счётчик."""

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


class RateLimiter:
    """
    Простой token-bucket с активным ожиданием.

    max_sleep ограничивает длину одного sleep, чтобы избежать
    накопленных бурстов после долгого ожидания (например, пробуждение
    после 200 мс → N накопившихся «разрешений» → пачка вместо ровного потока).
    """

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be > 0")
        self.interval = 1.0 / rate_per_second
        # Не спать дольше 10 интервалов (но не более 50 мс),
        # чтобы не накапливать «долг» и не давать бурстов.
        self.max_sleep = min(self.interval * 10, 0.05)
        self.next_at = time.monotonic()

    def acquire(self, running: threading.Event) -> bool:
        """Ждёт следующего «разрешения». Возвращает False если running сброшен."""
        while running.is_set():
            now = time.monotonic()
            if now >= self.next_at:
                # Не даём накапливать «долг» больше одного интервала назад
                self.next_at = max(self.next_at + self.interval, now - self.interval)
                return True
            sleep_for = min(self.next_at - now, self.max_sleep)
            if sleep_for > 0:
                time.sleep(sleep_for)
        return False


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
