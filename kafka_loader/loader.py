from __future__ import annotations

import signal
import sys
import threading
import time
from typing import Dict, List, Optional

try:
    from confluent_kafka import Producer
except ImportError as exc:
    raise SystemExit(
        "Missing dependency: confluent-kafka. Install it with 'pip install confluent-kafka'."
    ) from exc

from .config import AppConfig, ConfigError
from .credentials import load_ssl_credentials
from .ssl import PreparedSslConfig, SslMaterialPreparer
from .utils import AtomicCounter, load_jsonl_lines  # noqa: F401 (re-export)

DEFAULT_STATS_PERIOD_SECONDS = 5


class KafkaLoadGenerator:
    def __init__(self, config: AppConfig, lines: List[str]) -> None:
        self.config = config
        self.lines = lines
        self.total_threads = len(config.topics) * config.threads_per_topic
        self.expected_total_eps = len(config.topics) * config.eps_per_topic

        self.running = threading.Event()
        self.running.set()

        self.start_event = threading.Event()
        self._shutdown_lock = threading.Lock()

        self.sent_ok = AtomicCounter()
        self.sent_failed = AtomicCounter()
        self._last_ok = 0
        self._last_failed = 0

        self.threads: List[threading.Thread] = []
        self.poll_thread: Optional[threading.Thread] = None
        self.stats_thread: Optional[threading.Thread] = None
        self.producer: Optional[Producer] = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self) -> None:
        ssl_config: Optional[PreparedSslConfig] = None
        if self.config.is_ssl:
            credentials = load_ssl_credentials(self.config)
            ssl_config = SslMaterialPreparer().prepare(self.config, credentials)

        self.producer = Producer(self._build_producer_config(ssl_config))

        self._install_signal_handlers()
        self._print_startup_info(ssl_config)
        self._start_poller()
        self._start_stats()
        self._start_workers()

        for thread in self.threads:
            thread.join()

        self.shutdown()

    def shutdown(self) -> None:
        with self._shutdown_lock:
            if not self.running.is_set():
                return
            self.running.clear()

        print("Shutdown requested...", flush=True)

        current = threading.current_thread()

        for thread in self.threads:
            if thread is not current:
                thread.join(timeout=2)

        if self.stats_thread and self.stats_thread is not current:
            self.stats_thread.join(timeout=2)

        if self.poll_thread and self.poll_thread is not current:
            self.poll_thread.join(timeout=2)

        if self.producer is not None:
            try:
                self.producer.flush(5.0)
            except Exception as exc:
                print(f"Producer flush error during shutdown: {exc}", file=sys.stderr, flush=True)

        print("Shutdown complete.", flush=True)

    # ------------------------------------------------------------------
    # Internal methods
    # ------------------------------------------------------------------

    def _build_producer_config(self, ssl_config: Optional[PreparedSslConfig]) -> Dict[str, object]:
        conf: Dict[str, object] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "acks": self.config.acks,
            "linger.ms": self.config.linger_ms,
            "batch.size": self.config.batch_size,
            "queue.buffering.max.kbytes": max(1, self.config.buffer_memory // 1024),
            "compression.type": self.config.compression_type,
            "security.protocol": self.config.security_protocol,
            "delivery.report.only.error": True,
        }

        if self.config.is_ssl:
            if ssl_config is None:
                raise ConfigError("SSL config was expected but not prepared")
            conf["ssl.ca.location"] = ssl_config.ca_location
            if ssl_config.keystore_location:
                conf["ssl.keystore.location"] = ssl_config.keystore_location
                conf["ssl.keystore.password"] = ssl_config.keystore_password

        return conf

    def _install_signal_handlers(self) -> None:
        def handle_signal(signum: int, _frame: object) -> None:
            print(f"Received signal {signum}", flush=True)
            self.shutdown()

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

    def _print_startup_info(self, ssl_config: Optional[PreparedSslConfig]) -> None:
        lines = [
            "Config loaded successfully",
            f"Bootstrap servers : {self.config.bootstrap_servers}",
            f"Topics            : {self.config.topics}",
            f"File path         : {self.config.file_path}",
            f"Security protocol : {self.config.security_protocol}",
            f"Compression       : {self.config.compression_type}",
            f"Run mode          : {self.config.run_mode}",
            f"Threads per topic : {self.config.threads_per_topic}",
            f"EPS per topic     : {self.config.eps_per_topic}",
            f"Expected total EPS: {self.expected_total_eps}",
            f"Total threads     : {self.total_threads}",
            f"Loaded lines      : {len(self.lines)}",
        ]

        if ssl_config:
            lines.append(f"CA material       : {ssl_config.ca_location}")
            if ssl_config.keystore_location:
                lines.append(f"Client keystore   : {ssl_config.keystore_location}")
                lines.append("SSL mode          : mTLS")
            else:
                lines.append("SSL mode          : server authentication only")

        print("\n".join(lines), flush=True)

    # ------------------------------------------------------------------
    # Threads
    # ------------------------------------------------------------------

    def _start_workers(self) -> None:
        if self.producer is None:
            raise ConfigError("Producer was not initialized")

        for topic in self.config.topics:
            cursor = AtomicCounter()
            eps_per_thread = self.config.eps_per_topic / self.config.threads_per_topic

            msg = (
                f"Starting workers: topic={topic}, "
                f"threads={self.config.threads_per_topic}, "
                f"eps/thread≈{eps_per_thread:.2f}"
            )

            if self.config.run_mode == "throttled":
                target_messages_per_window = 100
                window_seconds = target_messages_per_window / eps_per_thread
                window_seconds = min(max(window_seconds, 0.02), 0.2)
                messages_per_window = max(1, round(eps_per_thread * window_seconds))
                msg += (
                    f", window≈{window_seconds * 1000:.1f}ms"
                    f", msgs/window≈{messages_per_window}"
                )

            print(msg, flush=True)

            for index in range(self.config.threads_per_topic):
                t = threading.Thread(
                    target=self._worker_loop,
                    name=f"worker-{topic}-{index}",
                    args=(topic, cursor, eps_per_thread),
                    daemon=True,
                )
                self.threads.append(t)

        for t in self.threads:
            t.start()

        self.start_event.set()
        print("All workers started simultaneously.", flush=True)

    def _worker_loop(self, topic: str, cursor: AtomicCounter, eps_per_thread: float) -> None:
        while self.running.is_set() and not self.start_event.wait(timeout=0.1):
            pass

        if not self.running.is_set():
            return

        if self.producer is None:
            return

        if self.config.run_mode == "throttled":
            target_messages_per_window = 100
            window_seconds = target_messages_per_window / eps_per_thread
            window_seconds = min(max(window_seconds, 0.02), 0.2)
            messages_per_window = max(1, round(eps_per_thread * window_seconds))
            next_window_at = time.monotonic() + window_seconds
        else:
            window_seconds = 0.0
            messages_per_window = 1
            next_window_at = 0.0

        def delivery_report(err: object, _msg: object) -> None:
            if err is None:
                return

            self.sent_failed.increment()
            if self.running.is_set():
                print(f"Delivery failed topic={topic}: {err}", file=sys.stderr, flush=True)

        while self.running.is_set():
            try:
                if self.config.run_mode == "throttled":
                    now = time.monotonic()
                    sleep_for = next_window_at - now
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                    else:
                        # Если поток отстал, не копим бесконечный "долг" по окнам.
                        # Переставляем следующее окно относительно текущего времени.
                        next_window_at = now

                    batch_count = messages_per_window
                    next_window_at += window_seconds
                else:
                    batch_count = 1

                for _ in range(batch_count):
                    if not self.running.is_set():
                        break

                    idx = (cursor.increment() - 1) % len(self.lines)
                    self.producer.produce(
                        topic=topic,
                        value=self.lines[idx].encode("utf-8"),
                        on_delivery=delivery_report,
                    )
                    self.sent_ok.increment()

            except BufferError:
                # Очередь producer-а временно заполнена: даём librdkafka обработать события.
                if self.producer is not None:
                    self.producer.poll(0.1)

            except Exception as exc:
                self.sent_failed.increment()
                if self.running.is_set():
                    print(f"Worker error topic={topic}: {exc}", file=sys.stderr, flush=True)

    def _start_poller(self) -> None:
        if self.producer is None:
            raise ConfigError("Producer was not initialized")

        def poll_loop() -> None:
            while self.running.is_set():
                try:
                    self.producer.poll(0.2)  # type: ignore[union-attr]
                except Exception as exc:
                    if self.running.is_set():
                        print(f"Producer poll error: {exc}", file=sys.stderr, flush=True)

            try:
                self.producer.poll(0)  # type: ignore[union-attr]
            except Exception:
                pass

        self.poll_thread = threading.Thread(target=poll_loop, name="producer-poller", daemon=True)
        self.poll_thread.start()

    def _start_stats(self) -> None:
        def stats_loop() -> None:
            while self.running.is_set():
                time.sleep(DEFAULT_STATS_PERIOD_SECONDS)
                if not self.running.is_set():
                    break

                ok_total = self.sent_ok.get()
                failed_total = self.sent_failed.get()

                ok_delta = ok_total - self._last_ok
                failed_delta = failed_total - self._last_failed

                self._last_ok = ok_total
                self._last_failed = failed_total

                ok_eps = ok_delta / DEFAULT_STATS_PERIOD_SECONDS
                failed_eps = failed_delta / DEFAULT_STATS_PERIOD_SECONDS

                print(
                    f"Stats: expected_total_eps={self.expected_total_eps}, "
                    f"actual_ok_eps={ok_eps:.2f}, "
                    f"actual_failed_eps={failed_eps:.2f}, "
                    f"sent_ok_total={ok_total}, "
                    f"sent_failed_total={failed_total}",
                    flush=True,
                )

        self.stats_thread = threading.Thread(target=stats_loop, name="stats", daemon=True)
        self.stats_thread.start()
