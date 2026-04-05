from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


SECURITY_PROTOCOL_PLAINTEXT = "PLAINTEXT"
SECURITY_PROTOCOL_SSL = "SSL"

ALLOWED_RUN_MODES = {"throttled", "max"}
ALLOWED_COMPRESSION_TYPES = {"none", "gzip", "snappy", "lz4", "zstd"}
ALLOWED_SECURITY_PROTOCOLS = {SECURITY_PROTOCOL_PLAINTEXT, SECURITY_PROTOCOL_SSL}


class ConfigError(ValueError):
    pass


def required(props: Dict[str, str], key: str) -> str:
    value = props.get(key)
    if value is None or not value.strip():
        raise ConfigError(f"Missing required property: {key}")
    return value.strip()


def blank_to_none(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = value.strip()
    return value or None


def parse_int(value: str, key: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"Property {key} must be an integer, got: {value}") from exc


@dataclass(frozen=True)
class AppConfig:
    bootstrap_servers: str
    topics: List[str]
    file_path: str
    run_mode: str
    threads_per_topic: int
    eps_per_topic: int
    compression_type: str
    security_protocol: str
    truststore_location: Optional[str]
    keystore_location: Optional[str]
    acks: str
    linger_ms: int
    batch_size: int
    buffer_memory: int

    @property
    def is_ssl(self) -> bool:
        return self.security_protocol == SECURITY_PROTOCOL_SSL

    @property
    def is_mtls(self) -> bool:
        return self.is_ssl and bool(self.keystore_location)

    @classmethod
    def from_properties(cls, props: Dict[str, str]) -> "AppConfig":
        topics: List[str] = []
        seen: set = set()
        for item in required(props, "topics").split(","):
            item = item.strip()
            if item and item not in seen:
                topics.append(item)
                seen.add(item)

        cfg = cls(
            bootstrap_servers=required(props, "bootstrap.servers"),
            topics=topics,
            file_path=required(props, "file.path"),
            threads_per_topic=parse_int(required(props, "threads.per.topic"), "threads.per.topic"),
            run_mode=props.get("run.mode", "throttled").strip().lower(),
            eps_per_topic=parse_int(required(props, "eps.per.topic"), "eps.per.topic"),
            compression_type=props.get("compression.type", "none").strip().lower(),
            security_protocol=props.get("security.protocol", SECURITY_PROTOCOL_PLAINTEXT).strip().upper(),
            truststore_location=blank_to_none(props.get("ssl.truststore.location")),
            keystore_location=blank_to_none(props.get("ssl.keystore.location")),
            acks=props.get("acks", "1").strip(),
            linger_ms=parse_int(props.get("linger.ms", "1").strip(), "linger.ms"),
            batch_size=parse_int(props.get("batch.size", "16384").strip(), "batch.size"),
            buffer_memory=parse_int(props.get("buffer.memory", "67108864").strip(), "buffer.memory"),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.topics:
            raise ConfigError("Property topics must contain at least one topic")
        if self.run_mode not in ALLOWED_RUN_MODES:
            raise ConfigError(
                f"run.mode must be one of {sorted(ALLOWED_RUN_MODES)}, got: {self.run_mode}"
            )
        if self.threads_per_topic <= 0:
            raise ConfigError("threads.per.topic must be > 0")
        if self.eps_per_topic <= 0:
            raise ConfigError("eps.per.topic must be > 0")
        if self.compression_type not in ALLOWED_COMPRESSION_TYPES:
            raise ConfigError(
                f"compression.type must be one of {sorted(ALLOWED_COMPRESSION_TYPES)}, "
                f"got: {self.compression_type}"
            )
        if self.security_protocol not in ALLOWED_SECURITY_PROTOCOLS:
            raise ConfigError(
                f"security.protocol must be one of {sorted(ALLOWED_SECURITY_PROTOCOLS)}, "
                f"got: {self.security_protocol}"
            )

        file_path = Path(self.file_path)
        if not file_path.exists():
            raise ConfigError(f"Input file does not exist: {file_path}")
        if not file_path.is_file():
            raise ConfigError(f"Input file is not a regular file: {file_path}")

        if self.is_ssl:
            if not self.truststore_location:
                raise ConfigError("ssl.truststore.location is required when security.protocol=SSL")
            truststore = Path(self.truststore_location)
            if not truststore.exists() or not truststore.is_file():
                raise ConfigError(f"Truststore does not exist or is not a file: {truststore}")
            if self.keystore_location:
                keystore = Path(self.keystore_location)
                if not keystore.exists() or not keystore.is_file():
                    raise ConfigError(f"Keystore does not exist or is not a file: {keystore}")


def parse_properties(path: Path) -> Dict[str, str]:
    props: Dict[str, str] = {}
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("!"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
        elif ":" in line:
            key, value = line.split(":", 1)
        else:
            raise ConfigError(f"Invalid properties syntax at line {line_no}: {raw}")
        props[key.strip()] = value.strip()
    return props
