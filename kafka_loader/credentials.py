"""
Загрузка credentials из CREDENTIALS_DIRECTORY (systemd LoadCredential=).

Структура директории для SSL:
    $CREDENTIALS_DIRECTORY/
        truststore_password
        keystore_password   (только для mTLS)
        key_password        (только для mTLS)

Будущее расширение для SASL (PLAIN / SCRAM):
    $CREDENTIALS_DIRECTORY/
        sasl_username
        sasl_password
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from .config import AppConfig, ConfigError

if TYPE_CHECKING:
    pass

CRED_TRUSTSTORE_PASSWORD = "truststore_password"
CRED_KEYSTORE_PASSWORD = "keystore_password"
CRED_KEY_PASSWORD = "key_password"


@dataclass(frozen=True)
class SslCredentials:
    truststore_password: str
    keystore_password: Optional[str] = None
    key_password: Optional[str] = None


def _credentials_dir() -> Path:
    raw = os.environ.get("CREDENTIALS_DIRECTORY", "").strip()
    if not raw:
        raise ConfigError(
            "SSL is enabled, but CREDENTIALS_DIRECTORY is not set. "
            "Start the service with systemd LoadCredential= configured."
        )
    path = Path(raw)
    if not path.exists() or not path.is_dir():
        raise ConfigError(
            f"CREDENTIALS_DIRECTORY does not exist or is not a directory: {raw}"
        )
    return path


def read_credential(file_name: str) -> str:
    """Читает одно credential-значение из CREDENTIALS_DIRECTORY."""
    cred_dir = _credentials_dir()
    cred_file = cred_dir / file_name
    if not cred_file.exists():
        raise ConfigError(f"Missing required credential file: {cred_file}")
    if not cred_file.is_file():
        raise ConfigError(f"Credential path is not a regular file: {cred_file}")
    value = cred_file.read_text(encoding="utf-8").strip()
    if not value:
        raise ConfigError(f"Credential file is empty: {cred_file}")
    return value


def load_ssl_credentials(config: AppConfig) -> SslCredentials:
    """Загружает SSL-пароли из CREDENTIALS_DIRECTORY."""
    truststore_password = read_credential(CRED_TRUSTSTORE_PASSWORD)
    if config.is_mtls:
        return SslCredentials(
            truststore_password=truststore_password,
            keystore_password=read_credential(CRED_KEYSTORE_PASSWORD),
            key_password=read_credential(CRED_KEY_PASSWORD),
        )
    return SslCredentials(truststore_password=truststore_password)
