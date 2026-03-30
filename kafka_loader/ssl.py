"""
Подготовка SSL-материалов для confluent-kafka Producer.

confluent-kafka (librdkafka) принимает:
  - ca_location  : PEM-файл с CA-сертификатами
  - keystore     : PKCS12-файл (.p12) для mTLS

Этот модуль конвертирует JKS → PEM/PKCS12 через keytool (JRE).
Временные файлы живут в RuntimeFiles и удаляются при завершении процесса.
"""
from __future__ import annotations

import atexit
import os
import shutil
import subprocess
import tempfile
import threading
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from .config import AppConfig, ConfigError
from .credentials import SslCredentials


# ---------------------------------------------------------------------------
# Временные файлы
# ---------------------------------------------------------------------------

class RuntimeFiles:
    """Управляет временными директориями; удаляет их при завершении процесса."""

    def __init__(self) -> None:
        self._tempdirs: deque[Path] = deque()
        self._lock = threading.Lock()
        atexit.register(self.cleanup)

    def make_tempdir(self, prefix: str = "kafka-loader-") -> Path:
        path = Path(tempfile.mkdtemp(prefix=prefix))
        with self._lock:
            self._tempdirs.append(path)
        return path

    def cleanup(self) -> None:
        with self._lock:
            while self._tempdirs:
                path = self._tempdirs.pop()
                shutil.rmtree(path, ignore_errors=True)


RUNTIME_FILES = RuntimeFiles()


# ---------------------------------------------------------------------------
# JKS → PEM / PKCS12
# ---------------------------------------------------------------------------

class JksConverter:
    """Конвертирует JKS/PKCS12 хранилища в форматы, понятные librdkafka."""

    def __init__(self) -> None:
        self.keytool_path = shutil.which("keytool")

    def _keytool(self) -> str:
        if not self.keytool_path:
            raise ConfigError(
                "JKS/PKCS12 conversion requires 'keytool', but it was not found in PATH. "
                "Install a JRE/JDK package that provides keytool."
            )
        return self.keytool_path

    def detect_store_type(self, store_path: Path) -> str:
        suffix = store_path.suffix.lower()
        if suffix == ".jks":
            return "JKS"
        if suffix in {".p12", ".pfx", ".pkcs12"}:
            return "PKCS12"
        return "JKS"

    def convert_truststore_to_ca_pem(self, store_path: Path, store_password: str) -> Path:
        aliases = self._list_aliases(store_path, store_password)
        if not aliases:
            raise ConfigError(f"No certificate aliases found in truststore: {store_path}")

        tempdir = RUNTIME_FILES.make_tempdir()
        output_path = tempdir / "ca.pem"
        with output_path.open("w", encoding="utf-8") as out:
            for alias in aliases:
                result = self._run([
                    self._keytool(), "-exportcert", "-rfc",
                    "-alias", alias,
                    "-keystore", str(store_path),
                    "-storepass", store_password,
                ])
                out.write(result.stdout)
                if not result.stdout.endswith("\n"):
                    out.write("\n")
        os.chmod(output_path, 0o600)
        return output_path

    def convert_keystore_to_pkcs12(
        self,
        store_path: Path,
        store_password: str,
        key_password: Optional[str],
    ) -> Path:
        tempdir = RUNTIME_FILES.make_tempdir()
        output_path = tempdir / "client.p12"
        store_type = self.detect_store_type(store_path)

        cmd = [
            self._keytool(), "-importkeystore",
            "-srckeystore", str(store_path),
            "-srcstorepass", store_password,
            "-srcstoretype", store_type,
            "-destkeystore", str(output_path),
            "-deststoretype", "PKCS12",
            "-deststorepass", store_password,
            "-noprompt",
        ]

        src_alias = self._find_single_private_key_alias(store_path, store_password, store_type)
        if src_alias:
            cmd.extend(["-srcalias", src_alias])
            if key_password:
                cmd.extend(["-srckeypass", key_password])

        self._run(cmd)
        os.chmod(output_path, 0o600)
        return output_path

    def _list_aliases(self, store_path: Path, store_password: str) -> List[str]:
        result = self._run([
            self._keytool(), "-list",
            "-keystore", str(store_path),
            "-storepass", store_password,
        ])
        aliases: List[str] = []
        for line in result.stdout.splitlines():
            if "," not in line:
                continue
            alias = line.split(",", 1)[0].strip()
            if alias and alias.lower() not in {
                "keystore type", "keystore provider", "your keystore contains"
            }:
                aliases.append(alias)
        return aliases

    def _find_single_private_key_alias(
        self, store_path: Path, store_password: str, store_type: str
    ) -> Optional[str]:
        result = self._run([
            self._keytool(), "-list", "-v",
            "-keystore", str(store_path),
            "-storepass", store_password,
            "-storetype", store_type,
        ])

        aliases: List[str] = []
        current_alias: Optional[str] = None
        current_is_private = False

        for raw_line in result.stdout.splitlines():
            line = raw_line.strip()
            if line.startswith("Alias name:"):
                if current_alias and current_is_private:
                    aliases.append(current_alias)
                current_alias = line.split(":", 1)[1].strip()
                current_is_private = False
                continue
            if "Entry type:" in line and "PrivateKeyEntry" in line:
                current_is_private = True

        if current_alias and current_is_private:
            aliases.append(current_alias)

        return aliases[0] if len(aliases) == 1 else None

    @staticmethod
    def _run(cmd: List[str]) -> subprocess.CompletedProcess[str]:
        # Маскируем пароли в логе ошибки — ищем аргументы после известных флагов
        _PASSWORD_FLAGS = {"-storepass", "-keypass", "-srcstorepass", "-deststorepass", "-srckeypass"}
        safe_cmd: List[str] = []
        skip_next = False
        for token in cmd:
            if skip_next:
                safe_cmd.append("***")
                skip_next = False
            elif token in _PASSWORD_FLAGS:
                safe_cmd.append(token)
                skip_next = True
            else:
                safe_cmd.append(token)

        result = subprocess.run(cmd, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            raise ConfigError(
                f"Command failed ({result.returncode}): {' '.join(safe_cmd)}\n"
                f"stdout: {result.stdout.strip()}\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result


# ---------------------------------------------------------------------------
# Высокоуровневая подготовка SSL
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PreparedSslConfig:
    ca_location: str
    keystore_location: Optional[str] = None
    keystore_password: Optional[str] = None


class SslMaterialPreparer:
    """Готовит CA PEM и клиентский PKCS12 из truststore/keystore."""

    def __init__(self) -> None:
        self.converter = JksConverter()

    def prepare(self, config: AppConfig, credentials: SslCredentials) -> PreparedSslConfig:
        ca_location = self._prepare_ca(
            Path(config.truststore_location or ""),
            credentials.truststore_password,
        )
        if not config.is_mtls:
            return PreparedSslConfig(ca_location=str(ca_location))

        pkcs12_path = self._prepare_keystore(
            Path(config.keystore_location or ""),
            credentials.keystore_password or "",
            credentials.key_password,
        )
        return PreparedSslConfig(
            ca_location=str(ca_location),
            keystore_location=str(pkcs12_path),
            keystore_password=credentials.keystore_password,
        )

    def _prepare_ca(self, path: Path, password: str) -> Path:
        suffix = path.suffix.lower()
        if suffix in {".pem", ".crt", ".cer"}:
            return path
        if suffix in {".jks", ".p12", ".pfx", ".pkcs12"}:
            return self.converter.convert_truststore_to_ca_pem(path, password)
        raise ConfigError(
            f"Unsupported truststore format: {path}. "
            "Supported: PEM/CRT/CER, JKS, PKCS12/PFX."
        )

    def _prepare_keystore(self, path: Path, password: str, key_password: Optional[str]) -> Path:
        suffix = path.suffix.lower()
        if suffix in {".p12", ".pfx", ".pkcs12"}:
            return path
        if suffix == ".jks":
            return self.converter.convert_keystore_to_pkcs12(path, password, key_password)
        raise ConfigError(
            f"Unsupported keystore format: {path}. "
            "Supported for mTLS: JKS or PKCS12/PFX."
        )
