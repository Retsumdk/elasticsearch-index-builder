"""Layered configuration: defaults < JSON file < environment."""

from __future__ import annotations

import base64
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from .errors import ConfigError

__all__ = ["ENV_PREFIX", "Config"]

ENV_PREFIX = "ESIB_"

_INT_FIELDS = frozenset({"batch_size", "batch_bytes", "max_attempts"})


@dataclass(frozen=True, slots=True)
class Config:
    """Connection and batching settings.

    Every field can be overridden from the environment by upper-casing its name
    and prefixing ``ESIB_`` (``ESIB_BASE_URL``, ``ESIB_BATCH_SIZE``, ...). A JSON
    file supplies the same keys; the environment always wins over the file.
    """

    base_url: str = "http://localhost:9200"
    api_key: str | None = None
    username: str | None = None
    password: str | None = None
    timeout: float = 30.0
    batch_size: int = 500
    batch_bytes: int = 5 * 1024 * 1024
    max_attempts: int = 4
    index_prefix: str = ""
    ca_bundle: str | None = None
    user_agent: str = "elasticsearch-index-builder/1.0"

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        parts = urlsplit(self.base_url)
        if parts.scheme not in ("http", "https"):
            raise ConfigError(
                f"base_url must be http:// or https:// (got {self.base_url!r})",
                field="base_url",
            )
        if not parts.hostname:
            raise ConfigError(f"base_url has no host (got {self.base_url!r})", field="base_url")
        if parts.query or parts.fragment:
            raise ConfigError(
                "base_url must not carry a query string or fragment", field="base_url"
            )
        if self.batch_size < 1:
            raise ConfigError("batch_size must be >= 1", field="batch_size")
        if self.batch_bytes < 1024:
            raise ConfigError("batch_bytes must be >= 1024", field="batch_bytes")
        if self.timeout <= 0:
            raise ConfigError("timeout must be > 0", field="timeout")
        if self.max_attempts < 1:
            raise ConfigError("max_attempts must be >= 1", field="max_attempts")
        if self.api_key and (self.username or self.password):
            raise ConfigError("set either api_key or username/password, not both", field="api_key")
        if bool(self.username) != bool(self.password):
            raise ConfigError("username and password must be set together", field="username")

    @property
    def auth_header(self) -> str | None:
        """Authorization header value, or ``None`` for an unauthenticated cluster."""

        if self.api_key:
            key = self.api_key.strip()
            if key[:7].lower() == "apikey ":
                return f"ApiKey {key[7:].strip()}"
            if key[:7].lower() == "bearer ":
                return f"Bearer {key[7:].strip()}"
            return f"ApiKey {key}"
        if self.username and self.password:
            raw = f"{self.username}:{self.password}".encode()
            return "Basic " + base64.b64encode(raw).decode("ascii")
        return None

    def with_changes(self, **changes: Any) -> Config:
        """Return a copy with validated overrides (unknown keys are rejected)."""

        known = {f.name for f in fields(self)}
        unknown = set(changes) - known
        if unknown:
            raise ConfigError(f"unknown config keys: {', '.join(sorted(unknown))}")
        return replace(self, **{k: v for k, v in changes.items() if v is not None})

    @classmethod
    def from_file(cls, path: str | Path) -> Config:
        """Load a JSON config file. Unknown keys are an error, not a silent no-op."""

        file = Path(path)
        if not file.exists():
            raise ConfigError(f"config file not found: {file}", field=str(file))
        try:
            raw = json.loads(file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigError(f"{file} is not valid JSON: {exc}", field=str(file)) from exc
        if not isinstance(raw, Mapping):
            raise ConfigError(f"{file} must contain a JSON object", field=str(file))
        return cls.from_mapping(raw)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> Config:
        known = {f.name for f in fields(cls)}
        coerced: dict[str, Any] = {}
        for key, value in values.items():
            if key not in known:
                raise ConfigError(
                    f"unknown config key: {key!r} (known: {', '.join(sorted(known))})"
                )
            coerced[key] = _coerce(key, value)
        return cls().with_changes(**coerced)

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Config:
        """Read ``ESIB_*`` variables on top of the defaults."""

        env = os.environ if environ is None else environ
        return cls().with_changes(**_env_changes(env))

    @classmethod
    def load(
        cls, path: str | Path | None = None, *, environ: Mapping[str, str] | None = None
    ) -> Config:
        """Defaults, then the file if given, then the environment."""

        config = cls.from_file(path) if path is not None else cls()
        env = os.environ if environ is None else environ
        return config.with_changes(**_env_changes(env))

    def redacted(self) -> dict[str, Any]:
        """Safe-to-print view: credentials replaced, nothing else hidden."""

        return {
            "base_url": self.base_url,
            "auth": "api_key" if self.api_key else ("basic" if self.username else "none"),
            "timeout": self.timeout,
            "batch_size": self.batch_size,
            "batch_bytes": self.batch_bytes,
            "max_attempts": self.max_attempts,
            "index_prefix": self.index_prefix,
            "ca_bundle": self.ca_bundle,
        }


def _env_changes(env: Mapping[str, str]) -> dict[str, Any]:
    changes: dict[str, Any] = {}
    for field in fields(Config):
        raw = env.get(ENV_PREFIX + field.name.upper())
        if raw is not None and raw != "":
            changes[field.name] = _coerce(field.name, raw)
    return changes


def _coerce(key: str, value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip()
        if key in _INT_FIELDS:
            try:
                return int(text)
            except ValueError as exc:
                raise ConfigError(f"{key} must be an integer (got {value!r})", field=key) from exc
        if key == "timeout":
            try:
                return float(text)
            except ValueError as exc:
                raise ConfigError(f"{key} must be a number (got {value!r})", field=key) from exc
        return text
    if key in _INT_FIELDS and not isinstance(value, int):
        raise ConfigError(f"{key} must be an integer (got {value!r})", field=key)
    if key == "timeout" and not isinstance(value, int | float):
        raise ConfigError(f"{key} must be a number (got {value!r})", field=key)
    return value
