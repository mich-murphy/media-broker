"""Configuration and secret-file handling for the media broker."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit


class ConfigError(ValueError):
    """Raised when required broker configuration is absent or unsafe."""


_BEARER_TOKEN = re.compile(r"^[A-Za-z0-9_-]{32,256}$")


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"missing required configuration: {name}")
    return value


def read_secret_file(path_value: str, name: str) -> str:
    """Read one secret from a configured file without ever logging its contents."""
    path = Path(path_value).expanduser()
    try:
        if not path.is_file():
            raise ConfigError(f"{name} must name a regular secret file")
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConfigError(f"unable to read {name}") from exc
    if not value or len(value) > 4096:
        raise ConfigError(f"{name} is empty or too large")
    return value


def validate_endpoint(name: str, value: str) -> str:
    """Validate an upstream base URL and return it without a trailing slash."""
    if any(character.isspace() for character in value):
        raise ConfigError(f"invalid {name}: whitespace is not allowed")
    try:
        parsed = urlsplit(value)
        hostname = parsed.hostname
        parsed.port
    except ValueError as exc:
        raise ConfigError(f"invalid {name}: malformed URL") from exc
    if parsed.scheme not in {"http", "https"} or not hostname:
        raise ConfigError(f"invalid {name}: expected an http(s) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ConfigError(
            f"invalid {name}: userinfo, query, and fragment are not allowed"
        )
    return value.rstrip("/")


def _flag(name: str, default: bool = False) -> bool:
    """Parse an explicit boolean configuration value; never guess on typos."""
    raw = os.environ.get(name, str(default).lower()).strip().casefold()
    if raw == "true":
        return True
    if raw == "false":
        return False
    raise ConfigError(f"{name} must be true or false")


def _csv(
    name: str,
    default: tuple[str, ...] | None,
    validator: Callable[[str, str], str] | None = None,
) -> tuple[str, ...]:
    raw = os.environ.get(name)
    values = (
        default
        if raw is None and default is not None
        else tuple(item.strip() for item in (raw or "").split(",") if item.strip())
    )
    if not values:
        raise ConfigError(f"{name} must contain at least one value")
    if validator is not None:
        for value in values:
            validator(name, value)
    return values


def validate_allowed_host(name: str, value: str) -> str:
    """Validate one exact HTTP Host authority; wildcard patterns are forbidden."""
    if not value or any(character.isspace() for character in value) or "*" in value:
        raise ConfigError(f"invalid {name}: expected an exact Host authority")
    if value.count(":") > 1 and not value.startswith("["):
        raise ConfigError(f"invalid {name}: IPv6 Host values must be bracketed")
    try:
        parsed = urlsplit("//" + value)
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ConfigError(f"invalid {name}: expected an exact Host authority")
        parsed.port
    except ValueError as exc:
        raise ConfigError(f"invalid {name}: malformed Host authority") from exc
    return value


def validate_allowed_origin(name: str, value: str) -> str:
    """Validate one exact origin without wildcard, path, or credential syntax."""
    if not value or any(character.isspace() for character in value) or "*" in value:
        raise ConfigError(f"invalid {name}: expected an exact Origin")
    try:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ConfigError(f"invalid {name}: expected an exact Origin")
        parsed.port
    except ValueError as exc:
        raise ConfigError(f"invalid {name}: malformed Origin") from exc
    return value


@dataclass(frozen=True, slots=True)
class Settings:
    """Fully loaded settings; credential fields are excluded from repr output."""

    bearer_token: str = field(repr=False)
    upstreams: dict[str, "Upstream"]
    allowed_hosts: tuple[str, ...]
    allowed_origins: tuple[str, ...]
    bind_host: str
    port: int
    timeout_seconds: float = 10.0
    max_response_bytes: int = 2_000_000
    max_items: int = 10_000


@dataclass(frozen=True, slots=True)
class Upstream:
    name: str
    base_url: str
    api_key: str = field(repr=False)
    api_version: str


def load_settings() -> Settings:
    """Load all required values from environment and explicitly named files."""
    direct_key_names = (
        "SONARR_API_KEY",
        "RADARR_API_KEY",
        "LIDARR_API_KEY",
        "TAUTULLI_API_KEY",
    )
    if any(os.environ.get(name) for name in direct_key_names):
        raise ConfigError("upstream API keys must be configured with *_API_KEY_FILE")

    bind_host = os.environ.get("MEDIA_BROKER_BIND_HOST", "127.0.0.1").strip()
    public_bind = _flag("MEDIA_BROKER_ALLOW_PUBLIC_BIND")
    if bind_host not in {"127.0.0.1", "::1", "localhost", "0.0.0.0"}:
        raise ConfigError("MEDIA_BROKER_BIND_HOST must be loopback or 0.0.0.0")
    if bind_host == "0.0.0.0" and not public_bind:
        raise ConfigError(
            "MEDIA_BROKER_ALLOW_PUBLIC_BIND=true is required for 0.0.0.0"
        )
    if bind_host != "0.0.0.0" and public_bind:
        raise ConfigError(
            "MEDIA_BROKER_ALLOW_PUBLIC_BIND is only valid with 0.0.0.0"
        )
    try:
        port = int(os.environ.get("MEDIA_BROKER_PORT", "8000"))
        timeout = float(os.environ.get("MEDIA_BROKER_TIMEOUT_SECONDS", "10"))
        max_response = int(os.environ.get("MEDIA_BROKER_MAX_RESPONSE_BYTES", "2000000"))
    except ValueError as exc:
        raise ConfigError("numeric broker configuration is invalid") from exc
    if (
        not 1 <= port <= 65535
        or not 0 < timeout <= 60
        or not 1_000 <= max_response <= 50_000_000
    ):
        raise ConfigError("numeric broker configuration is out of bounds")

    # Exact defaults are loopback-only, never a universal Host allow-list.
    authority_host = f"[{bind_host}]" if ":" in bind_host else bind_host
    default_host = f"{authority_host}:{port}"
    upstream_specs = {
        "sonarr": ("SONARR_URL", "SONARR_API_KEY_FILE", "v3"),
        "radarr": ("RADARR_URL", "RADARR_API_KEY_FILE", "v3"),
        "lidarr": ("LIDARR_URL", "LIDARR_API_KEY_FILE", "v1"),
        "tautulli": ("TAUTULLI_URL", "TAUTULLI_API_KEY_FILE", "v2"),
    }
    upstreams: dict[str, Upstream] = {}
    for name, (url_name, key_name, version) in upstream_specs.items():
        upstreams[name] = Upstream(
            name=name,
            base_url=validate_endpoint(url_name, _required(url_name)),
            api_key=read_secret_file(_required(key_name), key_name),
            api_version=version,
        )

    if bind_host == "0.0.0.0":
        # Public binding has no safe implicit authority. Both lists must be
        # explicitly supplied and every value is validated as an exact match.
        allowed_hosts = _csv("MEDIA_BROKER_ALLOWED_HOSTS", None, validate_allowed_host)
        allowed_origins = _csv(
            "MEDIA_BROKER_ALLOWED_ORIGINS", None, validate_allowed_origin
        )
    else:
        allowed_hosts = _csv(
            "MEDIA_BROKER_ALLOWED_HOSTS", (default_host,), validate_allowed_host
        )
        allowed_origins = _csv(
            "MEDIA_BROKER_ALLOWED_ORIGINS",
            (f"http://{default_host}",),
            validate_allowed_origin,
        )
    bearer_token = read_secret_file(
        _required("MEDIA_BROKER_TOKEN_FILE"), "MEDIA_BROKER_TOKEN_FILE"
    )
    if not _BEARER_TOKEN.fullmatch(bearer_token):
        raise ConfigError(
            "MEDIA_BROKER_TOKEN_FILE must contain 32-256 URL-safe characters"
        )
    return Settings(
        bearer_token=bearer_token,
        upstreams=upstreams,
        allowed_hosts=allowed_hosts,
        allowed_origins=allowed_origins,
        bind_host=bind_host,
        port=port,
        timeout_seconds=timeout,
        max_response_bytes=max_response,
    )
