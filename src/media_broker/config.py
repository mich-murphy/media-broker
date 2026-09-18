"""Configuration and secret-file handling for the media broker."""

# Write tools are opt-in: requests enable brokered adds and unmonitoring, and
# deletes enable the two-phase destructive media removal tool.

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import SplitResult, urlsplit


class ConfigError(ValueError):
    """Raised when required broker configuration is absent or unsafe."""


_TOKEN_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)
_UPSTREAM_API_VERSIONS = {
    "sonarr": "v3",
    "radarr": "v3",
    "lidarr": "v1",
    "tautulli": "v2",
    "jellyfin": "v19",
}
_LOOPBACK = frozenset({"127.0.0.1", "::1", "localhost"})
_PUBLIC = "0.0.0.0"  # noqa: S104 - permitted only with an explicit opt-in flag


@dataclass(frozen=True, slots=True)
class Upstream:
    base_url: str
    api_key: str = field(repr=False)
    api_version: str

    def api(self, *parts: str | int) -> str:
        """Join path segments under the pinned API version into a full URL."""
        return self.base_url + "/api/" + "/".join((self.api_version, *map(str, parts)))


@dataclass(frozen=True, slots=True)
class Settings:
    """Fully loaded settings; credential fields are excluded from repr output."""

    bearer_token: str = field(repr=False)
    upstreams: dict[str, Upstream]
    allowed_hosts: tuple[str, ...]
    allowed_origins: tuple[str, ...]
    bind_host: str
    port: int
    timeout_seconds: float = 10.0
    max_response_bytes: int = 2_000_000
    enable_requests: bool = False
    enable_deletes: bool = False


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(f"missing required configuration: {name}")
    return value


def _secret(name: str) -> str:
    """Read the secret file named by an environment variable, never logging it."""
    path = Path(_required(name)).expanduser()
    try:
        if not path.is_file():
            raise ConfigError(f"{name} must name a regular secret file")
        if path.stat().st_mode & 0o007:
            raise ConfigError(f"{name} must not be world-readable")
        value = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ConfigError(f"unable to read {name}") from exc
    if not value or len(value) > 4096:
        raise ConfigError(f"{name} is empty or too large")
    return value


def _split(name: str, value: str, expected: str) -> SplitResult:
    """Split a URL or authority, rejecting wildcards, credentials, and bad ports."""
    if not value or "*" in value or any(character.isspace() for character in value):
        raise ConfigError(f"invalid {name}: expected an exact {expected}")
    try:
        parsed = urlsplit(value if "://" in value else "//" + value)
        _ = parsed.port
    except ValueError as exc:
        raise ConfigError(f"invalid {name}: malformed {expected}") from exc
    if not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
        raise ConfigError(f"invalid {name}: expected an exact {expected}")
    return parsed


def validate_endpoint(name: str, value: str) -> str:
    """Validate an upstream base URL and return it without a trailing slash."""
    if _split(name, value, "http(s) URL").scheme not in {"http", "https"}:
        raise ConfigError(f"invalid {name}: expected an http(s) URL")
    return value.rstrip("/")


def validate_allowed_host(name: str, value: str) -> str:
    """Validate one exact HTTP Host authority; wildcard patterns are forbidden."""
    parsed = _split(name, value, "Host authority")
    if parsed.scheme or parsed.path:
        raise ConfigError(f"invalid {name}: expected an exact Host authority")
    return value


def validate_allowed_origin(name: str, value: str) -> str:
    """Validate one exact origin without wildcard, path, or credential syntax."""
    parsed = _split(name, value, "Origin")
    if parsed.scheme not in {"http", "https"} or parsed.path:
        raise ConfigError(f"invalid {name}: expected an exact Origin")
    return value


def _flag(name: str) -> bool:
    """Parse an explicit boolean configuration value; never guess on typos."""
    raw = os.environ.get(name, "false").strip().casefold()
    if raw not in {"true", "false"}:
        raise ConfigError(f"{name} must be true or false")
    return raw == "true"


def _number[N: (int, float)](name: str, default: N, low: N, high: N) -> N:
    try:
        value = type(default)(os.environ.get(name, str(default)))
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if not low <= value <= high:
        raise ConfigError(f"{name} must be between {low} and {high}")
    return value


def _csv(
    name: str,
    default: tuple[str, ...] | None,
    validate: Callable[[str, str], str],
) -> tuple[str, ...]:
    raw = os.environ.get(name)
    values = (
        default
        if raw is None and default is not None
        else tuple(item.strip() for item in (raw or "").split(",") if item.strip())
    )
    if not values:
        raise ConfigError(f"{name} must contain at least one value")
    return tuple(validate(name, value) for value in values)


def _upstream(service: str, api_version: str) -> Upstream:
    """Load one upstream's URL and secret-file key; inline keys are forbidden."""
    env = service.upper()
    if os.environ.get(f"{env}_API_KEY"):
        raise ConfigError("upstream API keys must be configured with *_API_KEY_FILE")
    return Upstream(
        base_url=validate_endpoint(f"{env}_URL", _required(f"{env}_URL")),
        api_key=_secret(f"{env}_API_KEY_FILE"),
        api_version=api_version,
    )


def load_settings() -> Settings:
    """Load all required values from environment and explicitly named files."""
    bind_host = os.environ.get("MEDIA_BROKER_BIND_HOST", "127.0.0.1").strip()
    if bind_host not in _LOOPBACK | {_PUBLIC}:
        raise ConfigError("MEDIA_BROKER_BIND_HOST must be loopback or 0.0.0.0")
    public = bind_host == _PUBLIC
    if _flag("MEDIA_BROKER_ALLOW_PUBLIC_BIND") != public:
        raise ConfigError(
            "MEDIA_BROKER_ALLOW_PUBLIC_BIND=true is required for 0.0.0.0 only"
        )
    port = _number("MEDIA_BROKER_PORT", 8000, 1, 65535)
    # Loopback binding defaults to its own exact authority; public binding has
    # no safe implicit authority, so both allow-lists must be supplied.
    host = f"[{bind_host}]" if ":" in bind_host else bind_host
    authority = None if public else (f"{host}:{port}",)
    origin = None if public else (f"http://{host}:{port}",)
    bearer_token = _secret("MEDIA_BROKER_TOKEN_FILE")
    if not 32 <= len(bearer_token) <= 256 or any(
        char not in _TOKEN_CHARS for char in bearer_token
    ):
        raise ConfigError(
            "MEDIA_BROKER_TOKEN_FILE must contain 32-256 URL-safe characters"
        )
    return Settings(
        bearer_token=bearer_token,
        upstreams={
            service: _upstream(service, version)
            for service, version in _UPSTREAM_API_VERSIONS.items()
        },
        allowed_hosts=_csv(
            "MEDIA_BROKER_ALLOWED_HOSTS", authority, validate_allowed_host
        ),
        allowed_origins=_csv(
            "MEDIA_BROKER_ALLOWED_ORIGINS", origin, validate_allowed_origin
        ),
        bind_host=bind_host,
        port=port,
        timeout_seconds=_number("MEDIA_BROKER_TIMEOUT_SECONDS", 10.0, 0.1, 60.0),
        max_response_bytes=_number(
            "MEDIA_BROKER_MAX_RESPONSE_BYTES", 2_000_000, 1_000, 50_000_000
        ),
        enable_requests=_flag("MEDIA_BROKER_ENABLE_REQUESTS"),
        enable_deletes=_flag("MEDIA_BROKER_ENABLE_DELETES"),
    )
