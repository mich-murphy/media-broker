from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from starlette.testclient import TestClient

from media_broker.adapters import (
    MediaClient,
    ParameterError,
    UpstreamError,
    _project_history_item,
)
from media_broker.config import (
    ConfigError,
    Settings,
    Upstream,
    load_settings,
    validate_allowed_host,
    validate_allowed_origin,
    validate_endpoint,
)
from media_broker.server import BearerMiddleware, create_mcp


@pytest.fixture
def settings() -> Settings:
    return Settings(
        bearer_token="broker-secret",
        upstreams={
            "sonarr": Upstream("sonarr", "http://sonarr.test", "sonarr-key", "v3"),
            "radarr": Upstream("radarr", "http://radarr.test", "radarr-key", "v3"),
            "lidarr": Upstream("lidarr", "http://lidarr.test", "lidarr-key", "v1"),
            "tautulli": Upstream(
                "tautulli", "http://tautulli.test", "tautulli-key", "v2"
            ),
        },
        allowed_hosts=("testserver",),
        allowed_origins=("http://testserver",),
        bind_host="127.0.0.1",
        port=8000,
    )


def test_endpoint_rejects_userinfo_query_and_fragment() -> None:
    for value in (
        "https://user:pass@example.test",
        "https://example.test/?x=1",
        "https://example.test/#x",
    ):
        with pytest.raises(ConfigError):
            validate_endpoint("UPSTREAM_URL", value)


def test_allow_lists_reject_wildcards_and_malformed_values() -> None:
    with pytest.raises(ConfigError):
        validate_allowed_host("HOSTS", "localhost:*")
    with pytest.raises(ConfigError):
        validate_allowed_host("HOSTS", "::1")
    for host in ("example.test/path", "example.test/", "[::1]:8000/path"):
        with pytest.raises(ConfigError):
            validate_allowed_host("HOSTS", host)
    with pytest.raises(ConfigError):
        validate_allowed_origin("ORIGINS", "http://localhost:*")
    with pytest.raises(ConfigError):
        validate_allowed_origin("ORIGINS", "http://localhost/path")


def test_ipv6_loopback_defaults_are_bracketed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = tmp_path / "token"
    token.write_text("B" * 32)
    monkeypatch.setenv("MEDIA_BROKER_TOKEN_FILE", str(token))
    for name in ("SONARR", "RADARR", "LIDARR", "TAUTULLI"):
        key_file = tmp_path / name.lower()
        key_file.write_text("upstream-key")
        monkeypatch.setenv(f"{name}_URL", f"http://{name.lower()}.test")
        monkeypatch.setenv(f"{name}_API_KEY_FILE", str(key_file))
    monkeypatch.setenv("MEDIA_BROKER_BIND_HOST", "::1")
    loaded = load_settings()
    assert loaded.allowed_hosts == ("[::1]:8000",)
    assert loaded.allowed_origins == ("http://[::1]:8000",)


def test_public_bind_requires_opt_in_and_exact_allow_lists(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = tmp_path / "token"
    token.write_text("T" * 32)
    monkeypatch.setenv("MEDIA_BROKER_TOKEN_FILE", str(token))
    for name, port in (("SONARR", 8989), ("RADARR", 7878), ("LIDARR", 8686), ("TAUTULLI", 8181)):
        key_file = tmp_path / name.lower()
        key_file.write_text("upstream-key")
        monkeypatch.setenv(f"{name}_URL", f"http://{name.lower()}:{port}")
        monkeypatch.setenv(f"{name}_API_KEY_FILE", str(key_file))
    monkeypatch.setenv("MEDIA_BROKER_BIND_HOST", "0.0.0.0")
    monkeypatch.setenv("MEDIA_BROKER_ALLOWED_HOSTS", "media.example.test:8765")
    monkeypatch.setenv("MEDIA_BROKER_ALLOWED_ORIGINS", "https://media.example.test")
    with pytest.raises(ConfigError, match="ALLOW_PUBLIC_BIND"):
        load_settings()
    monkeypatch.setenv("MEDIA_BROKER_ALLOW_PUBLIC_BIND", "true")
    loaded = load_settings()
    assert loaded.bind_host == "0.0.0.0"
    assert loaded.allowed_hosts == ("media.example.test:8765",)
    monkeypatch.setenv("MEDIA_BROKER_ALLOWED_HOSTS", "media.example.test:*")
    with pytest.raises(ConfigError, match="exact Host"):
        load_settings()


def test_bearer_file_requires_ai_dev_compatible_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token = tmp_path / "token"
    token.write_text("short")
    monkeypatch.setenv("MEDIA_BROKER_TOKEN_FILE", str(token))
    for name in ("SONARR", "RADARR", "LIDARR", "TAUTULLI"):
        key_file = tmp_path / name.lower()
        key_file.write_text("upstream-key")
        monkeypatch.setenv(f"{name}_URL", f"http://{name.lower()}.test")
        monkeypatch.setenv(f"{name}_API_KEY_FILE", str(key_file))
    with pytest.raises(ConfigError, match="32-256"):
        load_settings()


@pytest.mark.asyncio
async def test_arr_projection_and_no_redirects(settings: Settings) -> None:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json=[
                {
                    "id": 4,
                    "title": "Example",
                    "year": 2024,
                    "status": "continuing",
                    "monitored": True,
                    "qualityProfileId": 7,
                    "rootFolderPath": "/media/tv",
                    "secret": "must not pass",
                }
            ],
        )

    client = MediaClient(settings, httpx.MockTransport(handler))
    assert client._client._trust_env is False
    try:
        result = await client.inventory("sonarr", 1, 10)
    finally:
        await client.aclose()
    assert result["items"] == [
        {
            "id": 4,
            "title": "Example",
            "year": 2024,
            "status": "continuing",
            "monitored": True,
            "quality_profile_id": 7,
            "root_folder_path": "/media/tv",
        }
    ]
    assert seen[0].url.path == "/api/v3/series"
    assert seen[0].headers["x-api-key"] == "sonarr-key"
    assert "sonarr-key" not in repr(settings)
    assert "broker-secret" not in repr(settings)


@pytest.mark.asyncio
async def test_redirect_and_oversized_response_are_rejected(settings: Settings) -> None:
    async def redirect(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://elsewhere.test"})

    client = MediaClient(settings, httpx.MockTransport(redirect))
    try:
        with pytest.raises(UpstreamError, match="redirect"):
            await client.inventory("radarr", 1, 1)
    finally:
        await client.aclose()

    async def oversized(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{}", headers={"content-length": "2000001"})

    client = MediaClient(settings, httpx.MockTransport(oversized))
    try:
        with pytest.raises(UpstreamError, match="size limit"):
            await client.inventory("radarr", 1, 1)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_timeout_is_a_total_deadline(settings: Settings) -> None:
    class SlowStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            await asyncio.sleep(0.05)
            yield b"[]"

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=SlowStream())

    client = MediaClient(
        replace(settings, timeout_seconds=0.01), httpx.MockTransport(handler)
    )
    try:
        with pytest.raises(UpstreamError, match="timed out"):
            await client.inventory("sonarr", 1, 1)
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_history_requires_success_envelope_and_terminal_metadata(
    settings: Settings,
) -> None:
    async def error_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "response": {"result": "error", "message": "private upstream detail"}
            },
        )

    client = MediaClient(settings, httpx.MockTransport(error_handler))
    try:
        with pytest.raises(UpstreamError, match="not successful"):
            await client.history("movie", "2024-01-01", "2024-01-01", 1, 1)
    finally:
        await client.aclose()

    async def missing_data_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"response": {"result": "success"}})

    client = MediaClient(settings, httpx.MockTransport(missing_data_handler))
    try:
        with pytest.raises(UpstreamError, match="unexpected history"):
            await client.history("movie", "2024-01-01", "2024-01-01", 1, 1)
    finally:
        await client.aclose()

    async def terminal_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "response": {
                    "result": "success",
                    "data": {"recordsFiltered": 1, "data": [{"title": "Movie"}]},
                }
            },
        )

    client = MediaClient(settings, httpx.MockTransport(terminal_handler))
    try:
        result = await client.history("movie", "2024-01-01", "2024-01-01", 1, 1)
    finally:
        await client.aclose()
    assert result["pagination"]["has_more"] is False


@pytest.mark.asyncio
async def test_httpx_and_httpcore_logs_never_include_credentials(
    settings: Settings, caplog: pytest.LogCaptureFixture
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = MediaClient(settings, httpx.MockTransport(handler))
    try:
        with caplog.at_level(logging.DEBUG):
            logging.getLogger("httpx").info(
                "GET https://tautulli.test/api/v2?apikey=fake-history-key"
            )
            logging.getLogger("httpcore").debug("apikey=fake-history-key")
            await client.inventory("sonarr", 1, 1)
    finally:
        await client.aclose()
    assert "fake-history-key" not in caplog.text
    assert logging.getLogger("httpx").disabled
    assert logging.getLogger("httpcore").disabled


@pytest.mark.asyncio
async def test_history_query_bounds_and_projection(settings: Settings) -> None:
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "response": {
                    "result": "success",
                    "data": {
                        "recordsFiltered": 1,
                        "data": [
                            {
                                "id": 91,
                                "user_id": 7,
                                "rating_key": "opaque-22",
                                "media_type": "episode",
                                "title": "Episode",
                                "parent_title": "Season",
                                "grandparent_title": "Show",
                                "started": 1700000000,
                                "duration": 120,
                                "watched_status": "not-provided",
                                "user": "private",
                                "ip_address": "private",
                            }
                        ],
                    },
                }
            },
        )

    client = MediaClient(settings, httpx.MockTransport(handler))
    try:
        result = await client.history("episode", "2024-01-01", "2024-01-31", 1, 10)
        filtered = await client.history("episode", "2024-01-01", "2024-01-31", 1, 10, 7)
    finally:
        await client.aclose()
    assert result["items"][0]["tautulli_user_id"] == 7
    assert result["items"][0]["tautulli_rating_key"] == "opaque-22"
    assert result["items"][0]["tautulli_history_id"] == 91
    assert result["items"][0]["completed"] is None
    assert "user" not in result["items"][0]
    assert "ip_address" not in result["items"][0]
    assert all("apikey" not in request.url.params for request in seen)
    assert all("tautulli-key" not in str(request.url) for request in seen)
    assert all(request.headers["x-api-key"] == "tautulli-key" for request in seen)
    assert seen[0].url.params["cmd"] == "get_history"
    assert seen[0].url.params["after"] == "2024-01-01"
    assert seen[0].url.params["before"] == "2024-01-31"
    assert seen[0].url.params["grouping"] == "0"
    assert seen[0].url.params["include_activity"] == "0"
    assert filtered["user_id_filter"] == 7
    assert seen[1].url.params["user_id"] == "7"
    invalid_client = MediaClient(settings, httpx.MockTransport(handler))
    try:
        with pytest.raises(ParameterError):
            await invalid_client.history("movie", "2024-01-01", "2024-03-01", 1, 1)
        with pytest.raises(ParameterError, match="strict"):
            await invalid_client.history("movie", "2024-1-01", "2024-01-01", 1, 1)
        with pytest.raises(ParameterError, match="user_id"):
            await invalid_client.history("movie", "2024-01-01", "2024-01-01", 1, 1, -1)
    finally:
        await invalid_client.aclose()


@pytest.mark.parametrize(
    ("status", "completed"),
    [
        (0, False),
        (0.25, False),
        (0.5, False),
        (0.75, False),
        (1, True),
        (None, None),
        ("watched", None),
        ("1", None),
        (True, None),
        ({"private": "value"}, None),
        (2, None),
    ],
)
def test_history_completion_domain(status: object, completed: bool | None) -> None:
    assert (
        _project_history_item({"watched_status": status}, "track")["completed"]
        is completed
    )


def _mcp_message(response_text: str) -> dict:
    data = next(
        line[6:] for line in response_text.splitlines() if line.startswith("data: ")
    )
    return json.loads(data)


def test_authenticated_discovery_call_and_no_writes(settings: Settings) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = MediaClient(settings, transport)
    app = BearerMiddleware(
        create_mcp(settings, client).streamable_http_app(), settings.bearer_token
    )
    headers = {
        "Authorization": "Bearer broker-secret",
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    initialize = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-03-26",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1"},
        },
    }
    with TestClient(app) as test_client:
        # A GET with the MCP stream negotiation header would intentionally remain open.
        assert (
            test_client.get(
                "/mcp", headers={"Authorization": "Bearer broker-secret"}
            ).status_code
            == 406
        )
        assert (
            test_client.post(
                "/mcp",
                headers={
                    "Accept": headers["Accept"],
                    "Content-Type": headers["Content-Type"],
                },
                json=initialize,
            ).status_code
            == 401
        )
        init_response = test_client.post("/mcp", headers=headers, json=initialize)
        assert init_response.status_code == 200
        tools = test_client.post(
            "/mcp",
            headers=headers,
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        )
        call = test_client.post(
            "/mcp",
            headers=headers,
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "arr_library_inventory",
                    "arguments": {"service": "sonarr"},
                },
            },
        )
        assert call.status_code == 200
        call_result = _mcp_message(call.text)["result"]
        assert json.loads(call_result["content"][0]["text"])["items"] == []
    discovered_tools = _mcp_message(tools.text)["result"]["tools"]
    names = {tool["name"] for tool in discovered_tools}
    assert names == {
        "arr_library_inventory",
        "arr_quality_profiles",
        "arr_root_folders",
        "tautulli_play_history",
    }
    assert all(tool["annotations"]["readOnlyHint"] for tool in discovered_tools)
    assert all(
        tool["annotations"]["destructiveHint"] is False for tool in discovered_tools
    )


def test_host_and_origin_are_allow_listed(settings: Settings) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = MediaClient(settings, httpx.MockTransport(handler))
    app = BearerMiddleware(
        create_mcp(settings, client).streamable_http_app(), settings.bearer_token
    )
    with TestClient(app) as test_client:
        assert (
            test_client.get(
                "/mcp",
                headers={"Host": "unexpected", "Authorization": "Bearer broker-secret"},
            ).status_code
            == 421
        )
        assert (
            test_client.get(
                "/mcp",
                headers={
                    "Host": "testserver",
                    "Origin": "http://unexpected",
                    "Authorization": "Bearer broker-secret",
                },
            ).status_code
            == 403
        )


def test_settings_requires_secret_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("MEDIA_BROKER_TOKEN_FILE", str(tmp_path / "token"))
    with pytest.raises(ConfigError):
        load_settings()
    monkeypatch.setenv("SONARR_API_KEY", "not-a-file-secret")
    with pytest.raises(ConfigError, match="API_KEY_FILE"):
        load_settings()
