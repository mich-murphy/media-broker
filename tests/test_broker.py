"""Behavioural tests for configuration, upstream hardening, and the MCP surface."""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from media_broker.adapters import (
    ArrService,
    MediaClient,
    ParameterError,
    UpstreamError,
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
from media_broker.server import build_app

TOKEN = "broker-secret-token-0123456789abcdef"
SERVICES = {"sonarr": "v3", "radarr": "v3", "lidarr": "v1", "tautulli": "v2"}
Handler = Callable[[httpx.Request], httpx.Response]
ClientFactory = Callable[..., MediaClient]
TOOL_NAMES = {
    "arr_library_inventory",
    "arr_quality_profiles",
    "arr_root_folders",
    "tautulli_play_history",
}


def reply(payload: Any, status: int = 200, **kwargs: Any) -> Handler:
    """A fake upstream that answers every request with the same JSON payload."""
    return lambda _: httpx.Response(status, json=payload, **kwargs)


def history_reply(rows: list[dict[str, Any]], total: Any = None) -> Handler:
    data = {"recordsFiltered": len(rows) if total is None else total, "data": rows}
    return reply({"response": {"result": "success", "data": data}})


@pytest.fixture
def settings() -> Settings:
    return Settings(
        bearer_token=TOKEN,
        upstreams={
            name: Upstream(f"http://{name}.test", f"{name}-key", version)
            for name, version in SERVICES.items()
        },
        allowed_hosts=("testserver",),
        allowed_origins=("http://testserver",),
        bind_host="127.0.0.1",
        port=8000,
    )


@pytest.fixture
async def make_client(settings: Settings) -> AsyncIterator[ClientFactory]:
    clients: list[MediaClient] = []

    def factory(handler: Handler, **overrides: Any) -> MediaClient:
        client = MediaClient(
            replace(settings, **overrides), httpx.MockTransport(handler)
        )
        clients.append(client)
        return client

    yield factory
    for client in clients:
        await client.aclose()


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Callable[..., Settings]:
    """A complete, valid environment; overrides replace or (with None) unset values."""

    def secret(name: str, value: str) -> str:
        path = tmp_path / name
        path.write_text(value)
        path.chmod(0o600)
        return str(path)

    values = {"MEDIA_BROKER_TOKEN_FILE": secret("token", TOKEN)}
    for name in SERVICES:
        values[f"{name.upper()}_URL"] = f"http://{name}.test"
        values[f"{name.upper()}_API_KEY_FILE"] = secret(name, f"{name}-key")

    def load(**overrides: str | None) -> Settings:
        for name, value in {**values, **overrides}.items():
            if value is None:
                monkeypatch.delenv(name, raising=False)
            else:
                monkeypatch.setenv(name, value)
        return load_settings()

    return load


# --- configuration -----------------------------------------------------------


@pytest.mark.parametrize(
    ("validator", "value", "accepted"),
    [
        (validate_endpoint, "https://sonarr.test", True),
        (validate_endpoint, "http://sonarr.test:8989/base/", True),
        (validate_endpoint, "https://user:pass@example.test", False),
        (validate_endpoint, "https://example.test/?x=1", False),
        (validate_endpoint, "https://example.test/#x", False),
        (validate_endpoint, "ftp://example.test", False),
        (validate_endpoint, "example.test:8989", False),
        (validate_endpoint, "http://exa mple.test", False),
        (validate_allowed_host, "127.0.0.1:8000", True),
        (validate_allowed_host, "[::1]:8000", True),
        (validate_allowed_host, "media.example.test", True),
        (validate_allowed_host, "localhost:*", False),
        (validate_allowed_host, "::1", False),
        (validate_allowed_host, "example.test/", False),
        (validate_allowed_host, "[::1]:8000/path", False),
        (validate_allowed_host, "http://example.test", False),
        (validate_allowed_host, "example.test:99999", False),
        (validate_allowed_origin, "https://media.example.test", True),
        (validate_allowed_origin, "http://[::1]:8000", True),
        (validate_allowed_origin, "http://localhost:*", False),
        (validate_allowed_origin, "http://localhost/path", False),
        (validate_allowed_origin, "localhost:8000", False),
        (validate_allowed_origin, "http://u@localhost", False),
    ],
)
def test_url_and_authority_validators(
    validator: Callable[[str, str], str], value: str, accepted: bool
) -> None:
    if accepted:
        assert validator("NAME", value) == value.rstrip("/")
    else:
        with pytest.raises(ConfigError, match="NAME"):
            validator("NAME", value)


@pytest.mark.parametrize(
    ("bind_host", "authority"), [(None, "127.0.0.1:8000"), ("::1", "[::1]:8000")]
)
def test_loopback_binding_defaults_to_its_exact_authority(
    env: Callable[..., Settings], bind_host: str | None, authority: str
) -> None:
    loaded = env(MEDIA_BROKER_BIND_HOST=bind_host)
    assert loaded.allowed_hosts == (authority,)
    assert loaded.allowed_origins == (f"http://{authority}",)
    assert loaded.upstreams["lidarr"].api_key == "lidarr-key"
    assert "lidarr-key" not in repr(loaded)
    assert TOKEN not in repr(loaded)


def test_public_binding_requires_opt_in_and_explicit_exact_allow_lists(
    env: Callable[..., Settings],
) -> None:
    public = {
        "MEDIA_BROKER_BIND_HOST": "0.0.0.0",
        "MEDIA_BROKER_ALLOWED_HOSTS": "media.example.test:8765",
        "MEDIA_BROKER_ALLOWED_ORIGINS": "https://media.example.test",
    }
    with pytest.raises(ConfigError, match="ALLOW_PUBLIC_BIND"):
        env(**public)
    opted_in = public | {"MEDIA_BROKER_ALLOW_PUBLIC_BIND": "true"}
    assert env(**opted_in).allowed_hosts == ("media.example.test:8765",)
    with pytest.raises(ConfigError, match="ALLOWED_HOSTS"):
        env(**opted_in | {"MEDIA_BROKER_ALLOWED_HOSTS": None})
    with pytest.raises(ConfigError, match="exact Host"):
        env(**opted_in | {"MEDIA_BROKER_ALLOWED_HOSTS": "x:*"})


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"SONARR_API_KEY": "inline-secret"}, "API_KEY_FILE"),
        ({"RADARR_URL": None}, "RADARR_URL"),
        ({"RADARR_API_KEY_FILE": "/nonexistent/key"}, "regular secret file"),
        ({"MEDIA_BROKER_BIND_HOST": "192.168.1.5"}, "loopback"),
        ({"MEDIA_BROKER_ALLOW_PUBLIC_BIND": "true"}, "ALLOW_PUBLIC_BIND"),
        ({"MEDIA_BROKER_ALLOW_PUBLIC_BIND": "yes"}, "true or false"),
        ({"MEDIA_BROKER_PORT": "0"}, "PORT must be between"),
        ({"MEDIA_BROKER_TIMEOUT_SECONDS": "soon"}, "TIMEOUT_SECONDS must be a number"),
        ({"MEDIA_BROKER_TIMEOUT_SECONDS": "61"}, "TIMEOUT_SECONDS must be between"),
        (
            {"MEDIA_BROKER_MAX_RESPONSE_BYTES": "10"},
            "MAX_RESPONSE_BYTES must be between",
        ),
        ({"MEDIA_BROKER_ALLOWED_HOSTS": " , "}, "at least one value"),
    ],
)
def test_unsafe_configuration_is_rejected(
    env: Callable[..., Settings], overrides: dict[str, str | None], message: str
) -> None:
    with pytest.raises(ConfigError, match=message):
        env(**overrides)


@pytest.mark.parametrize(
    ("content", "mode", "message"),
    [
        ("short", 0o600, "32-256"),
        (TOKEN, 0o644, "world-readable"),
        ("", 0o600, "empty or too large"),
    ],
)
def test_bearer_token_file_must_be_private_and_well_formed(
    env: Callable[..., Settings], tmp_path: Path, content: str, mode: int, message: str
) -> None:
    token = tmp_path / "other-token"
    token.write_text(content)
    token.chmod(mode)
    with pytest.raises(ConfigError, match=message):
        env(MEDIA_BROKER_TOKEN_FILE=str(token))


# --- upstream adapters -------------------------------------------------------


@pytest.mark.parametrize(
    ("service", "path", "upstream_item", "title"),
    [
        ("sonarr", "/api/v3/series", {"title": "Example"}, "Example"),
        ("lidarr", "/api/v1/artist", {"artistName": "Band"}, "Band"),
    ],
)
async def test_inventory_projects_allow_listed_fields_only(
    make_client: ClientFactory,
    service: ArrService,
    path: str,
    upstream_item: dict[str, str],
    title: str,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        item = {
            "id": 4,
            "year": 2024,
            "status": "continuing",
            "monitored": True,
            "qualityProfileId": 7,
            "rootFolderPath": "/media",
            "path": "/media/private",
            "images": [{"url": "must not pass"}],
        }
        return httpx.Response(200, json=[item | upstream_item])

    result = await make_client(handler).inventory(service, 1, 10)
    assert result["items"] == [
        {
            "id": 4,
            "title": title,
            "year": 2024,
            "status": "continuing",
            "monitored": True,
            "quality_profile_id": 7,
            "root_folder_path": "/media",
        }
    ]
    assert seen[0].url.path == path
    assert seen[0].headers["x-api-key"] == f"{service}-key"
    assert f"{service}-key" not in str(seen[0].url)


async def test_inventory_search_and_local_pagination(
    make_client: ClientFactory,
) -> None:
    titles = ["Alpha One", "alpha two", "Beta"]
    client = make_client(reply([{"id": i, "title": t} for i, t in enumerate(titles)]))
    first = await client.inventory("radarr", 1, 2)
    assert [i["title"] for i in first["items"]] == titles[:2]
    assert first["pagination"] == {
        "page": 1,
        "page_size": 2,
        "returned_count": 2,
        "total_count": 3,
        "has_more": True,
        "upstream_paginated": False,
        "upstream_truncated": False,
    }
    last = await client.inventory("radarr", 2, 2)
    assert [i["title"] for i in last["items"]] == titles[2:]
    assert last["pagination"]["has_more"] is False
    searched = await client.inventory("radarr", 1, 10, " ALPHA ")
    assert [i["title"] for i in searched["items"]] == titles[:2]


async def test_quality_profiles_and_root_folders_are_projected(
    make_client: ClientFactory,
) -> None:
    profile = {
        "id": 1,
        "name": "HD",
        "upgradeAllowed": True,
        "cutoff": 7,
        "items": ["x"],
    }
    profiles = await make_client(reply([profile])).quality_profiles("sonarr")
    assert profiles == {
        "service": "sonarr",
        "items": [{"id": 1, "name": "HD", "upgrade_allowed": True, "cutoff": 7}],
        "upstream_truncated": False,
    }
    folder = {
        "id": 2,
        "path": "/tv",
        "accessible": True,
        "freeSpace": 5,
        "totalSpace": 9,
    }
    folders = await make_client(reply([folder, "junk"])).root_folders("lidarr")
    assert folders["items"] == [
        {
            "id": 2,
            "path": "/tv",
            "accessible": True,
            "free_space_bytes": 5,
            "total_space_bytes": 9,
        }
    ]


def _raise_connect_error(_: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError("dial tcp: private diagnostic")


@pytest.mark.parametrize(
    ("handler", "message"),
    [
        (reply({}, 302, headers={"location": "https://elsewhere.test"}), "HTTP 302"),
        (reply({"detail": "private upstream detail"}, 500), "HTTP 500"),
        (
            lambda _: httpx.Response(200, content=b"<html>private</html>"),
            "invalid JSON",
        ),
        (reply({"not": "a list"}), "unexpected series response"),
        (reply([{}] * 10_001), "unexpected series response"),
        (_raise_connect_error, "request failed"),
        (reply({}, headers={"content-length": "2000001"}), "size limit"),
    ],
)
async def test_upstream_failures_are_sanitized(
    make_client: ClientFactory, handler: Handler, message: str
) -> None:
    with pytest.raises(UpstreamError, match=message) as error:
        await make_client(handler).inventory("sonarr", 1, 1)
    assert "private" not in str(error.value)


async def test_streamed_bodies_are_capped_without_content_length(
    make_client: ClientFactory,
) -> None:
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            for _ in range(3):
                yield b"x" * 500

    client = make_client(
        lambda _: httpx.Response(200, stream=Stream()), max_response_bytes=1000
    )
    with pytest.raises(UpstreamError, match="size limit"):
        await client.inventory("sonarr", 1, 1)


async def test_timeout_is_a_total_deadline(make_client: ClientFactory) -> None:
    class SlowStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            await asyncio.sleep(0.05)
            yield b"[]"

    client = make_client(
        lambda _: httpx.Response(200, stream=SlowStream()), timeout_seconds=0.01
    )
    with pytest.raises(UpstreamError, match="timed out"):
        await client.inventory("sonarr", 1, 1)


async def test_upstream_urls_and_keys_never_reach_logs(
    make_client: ClientFactory, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG):
        await make_client(reply([])).inventory("sonarr", 1, 1)
    assert "sonarr" not in caplog.text


async def test_history_request_shape_and_projection(make_client: ClientFactory) -> None:
    seen: list[httpx.Request] = []
    row = {
        "id": 91,
        "user_id": 7,
        "rating_key": "opaque-22",
        "media_type": "episode",
        "title": "Episode",
        "parent_title": "Season",
        "grandparent_title": "Show",
        "started": 1700000000,
        "duration": 120,
        "watched_status": 1,
        "user": "private name",
        "ip_address": "10.0.0.9",
    }
    other_user = row | {"user_id": 8, "title": "Other household member"}
    other_type = row | {"media_type": "movie", "title": "Not an episode"}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return history_reply([row, other_user, other_type])(request)

    result = await make_client(handler).history(
        "episode", "2024-01-01", "2024-01-31", 1, 10, 7
    )
    assert result["items"] == [
        {
            "media_type": "episode",
            "tautulli_user_id": 7,
            "tautulli_rating_key": "opaque-22",
            "tautulli_history_id": 91,
            "title": "Episode",
            "parent_title": "Season",
            "grandparent_title": "Show",
            "played_at": 1700000000,
            "duration_seconds": 120,
            "completed": True,
        }
    ]
    assert result["user_id_filter"] == 7
    params = seen[0].url.params
    assert dict(params) == {
        "cmd": "get_history",
        "start": "0",
        "length": "10",
        "media_type": "episode",
        "after": "2024-01-01",
        "before": "2024-01-31",
        "grouping": "0",
        "include_activity": "0",
        "user_id": "7",
    }
    assert seen[0].headers["x-api-key"] == "tautulli-key"
    assert "tautulli-key" not in str(seen[0].url)


@pytest.mark.parametrize(
    ("status", "completed"),
    [
        (0, False),
        (0.25, False),
        (0.75, False),
        (1, True),
        (None, None),
        ("1", None),
        (True, None),
        (2, None),
    ],
)
async def test_history_completion_domain(
    make_client: ClientFactory, status: object, completed: bool | None
) -> None:
    client = make_client(history_reply([{"watched_status": status}]))
    result = await client.history("track", "2024-01-01", "2024-01-01", 1, 1)
    assert result["items"][0]["completed"] is completed


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        (
            {"response": {"result": "error", "message": "private detail"}},
            "not successful",
        ),
        ({"response": {"result": "success"}}, "unexpected history"),
        (
            {"response": {"result": "success", "data": {"data": [{}, {}]}}},
            "unexpected history",
        ),
        ([], "not successful"),
    ],
)
async def test_history_requires_a_well_formed_success_envelope(
    make_client: ClientFactory, payload: Any, message: str
) -> None:
    with pytest.raises(UpstreamError, match=message) as error:
        await make_client(reply(payload)).history(
            "movie", "2024-01-01", "2024-01-01", 1, 1
        )
    assert "private" not in str(error.value)


@pytest.mark.parametrize(
    ("rows", "total", "page", "has_more", "truncated", "reported_total"),
    [
        (1, 1, 1, False, False, 1),
        (2, 5, 1, True, False, 5),
        (2, "many", 1, True, True, None),
        (2, 1, 2, True, True, None),
        (1, 1, 2, False, False, None),
    ],
)
async def test_history_pagination_never_trusts_an_inconsistent_total(
    make_client: ClientFactory,
    rows: int,
    total: Any,
    page: int,
    has_more: bool,
    truncated: bool,
    reported_total: int | None,
) -> None:
    client = make_client(history_reply([{"title": "Movie"}] * rows, total))
    result = await client.history("movie", "2024-01-01", "2024-01-01", page, 2)
    pagination = result["pagination"]
    assert pagination["has_more"] is has_more
    assert pagination["upstream_truncated"] is truncated
    assert pagination["upstream_total_count"] == reported_total


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2024-01-01", "2024-02-01", "31 inclusive days"),
        ("2024-01-02", "2024-01-01", "on or after"),
        ("2024-02-30", "2024-02-30", "calendar dates"),
    ],
)
async def test_history_date_range_bounds(
    make_client: ClientFactory, start: str, end: str, message: str
) -> None:
    client = make_client(history_reply([]))
    assert (await client.history("movie", "2024-01-01", "2024-01-31", 1, 1))[
        "items"
    ] == []
    with pytest.raises(ParameterError, match=message):
        await client.history("movie", start, end, 1, 1)


# --- authenticated MCP surface ------------------------------------------------

MCP_HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


def route_upstreams(request: httpx.Request) -> httpx.Response:
    """A fake upstream that answers Arr and Tautulli endpoints with one item each."""
    if request.url.path == "/api/v2":
        return history_reply([{"title": "Movie", "user_id": 3}])(request)
    return httpx.Response(
        200, json=[{"id": 1, "title": "Item", "name": "HD", "path": "/tv"}]
    )


class RecordingTransport(httpx.MockTransport):
    closed = False

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def mcp(settings: Settings) -> Any:
    transport = RecordingTransport(route_upstreams)
    with TestClient(build_app(settings, transport)) as client:
        yield client
    assert transport.closed, "upstream client must be closed when the server stops"


def rpc(
    client: TestClient, method: str, params: Any = None, **headers: Any
) -> httpx.Response:
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    response: httpx.Response = client.post(
        "/mcp", headers={**MCP_HEADERS, **headers}, json=body
    )
    return response


def call_tool(client: TestClient, name: str, **arguments: Any) -> dict[str, Any]:
    response = rpc(client, "tools/call", {"name": name, "arguments": arguments})
    assert response.status_code == 200, response.text
    result: dict[str, Any] = response.json()["result"]
    return result


@pytest.mark.parametrize(
    "authorization",
    [
        None,
        f"Bearer {TOKEN[:-1]}",
        f"Basic {TOKEN}",
        TOKEN,
        b"Bearer \xc3\xa9\xc3\xa9",
        [f"Bearer {TOKEN}", f"Bearer {TOKEN}"],
    ],
)
def test_every_request_without_exactly_one_valid_bearer_is_rejected(
    mcp: TestClient, authorization: Any
) -> None:
    values = authorization if isinstance(authorization, list) else [authorization]
    headers = [("authorization", value) for value in values if value is not None]
    for method in ("get", "post", "delete"):
        response = mcp.request(method, "/mcp", headers=headers, json={"jsonrpc": "2.0"})
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"
        assert response.json() == {"error": "authentication required"}
        assert "server" not in response.headers


def test_host_and_origin_allow_lists_are_enforced(mcp: TestClient) -> None:
    assert rpc(mcp, "tools/list", Host="unexpected").status_code == 421
    assert rpc(mcp, "tools/list", Origin="http://unexpected").status_code == 403
    assert rpc(mcp, "tools/list", Origin="http://testserver").status_code == 200


def test_tools_are_discoverable_read_only_and_schema_bounded(mcp: TestClient) -> None:
    tools = {
        tool["name"]: tool for tool in rpc(mcp, "tools/list").json()["result"]["tools"]
    }
    assert set(tools) == TOOL_NAMES
    for tool in tools.values():
        assert tool["annotations"]["readOnlyHint"] is True
        assert tool["annotations"]["destructiveHint"] is False
    inventory = tools["arr_library_inventory"]["inputSchema"]["properties"]
    assert inventory["service"]["enum"] == ["sonarr", "radarr", "lidarr"]
    assert (inventory["page_size"]["minimum"], inventory["page_size"]["maximum"]) == (
        1,
        100,
    )
    history = tools["tautulli_play_history"]["inputSchema"]["properties"]
    assert history["start_date"]["pattern"] == r"^\d{4}-\d{2}-\d{2}$"


@pytest.mark.parametrize(
    ("name", "arguments", "expected"),
    [
        (
            "arr_library_inventory",
            {"service": "sonarr"},
            {"items": [{"title": "Item"}]},
        ),
        ("arr_quality_profiles", {"service": "radarr"}, {"items": [{"name": "HD"}]}),
        ("arr_root_folders", {"service": "lidarr"}, {"items": [{"path": "/tv"}]}),
        (
            "tautulli_play_history",
            {
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-01-31",
            },
            {"items": [{"title": "Movie", "tautulli_user_id": 3}]},
        ),
    ],
)
def test_each_tool_answers_over_streamable_http(
    mcp: TestClient, name: str, arguments: dict[str, Any], expected: dict[str, Any]
) -> None:
    result = call_tool(mcp, name, **arguments)
    assert not result.get("isError")
    content = result["structuredContent"]
    assert content["items"][0].items() >= expected["items"][0].items()
    assert json.loads(result["content"][0]["text"]) == content


@pytest.mark.parametrize(
    ("name", "arguments", "field"),
    [
        ("arr_library_inventory", {"service": "plex"}, "service"),
        ("arr_library_inventory", {"service": "sonarr", "page": 0}, "page"),
        ("arr_library_inventory", {"service": "sonarr", "page_size": 101}, "page_size"),
        ("arr_library_inventory", {"service": "sonarr", "search": "x" * 201}, "search"),
        (
            "tautulli_play_history",
            {
                "media_type": "movie",
                "start_date": "2024-1-01",
                "end_date": "2024-01-01",
            },
            "start_date",
        ),
        (
            "tautulli_play_history",
            {
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-01-01",
                "user_id": -1,
            },
            "user_id",
        ),
        (
            "tautulli_play_history",
            {
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-03-01",
            },
            "31 inclusive",
        ),
    ],
)
def test_out_of_bounds_arguments_are_rejected_before_any_upstream_call(
    mcp: TestClient, name: str, arguments: dict[str, Any], field: str
) -> None:
    result = call_tool(mcp, name, **arguments)
    assert result["isError"] is True
    assert field in result["content"][0]["text"]


def test_unexpected_failures_are_reported_without_detail(settings: Settings) -> None:
    def explode(_: httpx.Request) -> httpx.Response:
        raise RuntimeError("private stack detail")

    with TestClient(build_app(settings, httpx.MockTransport(explode))) as client:
        result = call_tool(client, "arr_root_folders", service="sonarr")
    assert result["isError"] is True
    assert (
        result["content"][0]["text"]
        == "Error executing tool arr_root_folders: operation failed"
    )
