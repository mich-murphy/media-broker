"""Behavioural tests for configuration, upstream hardening, and the MCP surface."""

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator, Callable, Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from starlette.testclient import TestClient

from media_broker.adapters import (
    ArrService,
    JellyfinMediaType,
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
SERVICES = {
    "sonarr": "v3",
    "radarr": "v3",
    "lidarr": "v1",
    "tautulli": "v2",
    "jellyfin": "v19",
}
Handler = Callable[[httpx.Request], httpx.Response]
ClientFactory = Callable[..., MediaClient]
TOOL_NAMES = {
    "arr_library_inventory",
    "arr_quality_profiles",
    "arr_root_folders",
    "arr_search_candidates",
    "tautulli_play_history",
    "jellyfin_play_history",
}
WRITE_TOOL_NAMES = {"arr_request_media", "arr_unmonitor_media", "arr_delete_media"}


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

    def factory(
        source: Handler | httpx.AsyncBaseTransport, **overrides: Any
    ) -> MediaClient:
        transport = (
            source
            if isinstance(source, httpx.AsyncBaseTransport)
            else httpx.MockTransport(source)
        )
        client = MediaClient(replace(settings, **overrides), transport)
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
        ({"JELLYFIN_API_KEY": "inline-secret"}, "API_KEY_FILE"),
        ({"RADARR_URL": None}, "RADARR_URL"),
        ({"JELLYFIN_URL": None}, "JELLYFIN_URL"),
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


JELLYFIN_USER = "0123456789abcdef0123456789abcdef"


async def test_jellyfin_history_iterates_dates_and_projects_only_allowed_fields(
    make_client: ClientFactory,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        day = request.url.path.split("/")[3]
        return httpx.Response(
            200,
            json=[
                {
                    "Time": "01:02:03",
                    "Id": f"item-{day}",
                    "Name": f"Title {day}",
                    "Type": "Episode",
                    "Duration": 91.5,
                    "RowId": 7,
                    "Client": "private client",
                    "Method": "private method",
                    "Device": "private device",
                    "UserName": "private username",
                    "Unknown": "must not pass",
                }
            ],
        )

    result = await make_client(handler).jellyfin_history(
        JELLYFIN_USER, "episode", "2024-01-01", "2024-01-02", 1, 10, 5.5
    )
    assert [request.url.path for request in seen] == [
        f"/user_usage_stats/{JELLYFIN_USER}/2024-01-01/GetItems",
        f"/user_usage_stats/{JELLYFIN_USER}/2024-01-02/GetItems",
    ]
    assert [dict(request.url.params) for request in seen] == [
        {"filter": "Episode", "timezoneOffset": "5.5"},
        {"filter": "Episode", "timezoneOffset": "5.5"},
    ]
    assert all(request.headers["x-emby-token"] == "jellyfin-key" for request in seen)
    assert all("x-api-key" not in request.headers for request in seen)
    assert result["items"] == [
        {
            "jellyfin_user_id": JELLYFIN_USER,
            "media_type": "episode",
            "played_at": "2024-01-01T01:02:03",
            "jellyfin_item_id": "item-2024-01-01",
            "jellyfin_history_id": 7,
            "title": "Title 2024-01-01",
            "duration_seconds": 91.5,
        },
        {
            "jellyfin_user_id": JELLYFIN_USER,
            "media_type": "episode",
            "played_at": "2024-01-02T01:02:03",
            "jellyfin_item_id": "item-2024-01-02",
            "jellyfin_history_id": 7,
            "title": "Title 2024-01-02",
            "duration_seconds": 91.5,
        },
    ]
    assert "completed" not in result["items"][0]


@pytest.mark.parametrize(
    ("media_type", "expected_filter"),
    [("movie", "Movie"), ("episode", "Episode"), ("track", "Audio")],
)
async def test_jellyfin_media_type_filters(
    make_client: ClientFactory,
    media_type: JellyfinMediaType,
    expected_filter: str,
) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[])

    client = make_client(handler)
    await client.jellyfin_history(
        JELLYFIN_USER, media_type, "2024-01-01", "2024-01-01", 1, 1, 0
    )
    assert dict(seen[0].url.params) == {
        "filter": expected_filter,
        "timezoneOffset": "0",
    }


async def test_jellyfin_history_bounds_projected_strings(
    make_client: ClientFactory,
) -> None:
    oversized = "x" * 1025
    result = await make_client(
        reply(
            [
                {
                    "Id": oversized,
                    "RowId": oversized,
                    "Name": oversized,
                    "Duration": 120,
                    "Time": oversized,
                }
            ]
        )
    ).jellyfin_history(JELLYFIN_USER, "movie", "2024-01-01", "2024-01-01", 1, 1, 0)
    item = result["items"][0]
    assert item["jellyfin_item_id"] is None
    assert item["jellyfin_history_id"] is None
    assert item["title"] is None
    assert item["played_at"] is None


async def test_jellyfin_history_scalar_validation_and_missing_fields(
    make_client: ClientFactory,
) -> None:
    result = await make_client(
        reply(
            [
                {
                    "Id": True,
                    "RowId": {"private": "data"},
                    "Name": 12,
                    "Duration": "120",
                    "Time": None,
                    "Client": "must not pass",
                }
            ]
        )
    ).jellyfin_history(JELLYFIN_USER, "track", "2024-01-01", "2024-01-01", 1, 1, 0)
    assert result["items"] == [
        {
            "jellyfin_user_id": JELLYFIN_USER,
            "media_type": "track",
            "played_at": None,
            "jellyfin_item_id": None,
            "jellyfin_history_id": None,
            "title": None,
            "duration_seconds": None,
        }
    ]


async def test_jellyfin_history_local_pagination(
    make_client: ClientFactory,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        day = request.url.path.split("/")[3]
        count = 2 if day == "2024-01-01" else 1
        return httpx.Response(
            200,
            json=[
                {
                    "Time": "00:00:01",
                    "Id": f"{day}-{index}",
                    "RowId": index,
                    "Name": f"{day}-{index}",
                    "Duration": index,
                }
                for index in range(count)
            ],
        )

    result = await make_client(handler).jellyfin_history(
        JELLYFIN_USER, "movie", "2024-01-01", "2024-01-02", 2, 2, 0
    )
    assert [item["title"] for item in result["items"]] == ["2024-01-02-0"]
    assert result["pagination"] == {
        "page": 2,
        "page_size": 2,
        "returned_count": 1,
        "total_count": 3,
        "has_more": False,
        "upstream_paginated": False,
        "upstream_truncated": False,
    }


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"not": "a list"}, "unexpected Jellyfin history"),
        ([{"Time": "00:00:00"}, "not an object"], "unexpected Jellyfin history"),
    ],
)
async def test_jellyfin_history_rejects_malformed_day_results(
    make_client: ClientFactory, payload: Any, message: str
) -> None:
    with pytest.raises(UpstreamError, match=message):
        await make_client(reply(payload)).jellyfin_history(
            JELLYFIN_USER, "movie", "2024-01-01", "2024-01-01", 1, 1, 0
        )


async def test_jellyfin_history_rejects_excess_aggregate_rows(
    make_client: ClientFactory,
) -> None:
    with pytest.raises(UpstreamError, match="too many Jellyfin history rows"):
        await make_client(reply([{}] * 10_001)).jellyfin_history(
            JELLYFIN_USER, "movie", "2024-01-01", "2024-01-01", 1, 1, 0
        )


@pytest.mark.parametrize(
    ("user_id", "timezone_offset"),
    [("short", 0), ("g" * 32, 0), (JELLYFIN_USER, -15), (JELLYFIN_USER, 15)],
)
async def test_jellyfin_history_rejects_unsafe_arguments(
    make_client: ClientFactory, user_id: str, timezone_offset: int
) -> None:
    message = "user_id" if user_id != JELLYFIN_USER else "timezone_offset"
    with pytest.raises(ParameterError, match=message):
        await make_client(reply([])).jellyfin_history(
            user_id, "movie", "2024-01-01", "2024-01-01", 1, 1, timezone_offset
        )


async def test_jellyfin_history_propagates_authorization_failure(
    make_client: ClientFactory,
) -> None:
    with pytest.raises(UpstreamError, match="HTTP 403"):
        await make_client(reply({"private": "detail"}, 403)).jellyfin_history(
            JELLYFIN_USER, "movie", "2024-01-01", "2024-01-01", 1, 1, 0
        )


# --- authenticated MCP surface ------------------------------------------------

MCP_HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


def route_upstreams(request: httpx.Request) -> httpx.Response:
    """A fake upstream that answers Arr and history endpoints with one item each."""
    if request.url.path == "/api/v2":
        return history_reply([{"title": "Movie", "user_id": 3}])(request)
    if request.url.path.startswith("/user_usage_stats/"):
        return httpx.Response(
            200,
            json=[
                {
                    "Id": "item-id",
                    "RowId": 8,
                    "Name": "Movie",
                    "Type": "Movie",
                    "Time": "12:34:56",
                    "Duration": 120,
                }
            ],
        )
    return httpx.Response(
        200, json=[{"id": 1, "title": "Item", "name": "HD", "path": "/tv"}]
    )


class RecordingTransport(httpx.MockTransport):
    """A routed fake upstream that records requests and tracks closure."""

    def __init__(self, handler: Handler) -> None:
        super().__init__(handler)
        self.requests: list[httpx.Request] = []
        self.closed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return await super().handle_async_request(request)

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def upstream() -> RecordingTransport:
    return RecordingTransport(route_upstreams)


@pytest.fixture
def mcp(settings: Settings, upstream: RecordingTransport) -> Iterator[TestClient]:
    with TestClient(build_app(settings, upstream)) as client:
        yield client
    assert upstream.closed, "upstream client must be closed when the server stops"


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
    jellyfin = tools["jellyfin_play_history"]["inputSchema"]["properties"]
    assert jellyfin["media_type"]["enum"] == ["movie", "episode", "track"]
    assert jellyfin["user_id"]["pattern"] == r"^[0-9a-fA-F]{32}$"
    assert (
        jellyfin["timezone_offset"]["minimum"],
        jellyfin["timezone_offset"]["maximum"],
    ) == (-14, 14)


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
        (
            "jellyfin_play_history",
            {
                "user_id": "0123456789abcdef0123456789abcdef",
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-01-01",
            },
            {
                "items": [
                    {
                        "title": "Movie",
                        "jellyfin_user_id": "0123456789abcdef0123456789abcdef",
                    }
                ]
            },
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
        (
            "jellyfin_play_history",
            {
                "user_id": "not-a-user-id",
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-01-01",
            },
            "user_id",
        ),
        (
            "jellyfin_play_history",
            {
                "user_id": JELLYFIN_USER,
                "media_type": "movie",
                "start_date": "2024-01-01",
                "end_date": "2024-01-01",
                "timezone_offset": 15,
            },
            "timezone_offset",
        ),
    ],
)
def test_out_of_bounds_arguments_are_rejected_before_any_upstream_call(
    mcp: TestClient,
    upstream: RecordingTransport,
    name: str,
    arguments: dict[str, Any],
    field: str,
) -> None:
    result = call_tool(mcp, name, **arguments)
    assert result["isError"] is True
    assert field in result["content"][0]["text"]
    assert upstream.requests == []


# --- gated write gates -------------------------------------------------------


def test_write_gates_default_to_disabled(env: Callable[..., Settings]) -> None:
    loaded = env()
    assert loaded.enable_requests is False
    assert loaded.enable_deletes is False


def test_write_gates_need_an_explicit_boolean(env: Callable[..., Settings]) -> None:
    loaded = env(
        MEDIA_BROKER_ENABLE_REQUESTS="true", MEDIA_BROKER_ENABLE_DELETES="true"
    )
    assert loaded.enable_requests is True
    assert loaded.enable_deletes is True
    with pytest.raises(ConfigError, match="ENABLE_DELETES must be true or false"):
        env(MEDIA_BROKER_ENABLE_DELETES="1")


# --- write adapters ----------------------------------------------------------

MBID = "f59c5520-5f46-4d2c-b2c4-822eabf53419"
ADD_RESOURCES = {
    "sonarr": "/api/v3/series",
    "radarr": "/api/v3/movie",
    "lidarr": "/api/v1/artist",
}
EXTERNAL_IDS = {"sonarr": "81189", "radarr": "603", "lidarr": MBID}
LOOKUP_TERMS = {
    "sonarr": "tvdb:81189",
    "radarr": "tmdb:603",
    "lidarr": f"lidarr:{MBID}",
}
SONARR_CANDIDATE: dict[str, Any] = {
    "id": 0,
    "tvdbId": 81189,
    "title": "Example Show",
    "titleSlug": "example-show",
    "seriesType": "anime",
    "year": 2021,
    "status": "continuing",
    "seasons": [{"seasonNumber": 1}, {"seasonNumber": 2}],
    "network": "must not pass",
}
RADARR_CANDIDATE: dict[str, Any] = {
    "id": 0,
    "tmdbId": 603,
    "title": "Example Film",
    "titleSlug": "example-film-603",
    "year": 1999,
    "status": "released",
    "images": [{"url": "must not pass"}],
}
LIDARR_CANDIDATE: dict[str, Any] = {
    "id": 0,
    "foreignArtistId": MBID,
    "artistName": "Example Band",
    "status": "ended",
    "genres": ["must not pass"],
}
CANDIDATES = {
    "sonarr": SONARR_CANDIDATE,
    "radarr": RADARR_CANDIDATE,
    "lidarr": LIDARR_CANDIDATE,
}
EXPECTED_ADD_BODIES: dict[str, dict[str, Any]] = {
    "sonarr": {
        "title": "Example Show",
        "titleSlug": "example-show",
        "tvdbId": 81189,
        "qualityProfileId": 7,
        "rootFolderPath": "/media",
        "seriesType": "anime",
        "monitored": True,
        "seasonFolder": True,
        "seasons": [
            {"seasonNumber": 1, "monitored": True},
            {"seasonNumber": 2, "monitored": True},
        ],
        "addOptions": {
            "monitor": "all",
            "searchForMissingEpisodes": True,
            "searchForCutoffUnmetEpisodes": False,
        },
    },
    "radarr": {
        "title": "Example Film",
        "titleSlug": "example-film-603",
        "tmdbId": 603,
        "year": 1999,
        "qualityProfileId": 7,
        "rootFolderPath": "/media",
        "minimumAvailability": "released",
        "monitored": True,
        "addOptions": {"searchForMovie": True},
    },
    "lidarr": {
        "artistName": "Example Band",
        "foreignArtistId": MBID,
        "qualityProfileId": 7,
        "metadataProfileId": 1,
        "rootFolderPath": "/media",
        "monitored": True,
        "monitorNewItems": "all",
        "addOptions": {
            "monitor": "all",
            "monitored": True,
            "searchForMissingAlbums": True,
        },
    },
}


def write_backend(
    candidates: list[Any],
    *,
    item: dict[str, Any] | None = None,
    profiles: list[dict[str, Any]] | None = None,
    folders: list[dict[str, Any]] | None = None,
    metadata: list[dict[str, Any]] | None = None,
) -> Handler:
    """A routed write-capable fake upstream sharing every write fixture."""
    record = {"id": 42, "title": "Item"} if item is None else item
    data = {
        "/lookup": candidates,
        "/qualityprofile": [{"id": 7}] if profiles is None else profiles,
        "/rootfolder": [{"path": "/media"}] if folders is None else folders,
        "/metadataprofile": [{"id": 1}] if metadata is None else metadata,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        payload = next(
            (
                body
                for suffix, body in data.items()
                if request.url.path.endswith(suffix)
            ),
            None,
        )
        if payload is not None:
            return httpx.Response(200, json=payload)
        if request.method == "DELETE":
            return httpx.Response(200)
        if request.method == "PUT":
            return httpx.Response(200, json=record | {"monitored": False})
        if request.method == "POST":
            return httpx.Response(200, json=record | {"id": 42})
        return httpx.Response(200, json=record)

    return handler


async def test_search_candidates_are_projected_and_bounded(
    make_client: ClientFactory,
) -> None:
    transport = RecordingTransport(
        write_backend(
            [RADARR_CANDIDATE | {"id": 5}, RADARR_CANDIDATE | {"junk": True}, "junk"]
        )
    )
    seen = transport.requests
    result = await make_client(transport).search_candidates("radarr", "example", 1)
    assert result == {
        "service": "radarr",
        "items": [
            {
                "id": 5,
                "tmdb_id": 603,
                "title": "Example Film",
                "year": 1999,
                "status": "released",
                "in_library": True,
            }
        ],
        "upstream_truncated": True,
    }
    assert seen[0].url.path == "/api/v3/movie/lookup"
    assert seen[0].url.params["term"] == "example"
    assert seen[0].headers["x-api-key"] == "radarr-key"
    assert "radarr-key" not in str(seen[0].url)


SEARCH_OPTIONS = {
    "sonarr": "searchForMissingEpisodes",
    "radarr": "searchForMovie",
    "lidarr": "searchForMissingAlbums",
}


@pytest.mark.parametrize("service", ["sonarr", "radarr", "lidarr"])
@pytest.mark.parametrize("search", [True, False])
async def test_request_media_adds_a_broker_built_record(
    make_client: ClientFactory, service: ArrService, search: bool
) -> None:
    """The broker builds the add body alone; search_on_add only toggles searching."""
    transport = RecordingTransport(write_backend([CANDIDATES[service]]))
    seen = transport.requests
    result = await make_client(transport).request_media(
        service, EXTERNAL_IDS[service], 7, "/media/", search
    )
    post = next(request for request in seen if request.method == "POST")
    expected = EXPECTED_ADD_BODIES[service]
    assert post.url.path == ADD_RESOURCES[service]
    assert json.loads(post.content) == expected | {
        "addOptions": expected["addOptions"] | {SEARCH_OPTIONS[service]: search}
    }
    assert post.headers["x-api-key"] == f"{service}-key"
    assert f"{service}-key" not in str(post.url)
    lookup = next(r for r in seen if r.url.path.endswith("/lookup"))
    assert lookup.url.params["term"] == LOOKUP_TERMS[service]
    assert result["item"]["id"] == 42
    assert result["item"]["in_library"] is True
    assert result["search_on_add"] is search


@pytest.mark.parametrize(
    ("service", "external_id"),
    [
        ("sonarr", "81189x"),
        ("sonarr", "0"),
        ("radarr", "2147483648"),
        ("radarr", "-5"),
        ("lidarr", "not-a-musicbrainz-id"),
    ],
)
async def test_request_media_rejects_malformed_external_ids_before_any_call(
    make_client: ClientFactory, service: ArrService, external_id: str
) -> None:
    def forbidden(_: httpx.Request) -> httpx.Response:
        raise AssertionError("upstream must not be called")

    with pytest.raises(ParameterError, match="external_id"):
        await make_client(forbidden).request_media(
            service, external_id, 7, "/media", True
        )


async def test_request_media_rejects_items_already_in_the_library(
    make_client: ClientFactory,
) -> None:
    rows = [SONARR_CANDIDATE | {"id": 9}]
    transport = RecordingTransport(write_backend(rows))
    seen = transport.requests
    with pytest.raises(ParameterError, match="already in the library"):
        await make_client(transport).request_media("sonarr", "81189", 7, "/media", True)
    assert all(request.method == "GET" for request in seen)


async def test_request_media_requires_a_candidate_match(
    make_client: ClientFactory,
) -> None:
    transport = RecordingTransport(write_backend([RADARR_CANDIDATE]))
    seen = transport.requests
    with pytest.raises(ParameterError, match="no upstream candidate"):
        await make_client(transport).request_media("sonarr", "81189", 7, "/media", True)
    assert all(request.method == "GET" for request in seen)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"profiles": []}, "quality_profile_id"),
        ({"folders": []}, "root_folder_path"),
        ({"folders": [{"path": "/other"}]}, "root_folder_path"),
    ],
)
async def test_request_media_validates_local_configuration_before_adding(
    make_client: ClientFactory, overrides: dict[str, Any], message: str
) -> None:
    transport = RecordingTransport(write_backend([SONARR_CANDIDATE], **overrides))
    seen = transport.requests
    with pytest.raises(ParameterError, match=message):
        await make_client(transport).request_media("sonarr", "81189", 7, "/media", True)
    assert all(request.method == "GET" for request in seen)


async def test_lidarr_requests_use_the_lowest_metadata_profile(
    make_client: ClientFactory,
) -> None:
    transport = RecordingTransport(
        write_backend([LIDARR_CANDIDATE], metadata=[{"id": 3}, {"id": 2}])
    )
    seen = transport.requests
    await make_client(transport).request_media("lidarr", MBID, 7, "/media", True)
    post = next(request for request in seen if request.method == "POST")
    assert json.loads(post.content)["metadataProfileId"] == 2


async def test_sonarr_requests_need_season_metadata(make_client: ClientFactory) -> None:
    candidate = {k: v for k, v in SONARR_CANDIDATE.items() if k != "seasons"}
    transport = RecordingTransport(write_backend([candidate]))
    seen = transport.requests
    with pytest.raises(ParameterError, match="season"):
        await make_client(transport).request_media("sonarr", "81189", 7, "/media", True)
    assert all(request.method == "GET" for request in seen)


async def test_unmonitor_media_merges_and_returns_one_projection(
    make_client: ClientFactory,
) -> None:
    item = {
        "id": 3,
        "title": "Show",
        "monitored": True,
        "seasons": [],
        "path": "/media/Show",
        "secret": "upstream-only",
    }
    transport = RecordingTransport(write_backend([], item=item))
    seen = transport.requests
    result = await make_client(transport).unmonitor_media("sonarr", 3)
    put = next(request for request in seen if request.method == "PUT")
    assert put.url.path == "/api/v3/series/3"
    assert json.loads(put.content) == item | {"monitored": False}
    assert result == {
        "service": "sonarr",
        "item": {"id": 3, "title": "Show", "monitored": False},
    }


@pytest.mark.parametrize(("delete_files", "flag"), [(True, "true"), (False, "false")])
async def test_delete_media_is_a_two_phase_confirmed_operation(
    make_client: ClientFactory, delete_files: bool, flag: str
) -> None:
    item = {"id": 5, "title": "Old Show", "monitored": True, "path": "/media/Old Show"}
    transport = RecordingTransport(write_backend([], item=item))
    seen = transport.requests
    client = make_client(transport)
    preview = await client.delete_media("sonarr", 5, delete_files)
    assert preview["confirmation_required"] is True
    assert preview["item"] == {"id": 5, "title": "Old Show", "monitored": True}
    assert preview["delete_files"] is delete_files
    assert preview["confirmation_expires_at"] > int(time.time())
    assert [request.method for request in seen] == ["GET"]
    completed = await client.delete_media(
        "sonarr", 5, delete_files, preview["confirmation"]
    )
    assert completed == {
        "deleted": True,
        "service": "sonarr",
        "item": {"id": 5, "title": "Old Show", "monitored": True},
        "delete_files": delete_files,
    }
    assert [request.method for request in seen] == ["GET", "GET", "DELETE"]
    delete = seen[-1]
    assert delete.url.path == "/api/v3/series/5"
    assert delete.url.params["deleteFiles"] == flag
    assert delete.url.params["addImportListExclusion"] == "false"


async def test_delete_media_rejects_unbound_tampered_or_expired_confirmation(
    make_client: ClientFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    transport = RecordingTransport(write_backend([], item={"id": 5, "title": "Old"}))
    seen = transport.requests
    client = make_client(transport)
    preview = await client.delete_media("sonarr", 5, True)
    token = preview["confirmation"]
    with pytest.raises(ParameterError, match="confirmation"):
        await client.delete_media("sonarr", 5, False, token)
    with pytest.raises(ParameterError, match="confirmation"):
        await client.delete_media("radarr", 5, True, token)
    with pytest.raises(ParameterError, match="confirmation"):
        await client.delete_media("sonarr", 5, True, token + "tampered")
    # Simulate the token's TTL elapsing without sleeping.
    monkeypatch.setattr("media_broker.adapters._CONFIRM_TTL_SECONDS", -1)
    with pytest.raises(ParameterError, match="expired"):
        await client.delete_media("sonarr", 5, True, token)
    assert all(request.method == "GET" for request in seen)


# --- gated MCP write surface --------------------------------------------------


@pytest.fixture
def write_upstream() -> RecordingTransport:
    item = {"id": 3, "title": "Item", "monitored": True}
    return RecordingTransport(write_backend([SONARR_CANDIDATE], item=item))


@pytest.fixture
def mcp_writes(
    settings: Settings, write_upstream: RecordingTransport
) -> Iterator[TestClient]:
    writes = replace(settings, enable_requests=True, enable_deletes=True)
    with TestClient(build_app(writes, write_upstream)) as client:
        yield client
    assert write_upstream.closed, "upstream client must be closed when the server stops"


def test_search_candidates_answer_over_streamable_http(mcp: TestClient) -> None:
    result = call_tool(
        mcp, "arr_search_candidates", service="sonarr", query="example", limit=5
    )
    assert not result.get("isError")
    content = result["structuredContent"]
    assert content["service"] == "sonarr"
    assert content["items"][0]["title"] == "Item"
    assert content["upstream_truncated"] is False


def test_write_tools_appear_only_behind_their_gates(
    mcp: TestClient, mcp_writes: TestClient
) -> None:
    listed = rpc(mcp, "tools/list").json()["result"]["tools"]
    assert {tool["name"] for tool in listed} == TOOL_NAMES
    rejected = rpc(
        mcp,
        "tools/call",
        {"name": "arr_delete_media", "arguments": {"service": "sonarr", "item_id": 3}},
    ).json()
    assert rejected.get("error") or rejected["result"]["isError"]
    listed_writes = rpc(mcp_writes, "tools/list").json()["result"]["tools"]
    assert {tool["name"] for tool in listed_writes} == TOOL_NAMES | WRITE_TOOL_NAMES
    annotations = {tool["name"]: tool["annotations"] for tool in listed_writes}
    assert annotations["arr_request_media"]["readOnlyHint"] is False
    assert annotations["arr_request_media"]["idempotentHint"] is False
    assert annotations["arr_unmonitor_media"]["idempotentHint"] is True
    assert annotations["arr_delete_media"]["readOnlyHint"] is False
    assert annotations["arr_delete_media"]["destructiveHint"] is True


def test_request_and_unmonitor_answer_over_streamable_http(
    mcp_writes: TestClient, write_upstream: RecordingTransport
) -> None:
    requested = call_tool(
        mcp_writes,
        "arr_request_media",
        service="sonarr",
        external_id="81189",
        quality_profile_id=7,
        root_folder_path="/media",
    )
    assert not requested.get("isError")
    content = requested["structuredContent"]
    assert content["item"]["id"] == 42
    assert content["search_on_add"] is True
    assert len([r for r in write_upstream.requests if r.method == "POST"]) == 1
    unmonitored = call_tool(
        mcp_writes, "arr_unmonitor_media", service="sonarr", item_id=3
    )
    assert not unmonitored.get("isError")
    assert unmonitored["structuredContent"]["item"]["monitored"] is False
    assert any(r.method == "PUT" for r in write_upstream.requests)


def test_delete_media_flows_over_streamable_http_with_confirmation(
    mcp_writes: TestClient, write_upstream: RecordingTransport
) -> None:
    preview = call_tool(mcp_writes, "arr_delete_media", service="sonarr", item_id=3)
    assert not preview.get("isError")
    content = preview["structuredContent"]
    assert content["confirmation_required"] is True
    assert content["delete_files"] is False
    assert not any(r.method == "DELETE" for r in write_upstream.requests)
    completed = call_tool(
        mcp_writes,
        "arr_delete_media",
        service="sonarr",
        item_id=3,
        confirmation=content["confirmation"],
    )
    assert not completed.get("isError")
    assert completed["structuredContent"]["deleted"] is True
    deletes = [r for r in write_upstream.requests if r.method == "DELETE"]
    assert len(deletes) == 1
    assert deletes[0].url.params["deleteFiles"] == "false"


@pytest.mark.parametrize(
    ("name", "arguments", "field"),
    [
        ("arr_search_candidates", {"service": "sonarr", "query": ""}, "query"),
        (
            "arr_search_candidates",
            {"service": "sonarr", "query": "x", "limit": 0},
            "limit",
        ),
        (
            "arr_request_media",
            {
                "service": "radarr",
                "external_id": "x" * 65,
                "quality_profile_id": 7,
                "root_folder_path": "/media",
            },
            "external_id",
        ),
        (
            "arr_request_media",
            {
                "service": "radarr",
                "external_id": "603",
                "quality_profile_id": 0,
                "root_folder_path": "/media",
            },
            "quality_profile_id",
        ),
        (
            "arr_request_media",
            {
                "service": "radarr",
                "external_id": "603",
                "quality_profile_id": 7,
                "root_folder_path": "",
            },
            "root_folder_path",
        ),
        ("arr_unmonitor_media", {"service": "sonarr", "item_id": 0}, "item_id"),
        ("arr_delete_media", {"service": "sonarr", "item_id": 2**31}, "item_id"),
        (
            "arr_delete_media",
            {"service": "sonarr", "item_id": 3, "confirmation": "x" * 129},
            "confirmation",
        ),
    ],
)
def test_write_tool_arguments_are_rejected_before_any_upstream_call(
    mcp_writes: TestClient,
    write_upstream: RecordingTransport,
    name: str,
    arguments: dict[str, Any],
    field: str,
) -> None:
    result = call_tool(mcp_writes, name, **arguments)
    assert result["isError"] is True
    assert field in result["content"][0]["text"]
    assert write_upstream.requests == []


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
