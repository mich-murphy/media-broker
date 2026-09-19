"""MCP server exposing bounded, projected inventory, history, and gated writes."""

import secrets
from collections.abc import Awaitable, Iterable
from datetime import date
from typing import Annotated, Any

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import AfterValidator, BeforeValidator, Field
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from .adapters import (
    ArrService,
    JellyfinMediaType,
    LidarrService,
    MediaClient,
    ParameterError,
    SonarrService,
    TautulliMediaType,
    UpstreamError,
    is_jellyfin_user_id,
)
from .config import Settings, load_settings

_READ_ONLY = ToolAnnotations(
    readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
)
_WRITE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=False, idempotentHint=False, openWorldHint=False
)
_UNMONITOR = ToolAnnotations(
    readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False
)
_DESTRUCTIVE = ToolAnnotations(
    readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False
)


def _iso_date(value: Any) -> Any:
    """Accept JSON strings only; pydantic then coerces to a calendar date."""
    if not isinstance(value, str):
        raise ValueError("must be an ISO 8601 calendar date")
    return value


def _hex_user_id(value: str) -> str:
    if not is_jellyfin_user_id(value):
        raise ValueError("must contain exactly 32 hexadecimal characters")
    return value


# Argument bounds live in the tool schema so callers see them before calling.
Page = Annotated[int, Field(ge=1, le=100_000)]
PageSize = Annotated[int, Field(ge=1, le=100)]
Search = Annotated[str | None, Field(max_length=200)]
IsoDate = Annotated[date, BeforeValidator(_iso_date)]
UserId = Annotated[int | None, Field(ge=0, le=2_147_483_647)]
JellyfinUserId = Annotated[
    str, Field(min_length=32, max_length=32), AfterValidator(_hex_user_id)
]
TimezoneOffset = Annotated[float, Field(ge=-14, le=14)]
Query = Annotated[str, Field(min_length=1, max_length=200)]
Limit = Annotated[int, Field(ge=1, le=100)]
ExternalId = Annotated[str, Field(min_length=1, max_length=64)]
Identifier = Annotated[int, Field(ge=1, le=2_147_483_647)]
RootPath = Annotated[str, Field(min_length=1, max_length=1024)]
Confirmation = Annotated[str | None, Field(max_length=128)]
SeasonSelection = Annotated[
    list[Annotated[int, Field(ge=0, le=1000)]], Field(min_length=1, max_length=100)
]


class BearerMiddleware:
    """Require the configured bearer on every HTTP route, including MCP discovery."""

    def __init__(self, app: ASGIApp, token: str) -> None:
        self.app = app
        self._token = token.encode()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and not self._authorized(scope["headers"]):
            response = JSONResponse(
                {"error": "authentication required"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)

    def _authorized(self, headers: Iterable[tuple[bytes, bytes]]) -> bool:
        values = [value for name, value in headers if name == b"authorization"]
        if len(values) != 1:
            return False
        scheme, _, candidate = values[0].partition(b" ")
        return scheme.lower() == b"bearer" and secrets.compare_digest(
            candidate, self._token
        )


class ClosingMiddleware:
    """Close the shared upstream client after the server lifespan ends."""

    def __init__(self, app: ASGIApp, client: MediaClient) -> None:
        self.app = app
        self.client = client

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await self.app(scope, receive, send)
        finally:
            if scope["type"] == "lifespan":
                await self.client.aclose()


async def _guarded(operation: Awaitable[dict[str, Any]]) -> dict[str, Any]:
    """Surface only sanitized broker errors; internals never reach the caller."""
    try:
        return await operation
    except (ParameterError, UpstreamError):
        raise
    except Exception as exc:
        raise ToolError("operation failed") from exc


def create_mcp(settings: Settings, client: MediaClient) -> FastMCP:
    """Construct an SDK-backed MCP server whose tools accept only bounded input."""
    mcp = FastMCP(
        name="media-broker",
        instructions=(
            "Projected media inventory and playback history; media requests and "
            "library cleanup are available only where explicitly enabled."
        ),
        streamable_http_path="/mcp",
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=list(settings.allowed_hosts),
            allowed_origins=list(settings.allowed_origins),
        ),
    )

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_library_inventory(
        service: ArrService,
        page: Page = 1,
        page_size: PageSize = 50,
        search: Search = None,
    ) -> dict[str, Any]:
        """Return a bounded page of Sonarr, Radarr, or Lidarr library inventory."""
        return await _guarded(client.inventory(service, page, page_size, search))

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_quality_profiles(service: ArrService) -> dict[str, Any]:
        """Return projected quality-profile summaries for one Arr service."""
        return await _guarded(client.quality_profiles(service))

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_root_folders(service: ArrService) -> dict[str, Any]:
        """Return projected root-folder summaries for one Arr service."""
        return await _guarded(client.root_folders(service))

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_search_candidates(
        service: ArrService, query: Query, limit: Limit = 10
    ) -> dict[str, Any]:
        """Search one Arr catalog for requestable media by title or keyword."""
        return await _guarded(client.search_candidates(service, query, limit))

    _register_history_tools(mcp, client)
    _register_detail_read_tools(mcp, client)
    if settings.enable_requests:
        _register_request_tools(mcp, client)
    if settings.enable_deletes:
        _register_delete_tools(mcp, client)

    return mcp


def _register_detail_read_tools(mcp: FastMCP, client: MediaClient) -> None:
    """Register the always-on season and album detail read tools."""

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_season_inventory(
        service: SonarrService, item_id: Identifier
    ) -> dict[str, Any]:
        """Return per-season monitoring and episode file counts for one series."""
        return await _guarded(client.season_inventory(service, item_id))

    @mcp.tool(annotations=_READ_ONLY)
    async def arr_album_inventory(
        service: LidarrService, artist_id: Identifier
    ) -> dict[str, Any]:
        """Return per-album monitoring, file, and size details for one artist."""
        return await _guarded(client.album_inventory(service, artist_id))


def _register_history_tools(mcp: FastMCP, client: MediaClient) -> None:
    """Register the always-on playback-history read tools."""

    @mcp.tool(annotations=_READ_ONLY)
    async def jellyfin_users() -> dict[str, Any]:
        """List Jellyfin users as id/name pairs for jellyfin_play_history."""
        return await _guarded(client.jellyfin_users())

    @mcp.tool(annotations=_READ_ONLY)
    async def jellyfin_play_history(
        user_id: JellyfinUserId,
        media_type: JellyfinMediaType,
        start_date: IsoDate,
        end_date: IsoDate,
        page: Page = 1,
        page_size: PageSize = 50,
        timezone_offset: TimezoneOffset = 0,
    ) -> dict[str, Any]:
        """Return sanitized Playback Reporting history over at most 31 days."""
        return await _guarded(
            client.jellyfin_history(
                user_id,
                media_type,
                start_date,
                end_date,
                page,
                page_size,
                timezone_offset,
            )
        )

    @mcp.tool(annotations=_READ_ONLY)
    async def tautulli_play_history(
        media_type: TautulliMediaType,
        start_date: IsoDate,
        end_date: IsoDate,
        page: Page = 1,
        page_size: PageSize = 50,
        user_id: UserId = None,
    ) -> dict[str, Any]:
        """Return sanitized playback history for one media type over at most 31 days."""
        return await _guarded(
            client.history(media_type, start_date, end_date, page, page_size, user_id)
        )


def _register_request_tools(mcp: FastMCP, client: MediaClient) -> None:
    """Register brokered add and unmonitor tools behind the requests gate."""

    @mcp.tool(annotations=_WRITE)
    async def arr_request_media(
        service: ArrService,
        external_id: ExternalId,
        quality_profile_id: Identifier,
        root_folder_path: RootPath,
        search_on_add: bool = True,
        seasons: SeasonSelection | None = None,
    ) -> dict[str, Any]:
        """Add one exact catalog item; searching for downloads is the default.

        Sonarr accepts an optional seasons list so only those seasons are
        monitored and searched. For a selective Lidarr album add: add with
        search_on_add=false, then arr_album_inventory, arr_set_album_monitored,
        and arr_search_item.
        """
        return await _guarded(
            client.request_media(
                service,
                external_id,
                quality_profile_id,
                root_folder_path,
                search_on_add,
                seasons,
            )
        )

    @mcp.tool(annotations=_UNMONITOR)
    async def arr_unmonitor_media(
        service: ArrService, item_id: Identifier
    ) -> dict[str, Any]:
        """Stop monitoring one library item, keeping metadata and files."""
        return await _guarded(client.set_monitoring(service, item_id, False))

    @mcp.tool(annotations=_UNMONITOR)
    async def arr_monitor_media(
        service: ArrService, item_id: Identifier
    ) -> dict[str, Any]:
        """Resume monitoring one library item, reversing arr_unmonitor_media."""
        return await _guarded(client.set_monitoring(service, item_id, True))

    @mcp.tool(annotations=_UNMONITOR)
    async def arr_set_season_monitoring(
        service: SonarrService,
        item_id: Identifier,
        seasons: SeasonSelection,
        monitored: bool,
    ) -> dict[str, Any]:
        """Monitor or unmonitor a set of seasons on one series; reversible."""
        return await _guarded(
            client.set_season_monitoring(service, item_id, seasons, monitored)
        )

    @mcp.tool(annotations=_UNMONITOR)
    async def arr_set_album_monitored(
        service: LidarrService, album_id: Identifier, monitored: bool
    ) -> dict[str, Any]:
        """Monitor or unmonitor one Lidarr album; reversible in both directions."""
        return await _guarded(client.set_album_monitored(service, album_id, monitored))

    @mcp.tool(annotations=_WRITE)
    async def arr_search_item(
        service: ArrService, item_id: Identifier
    ) -> dict[str, Any]:
        """Queue an upstream search for one item's monitored missing content."""
        return await _guarded(client.search_item(service, item_id))


def _register_delete_tools(mcp: FastMCP, client: MediaClient) -> None:
    """Register the two-phase destructive delete tool behind the deletes gate."""

    @mcp.tool(annotations=_DESTRUCTIVE)
    async def arr_delete_media(
        service: ArrService,
        item_id: Identifier,
        delete_files: bool = False,
        confirmation: Confirmation = None,
        album_id: Identifier | None = None,
    ) -> dict[str, Any]:
        """Delete one library item behind a confirmed two-phase preview.

        For Lidarr, an optional album_id deletes a single album of the artist
        identified by item_id instead of the whole artist.
        """
        return await _guarded(
            client.delete_media(service, item_id, delete_files, confirmation, album_id)
        )


def build_app(
    settings: Settings, transport: httpx.AsyncBaseTransport | None = None
) -> ASGIApp:
    """Build the authenticated Streamable HTTP ASGI application for uvicorn or tests."""
    client = MediaClient(settings, transport)
    app = create_mcp(settings, client).streamable_http_app()
    return ClosingMiddleware(BearerMiddleware(app, settings.bearer_token), client)


def main() -> None:
    settings = load_settings()
    uvicorn.run(
        build_app(settings),
        host=settings.bind_host,
        port=settings.port,
        server_header=False,
    )
