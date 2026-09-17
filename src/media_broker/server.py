"""MCP server exposing only bounded, read-only media inventory queries."""

import secrets
from collections.abc import Awaitable, Iterable
from typing import Annotated, Any

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from pydantic import Field
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from .adapters import (
    ArrService,
    MediaClient,
    ParameterError,
    TautulliMediaType,
    UpstreamError,
)
from .config import Settings, load_settings

_READ_ONLY = ToolAnnotations(
    readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
)
# Argument bounds live in the tool schema so callers see them before calling.
Page = Annotated[int, Field(ge=1, le=100_000)]
PageSize = Annotated[int, Field(ge=1, le=100)]
Search = Annotated[str | None, Field(max_length=200)]
IsoDate = Annotated[str, Field(pattern=r"^\d{4}-\d{2}-\d{2}$")]
UserId = Annotated[int | None, Field(ge=0, le=2_147_483_647)]


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
        instructions="Read-only, projected media inventory and playback history.",
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

    return mcp


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
