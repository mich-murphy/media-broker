"""MCP server exposing only bounded, read-only media inventory queries."""

from __future__ import annotations

import asyncio
import secrets
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.types import Receive, Scope, Send

from .adapters import (
    ArrService,
    MediaClient,
    ParameterError,
    TautulliMediaType,
    UpstreamError,
)
from .config import Settings, load_settings


READ_ONLY_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
)


class BearerMiddleware:
    """Require the configured bearer on every HTTP route, including MCP discovery."""

    def __init__(self, app: Any, token: str):
        self.app = app
        self.token = token

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers", []))
        authorization = headers.get(b"authorization", b"").decode("latin-1")
        scheme, separator, candidate = authorization.partition(" ")
        valid = (
            separator == " "
            and scheme.casefold() == "bearer"
            and bool(candidate)
            and len(candidate) <= 4096
            and secrets.compare_digest(candidate, self.token)
        )
        if not valid:
            body = b'{"error":"authentication required"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(body)).encode()),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": body})
            return
        await self.app(scope, receive, send)


def create_mcp(settings: Settings, client: MediaClient | None = None) -> FastMCP:
    """Construct an SDK-backed MCP server; no tool accepts arbitrary upstream input."""
    owned_client = client or MediaClient(settings)
    mcp = FastMCP(
        name="media-broker",
        instructions="Read-only, projected media-service inventory and household playback history.",
        host=settings.bind_host,
        port=settings.port,
        streamable_http_path="/mcp",
        stateless_http=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=list(settings.allowed_hosts),
            allowed_origins=list(settings.allowed_origins),
        ),
    )

    @mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
    async def arr_library_inventory(
        service: ArrService,
        page: int = 1,
        page_size: int = 50,
        search: str | None = None,
    ) -> dict[str, Any]:
        """Return a bounded page of Sonarr, Radarr, or Lidarr library inventory."""
        return await _safe(
            lambda: owned_client.inventory(service, page, page_size, search)
        )

    @mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
    async def arr_quality_profiles(service: ArrService) -> dict[str, Any]:
        """Return projected quality-profile summaries for one Arr service."""
        return await _safe(lambda: owned_client.quality_profiles(service))

    @mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
    async def arr_root_folders(service: ArrService) -> dict[str, Any]:
        """Return projected root-folder summaries for one Arr service."""
        return await _safe(lambda: owned_client.root_folders(service))

    @mcp.tool(annotations=READ_ONLY_ANNOTATIONS)
    async def tautulli_play_history(
        media_type: TautulliMediaType,
        start_date: str,
        end_date: str,
        page: int = 1,
        page_size: int = 50,
        user_id: int | None = None,
    ) -> dict[str, Any]:
        """Return sanitized playback history for one media type and <=31 inclusive days."""
        return await _safe(
            lambda: owned_client.history(
                media_type, start_date, end_date, page, page_size, user_id
            )
        )

    return mcp


async def _safe(operation: Any) -> dict[str, Any]:
    try:
        return await operation()
    except ParameterError as exc:
        # Tool callers receive validation without implementation details.
        return {"error": str(exc)}
    except UpstreamError as exc:
        return {"error": str(exc)}
    except (KeyError, TypeError):
        return {"error": "invalid service or parameters"}
    except Exception:
        # Do not let SDK/client internals or upstream payloads reach the caller.
        return {"error": "operation failed"}


class ClosingMiddleware:
    """Close the shared upstream client after the server lifespan ends."""

    def __init__(self, app: Any, client: MediaClient):
        self.app = app
        self.client = client

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "lifespan":
            await self.app(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send)
        finally:
            await self.client.aclose()


def build_app(settings: Settings) -> Any:
    """Build the authenticated Streamable HTTP ASGI application for tests or uvicorn."""
    client = MediaClient(settings)
    mcp_app = create_mcp(settings, client).streamable_http_app()
    return ClosingMiddleware(BearerMiddleware(mcp_app, settings.bearer_token), client)


def main() -> None:
    import uvicorn

    settings = load_settings()
    app = build_app(settings)
    asyncio.run(
        uvicorn.Server(
            uvicorn.Config(app, host=settings.bind_host, port=settings.port)
        ).serve()
    )
