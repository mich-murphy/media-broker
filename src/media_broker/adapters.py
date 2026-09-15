"""Small read-only adapters with bounded responses and explicit projections."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date
from typing import Any, Literal

import httpx

from .config import Settings, Upstream

ArrService = Literal["sonarr", "radarr", "lidarr"]
TautulliMediaType = Literal["movie", "episode", "track"]


class UpstreamError(RuntimeError):
    """A sanitized upstream failure suitable for returning from an MCP tool."""


class ParameterError(ValueError):
    """A tool argument is outside the broker's safe bounds."""


def _disable_credential_logging() -> None:
    """Disable HTTPX/httpcore request logs, which include complete URLs."""
    for logger_name in ("httpx", "httpcore"):
        logger = logging.getLogger(logger_name)
        logger.disabled = True
        logger.setLevel(logging.CRITICAL + 1)


async def _bounded_body(response: httpx.Response, maximum: int) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > maximum:
                raise UpstreamError(
                    "upstream response exceeds the configured size limit"
                )
        except ValueError as exc:
            raise UpstreamError("upstream returned an invalid response length") from exc
    chunks: list[bytes] = []
    size = 0
    async for chunk in response.aiter_bytes():
        size += len(chunk)
        if size > maximum:
            raise UpstreamError("upstream response exceeds the configured size limit")
        chunks.append(chunk)
    return b"".join(chunks)


class MediaClient:
    """HTTP client for the four allow-listed upstream APIs."""

    def __init__(
        self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None
    ):
        _disable_credential_logging()
        self.settings = settings
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=httpx.Timeout(settings.timeout_seconds),
            verify=True,
            trust_env=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _json(
        self, upstream: Upstream, path: str, *, params: dict[str, Any] | None = None
    ) -> Any:
        headers = {"X-Api-Key": upstream.api_key}
        try:
            async with asyncio.timeout(self.settings.timeout_seconds):
                async with self._client.stream(
                    "GET", f"{upstream.base_url}{path}", headers=headers, params=params
                ) as response:
                    if 300 <= response.status_code < 400:
                        raise UpstreamError("upstream redirects are not accepted")
                    if response.status_code >= 400:
                        raise UpstreamError(
                            f"upstream returned HTTP {response.status_code}"
                        )
                    body = await _bounded_body(
                        response, self.settings.max_response_bytes
                    )
        except UpstreamError:
            raise
        except TimeoutError as exc:
            raise UpstreamError("upstream request timed out") from exc
        except (httpx.HTTPError, OSError) as exc:
            raise UpstreamError("upstream request failed") from exc
        try:
            return json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UpstreamError("upstream returned invalid JSON") from exc

    def _arr(self, service: ArrService) -> Upstream:
        return self.settings.upstreams[service]

    async def inventory(
        self, service: ArrService, page: int, page_size: int, search: str | None = None
    ) -> dict[str, Any]:
        _validate_page(page, page_size)
        if search is not None and (not isinstance(search, str) or len(search) > 200):
            raise ParameterError("search must be at most 200 characters")
        endpoint = {
            "sonarr": "/api/v3/series",
            "radarr": "/api/v3/movie",
            "lidarr": "/api/v1/artist",
        }[service]
        payload = await self._json(self._arr(service), endpoint)
        if not isinstance(payload, list):
            raise UpstreamError("upstream returned an unexpected inventory")
        if len(payload) > self.settings.max_items:
            raise UpstreamError("upstream inventory exceeds the configured item limit")
        query = search.strip().casefold() if search else None
        projected = [
            _project_arr_item(item) for item in payload if isinstance(item, dict)
        ]
        if query:
            projected = [
                item for item in projected if query in (item["title"] or "").casefold()
            ]
        start = (page - 1) * page_size
        items = projected[start : start + page_size]
        return {
            "service": service,
            "items": items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "returned_count": len(items),
                "total_count": len(projected),
                "has_more": start + len(items) < len(projected),
                "upstream_paginated": False,
                "upstream_truncated": False,
            },
        }

    async def quality_profiles(self, service: ArrService) -> dict[str, Any]:
        payload = await self._json(
            self._arr(service),
            "/api/" + self._arr(service).api_version + "/qualityprofile",
        )
        if not isinstance(payload, list) or len(payload) > self.settings.max_items:
            raise UpstreamError(
                "upstream returned an unexpected quality profile response"
            )
        items = []
        for item in payload:
            if isinstance(item, dict):
                items.append(
                    {
                        "id": _integer(item.get("id")),
                        "name": _string(item.get("name")),
                        "upgrade_allowed": _boolean(item.get("upgradeAllowed")),
                        "cutoff": _integer(item.get("cutoff")),
                    }
                )
        return {"service": service, "items": items, "upstream_truncated": False}

    async def root_folders(self, service: ArrService) -> dict[str, Any]:
        payload = await self._json(
            self._arr(service), "/api/" + self._arr(service).api_version + "/rootfolder"
        )
        if not isinstance(payload, list) or len(payload) > self.settings.max_items:
            raise UpstreamError("upstream returned an unexpected root folder response")
        items = []
        for item in payload:
            if isinstance(item, dict):
                items.append(
                    {
                        "id": _integer(item.get("id")),
                        "path": _string(item.get("path")),
                        "accessible": _boolean(item.get("accessible")),
                        "free_space_bytes": _integer(item.get("freeSpace")),
                        "total_space_bytes": _integer(item.get("totalSpace")),
                    }
                )
        return {"service": service, "items": items, "upstream_truncated": False}

    async def history(
        self,
        media_type: TautulliMediaType,
        start_date: str,
        end_date: str,
        page: int,
        page_size: int,
        user_id: int | None = None,
    ) -> dict[str, Any]:
        _validate_page(page, page_size)
        _validate_user_id(user_id)
        start, end = _validate_dates(start_date, end_date)
        offset = (page - 1) * page_size
        upstream = self.settings.upstreams["tautulli"]
        params: dict[str, Any] = {
            "cmd": "get_history",
            "start": offset,
            "length": page_size,
            "media_type": media_type,
            # Tautulli uses inclusive date bounds named after/before.
            "after": start.isoformat(),
            "before": end.isoformat(),
            # Request discrete events, not grouped/activity-expanded rows.
            "grouping": 0,
            "include_activity": 0,
        }
        if user_id is not None:
            params["user_id"] = user_id
        payload = await self._json(upstream, "/api/v2", params=params)
        if not isinstance(payload, dict):
            raise UpstreamError("upstream returned an unexpected history response")
        response = payload.get("response")
        if not isinstance(response, dict) or response.get("result") != "success":
            raise UpstreamError("upstream history request was not successful")
        response_data = response.get("data")
        if not isinstance(response_data, dict):
            raise UpstreamError("upstream returned an unexpected history response")
        rows = response_data.get("data")
        if not isinstance(rows, list) or len(rows) > page_size:
            raise UpstreamError("upstream returned an unexpected history page")
        items = [
            _project_history_item(row, media_type)
            for row in rows
            if isinstance(row, dict)
            and row.get("media_type", media_type) == media_type
            and _matches_user_id(row.get("user_id"), user_id)
        ]
        total = response_data.get("recordsFiltered")
        if not isinstance(total, int) or total < offset + len(rows):
            total = None
        return {
            "media_type": media_type,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "user_id_filter": user_id,
            "items": items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "returned_count": len(items),
                "upstream_total_count": total,
                "has_more": (total is not None and offset + len(rows) < total)
                or (total is None and len(rows) == page_size),
                "upstream_paginated": True,
                "upstream_truncated": total is None and len(rows) == page_size,
            },
        }


def _validate_page(page: int, page_size: int) -> None:
    if not isinstance(page, int) or isinstance(page, bool) or not 1 <= page <= 100_000:
        raise ParameterError("page must be between 1 and 100000")
    if (
        not isinstance(page_size, int)
        or isinstance(page_size, bool)
        or not 1 <= page_size <= 100
    ):
        raise ParameterError("page_size must be between 1 and 100")


def _validate_user_id(user_id: int | None) -> None:
    if user_id is not None and (
        not isinstance(user_id, int)
        or isinstance(user_id, bool)
        or not 0 <= user_id <= 2_147_483_647
    ):
        raise ParameterError("user_id must be a non-negative integer")


def _validate_dates(start_value: str, end_value: str) -> tuple[date, date]:
    if (
        not isinstance(start_value, str)
        or not isinstance(end_value, str)
        or len(start_value) != 10
        or len(end_value) != 10
        or start_value[4] != "-"
        or start_value[7] != "-"
        or end_value[4] != "-"
        or end_value[7] != "-"
        or not start_value.replace("-", "").isdigit()
        or not end_value.replace("-", "").isdigit()
    ):
        raise ParameterError("dates must use strict YYYY-MM-DD")
    try:
        start = date.fromisoformat(start_value)
        end = date.fromisoformat(end_value)
    except (TypeError, ValueError) as exc:
        raise ParameterError("dates must use strict YYYY-MM-DD") from exc
    if end < start or (end - start).days > 30:
        raise ParameterError("date range must be at most 31 calendar days inclusive")
    return start, end


def _matches_user_id(value: Any, requested: int | None) -> bool:
    if requested is None:
        return True
    return value == requested or value == str(requested)


def _integer(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _number(value: Any) -> int | float | None:
    return (
        value
        if isinstance(value, (int, float)) and not isinstance(value, bool)
        else None
    )


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _identifier(value: Any) -> int | str | None:
    return (
        value if isinstance(value, (int, str)) and not isinstance(value, bool) else None
    )


def _project_arr_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _integer(item.get("id")),
        "title": _string(item.get("title") or item.get("artistName")),
        "year": _integer(item.get("year")),
        "status": _string(item.get("status")),
        "monitored": _boolean(item.get("monitored")),
        "quality_profile_id": _integer(item.get("qualityProfileId")),
        "root_folder_path": _string(item.get("rootFolderPath")),
    }


def _completion(value: Any) -> bool | None:
    # Tautulli 2.18.1 emits numeric quarter-step watched statuses.
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return None
    if value == 1:
        return True
    if value in (0, 0.25, 0.5, 0.75):
        return False
    return None


def _project_history_item(
    item: dict[str, Any], media_type: TautulliMediaType
) -> dict[str, Any]:
    # These stable opaque identifiers are intentionally source-prefixed and approved.
    return {
        "media_type": media_type,
        "tautulli_user_id": _identifier(item.get("user_id")),
        "tautulli_rating_key": _identifier(item.get("rating_key")),
        "tautulli_history_id": _identifier(item.get("id")),
        "title": _string(item.get("title")),
        "parent_title": _string(item.get("parent_title")),
        "grandparent_title": _string(item.get("grandparent_title")),
        "played_at": _number(item.get("started") or item.get("started_at")),
        "duration_seconds": _number(item.get("duration")),
        "completed": _completion(item.get("watched_status")),
    }
