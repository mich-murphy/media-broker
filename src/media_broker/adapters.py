"""Small read-only adapters with bounded responses and explicit projections."""

import asyncio
import json
import logging
from datetime import date
from typing import Any, Literal

import httpx

from .config import Settings, Upstream

ArrService = Literal["sonarr", "radarr", "lidarr"]
TautulliMediaType = Literal["movie", "episode", "track"]

_MAX_ITEMS = 10_000
_INVENTORY_RESOURCE = {"sonarr": "series", "radarr": "movie", "lidarr": "artist"}


class UpstreamError(RuntimeError):
    """A sanitized upstream failure suitable for returning from an MCP tool."""


class ParameterError(ValueError):
    """A tool argument is outside the broker's safe bounds."""


async def _bounded_body(response: httpx.Response, maximum: int) -> bytes:
    declared = response.headers.get("content-length", "0")
    if not declared.isdigit() or int(declared) > maximum:
        raise UpstreamError("upstream response exceeds the configured size limit")
    body = bytearray()
    async for chunk in response.aiter_bytes():
        body.extend(chunk)
        if len(body) > maximum:
            raise UpstreamError("upstream response exceeds the configured size limit")
    return bytes(body)


class MediaClient:
    """HTTP client for the four allow-listed upstream APIs."""

    def __init__(
        self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        # HTTPX and httpcore request logs contain complete upstream URLs.
        for name in ("httpx", "httpcore"):
            logging.getLogger(name).disabled = True
        self.settings = settings
        self._client = httpx.AsyncClient(
            follow_redirects=False,
            timeout=settings.timeout_seconds,
            verify=True,
            trust_env=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _json(
        self, upstream: Upstream, path: str, params: dict[str, Any] | None = None
    ) -> Any:
        headers = {"X-Api-Key": upstream.api_key}
        try:
            async with (
                asyncio.timeout(self.settings.timeout_seconds),
                self._client.stream(
                    "GET", upstream.base_url + path, headers=headers, params=params
                ) as response,
            ):
                if response.status_code >= 300:
                    raise UpstreamError(
                        f"upstream returned HTTP {response.status_code}"
                    )
                body = await _bounded_body(response, self.settings.max_response_bytes)
        except TimeoutError as exc:
            raise UpstreamError("upstream request timed out") from exc
        except (httpx.HTTPError, OSError) as exc:
            raise UpstreamError("upstream request failed") from exc
        try:
            return json.loads(body)
        except ValueError as exc:
            raise UpstreamError("upstream returned invalid JSON") from exc

    async def _arr_items(
        self, service: ArrService, resource: str
    ) -> list[dict[str, Any]]:
        upstream = self.settings.upstreams[service]
        payload = await self._json(upstream, f"/api/{upstream.api_version}/{resource}")
        if not isinstance(payload, list) or len(payload) > _MAX_ITEMS:
            raise UpstreamError(f"upstream returned an unexpected {resource} response")
        return [item for item in payload if isinstance(item, dict)]

    async def inventory(
        self, service: ArrService, page: int, page_size: int, search: str | None = None
    ) -> dict[str, Any]:
        resource = _INVENTORY_RESOURCE[service]
        items = [_project_arr_item(i) for i in await self._arr_items(service, resource)]
        if query := (search or "").strip().casefold():
            items = [i for i in items if query in (i["title"] or "").casefold()]
        start = (page - 1) * page_size
        page_items = items[start : start + page_size]
        return {
            "service": service,
            "items": page_items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "returned_count": len(page_items),
                "total_count": len(items),
                "has_more": start + len(page_items) < len(items),
                "upstream_paginated": False,
                "upstream_truncated": False,
            },
        }

    async def quality_profiles(self, service: ArrService) -> dict[str, Any]:
        payload = await self._arr_items(service, "qualityprofile")
        items = [_project_quality_profile(item) for item in payload]
        return {"service": service, "items": items, "upstream_truncated": False}

    async def root_folders(self, service: ArrService) -> dict[str, Any]:
        payload = await self._arr_items(service, "rootfolder")
        items = [_project_root_folder(item) for item in payload]
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
        start, end = _date_range(start_date, end_date)
        offset = (page - 1) * page_size
        params: dict[str, Any] = {
            "cmd": "get_history",
            "start": offset,
            "length": page_size,
            "media_type": media_type,
            # Tautulli's inclusive date bounds are named after/before.
            "after": start.isoformat(),
            "before": end.isoformat(),
            # Discrete playback events, not grouped or activity-expanded rows.
            "grouping": 0,
            "include_activity": 0,
        }
        if user_id is not None:
            params["user_id"] = user_id
        payload = await self._json(
            self.settings.upstreams["tautulli"], "/api/v2", params
        )
        rows, total = _history_page(payload, page_size)
        # Re-apply the requested filters locally so a misbehaving upstream
        # can never return another household member's rows.
        items = [
            _project_history_item(row, media_type)
            for row in rows
            if row.get("media_type", media_type) == media_type
            and (user_id is None or row.get("user_id") in (user_id, str(user_id)))
        ]
        if total is not None and total < offset + len(rows):
            total = None
        full_page = len(rows) == page_size
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
                "has_more": full_page if total is None else offset + len(rows) < total,
                "upstream_paginated": True,
                "upstream_truncated": total is None and full_page,
            },
        }


def _date_range(start_value: str, end_value: str) -> tuple[date, date]:
    try:
        start, end = date.fromisoformat(start_value), date.fromisoformat(end_value)
    except ValueError as exc:
        raise ParameterError("dates must be valid YYYY-MM-DD calendar dates") from exc
    if not 0 <= (end - start).days <= 30:
        raise ParameterError(
            "end_date must be on or after start_date and within 31 inclusive days"
        )
    return start, end


def _history_page(
    payload: Any, page_size: int
) -> tuple[list[dict[str, Any]], int | None]:
    """Unwrap Tautulli's success envelope into row dicts and the filtered total."""
    response = payload.get("response") if isinstance(payload, dict) else None
    if not isinstance(response, dict) or response.get("result") != "success":
        raise UpstreamError("upstream history request was not successful")
    data = response.get("data")
    if not isinstance(data, dict) or not isinstance(rows := data.get("data"), list):
        raise UpstreamError("upstream returned an unexpected history response")
    if len(rows) > page_size:
        raise UpstreamError("upstream returned an unexpected history response")
    dict_rows = [row for row in rows if isinstance(row, dict)]
    return dict_rows, _scalar(data.get("recordsFiltered"), int)


def _scalar(value: Any, *types: type) -> Any:
    """Return value only when it is exactly one of the given JSON scalar types."""
    if isinstance(value, bool) and bool not in types:
        return None
    return value if isinstance(value, types) else None


def _project_arr_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _scalar(item.get("id"), int),
        "title": _scalar(item.get("title") or item.get("artistName"), str),
        "year": _scalar(item.get("year"), int),
        "status": _scalar(item.get("status"), str),
        "monitored": _scalar(item.get("monitored"), bool),
        "quality_profile_id": _scalar(item.get("qualityProfileId"), int),
        "root_folder_path": _scalar(item.get("rootFolderPath"), str),
    }


def _project_quality_profile(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _scalar(item.get("id"), int),
        "name": _scalar(item.get("name"), str),
        "upgrade_allowed": _scalar(item.get("upgradeAllowed"), bool),
        "cutoff": _scalar(item.get("cutoff"), int),
    }


def _project_root_folder(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _scalar(item.get("id"), int),
        "path": _scalar(item.get("path"), str),
        "accessible": _scalar(item.get("accessible"), bool),
        "free_space_bytes": _scalar(item.get("freeSpace"), int),
        "total_space_bytes": _scalar(item.get("totalSpace"), int),
    }


def _completion(value: Any) -> bool | None:
    # Tautulli 2.18.1 emits numeric quarter-step watched statuses; 1 is complete.
    status = _scalar(value, int, float)
    return None if status not in (0, 0.25, 0.5, 0.75, 1) else status == 1


def _project_history_item(item: dict[str, Any], media_type: str) -> dict[str, Any]:
    # The source-prefixed identifiers are approved for household matching.
    return {
        "media_type": media_type,
        "tautulli_user_id": _scalar(item.get("user_id"), int, str),
        "tautulli_rating_key": _scalar(item.get("rating_key"), int, str),
        "tautulli_history_id": _scalar(item.get("id"), int, str),
        "title": _scalar(item.get("title"), str),
        "parent_title": _scalar(item.get("parent_title"), str),
        "grandparent_title": _scalar(item.get("grandparent_title"), str),
        "played_at": _scalar(item.get("started"), int, float),
        "duration_seconds": _scalar(item.get("duration"), int, float),
        "completed": _completion(item.get("watched_status")),
    }
