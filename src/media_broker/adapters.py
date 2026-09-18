"""Small adapters with bounded responses and explicit projections.

Read tools are always available. Write tools (request, unmonitor, delete) are
registered only when enabled in configuration; deletions additionally require
a short-lived HMAC confirmation bound to the exact action parameters.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import re
import time
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any, Literal

import httpx

from .config import Settings, Upstream

ArrService = Literal["sonarr", "radarr", "lidarr"]
TautulliMediaType = Literal["movie", "episode", "track"]
JellyfinMediaType = Literal["movie", "episode", "track"]

_MAX_ITEMS = 10_000
_MAX_PROJECTED_STRING = 1024
_JELLYFIN_FILTERS = {"movie": "Movie", "episode": "Episode", "track": "Audio"}
_JELLYFIN_USER_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_MAX_REQUEST_BYTES = 262_144
_CONFIRM_TTL_SECONDS = 300
_INVENTORY_RESOURCE = {"sonarr": "series", "radarr": "movie", "lidarr": "artist"}
_NUMERIC_IDENTIFIER = re.compile(r"^[1-9]\d{0,9}$")
_MUSICBRAINZ_ID = re.compile(r"^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
# Provider prefixes route an external identifier past each service's text search.
_LOOKUP_PREFIX = {"sonarr": "tvdb:", "radarr": "tmdb:", "lidarr": "lidarr:"}
_LOOKUP_ID = {
    "sonarr": ("tvdb_id", "tvdbId"),
    "radarr": ("tmdb_id", "tmdbId"),
    "lidarr": ("musicbrainz_id", "foreignArtistId"),
}
_DELETE_ACTION = "arr_delete_media"
# Tautulli 2.18.1 emits numeric quarter-step watched statuses; 1 is complete.
_WATCHED_STATUSES = frozenset({0, 0.25, 0.5, 0.75, 1})

# A projection maps each output key to its upstream source key and the JSON
# scalar types it must match; anything else projects to None.
_Fields = Mapping[str, tuple[str, *tuple[type, ...]]]

_ARR_ITEM: _Fields = {
    "id": ("id", int),
    "title": ("title", str),
    "year": ("year", int),
    "status": ("status", str),
    "monitored": ("monitored", bool),
    "quality_profile_id": ("qualityProfileId", int),
    "root_folder_path": ("rootFolderPath", str),
}
_QUALITY_PROFILE: _Fields = {
    "id": ("id", int),
    "name": ("name", str),
    "upgrade_allowed": ("upgradeAllowed", bool),
    "cutoff": ("cutoff", int),
}
_ROOT_FOLDER: _Fields = {
    "id": ("id", int),
    "path": ("path", str),
    "accessible": ("accessible", bool),
    "free_space_bytes": ("freeSpace", int),
    "total_space_bytes": ("totalSpace", int),
}
# The source-prefixed identifiers are approved for household matching.
_HISTORY_ITEM: _Fields = {
    "tautulli_user_id": ("user_id", int, str),
    "tautulli_rating_key": ("rating_key", int, str),
    "tautulli_history_id": ("id", int, str),
    "title": ("title", str),
    "parent_title": ("parent_title", str),
    "grandparent_title": ("grandparent_title", str),
    "played_at": ("started", int, float),
    "duration_seconds": ("duration", int, float),
}
_JELLYFIN_HISTORY_ITEM: _Fields = {
    "jellyfin_item_id": ("Id", int, str),
    "jellyfin_history_id": ("RowId", int, str),
    "title": ("Name", str),
    "duration_seconds": ("Duration", int, float),
}


def _bounded_request(body: dict[str, Any]) -> bytes:
    """Serialize a broker-built upstream body, refusing unbounded payloads."""
    content = json.dumps(body).encode("utf-8")
    if len(content) > _MAX_REQUEST_BYTES:
        raise ParameterError("request payload exceeds the broker limit")
    return content


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
    """HTTP client for the five allow-listed upstream APIs."""

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
        self,
        upstream: Upstream,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        method: str = "GET",
        request_body: dict[str, Any] | None = None,
        allow_empty: bool = False,
    ) -> Any:
        headers = (
            {"X-Emby-Token": upstream.api_key}
            if upstream is self.settings.upstreams.get("jellyfin")
            else {"X-Api-Key": upstream.api_key}
        )
        content = None if request_body is None else _bounded_request(request_body)
        try:
            async with (
                asyncio.timeout(self.settings.timeout_seconds),
                self._client.stream(
                    method,
                    upstream.base_url + path,
                    headers=headers,
                    params=params,
                    content=content,
                ) as response,
            ):
                if response.status_code >= 300:
                    raise UpstreamError(
                        f"upstream returned HTTP {response.status_code}"
                    )
                raw = await _bounded_body(response, self.settings.max_response_bytes)
        except TimeoutError as exc:
            raise UpstreamError("upstream request timed out") from exc
        except (httpx.HTTPError, OSError) as exc:
            raise UpstreamError("upstream request failed") from exc
        if allow_empty and not raw.strip():
            return None
        try:
            return json.loads(raw)
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
        rows = await self._arr_items(service, _INVENTORY_RESOURCE[service])
        items = [
            # Lidarr names its title field artistName; fold it into title first.
            _project({**r, "title": r.get("title") or r.get("artistName")}, _ARR_ITEM)
            for r in rows
        ]
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

    async def _summaries(
        self, service: ArrService, resource: str, fields: _Fields
    ) -> dict[str, Any]:
        rows = [_project(r, fields) for r in await self._arr_items(service, resource)]
        return {"service": service, "items": rows, "upstream_truncated": False}

    async def quality_profiles(self, service: ArrService) -> dict[str, Any]:
        return await self._summaries(service, "qualityprofile", _QUALITY_PROFILE)

    async def root_folders(self, service: ArrService) -> dict[str, Any]:
        return await self._summaries(service, "rootfolder", _ROOT_FOLDER)

    async def _lookup_rows(
        self, service: ArrService, term: str
    ) -> list[dict[str, Any]]:
        upstream = self.settings.upstreams[service]
        path = f"/api/{upstream.api_version}/{_INVENTORY_RESOURCE[service]}/lookup"
        payload = await self._json(upstream, path, {"term": term})
        if not isinstance(payload, list) or len(payload) > _MAX_ITEMS:
            raise UpstreamError("upstream returned an unexpected lookup response")
        return [row for row in payload if isinstance(row, dict)]

    async def search_candidates(
        self, service: ArrService, query: str, limit: int
    ) -> dict[str, Any]:
        items = [
            _lookup_item(row, service)
            for row in await self._lookup_rows(service, query)
        ]
        return {
            "service": service,
            "items": items[:limit],
            "upstream_truncated": len(items) > limit,
        }

    async def request_media(
        self,
        service: ArrService,
        external_id: str,
        quality_profile_id: int,
        root_folder_path: str,
        search_on_add: bool,
    ) -> dict[str, Any]:
        """Add one exact catalog item; the broker builds the add body itself."""
        identifier = _external_identifier(service, external_id)
        rows = await self._lookup_rows(
            service, f"{_LOOKUP_PREFIX[service]}{identifier}"
        )
        candidate = _matching_candidate(rows, service, identifier)
        if _scalar(candidate.get("id"), int):
            raise ParameterError("the requested media is already in the library")
        await self._profile_must_exist(service, quality_profile_id)
        root = await self._known_root_folder(service, root_folder_path)
        body = await self._add_body(
            service, candidate, quality_profile_id, root, search_on_add
        )
        upstream = self.settings.upstreams[service]
        added = await self._json(
            upstream,
            f"/api/{upstream.api_version}/{_INVENTORY_RESOURCE[service]}",
            method="POST",
            request_body=body,
        )
        if not isinstance(added, dict):
            raise UpstreamError("upstream returned an unexpected add response")
        return {
            "service": service,
            "item": _lookup_item(added, service),
            "search_on_add": search_on_add,
        }

    async def _profile_must_exist(self, service: ArrService, profile_id: int) -> None:
        profiles = await self._arr_items(service, "qualityprofile")
        if profile_id not in {_scalar(row.get("id"), int) for row in profiles}:
            raise ParameterError(
                "quality_profile_id is not a configured quality profile"
            )

    async def _known_root_folder(self, service: ArrService, requested: str) -> str:
        """Resolve the requested path to one configured root folder, refusing others."""
        wanted = requested.rstrip("/")
        for folder in await self._arr_items(service, "rootfolder"):
            path = _scalar(folder.get("path"), str)
            if isinstance(path, str) and path.rstrip("/") == wanted:
                return path
        raise ParameterError("root_folder_path is not a configured root folder")

    async def _add_body(
        self,
        service: ArrService,
        candidate: dict[str, Any],
        quality_profile_id: int,
        root_folder_path: str,
        search_on_add: bool,
    ) -> dict[str, Any]:
        if service == "sonarr":
            return _sonarr_add_body(
                candidate, quality_profile_id, root_folder_path, search_on_add
            )
        if service == "radarr":
            return _radarr_add_body(
                candidate, quality_profile_id, root_folder_path, search_on_add
            )
        metadata_profile_id = await self._metadata_profile_id(service)
        return _lidarr_add_body(
            candidate,
            quality_profile_id,
            metadata_profile_id,
            root_folder_path,
            search_on_add,
        )

    async def _metadata_profile_id(self, service: ArrService) -> int:
        """Pick Lidarr's lowest metadata profile id; Lidarr adds require one."""
        rows = await self._arr_items(service, "metadataprofile")
        known = [i for row in rows if isinstance(i := _scalar(row.get("id"), int), int)]
        if not known:
            raise UpstreamError(
                "upstream returned an unexpected metadata profile response"
            )
        return min(known)

    async def _get_item(self, service: ArrService, item_id: int) -> dict[str, Any]:
        upstream = self.settings.upstreams[service]
        path = f"/api/{upstream.api_version}/{_INVENTORY_RESOURCE[service]}/{item_id}"
        payload = await self._json(upstream, path)
        if not isinstance(payload, dict):
            raise UpstreamError("upstream returned an unexpected item response")
        return payload

    async def unmonitor_media(
        self, service: ArrService, item_id: int
    ) -> dict[str, Any]:
        """Flip one item's monitored flag off; everything else is left untouched."""
        upstream = self.settings.upstreams[service]
        path = f"/api/{upstream.api_version}/{_INVENTORY_RESOURCE[service]}/{item_id}"
        item = await self._get_item(service, item_id)
        updated = await self._json(
            upstream, path, method="PUT", request_body=item | {"monitored": False}
        )
        if not isinstance(updated, dict):
            raise UpstreamError("upstream returned an unexpected update response")
        return {"service": service, "item": _item_summary(updated, item_id)}

    async def delete_media(
        self,
        service: ArrService,
        item_id: int,
        delete_files: bool,
        confirmation: str | None = None,
    ) -> dict[str, Any]:
        """Two-phase delete: preview with a bound confirmation, then execute."""
        item = await self._get_item(service, item_id)
        action = f"{_DELETE_ACTION}:{service}:{item_id}:{int(delete_files)}"
        secret = self.settings.bearer_token
        if confirmation is not None:
            _confirmation_must_match(secret, action, confirmation)
            upstream = self.settings.upstreams[service]
            path = (
                f"/api/{upstream.api_version}/{_INVENTORY_RESOURCE[service]}/{item_id}"
            )
            await self._json(
                upstream,
                path,
                {
                    "deleteFiles": "true" if delete_files else "false",
                    "addImportListExclusion": "false",
                },
                method="DELETE",
                allow_empty=True,
            )
            return {
                "deleted": True,
                "service": service,
                "item": _item_summary(item, item_id),
                "delete_files": delete_files,
            }
        expires = int(time.time()) + _CONFIRM_TTL_SECONDS
        return {
            "confirmation_required": True,
            "service": service,
            "item": _item_summary(item, item_id),
            "delete_files": delete_files,
            "confirmation": _issue_confirmation(secret, action, expires),
            "confirmation_expires_at": expires,
        }

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
            {
                "media_type": media_type,
                **_project(row, _HISTORY_ITEM),
                "completed": _completion(row.get("watched_status")),
            }
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

    async def jellyfin_history(
        self,
        user_id: str,
        media_type: JellyfinMediaType,
        start_date: str,
        end_date: str,
        page: int,
        page_size: int,
        timezone_offset: float,
    ) -> dict[str, Any]:
        """Return projected Playback Reporting rows, fetched one day at a time."""
        start, end = _date_range(start_date, end_date)
        if not _JELLYFIN_USER_ID.fullmatch(user_id):
            raise ParameterError("user_id must contain exactly 32 hex characters")
        if not -14 <= timezone_offset <= 14:
            raise ParameterError("timezone_offset must be between -14 and 14 hours")

        upstream = self.settings.upstreams["jellyfin"]
        offset = (page - 1) * page_size
        items: list[dict[str, Any]] = []
        total_count = 0
        current = start
        while current <= end:
            payload = await self._json(
                upstream,
                f"/user_usage_stats/{user_id}/{current.isoformat()}/GetItems",
                {
                    "filter": _JELLYFIN_FILTERS[media_type],
                    "timezoneOffset": timezone_offset,
                },
            )
            if not isinstance(payload, list) or any(
                not isinstance(row, dict) for row in payload
            ):
                raise UpstreamError(
                    "upstream returned an unexpected Jellyfin history response"
                )
            # Count every row for honest local pagination, but retain only the
            # requested page and project it before the day's payload is dropped.
            for row in payload:
                if offset <= total_count < offset + page_size:
                    items.append(_jellyfin_project(user_id, media_type, current, row))
                total_count += 1
                if total_count > _MAX_ITEMS:
                    raise UpstreamError(
                        "upstream returned too many Jellyfin history rows"
                    )
            del payload
            current += timedelta(days=1)

        page_items = items
        return {
            "jellyfin_user_id": user_id,
            "media_type": media_type,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone_offset": timezone_offset,
            "items": page_items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "returned_count": len(page_items),
                "total_count": total_count,
                "has_more": offset + len(page_items) < total_count,
                "upstream_paginated": False,
                "upstream_truncated": False,
            },
        }


def _jellyfin_project(
    user_id: str,
    media_type: JellyfinMediaType,
    local_date: date,
    row: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "jellyfin_user_id": user_id,
        "media_type": media_type,
        "played_at": _jellyfin_played_at(local_date, row.get("Time")),
        **_project_bounded(row, _JELLYFIN_HISTORY_ITEM),
    }


def _jellyfin_played_at(local_date: date, value: Any) -> str | None:
    """Combine a plugin local time with its requested local calendar date."""
    time = _bounded_scalar(value, str)
    if time is None:
        return None
    played_at = f"{local_date.isoformat()}T{time}"
    return played_at if len(played_at) <= _MAX_PROJECTED_STRING else None


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
    if (
        not isinstance(data, dict)
        or not isinstance(rows := data.get("data"), list)
        or len(rows) > page_size
    ):
        raise UpstreamError("upstream returned an unexpected history response")
    return (
        [row for row in rows if isinstance(row, dict)],
        _scalar(data.get("recordsFiltered"), int),
    )


def _scalar(value: Any, *types: type) -> Any:
    """Return value only when it is exactly one of the given JSON scalar types."""
    if isinstance(value, bool) and bool not in types:
        return None
    return value if isinstance(value, types) else None


def _project(row: Mapping[str, Any], fields: _Fields) -> dict[str, Any]:
    """Project one upstream row to the allowed keys and scalar types only."""
    return {name: _scalar(row.get(spec[0]), *spec[1:]) for name, spec in fields.items()}


def _bounded_scalar(value: Any, *types: type) -> Any:
    scalar = _scalar(value, *types)
    if isinstance(scalar, str) and len(scalar) > _MAX_PROJECTED_STRING:
        return None
    return scalar


def _project_bounded(row: Mapping[str, Any], fields: _Fields) -> dict[str, Any]:
    return {
        name: _bounded_scalar(row.get(spec[0]), *spec[1:])
        for name, spec in fields.items()
    }


def _completion(value: Any) -> bool | None:
    status = _scalar(value, int, float)
    return None if status not in _WATCHED_STATUSES else status == 1


def _lookup_item(row: Mapping[str, Any], service: ArrService) -> dict[str, Any]:
    """Project one lookup or added record to approved keys plus in-library state."""
    name, source = _LOOKUP_ID[service]
    item_id = _scalar(row.get("id"), int)
    return {
        "id": item_id,
        name: _scalar(row.get(source), int, str),
        "title": _scalar(row.get("title") or row.get("artistName"), str),
        "year": _scalar(row.get("year"), int),
        "status": _scalar(row.get("status"), str),
        "in_library": bool(item_id),
    }


def _item_summary(row: Mapping[str, Any], fallback_id: int) -> dict[str, Any]:
    return {
        "id": _scalar(row.get("id"), int) or fallback_id,
        "title": _scalar(row.get("title") or row.get("artistName"), str),
        "monitored": _scalar(row.get("monitored"), bool),
    }


def _external_identifier(service: ArrService, value: str) -> str:
    """Validate one external catalog identifier for the target service."""
    text = value.strip()
    if service == "lidarr":
        if _MUSICBRAINZ_ID.fullmatch(text):
            return text
        raise ParameterError("external_id must be a MusicBrainz UUID for lidarr")
    if _NUMERIC_IDENTIFIER.fullmatch(text) and int(text) <= 2_147_483_647:
        return text
    raise ParameterError(
        f"external_id must be a positive numeric identifier for {service}"
    )


def _matching_candidate(
    rows: list[dict[str, Any]], service: ArrService, identifier: str
) -> dict[str, Any]:
    """Select the single lookup row whose external id matches exactly."""
    _, source = _LOOKUP_ID[service]
    for row in rows:
        value = _scalar(row.get(source), int, str)
        if value is not None and str(value).casefold() == identifier.casefold():
            return row
    raise ParameterError("no upstream candidate matches the external identifier")


def _sonarr_seasons(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = candidate.get("seasons")
    rows = raw if isinstance(raw, list) else []
    seasons = [
        {"seasonNumber": number, "monitored": True}
        for row in rows
        if isinstance(row, dict)
        and isinstance(number := _scalar(row.get("seasonNumber"), int), int)
    ]
    if not seasons:
        raise ParameterError("the lookup candidate has no season metadata")
    return seasons


def _sonarr_add_body(
    candidate: Mapping[str, Any], profile_id: int, root: str, search: bool
) -> dict[str, Any]:
    return {
        "title": _scalar(candidate.get("title"), str),
        "titleSlug": _scalar(candidate.get("titleSlug"), str),
        "tvdbId": _scalar(candidate.get("tvdbId"), int),
        "qualityProfileId": profile_id,
        "rootFolderPath": root,
        "seriesType": _scalar(candidate.get("seriesType"), str) or "standard",
        "monitored": True,
        "seasonFolder": True,
        "seasons": _sonarr_seasons(candidate),
        "addOptions": {
            "monitor": "all",
            "searchForMissingEpisodes": search,
            "searchForCutoffUnmetEpisodes": False,
        },
    }


def _radarr_add_body(
    candidate: Mapping[str, Any], profile_id: int, root: str, search: bool
) -> dict[str, Any]:
    return {
        "title": _scalar(candidate.get("title"), str),
        "titleSlug": _scalar(candidate.get("titleSlug"), str),
        "tmdbId": _scalar(candidate.get("tmdbId"), int),
        "year": _scalar(candidate.get("year"), int),
        "qualityProfileId": profile_id,
        "rootFolderPath": root,
        "minimumAvailability": "released",
        "monitored": True,
        "addOptions": {"searchForMovie": search},
    }


def _lidarr_add_body(
    candidate: Mapping[str, Any],
    profile_id: int,
    metadata_id: int,
    root: str,
    search: bool,
) -> dict[str, Any]:
    return {
        "artistName": _scalar(candidate.get("artistName"), str),
        "foreignArtistId": _scalar(candidate.get("foreignArtistId"), str),
        "qualityProfileId": profile_id,
        "metadataProfileId": metadata_id,
        "rootFolderPath": root,
        "monitored": True,
        "monitorNewItems": "all",
        "addOptions": {
            "monitor": "all",
            "monitored": True,
            "searchForMissingAlbums": search,
        },
    }


def _issue_confirmation(secret: str, action: str, expires_at: int) -> str:
    """Issue one stateless, parameter-bound confirmation token for an action."""
    payload = f"{expires_at}\n{action}".encode()
    digest = hmac.new(secret.encode(), payload, hashlib.sha256).digest()[:16]
    return f"{expires_at}.{base64.urlsafe_b64encode(digest).decode()}"


def _confirmation_must_match(secret: str, action: str, token: str) -> None:
    """Require a fresh token issued for exactly this action's parameters."""
    expiry, _, _ = token.partition(".")
    if not expiry.isdigit():
        raise ParameterError("confirmation is not valid for this action")
    expected = _issue_confirmation(secret, action, int(expiry))
    if not hmac.compare_digest(expected, token):
        raise ParameterError("confirmation is not valid for this action")
    if int(expiry) < int(time.time()):
        raise ParameterError("confirmation has expired; request a fresh preview")
