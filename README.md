# media-broker

Controlled MCP broker for household media services. This repository owns the
application source, its tests, and the published container image. The
production deployment (Portainer stack, host firewall, secret files) is owned
by the [`mich-murphy/home-infra`](https://github.com/mich-murphy/home-infra)
repository; see its `docs/hermes-media.md` for the deployment runbook.

This project exposes a small MCP server over the official Python SDK's
Streamable HTTP transport. The adapters target Sonarr 4.0.19 (API v3), Radarr
6.3.0 (API v3), Lidarr 3.1.0 (API v1), Tautulli 2.18.1, and the Jellyfin
Playback Reporting plugin (v19).

Read tools, always available:

- `arr_library_inventory` (Sonarr, Radarr, or Lidarr; bounded local pagination; includes the added date, genres, size on disk, and file counts for cleanup audits)
- `arr_quality_profiles`
- `arr_root_folders`
- `arr_search_candidates` (upstream catalog lookup; bounded, projected results)
- `tautulli_play_history` (movie, episode, or track; inclusive maximum 31-day range and optional numeric user filter)
- `jellyfin_play_history` (movie, episode, or track; inclusive maximum 31-day range, exact 32-hex-character user id, and local pagination)
- `jellyfin_users` (id/name pairs from `GET /Users`, so `jellyfin_play_history` user ids are discoverable without dashboard access)

Write tools, registered only when enabled:

- `arr_request_media` and `arr_unmonitor_media` behind
  `MEDIA_BROKER_ENABLE_REQUESTS=true`
- `arr_delete_media` behind `MEDIA_BROKER_ENABLE_DELETES=true`

There are no generic upstream requests, approval endpoints, Seerr, or Jellyfin
core endpoints. Arr APIs return arrays, so inventory pagination is performed
after one bounded response and reports that fact honestly. No read tool depends
on Tautulli: any playback history source (Tautulli, the Jellyfin Playback
Reporting plugin, or something else later) can drive request and cleanup
decisions, because the write tools operate on Arr identifiers only.

## Media requests

`arr_request_media` takes a service, an external catalog identifier (TVDB id
for Sonarr, TMDB id for Radarr, MusicBrainz artist UUID for Lidarr), a quality
profile id, and a root folder path. The broker resolves the candidate itself
through the upstream lookup endpoint, rejects external ids the catalog does not
know, refuses duplicates already in the library, and requires both the quality
profile and the root folder to exist upstream before posting one allow-listed
add body. Lidarr adds use the lowest configured metadata profile id. By default
the add starts searching for downloads immediately (`search_on_add=true`);
pass `false` for monitor-only additions.

## Library cleanup

`arr_unmonitor_media` stops monitoring one item while keeping its metadata and
files; it is reversible and needs no confirmation.

`arr_delete_media` removes one item and is destructive. It always runs in two
phases: the first call returns a preview of the projected item plus a
five-minute signed confirmation token (`itsdangerous`) bound to the exact
action (service, item id, and file mode), and only a second call carrying
that token executes the delete. `delete_files=false` (the default) removes the item but keeps files on
disk; `delete_files=true` removes files too. Import-list exclusions are never
added, so exclusion lists are managed by the operator, not the broker.

## Local run

Create one file for the broker bearer token and one file for each upstream API
key, with restrictive permissions. The environment contains paths only, never
key values:

```sh
export MEDIA_BROKER_TOKEN_FILE=/run/user/1000/media-broker/token
export SONARR_URL=http://127.0.0.1:8989
export SONARR_API_KEY_FILE=/run/user/1000/media-broker/sonarr
export RADARR_URL=http://127.0.0.1:7878
export RADARR_API_KEY_FILE=/run/user/1000/media-broker/radarr
export LIDARR_URL=http://127.0.0.1:8686
export LIDARR_API_KEY_FILE=/run/user/1000/media-broker/lidarr
export TAUTULLI_URL=http://127.0.0.1:8181
export TAUTULLI_API_KEY_FILE=/run/user/1000/media-broker/tautulli
export JELLYFIN_URL=http://127.0.0.1:8096
export JELLYFIN_API_KEY_FILE=/run/user/1000/media-broker/jellyfin
export MEDIA_BROKER_ALLOWED_HOSTS=127.0.0.1:8000
export MEDIA_BROKER_ALLOWED_ORIGINS=http://127.0.0.1:8000
uv run media-broker
```

All five upstreams and all secret files are required, and a secret file that is
world-readable is refused at startup. The broker defaults to loopback binding,
HTTPS certificate verification, no redirects, a 10-second total upstream
deadline (`MEDIA_BROKER_TIMEOUT_SECONDS`, 0.1-60), and a 2 MB upstream response
limit (`MEDIA_BROKER_MAX_RESPONSE_BYTES`, 1000-50000000). Host and Origin values
are exact allow-lists; wildcard syntax is rejected. Tautulli requests
use its inclusive `after`/`before` date bounds with `grouping=0` and
`include_activity=0` so each returned row represents a playback event.
Arr and Tautulli use `X-Api-Key` header authentication; Jellyfin uses its
`X-Emby-Token` header. The broker never includes credentials in query URLs.
Jellyfin Playback Reporting history requests one allow-listed `GetItems` route
per day with a requested `Movie`, `Episode`, or `Audio` filter and timezone
offset. Results are projected to the requested user and local date/time; no
completion claim is made because plugin rows may represent active sessions.

Inventory projections add the fields a cleanup audit needs on top of the base
metadata. `added` is the library add date on all three Arr services. `genres`
is a string list capped at 16 entries with each entry bounded like any
projected string. `size_on_disk_bytes` comes from Radarr's `movieFile.size`
and from `statistics.sizeOnDisk` on Sonarr and Lidarr. File presence is
Radarr's `hasFile` for movies; for series and artists the broker projects
`episode_file_count` or `track_file_count` from `statistics` and derives
`has_file` from a nonzero count. A missing or wrongly typed statistics block
projects to nulls, never to guesses.

History projections contain only scalar, validated fields. The explicitly
approved stable identifiers `tautulli_user_id`, `tautulli_rating_key`, and
`tautulli_history_id` come from Tautulli's `user_id`, `rating_key`, and `id`
fields respectively. Jellyfin history maps the requested `user_id` to
`jellyfin_user_id` and uses its `Id` and `RowId` for source-prefixed item and
history identifiers.
They are present for household selection and stable matching. Tautulli's
`friendly_name` is projected as `user_name` so a `tautulli_user_id` maps to a
readable account; emails, IP addresses, client/method/device identifiers, and
unknown fields are not returned. Tautulli's numeric watched status of `1`
means completed; `0`,
`0.25`, `0.5`, and `0.75` mean incomplete. A missing or unrecognized status
is reported as unknown, not false.

Tool argument bounds (page 1-100000, page size 1-100, search text up to 200
characters, strict `YYYY-MM-DD` dates, non-negative Tautulli `user_id`, Jellyfin
timezone offsets from -14 to 14 hours, external ids up to 64 characters, item
and profile ids within int32, candidate lists of at most 100, confirmation
tokens up to 128 characters) are declared in
each tool's input schema, so clients see them before calling. Rejected
arguments, upstream failures, and unexpected errors are all reported through the
MCP `isError` result with a sanitized message; upstream bodies, URLs, and keys
never reach the caller. Responses use the Streamable HTTP JSON mode because the
server is stateless and never streams server-initiated messages.

CI enforces formatting, linting, cyclomatic complexity (McCabe 8), strict type
checking, tests, a dependency vulnerability audit, and an image build:

```sh
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv export --no-dev --no-emit-project --no-hashes -o requirements.txt
uv run pip-audit --strict --disable-pip --no-deps -r requirements.txt
```

## Container image

CI builds the image on every merge to `main` and publishes it to GHCR as:

- `ghcr.io/mich-murphy/media-broker:main` — rolling tag; Renovate pins its
  digest in the home-infra Compose file and updates the pin on every publish.
- `ghcr.io/mich-murphy/media-broker:sha-<commit>` — immutable tag used for
  rollback and auditing.

The image is private to the `mich-murphy` account. Its runtime stage contains
only the virtual environment (no pip, uv, or source tree), runs as UID/GID
65532 with a read-only filesystem, listens on container port 8000, and expects
all credentials as file paths (`*_FILE` variables), never values.

`tests/container.sh` performs an isolated local build-and-probe against fake
upstreams using the `desktop-linux` Docker context; it creates and removes
every resource it uses.

MCP authentication proves possession of the configured bearer token, not human
consent or authorization for a particular media action. The default surface
is read-only; enabling the write gates extends that single credential to
media requests and, with the delete gate, confirmed destructive removal.
Upstream credentials and playback titles remain sensitive household data;
keep the broker on a trusted loopback or private network, review client
access, and enable the write gates only where clients are expected to mutate
the library.
