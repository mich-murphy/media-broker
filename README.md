# media-broker

Controlled MCP broker for household media services. This repository owns the
application source, its tests, and the published container image. The
production deployment (Portainer stack, host firewall, secret files) is owned
by the [`mich-murphy/home-infra`](https://github.com/mich-murphy/home-infra)
repository; see its `docs/hermes-media.md` for the deployment runbook.

This project exposes a small MCP server over the official Rust SDK's (`rmcp`)
Streamable HTTP transport. The adapters target Sonarr 4.0.19 (API v3), Radarr
6.3.0 (API v3), Lidarr 3.1.0 (API v1), Tautulli 2.18.1, the Jellyfin
Playback Reporting plugin (v19), and qBittorrent 5.1 (WebUI API v2).

Read tools, available when the relevant upstream is configured (at least one
upstream must be configured; each Arr tool needs any Arr service,
`arr_season_inventory` needs Sonarr, and `arr_album_inventory` needs Lidarr):

- `arr_library_inventory` (Sonarr, Radarr, or Lidarr; bounded local pagination; includes the added date, genres, size on disk, and file counts for cleanup audits; Sonarr items also list per-season monitored flags)
- `arr_quality_profiles`
- `arr_root_folders`
- `arr_search_candidates` (upstream catalog lookup; bounded, projected results)
- `arr_season_inventory` (Sonarr; per-season monitored flags with episode and episode-file counts aggregated from the episode endpoint)
- `arr_album_inventory` (Lidarr; per-album monitored flag, release date, track file count, size on disk, and file presence for one artist)
- `tautulli_play_history` (movie, episode, or track; inclusive maximum 31-day range and optional numeric user filter)
- `jellyfin_play_history` (movie, episode, or track; inclusive maximum 31-day range, exact 32-hex-character user id, and local pagination)
- `jellyfin_users` (id/name pairs from `GET /Users`, so `jellyfin_play_history` user ids are discoverable without dashboard access)

qBittorrent read tools, registered when `QBITTORRENT_URL` is configured:

- `torrent_client_stats` (global transfer totals, current speeds, and connection status)
- `torrent_client_inventory` (bounded local pagination over torrent swarm state, including save paths)
- `torrent_client_check_paths` (which 40- or 64-hex-character info-hashes are present, with save paths and recorded data completeness, for reseed-candidate audits)

`data_complete` reports the client's recorded progress; verifying bytes on
disk requires a recheck, which only the gated reseed tool performs. There is
no generic torrent add, start, or delete, no settings mutation, and no generic
request path: the read tools speak only `auth/login`, `torrents/info`, and
`transfer/info`, and the reseed tool adds `app/preferences` (read) and
`torrents/add`, `torrents/recheck`, `torrents/start`, and `torrents/delete`
for the one torrent it is driving.

Write tools, registered only when enabled:

- `arr_request_media`, `arr_unmonitor_media`, `arr_monitor_media`,
  `arr_set_season_monitoring`, `arr_set_album_monitored`, and `arr_search_item`
  behind `MEDIA_BROKER_ENABLE_REQUESTS=true`
- `arr_delete_media` (whole items and, for Lidarr, single albums) behind
  `MEDIA_BROKER_ENABLE_DELETES=true`
- `torrent_client_reseed` behind `MEDIA_BROKER_ENABLE_RESEEDS=true`, which
  requires qBittorrent and `QBITTORRENT_RESEED_SAVE_PATHS`

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

Sonarr adds accept an optional `seasons` list (for example `[1]` for a
season-one trial). The broker marks only those seasons monitored in the add
body and omits Sonarr's `monitor` option, so episode monitoring derives from
the submitted season flags and the add-search can never touch unselected
seasons. Season monitoring can be changed later with
`arr_set_season_monitoring` and verified through `arr_season_inventory`.

Lidarr adds monitor the whole discography, so a selective album add is a
sequence: add with `search_on_add=false`, list albums with
`arr_album_inventory`, unmonitor the unwanted albums (and delete their files
with `arr_delete_media` if the delete gate is enabled), then run
`arr_search_item` to search the artist's monitored missing albums.

## Library cleanup

`arr_unmonitor_media` stops monitoring one item while keeping its metadata and
files; it is reversible with `arr_monitor_media` and needs no confirmation.
`arr_set_season_monitoring` (Sonarr) and `arr_set_album_monitored` (Lidarr)
flip monitoring below the item level and are likewise reversible.

`arr_delete_media` removes one item and is destructive. It always runs in two
phases: the first call returns a preview of the projected item plus a
five-minute HMAC-SHA256 signed confirmation token bound to the exact
action (service, item id, album id, and file mode), and only a second call
carrying that token executes the delete. For Lidarr, an optional `album_id`
deletes a single album instead of the artist; the preview then shows both the
album and the artist so the caller can verify the target. `delete_files=false`
(the default) removes the item but keeps files on disk; `delete_files=true`
removes files too. Import-list exclusions are never added, so exclusion lists
are managed by the operator, not the broker.

## Torrent reseeds

`torrent_client_reseed` re-adds one `.torrent` file over data already on disk
and starts seeding only if a full recheck verifies every piece. The file comes
as base64, holds v1 metainfo only, and decodes to at most 1 MiB. The broker
computes the info-hash itself and only ever addresses that one torrent, so the
tool cannot be used as a generic add.

1. A hash already in the client that this tool did not add is left alone and
   reported as `already_present`.
2. Anything else is added stopped with `skip_checking=false`, the tag
   `media-broker-reseed`, and a 1 B/s download limit. The add also sends
   `autoTMM=false`, `useDownloadPath=false`, `contentLayout=Original`, and an
   explicit `savepath`, which overrides every client default that could move
   or reshape the files. qBittorrent loads the torrent asynchronously, so the
   broker waits for it to be listed and then forces a recheck.
3. If every piece of every file verifies, the broker starts the torrent and
   reports `reseeding`. Otherwise it removes the torrent with
   `deleteFiles=false` and reports `aborted_incomplete` with the recorded
   progress, so the operator can look for renamed, moved, missing, or
   retagged data.
4. A recheck still running after 45 seconds reports `checking`. Replaying the
   same call finds the tagged torrent and picks up where it stopped. It polls
   a running check, starts a stopped torrent that already verified complete,
   and rechecks one that did not. A batch interrupted anywhere, including by a
   client disconnect, can be re-run in full. The broker runs one reseed at a
   time.

`save_path` must exactly match an entry in `QBITTORRENT_RESEED_SAVE_PATHS`, a
comma-separated list of absolute directories. The filesystem root and dot
segments are refused. With a single entry, `save_path` may be omitted. Each
result carries `hash`, `outcome`, `name`, `state`, `progress`, `size_bytes`,
`amount_left_bytes`, and `save_path`.

No payload block is downloaded. When the files are on disk, qBittorrent pauses
the torrent the moment hashing ends, before any announce. When none of them
are, libtorrent skips hashing and can briefly resume the auto-managed torrent
before qBittorrent stops it again. The 1 B/s limit keeps any block from
completing in that window, and the broker removes a torrent it sees running
incomplete straight away.

Two client settings would let an incomplete recheck change files on disk, so
the broker reads the preferences first and refuses to reseed under either. One
is the extension qBittorrent appends to incomplete files, which would rename
them. The other is excluded file names combined with the unwanted folder,
which would move them. Excluded file names on their own only shrink the wanted
size, and a torrent with any file excluded never counts as complete. A
reseeded torrent keeps its tag and its 1 B/s limit as a reseed-only marker.

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
export QBITTORRENT_URL=http://127.0.0.1:8080
export QBITTORRENT_USERNAME=admin
export QBITTORRENT_PASSWORD_FILE=/run/user/1000/media-broker/qbittorrent
# Optional: enable torrent_client_reseed into exact save paths.
export MEDIA_BROKER_ENABLE_RESEEDS=true
export QBITTORRENT_RESEED_SAVE_PATHS=/data/torrents/music
export MEDIA_BROKER_ALLOWED_HOSTS=127.0.0.1:8000
export MEDIA_BROKER_ALLOWED_ORIGINS=http://127.0.0.1:8000
cargo run --release
```

`nix develop` (or direnv) provides the Rust toolchain.

Upstreams are optional, but at least one must be configured. An upstream is
enabled by its `*_URL` plus its credentials: `*_API_KEY_FILE` for the Arr
services, Tautulli, and Jellyfin, or `QBITTORRENT_USERNAME` and
`QBITTORRENT_PASSWORD_FILE` for qBittorrent. Credentials without their URL
fail at startup, and a secret file that is world-readable is refused. The
broker defaults to loopback binding,
HTTPS certificate verification, no redirects, a 10-second total upstream
deadline (`MEDIA_BROKER_TIMEOUT_SECONDS`, 0.1-60), and a 2 MB upstream response
limit (`MEDIA_BROKER_MAX_RESPONSE_BYTES`, 1000-50000000). Host and Origin values
are exact allow-lists; wildcard syntax is rejected. Tautulli requests
use its inclusive `after`/`before` date bounds with `grouping=0` and
`include_activity=0` so each returned row represents a playback event.
Arr and Tautulli use `X-Api-Key` header authentication; Jellyfin uses its
`X-Emby-Token` header. qBittorrent uses `WebUI` session authentication: the
broker logs in once, holds the session cookie in memory (never in URLs, logs,
or tool responses), and re-authenticates exactly once if a request is
rejected, because repeated logins can trigger qBittorrent's IP ban. The
broker never includes credentials in query URLs.
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
`has_file` from a nonzero count. Sonarr items also project `seasons`, a
per-season list of monitored flags capped at 64 entries. A missing or wrongly
typed statistics block projects to nulls, never to guesses.

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
timezone offsets from -14 to 14 hours, external ids up to 64 characters, item,
profile, and album ids within int32, season selections of 1-100 season numbers
between 0 and 1000, candidate lists of at most 100, confirmation tokens up to
128 characters) are declared in
each tool's input schema, so clients see them before calling. Torrent hash
lists hold 1-100 entries of up to 64 characters each, and reseed metainfo is
at most 1398104 base64 characters (1 MiB decoded). Rejected
arguments and upstream failures are reported through the MCP `isError` result
with a sanitized message naming the offending argument; every tool error is one
of those two typed kinds, so upstream bodies, URLs, and keys never reach the
caller. Responses use the Streamable HTTP JSON mode because the
server is stateless and never streams server-initiated messages.

CI enforces formatting, Clippy lints (pedantic group, warnings denied), tests,
a dependency vulnerability audit, and an image build:

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --locked
cargo audit
```

## Container image

CI builds the image on every merge to `main` and publishes it to GHCR as:

- `ghcr.io/mich-murphy/media-broker:main` — rolling tag; Renovate pins its
  digest in the home-infra Compose file and updates the pin on every publish.
- `ghcr.io/mich-murphy/media-broker:sha-<commit>` — immutable tag used for
  rollback and auditing.

The image is private to the `mich-murphy` account. Its runtime stage contains
only the binary on a distroless base (no shell, toolchain, or source tree),
runs as UID/GID 65532 with a read-only filesystem, listens on container port
8000, and expects all credentials as file paths (`*_FILE` variables), never
values. The image declares a `HEALTHCHECK` that runs the binary's own
`--healthcheck` probe (the distroless runtime has no shell or curl) against
the unauthenticated `GET /health` route, which answers only that exact method
and path with a static `{"status": "ok"}`; every other method and path still
requires the bearer token.

`tests/container.sh` performs an isolated local build-and-probe against fake
upstreams using the `desktop-linux` Docker context; it creates and removes
every resource it uses.

MCP authentication proves possession of the configured bearer token, not human
consent or authorization for a particular media action. The default surface
is read-only; enabling the write gates extends that single credential to
media requests, with the delete gate to confirmed destructive removal, and
with the reseed gate to seeding verified data from the allow-listed save
paths. A reseeded torrent announces to the trackers named in the supplied
metainfo, so a token holder can make qBittorrent announce to a tracker of
their choosing for data that already verifies on disk.
Upstream credentials and playback titles remain sensitive household data;
keep the broker on a trusted loopback or private network, review client
access, and enable the write gates only where clients are expected to mutate
the library.
