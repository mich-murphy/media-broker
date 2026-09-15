# media-broker

Read-only MCP broker for household media services. This repository owns the
application source, its tests, and the published container image. The
production deployment (Portainer stack, host firewall, secret files) is owned
by the [`mich-murphy/home-infra`](https://github.com/mich-murphy/home-infra)
repository; see its `docs/hermes-media.md` for the deployment runbook.

This project exposes a small MCP server over the official Python SDK's
Streamable HTTP transport. The adapters target Sonarr 4.0.19 (API v3), Radarr
6.3.0 (API v3), Lidarr 3.1.0 (API v1), and Tautulli 2.18.1. It has four tools:

- `arr_library_inventory` (Sonarr, Radarr, or Lidarr; bounded local pagination)
- `arr_quality_profiles`
- `arr_root_folders`
- `tautulli_play_history` (movie, episode, or track; inclusive maximum 31-day range and optional numeric user filter)

There are no write tools, generic upstream requests, approval endpoints, Seerr,
or Jellyfin integrations. Arr APIs return arrays, so inventory pagination is
performed after one bounded response and reports that fact honestly.

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
export MEDIA_BROKER_ALLOWED_HOSTS=127.0.0.1:8000
export MEDIA_BROKER_ALLOWED_ORIGINS=http://127.0.0.1:8000
uv run media-broker
```

All four upstreams and all secret files are required. The broker defaults to
loopback binding, HTTPS certificate verification, no redirects, a 10-second
total upstream deadline, and a 2 MB upstream response limit. Host and Origin
values are exact allow-lists; wildcard syntax is rejected. Tautulli requests
use its inclusive `after`/`before` date bounds with `grouping=0` and
`include_activity=0` so each returned row represents a playback event.
All upstreams use `X-Api-Key` header authentication. Tautulli 2.18.1 supports
this header; the broker never includes credentials in query URLs.

History projections contain only scalar, validated fields. The explicitly
approved stable identifiers `tautulli_user_id`, `tautulli_rating_key`, and
`tautulli_history_id` come from Tautulli's `user_id`, `rating_key`, and `id`
fields respectively. They are present for household selection and stable
matching; names, emails, IPs, device identifiers, and unknown fields are not
returned. Tautulli's numeric watched status of `1` means completed; `0`,
`0.25`, `0.5`, and `0.75` mean incomplete. A missing or unrecognized status
is reported as unknown, not false.

Run checks with `uv run --dev pytest` and `uv run --dev ruff check .`.

## Container image

CI builds the image on every merge to `main` and publishes it to GHCR as:

- `ghcr.io/mich-murphy/media-broker:main` — rolling tag. The home-infra
  deployment tracks it unpinned; a timer on the Docker host pulls new digests
  and redeploys automatically.
- `ghcr.io/mich-murphy/media-broker:sha-<commit>` — immutable tag used for
  rollback and auditing.

The image is private to the `mich-murphy` account. It runs as UID/GID 65532
with a read-only filesystem, listens on container port 8000, and expects all
credentials as file paths (`*_FILE` variables), never values.

`tests/container.sh` performs an isolated local build-and-probe against fake
upstreams using the `desktop-linux` Docker context; it creates and removes
every resource it uses.

MCP authentication proves possession of the configured bearer token, not human
consent or authorization for a particular media action. The broker is read-only
but upstream credentials and playback titles remain sensitive household data;
keep it on a trusted loopback or private network and review client access.
