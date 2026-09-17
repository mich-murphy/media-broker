#!/usr/bin/env bash
# Isolated local container test. It deliberately refuses every Docker context
# except the approved desktop-linux context and owns every created resource.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
for variable in DOCKER_HOST DOCKER_CONTEXT DOCKER_TLS_VERIFY DOCKER_CERT_PATH; do
  [[ -z "${!variable+x}" ]] || {
    echo "media-broker container test rejects ${variable} overrides" >&2
    exit 2
  }
done
docker_cmd() { docker --context desktop-linux "$@"; }
[[ "$(docker_cmd context show)" == desktop-linux ]] || {
  echo 'media-broker container test requires Docker context desktop-linux' >&2
  exit 2
}
endpoint=$(docker_cmd context inspect --format '{{(index .Endpoints "docker").Host}}' desktop-linux)
[[ "${endpoint}" == "unix://${HOME}/.docker/run/docker.sock" ]] || {
  echo "unexpected desktop-linux Docker endpoint: ${endpoint}" >&2
  exit 2
}

suffix="${USER:-test}-$$-${RANDOM}"
network="media-broker-test-${suffix}"
secrets_volume="media-broker-secrets-${suffix}"
backend="media-broker-backend-${suffix}"
broker="media-broker-${suffix}"
image="ghcr.io/mich-murphy/media-broker:test-${suffix}"
base='python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea'
port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
fake_backend=$(mktemp)
cleanup() {
  docker_cmd rm -f "${broker}" "${backend}" >/dev/null 2>&1 || true
  docker_cmd network rm "${network}" >/dev/null 2>&1 || true
  docker_cmd volume rm "${secrets_volume}" >/dev/null 2>&1 || true
  docker_cmd image rm "${image}" >/dev/null 2>&1 || true
  rm -f "${fake_backend}"
}
trap cleanup EXIT

cat >"${fake_backend}" <<'PY'
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from urllib.parse import urlparse

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path).path
        expected = "fake-upstream-key"
        if path.startswith("/user_usage_stats/"):
            authorized = self.headers.get("X-Emby-Token") == expected
        else:
            authorized = self.headers.get("X-Api-Key") == expected
        if not authorized:
            self.send_error(401)
            return
        if path.endswith("/series") or path.endswith("/movie") or path.endswith("/artist"):
            body = []
        elif path.endswith("/qualityprofile") or path.endswith("/rootfolder"):
            body = []
        elif path == "/api/v2":
            body = {"response": {"result": "success", "data": {"recordsFiltered": 0, "data": []}}}
        elif path.startswith("/user_usage_stats/") and path.endswith("/GetItems"):
            body = []
        else:
            self.send_error(404)
            return
        encoded = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)
    def log_message(self, *args):
        pass

HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
PY

docker_cmd network create "${network}" >/dev/null
docker_cmd volume create "${secrets_volume}" >/dev/null
# The fake files deliberately use the same non-world-readable numeric identity
# required by the production Compose secrets.
# shellcheck disable=SC2016 # expansion is intentional inside the container shell
docker_cmd run --rm --user 0 -v "${secrets_volume}:/run/secrets" "${base}" sh -ec '
  umask 027
  for name in media_broker_token sonarr_api_key radarr_api_key lidarr_api_key tautulli_api_key jellyfin_api_key; do
    value=fake-upstream-key
    [ "${name}" = media_broker_token ] && value=media_broker_token_fake_token_0123456789abcdef
    printf "%s" "${value}" > "/run/secrets/${name}"
    chown 65532:65532 "/run/secrets/${name}"
    chmod 0440 "/run/secrets/${name}"
  done
'
docker_cmd run -d --name "${backend}" --network "${network}" -v "${fake_backend}:/fake.py:ro" "${base}" python3 /fake.py >/dev/null

echo 'Building image (failure is terminal; no fallback mode is used).'
docker_cmd build --progress plain -t "${image}" "${repo_root}"

docker_cmd run -d --name "${broker}" --network "${network}" -p "127.0.0.1:${port}:8000" \
  --user 65532:65532 --read-only --cap-drop ALL --security-opt no-new-privileges:true \
  -v "${secrets_volume}:/run/secrets:ro" \
  -e SONARR_URL=http://"${backend}":8080 -e RADARR_URL=http://"${backend}":8080 \
  -e LIDARR_URL=http://"${backend}":8080 -e TAUTULLI_URL=http://"${backend}":8080 \
  -e JELLYFIN_URL=http://"${backend}":8080 \
  -e SONARR_API_KEY_FILE=/run/secrets/sonarr_api_key \
  -e RADARR_API_KEY_FILE=/run/secrets/radarr_api_key \
  -e LIDARR_API_KEY_FILE=/run/secrets/lidarr_api_key \
  -e TAUTULLI_API_KEY_FILE=/run/secrets/tautulli_api_key \
  -e JELLYFIN_API_KEY_FILE=/run/secrets/jellyfin_api_key \
  -e MEDIA_BROKER_TOKEN_FILE=/run/secrets/media_broker_token \
  -e MEDIA_BROKER_BIND_HOST=0.0.0.0 -e MEDIA_BROKER_ALLOW_PUBLIC_BIND=true \
  -e MEDIA_BROKER_ALLOWED_HOSTS=docker-host:8765 \
  -e MEDIA_BROKER_ALLOWED_ORIGINS=http://docker-host:8765 \
  -e MEDIA_BROKER_MAX_RESPONSE_BYTES=5242880 "${image}" >/dev/null

ready=false
for _ in $(seq 1 30); do
  if [[ "$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:${port}/mcp" || true)" == 401 ]]; then
    ready=true
    break
  fi
  sleep 1
done
[[ "${ready}" == true ]] || { echo 'broker did not become ready' >&2; exit 1; }
python3 - "${port}" <<'PY'
import json
import sys
import urllib.error
import urllib.request

port = sys.argv[1]
url = f"http://127.0.0.1:{port}/mcp"
body = json.dumps({
    "jsonrpc": "2.0", "id": 1, "method": "initialize",
    "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "container-test", "version": "1"}},
}).encode()

def request(payload, token=None, host="docker-host:8765"):
    headers = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}
    headers["Host"] = host
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=payload, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()

assert request(body)[0] == 401
assert request(body, "media_broker_token_fake_token_0123456789abcdef", "wrong-host:8765")[0] == 421
status, text = request(body, "media_broker_token_fake_token_0123456789abcdef")
assert status == 200, (status, text)
for request_body in (
    {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
    {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {
        "name": "arr_library_inventory", "arguments": {"service": "sonarr"}}},
):
    status, text = request(json.dumps(request_body).encode(), "media_broker_token_fake_token_0123456789abcdef")
    assert status == 200, (status, text)
    result = json.loads(text)["result"]
    if request_body["method"] == "tools/list":
        assert {tool["name"] for tool in result["tools"]} == {
            "arr_library_inventory", "arr_quality_profiles", "arr_root_folders", "tautulli_play_history", "jellyfin_play_history"
        }
    else:
        assert json.loads(result["content"][0]["text"])["items"] == []

# Exercise the fifth upstream's header and route through the isolated fake backend.
status, text = request(json.dumps({"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {
    "name": "jellyfin_play_history", "arguments": {
        "user_id": "0123456789abcdef0123456789abcdef", "media_type": "movie",
        "start_date": "2024-01-01", "end_date": "2024-01-01"
    }
}}).encode(), "media_broker_token_fake_token_0123456789abcdef")
assert status == 200, (status, text)
assert json.loads(json.loads(text)["result"]["content"][0]["text"])["items"] == []
PY

echo 'Isolated build, authenticated MCP discovery/read, and unauthenticated rejection passed.'
