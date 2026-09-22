//! Shared harness: the real broker router driven in-process against a
//! recording fake upstream on a loopback socket.

#![allow(dead_code)] // every test binary uses a different subset

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::Request,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::Response,
};
use media_broker::{
    config::{Auth, Secret, Service, Settings, Upstream},
    server,
};
use serde_json::{Value, json};
use tower::ServiceExt;

pub const TOKEN: &str = "broker-secret-token-0123456789abcdef";
pub const JELLYFIN_USER: &str = "0123456789abcdef0123456789abcdef";
pub const MBID: &str = "f59c5520-5f46-4d2c-b2c4-822eabf53419";
pub const READ_TOOLS: [&str; 9] = [
    "arr_library_inventory",
    "arr_quality_profiles",
    "arr_root_folders",
    "arr_search_candidates",
    "arr_season_inventory",
    "arr_album_inventory",
    "tautulli_play_history",
    "jellyfin_play_history",
    "jellyfin_users",
];
pub const REQUEST_TOOLS: [&str; 6] = [
    "arr_request_media",
    "arr_unmonitor_media",
    "arr_monitor_media",
    "arr_set_season_monitoring",
    "arr_set_album_monitored",
    "arr_search_item",
];
pub const DELETE_TOOL: &str = "arr_delete_media";
pub const TORRENT_TOOLS: [&str; 3] =
    ["torrent_client_stats", "torrent_client_inventory", "torrent_client_check_paths"];
pub const QBIT_SID: &str = "qbit-test-session-id";
pub const HASH_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const HASH_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

/// One request as the fake upstream received it.
#[derive(Clone, Debug)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub query: HashMap<String, String>,
    /// Raw path and query, for asserting that keys never travel in the URL.
    pub target: String,
    pub headers: HeaderMap,
    pub body: Value,
    /// The raw body as text, for the form-encoded qBittorrent login.
    pub text: String,
}

impl Seen {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Value,
}

pub struct Broker {
    app: Router,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Broker {
    /// A broker with default settings whose five upstreams all reach `upstream`.
    pub async fn start(upstream: impl Fn(&Seen) -> Response + Send + Sync + 'static) -> Self {
        Self::configured(|_| {}, upstream).await
    }

    /// A broker with both write gates open.
    pub async fn writes(upstream: impl Fn(&Seen) -> Response + Send + Sync + 'static) -> Self {
        Self::configured(open_gates, upstream).await
    }

    pub async fn configured(
        configure: impl FnOnce(&mut Settings),
        upstream: impl Fn(&Seen) -> Response + Send + Sync + 'static,
    ) -> Self {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let upstream = Arc::new(upstream);
        let fake = Router::new().fallback(move |request: Request| {
            let (log, upstream) = (Arc::clone(&log), Arc::clone(&upstream));
            async move {
                let (parts, body) = request.into_parts();
                let body = to_bytes(body, usize::MAX).await.unwrap();
                let query = parts.uri.query().unwrap_or_default();
                let record = Seen {
                    method: parts.method.to_string(),
                    path: parts.uri.path().to_owned(),
                    query: url::form_urlencoded::parse(query.as_bytes()).into_owned().collect(),
                    target: parts.uri.to_string(),
                    headers: parts.headers,
                    body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                    text: String::from_utf8_lossy(&body).into_owned(),
                };
                let response = upstream(&record);
                log.lock().unwrap().push(record);
                response
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, fake).await.unwrap() });

        let mut settings = Settings {
            bearer_token: Secret::new(TOKEN),
            upstreams: Service::ALL.map(|service| {
                (service != Service::Qbittorrent).then(|| Upstream {
                    base_url: base_url.clone(),
                    auth: Auth::Key(Secret::new(format!("{}-key", service.name()))),
                })
            }),
            allowed_hosts: vec!["testserver".into()],
            allowed_origins: vec!["http://testserver".into()],
            bind_host: "127.0.0.1".into(),
            port: 8000,
            timeout: Duration::from_secs(10),
            max_response_bytes: 2_000_000,
            enable_requests: false,
            enable_deletes: false,
            confirmation_ttl: Duration::from_secs(300),
        };
        configure(&mut settings);
        Self { app: server::router(settings), seen }
    }

    /// Every upstream request so far, oldest first.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    pub async fn send(&self, request: Request) -> Reply {
        let response = self.app.clone().oneshot(request).await.unwrap();
        let (parts, body) = response.into_parts();
        let body = to_bytes(body, usize::MAX).await.unwrap();
        Reply {
            status: parts.status,
            headers: parts.headers,
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        }
    }

    /// POST one JSON-RPC request to `/mcp`; `headers` replace the defaults.
    pub async fn rpc_with(&self, method: &str, params: Value, headers: &[(&str, &str)]) -> Reply {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let mut request = Request::post("/mcp").body(Body::from(body.to_string())).unwrap();
        let bearer = format!("Bearer {TOKEN}");
        let defaults = [
            ("host", "testserver"),
            ("authorization", bearer.as_str()),
            ("accept", "application/json, text/event-stream"),
            ("content-type", "application/json"),
        ];
        for (name, value) in defaults.iter().chain(headers) {
            request
                .headers_mut()
                .insert(name.parse::<HeaderName>().unwrap(), HeaderValue::from_str(value).unwrap());
        }
        self.send(request).await
    }

    pub async fn rpc(&self, method: &str, params: Value) -> Reply {
        self.rpc_with(method, params, &[]).await
    }

    /// The listed tools by name.
    pub async fn tools(&self) -> HashMap<String, Value> {
        let reply = self.rpc("tools/list", json!({})).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        let tools = reply.body["result"]["tools"].as_array().unwrap().clone();
        tools.into_iter().map(|tool| (tool["name"].as_str().unwrap().to_owned(), tool)).collect()
    }

    /// The `tools/call` result object.
    pub async fn call(&self, name: &str, arguments: Value) -> Value {
        let params = json!({"name": name, "arguments": arguments});
        let reply = self.rpc("tools/call", params).await;
        assert_eq!(reply.status, StatusCode::OK, "{name}: {}", reply.body);
        reply.body["result"].clone()
    }

    /// The structured content of a successful call, which the text block must mirror.
    pub async fn ok(&self, name: &str, arguments: Value) -> Value {
        let result = self.call(name, arguments).await;
        assert_ne!(result["isError"], true, "{name}: {result}");
        let content = result["structuredContent"].clone();
        let text = result["content"][0]["text"].as_str().unwrap();
        assert_eq!(serde_json::from_str::<Value>(text).unwrap(), content);
        content
    }

    /// The message of a call that must fail as an `isError` result.
    pub async fn err(&self, name: &str, arguments: Value) -> String {
        let result = self.call(name, arguments).await;
        assert_eq!(result["isError"], true, "{name}: {result}");
        result["content"][0]["text"].as_str().unwrap().to_owned()
    }
}

pub fn open_gates(settings: &mut Settings) {
    settings.enable_requests = true;
    settings.enable_deletes = true;
}

/// Point the qBittorrent slot at the fake upstream with session credentials.
pub fn with_qbit(settings: &mut Settings) {
    let base_url = settings.upstream(Service::Sonarr).unwrap().base_url.clone();
    settings.upstreams[Service::Qbittorrent as usize] = Some(Upstream {
        base_url,
        auth: Auth::Session { username: "admin".to_owned(), password: Secret::new("secret") },
    });
}

/// A fake qBittorrent `WebUI` with session-cookie authentication.
/// `login_ok=false` refuses the login; `accept_session=false` rejects every
/// session; `fail_first_get=true` answers the first authenticated GET with a
/// 403 before accepting the refreshed session.
#[allow(clippy::needless_pass_by_value)] // callers pass `json!` literals
#[allow(clippy::fn_params_excessive_bools)] // named flags of one fake
pub fn qbit_backend(
    torrents: Value,
    stats: Value,
    login_ok: bool,
    accept_session: bool,
    fail_first_get: bool,
) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
    let first_get = Mutex::new(fail_first_get);
    move |seen| {
        if seen.path == "/api/v2/auth/login" {
            if !login_ok {
                return respond(200, "Fails.");
            }
            return Response::builder()
                .status(200)
                .header("set-cookie", format!("SID={QBIT_SID}"))
                .body("Ok.".into())
                .unwrap();
        }
        let expected = format!("SID={QBIT_SID}");
        if !accept_session || seen.header("cookie") != Some(expected.as_str()) {
            return respond(403, Body::empty());
        }
        let mut first = first_get.lock().unwrap();
        if *first {
            *first = false;
            return respond(403, Body::empty());
        }
        drop(first);
        match seen.path.as_str() {
            "/api/v2/torrents/info" => respond(200, torrents.to_string()),
            "/api/v2/transfer/info" => respond(200, stats.to_string()),
            _ => respond(404, Body::empty()),
        }
    }
}

/// One torrent row carrying a field that must never pass through.
pub fn torrent_row() -> Value {
    json!({
        "hash": HASH_A,
        "name": "Example.Release.2024",
        "state": "stoppedUP",
        "size": 4_700_000_000_i64,
        "progress": 1.0,
        "ratio": 2.5,
        "seeding_time": 864_000,
        "save_path": "/mnt/data/torrents",
        "added_on": 1_700_000_000,
        "amount_left": 0,
        "tracker": "must not pass",
    })
}

pub fn respond(status: u16, body: impl Into<Body>) -> Response {
    Response::builder().status(status).body(body.into()).unwrap()
}

/// A fake upstream that answers every request with the same JSON payload.
pub fn reply(payload: Value) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
    reply_status(200, payload)
}

pub fn reply_status(
    status: u16,
    payload: Value,
) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
    move |_| respond(status, payload.to_string())
}

/// Tautulli's success envelope; `total` defaults to the row count.
#[allow(clippy::needless_pass_by_value)] // callers pass `json!` literals
pub fn history_reply(
    rows: Value,
    total: Option<Value>,
) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
    let total = total.unwrap_or_else(|| json!(rows.as_array().unwrap().len()));
    reply(
        json!({"response": {"result": "success", "data": {"recordsFiltered": total, "data": rows}}}),
    )
}

/// `base` with the keys of `extra` added or replaced.
#[allow(clippy::needless_pass_by_value)] // callers pass `json!` literals
pub fn with(mut base: Value, extra: Value) -> Value {
    for (key, value) in extra.as_object().unwrap() {
        base[key] = value.clone();
    }
    base
}

/// Assert that every key of `expected` has the same value in `actual`.
pub fn assert_contains(actual: &Value, expected: &Value, case: &str) {
    for (key, value) in expected.as_object().unwrap() {
        assert_eq!(&actual[key], value, "{case}: {key} in {actual}");
    }
}

/// An unadded lookup candidate carrying fields that must never pass through.
pub fn candidate(service: &str) -> Value {
    match service {
        "sonarr" => json!({
            "id": 0, "tvdbId": 81189, "title": "Example Show", "titleSlug": "example-show",
            "seriesType": "anime", "year": 2021, "status": "continuing",
            "seasons": [{"seasonNumber": 1}, {"seasonNumber": 2}], "network": "must not pass",
        }),
        "radarr" => json!({
            "id": 0, "tmdbId": 603, "title": "Example Film", "titleSlug": "example-film-603",
            "year": 1999, "status": "released", "images": [{"url": "must not pass"}],
        }),
        _ => json!({
            "id": 0, "foreignArtistId": MBID, "artistName": "Example Band", "status": "ended",
            "genres": ["must not pass"],
        }),
    }
}

/// A routed write-capable fake Arr upstream. `overrides` replaces any of the
/// default lists and records; the first album also answers single-album reads.
pub fn write_backend(overrides: Value) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
    let defaults = json!({
        "candidates": [], "item": {"id": 42, "title": "Item"}, "profiles": [{"id": 7}],
        "folders": [{"path": "/media"}], "metadata": [{"id": 1}], "episodes": [],
        "albums": [{"id": 11, "title": "Example Album", "artistId": 42, "monitored": true}],
    });
    for key in overrides.as_object().unwrap().keys() {
        assert!(defaults.get(key).is_some(), "unknown backend key: {key}");
    }
    let backend = with(defaults, overrides);
    move |seen| {
        let lists = [
            ("/lookup", "candidates"),
            ("/qualityprofile", "profiles"),
            ("/rootfolder", "folders"),
            ("/metadataprofile", "metadata"),
            ("/episode", "episodes"),
            ("/album", "albums"),
        ];
        let list = lists.iter().find(|(suffix, _)| seen.path.ends_with(suffix));
        let payload = match (list, seen.method.as_str()) {
            (Some((_, key)), _) => backend[*key].clone(),
            (None, "DELETE") => return respond(200, Body::empty()),
            (None, "GET") if seen.path.contains("/album/") => backend["albums"][0].clone(),
            (None, "PUT") => seen.body.clone(),
            (None, "POST") if seen.path.ends_with("/command") => {
                json!({"id": 9, "name": seen.body["name"], "status": "queued"})
            }
            (None, "POST") => with(backend["item"].clone(), json!({"id": 42})),
            _ => backend["item"].clone(),
        };
        respond(200, payload.to_string())
    }
}

/// A fake upstream answering each read endpoint with one representative item.
pub fn route_upstreams(seen: &Seen) -> Response {
    let path = seen.path.as_str();
    let last = path.rsplit('/').next().unwrap_or_default();
    let payload = if path == "/api/v2" {
        return history_reply(json!([{"title": "Movie", "user_id": 3}]), None)(seen);
    } else if path == "/Users" {
        json!([{"Id": JELLYFIN_USER, "Name": "Alice", "Policy": {}}])
    } else if path.starts_with("/user_usage_stats/") {
        json!([{"Id": "item-id", "RowId": 8, "Name": "Movie", "Type": "Movie", "Time": "12:34:56", "Duration": 120}])
    } else if path.ends_with("/episode") {
        json!([{"seasonNumber": 1, "hasFile": true}])
    } else if path.ends_with("/album") {
        json!([{"id": 11, "title": "Album", "statistics": {}}])
    } else if !last.is_empty() && last.bytes().all(|byte| byte.is_ascii_digit()) {
        json!({"id": 1, "title": "Item", "seasons": [{"seasonNumber": 1, "monitored": true}]})
    } else {
        json!([{"id": 1, "title": "Item", "name": "HD", "path": "/tv"}])
    };
    respond(200, payload.to_string())
}
