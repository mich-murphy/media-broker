//! The authenticated MCP surface: bearer, Host/Origin, discovery, gates, argument bounds.

mod common;

use std::collections::HashSet;

use axum::{
    body::Body,
    extract::Request,
    http::{HeaderValue, StatusCode},
};
use common::{
    Broker, DELETE_TOOL, HASH_A, JELLYFIN_USER, READ_TOOLS, REQUEST_TOOLS, TOKEN, TORRENT_TOOLS,
    assert_contains, qbit_backend, reply, route_upstreams, with, with_qbit,
};
use media_broker::config::{Service, Settings};
use serde_json::{Value, json};

/// The registered tool names as owned strings, for set comparisons.
async fn tool_names(broker: &Broker) -> HashSet<String> {
    broker.tools().await.keys().map(String::from).collect()
}

#[tokio::test]
async fn every_request_without_exactly_one_valid_bearer_is_rejected() {
    let bearer = format!("Bearer {TOKEN}");
    let cases: [(&str, Vec<&[u8]>); 6] = [
        ("missing", vec![]),
        ("wrong token", vec![&bearer.as_bytes()[..bearer.len() - 1]]),
        ("wrong scheme", vec![b"Basic broker-secret-token-0123456789abcdef"]),
        ("bare token", vec![TOKEN.as_bytes()]),
        ("not ASCII", vec![b"Bearer \xc3\xa9\xc3\xa9"]),
        ("duplicated", vec![bearer.as_bytes(), bearer.as_bytes()]),
    ];
    let broker = Broker::start(route_upstreams).await;
    for (case, values) in cases {
        for (method, path) in [
            ("GET", "/mcp"),
            ("POST", "/mcp"),
            ("DELETE", "/mcp"),
            ("GET", "/"),
            ("POST", "/other"),
        ] {
            let mut request = Request::builder()
                .method(method)
                .uri(path)
                .header("host", "testserver")
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json");
            for value in &values {
                request = request.header("authorization", HeaderValue::from_bytes(value).unwrap());
            }
            let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}).to_string();
            let reply = broker.send(request.body(Body::from(body)).unwrap()).await;
            assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{case}: {method} {path}");
            assert_eq!(reply.headers["www-authenticate"], "Bearer", "{case}: {method} {path}");
            assert_eq!(reply.body, json!({"error": "authentication required"}));
            assert!(!reply.headers.contains_key("server"));
        }
    }
    let lowercase = format!("bearer {TOKEN}");
    let reply = broker.rpc_with("tools/list", json!({}), &[("authorization", &lowercase)]).await;
    assert_eq!(reply.status, StatusCode::OK, "the scheme is case-insensitive");
    assert!(!reply.headers.contains_key("server"));
}

#[tokio::test]
async fn host_and_origin_allow_lists_are_enforced() {
    let broker = Broker::start(route_upstreams).await;
    let cases = [
        ("host", "unexpected", StatusCode::FORBIDDEN),
        ("host", "testserver.evil.test", StatusCode::FORBIDDEN),
        ("origin", "http://unexpected", StatusCode::FORBIDDEN),
        ("origin", "https://testserver", StatusCode::FORBIDDEN),
        ("origin", "http://testserver", StatusCode::OK),
    ];
    for (header, value, status) in cases {
        let reply = broker.rpc_with("tools/list", json!({}), &[(header, value)]).await;
        assert_eq!(reply.status, status, "{header}: {value}");
    }
}

#[tokio::test]
async fn tools_are_discoverable_read_only_and_schema_bounded() {
    let tools = Broker::start(route_upstreams).await.tools().await;
    assert_eq!(tools.keys().map(String::as_str).collect::<HashSet<_>>(), HashSet::from(READ_TOOLS));
    for (name, tool) in &tools {
        let expected = json!({"readOnlyHint": true, "destructiveHint": false, "idempotentHint": true,
                              "openWorldHint": false});
        assert_contains(&tool["annotations"], &expected, name);
        assert_eq!(tool["inputSchema"]["type"], "object", "{name}");
        assert!(!tool["description"].as_str().unwrap().is_empty(), "{name}");
        assert!(!tool["inputSchema"].to_string().contains("$ref"), "{name} must be self-contained");
    }
    let bounds =
        |schema: &Value| (schema["minimum"].as_f64().unwrap(), schema["maximum"].as_f64().unwrap());
    let inventory = &tools["arr_library_inventory"]["inputSchema"];
    assert_eq!(inventory["required"], json!(["service"]));
    assert_eq!(inventory["properties"]["service"]["enum"], json!(["sonarr", "radarr", "lidarr"]));
    assert_eq!(bounds(&inventory["properties"]["page_size"]), (1.0, 100.0));
    assert_eq!(inventory["properties"]["page_size"]["default"], 50);
    assert_eq!(inventory["properties"]["search"]["maxLength"], 200);
    let history = &tools["tautulli_play_history"]["inputSchema"]["properties"];
    assert_contains(
        &history["start_date"],
        &json!({"type": "string", "format": "date"}),
        "start_date",
    );
    let jellyfin = &tools["jellyfin_play_history"]["inputSchema"]["properties"];
    assert_eq!(jellyfin["media_type"]["enum"], json!(["movie", "episode", "track"]));
    assert_contains(&jellyfin["user_id"], &json!({"minLength": 32, "maxLength": 32}), "user_id");
    assert_eq!(bounds(&jellyfin["timezone_offset"]), (-14.0, 14.0));
    let seasons = &tools["arr_season_inventory"]["inputSchema"]["properties"];
    assert_eq!(seasons["service"]["enum"], json!(["sonarr"]));
    assert_eq!(bounds(&seasons["item_id"]), (1.0, 2_147_483_647.0));
}

#[tokio::test]
async fn each_read_tool_answers_over_streamable_http() {
    let history =
        json!({"media_type": "movie", "start_date": "2024-01-01", "end_date": "2024-01-31"});
    let jellyfin =
        with(history.clone(), json!({"user_id": JELLYFIN_USER, "end_date": "2024-01-01"}));
    // (tool, arguments, fields expected on the first item)
    #[rustfmt::skip]
    let cases = [
        ("arr_library_inventory", json!({"service": "sonarr"}), json!({"title": "Item"})),
        ("arr_quality_profiles", json!({"service": "radarr"}), json!({"name": "HD"})),
        ("arr_root_folders", json!({"service": "lidarr"}), json!({"path": "/tv"})),
        ("arr_search_candidates", json!({"service": "sonarr", "query": "example", "limit": 5}), json!({"title": "Item"})),
        ("arr_album_inventory", json!({"service": "lidarr", "artist_id": 42}), json!({"album_id": 11})),
        ("tautulli_play_history", history, json!({"title": "Movie", "tautulli_user_id": 3})),
        ("jellyfin_play_history", jellyfin, json!({"title": "Movie", "jellyfin_user_id": JELLYFIN_USER})),
        ("jellyfin_users", json!({}), json!({"name": "Alice", "jellyfin_user_id": JELLYFIN_USER})),
    ];
    let broker = Broker::start(route_upstreams).await;
    for (name, arguments, expected) in cases {
        let content = broker.ok(name, arguments).await;
        assert_contains(&content["items"][0], &expected, name);
    }
    let seasons =
        broker.ok("arr_season_inventory", json!({"service": "sonarr", "item_id": 3})).await;
    assert_eq!(
        seasons["seasons"],
        json!([{"season_number": 1, "monitored": true, "episode_count": 1, "episode_file_count": 1}])
    );
    // A tool without arguments may be called without an arguments object at all.
    let reply = broker.rpc("tools/call", json!({"name": "jellyfin_users"})).await;
    assert_eq!(reply.body["result"]["structuredContent"]["items"][0]["name"], "Alice");
}

#[tokio::test]
async fn out_of_bounds_arguments_are_rejected_before_any_upstream_call() {
    let inventory = |extra| with(json!({"service": "sonarr"}), extra);
    let tautulli = |extra| {
        with(
            json!({"media_type": "movie", "start_date": "2024-01-01", "end_date": "2024-01-01"}),
            extra,
        )
    };
    let jellyfin = |extra| with(tautulli(json!({"user_id": JELLYFIN_USER})), extra);
    // (tool, arguments, field named by the error)
    #[rustfmt::skip]
    let cases = [
        ("arr_library_inventory", json!({"service": "plex"}), "service"),
        ("arr_library_inventory", json!({}), "service"),
        ("arr_library_inventory", inventory(json!({"page": 0})), "page"),
        ("arr_library_inventory", inventory(json!({"page": 100_001})), "page"),
        ("arr_library_inventory", inventory(json!({"page": 1.5})), "page"),
        ("arr_library_inventory", inventory(json!({"page_size": 101})), "page_size"),
        ("arr_library_inventory", inventory(json!({"search": "x".repeat(201)})), "search"),
        ("tautulli_play_history", tautulli(json!({"start_date": "2024-1-01"})), "start_date"),
        ("tautulli_play_history", tautulli(json!({"start_date": "2024-02-30", "end_date": "2024-03-01"})), "start_date"),
        ("tautulli_play_history", tautulli(json!({"start_date": 20_240_101})), "start_date"),
        ("tautulli_play_history", json!({"media_type": "movie", "start_date": "2024-01-01"}), "end_date"),
        ("tautulli_play_history", tautulli(json!({"media_type": "photo"})), "media_type"),
        ("tautulli_play_history", tautulli(json!({"user_id": -1})), "user_id"),
        ("tautulli_play_history", tautulli(json!({"end_date": "2024-03-01"})), "31 inclusive"),
        ("jellyfin_play_history", jellyfin(json!({"user_id": "not-a-user-id"})), "user_id"),
        ("jellyfin_play_history", jellyfin(json!({"user_id": "g".repeat(32)})), "user_id"),
        ("jellyfin_play_history", jellyfin(json!({"timezone_offset": 15})), "timezone_offset"),
        ("jellyfin_play_history", jellyfin(json!({"timezone_offset": -14.5})), "timezone_offset"),
        ("arr_season_inventory", json!({"service": "radarr", "item_id": 3}), "service"),
        ("arr_album_inventory", json!({"service": "lidarr", "artist_id": 0}), "artist_id"),
        ("arr_search_candidates", json!({"service": "sonarr", "query": ""}), "query"),
        ("arr_search_candidates", json!({"service": "sonarr", "query": "x", "limit": 0}), "limit"),
    ];
    let broker = Broker::start(route_upstreams).await;
    for (name, arguments, field) in cases {
        let error = broker.err(name, arguments.clone()).await;
        assert!(error.contains(field), "{name} {arguments}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
}

#[tokio::test]
async fn write_tools_appear_only_behind_their_gates() {
    let reads: HashSet<&str> = READ_TOOLS.into();
    let requests: HashSet<&str> = REQUEST_TOOLS.into();
    for (enable_requests, enable_deletes) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let broker = Broker::configured(
            |settings| {
                (settings.enable_requests, settings.enable_deletes) =
                    (enable_requests, enable_deletes);
            },
            reply(json!({"id": 3, "title": "Item"})),
        )
        .await;
        let mut expected = reads.clone();
        expected.extend(requests.iter().filter(|_| enable_requests));
        expected.extend(enable_deletes.then_some(DELETE_TOOL));
        let tools = broker.tools().await;
        let case = format!("requests={enable_requests} deletes={enable_deletes}");
        assert_eq!(tools.keys().map(String::as_str).collect::<HashSet<_>>(), expected, "{case}");

        // A hidden tool is also uncallable, and never reaches an upstream.
        let arguments = json!({"service": "sonarr", "item_id": 3});
        for (name, enabled) in
            [("arr_unmonitor_media", enable_requests), (DELETE_TOOL, enable_deletes)]
        {
            let params = json!({"name": name, "arguments": arguments});
            let body = broker.rpc("tools/call", params).await.body;
            let refused = !body["error"].is_null() || body["result"]["isError"] == true;
            assert_eq!(refused, !enabled, "{case} {name}: {body}");
        }
        assert!(enable_requests || enable_deletes || broker.seen().is_empty(), "{case}");

        let hints = |name: &str| {
            let hint = |key: &str| tools[name]["annotations"][key].as_bool().unwrap();
            (hint("readOnlyHint"), hint("destructiveHint"), hint("idempotentHint"))
        };
        if enable_requests {
            assert_eq!(hints("arr_request_media"), (false, false, false));
            assert_eq!(hints("arr_search_item"), (false, false, false));
            for name in &REQUEST_TOOLS[1..5] {
                assert_eq!(hints(name), (false, false, true), "{name} is a reversible toggle");
            }
        }
        if enable_deletes {
            assert_eq!(hints(DELETE_TOOL), (false, true, false));
        }
    }
}

#[tokio::test]
async fn health_answers_liveness_only_and_everything_else_needs_a_bearer() {
    let broker = Broker::start(route_upstreams).await;
    let get_health = Request::builder()
        .method("GET")
        .uri("/health")
        .header("host", "testserver")
        .body(Body::empty())
        .unwrap();
    let reply = broker.send(get_health).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body, json!({"status": "ok"}));
    assert_eq!(reply.headers["cache-control"], "no-store");
    assert!(!reply.headers.contains_key("server"));
    // Any other method or path still falls through to the bearer boundary.
    for (method, path) in [("POST", "/health"), ("GET", "/health/"), ("HEAD", "/health")] {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("host", "testserver")
            .body(Body::empty())
            .unwrap();
        let reply = broker.send(request).await;
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{method} {path}");
        assert_eq!(reply.body, json!({"error": "authentication required"}));
    }
}

#[tokio::test]
async fn the_tool_surface_follows_the_configured_upstreams() {
    let set = |names: &[&str]| names.iter().map(ToString::to_string).collect::<HashSet<_>>();
    // One upstream only: exactly its own read tools appear.
    let only = |keep: Service| {
        move |settings: &mut Settings| {
            for (index, slot) in settings.upstreams.iter_mut().enumerate() {
                if Service::ALL[index] != keep {
                    *slot = None;
                }
            }
        }
    };
    let tautulli = Broker::configured(only(Service::Tautulli), route_upstreams).await;
    assert_eq!(tool_names(&tautulli).await, set(&["tautulli_play_history"]));
    let jellyfin = Broker::configured(only(Service::Jellyfin), route_upstreams).await;
    assert_eq!(tool_names(&jellyfin).await, set(&["jellyfin_play_history", "jellyfin_users"]));
    let sonarr = Broker::configured(only(Service::Sonarr), route_upstreams).await;
    assert_eq!(
        tool_names(&sonarr).await,
        set(&[
            "arr_library_inventory",
            "arr_quality_profiles",
            "arr_root_folders",
            "arr_search_candidates",
            "arr_season_inventory",
        ])
    );
    // qBittorrent adds its three read tools; with every arr slot empty they
    // are the whole surface.
    let qbit = Broker::configured(
        |settings| {
            with_qbit(settings);
            for service in [Service::Sonarr, Service::Radarr, Service::Lidarr] {
                settings.upstreams[service as usize] = None;
            }
            settings.upstreams[Service::Tautulli as usize] = None;
            settings.upstreams[Service::Jellyfin as usize] = None;
        },
        qbit_backend(json!([]), json!({}), true, true, false),
    )
    .await;
    assert_eq!(tool_names(&qbit).await, set(&TORRENT_TOOLS));
    // Torrent tools share the read-only annotations and bounded schemas.
    let tools = qbit.tools().await;
    for name in TORRENT_TOOLS {
        let expected = json!({"readOnlyHint": true, "destructiveHint": false,
                              "idempotentHint": true, "openWorldHint": false});
        assert_contains(&tools[name]["annotations"], &expected, name);
    }
    let bounds =
        |schema: &Value| (schema["minimum"].as_f64().unwrap(), schema["maximum"].as_f64().unwrap());
    let inventory = &tools["torrent_client_inventory"]["inputSchema"];
    // Every argument is optional, so schemars may omit `required` entirely.
    assert!(inventory["required"].as_array().is_none_or(Vec::is_empty));
    assert_eq!(bounds(&inventory["properties"]["page_size"]), (1.0, 100.0));
    assert_eq!(inventory["properties"]["search"]["maxLength"], 200);
    let hashes = &tools["torrent_client_check_paths"]["inputSchema"]["properties"]["hashes"];
    assert_eq!(hashes["minItems"], 1);
    assert_eq!(hashes["maxItems"], 100);
    assert_eq!(hashes["items"]["maxLength"], 64);
}

#[tokio::test]
async fn torrent_arguments_are_rejected_before_any_upstream_call() {
    let broker =
        Broker::configured(with_qbit, qbit_backend(json!([]), json!({}), true, true, false)).await;
    let many: Vec<&str> = [HASH_A].repeat(101);
    let long = "x".repeat(65);
    let cases = [
        ("torrent_client_inventory", json!({"page": 0}), "page"),
        ("torrent_client_inventory", json!({"page_size": 101}), "page_size"),
        ("torrent_client_inventory", json!({"search": "x".repeat(201)}), "search"),
        ("torrent_client_check_paths", json!({"hashes": []}), "hashes"),
        ("torrent_client_check_paths", json!({"hashes": many}), "hashes"),
        ("torrent_client_check_paths", json!({"hashes": [long]}), "hashes"),
        ("torrent_client_check_paths", json!({}), "hashes"),
    ];
    for (name, arguments, field) in cases {
        let error = broker.err(name, arguments.clone()).await;
        assert!(error.contains(field), "{name} {arguments}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
}
