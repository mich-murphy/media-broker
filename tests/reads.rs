//! Read tools: allow-listed projections, bounded pagination, sanitized failures.

mod common;

use std::{convert::Infallible, time::Duration};

use axum::{body::Body, response::Response};
use common::{
    Broker, HASH_A, HASH_B, JELLYFIN_USER, QBIT_SID, Seen, assert_contains, candidate,
    history_reply, qbit_backend, reply, reply_status, respond, torrent_row, with, with_qbit,
    write_backend,
};
use futures_util::StreamExt as _;
use media_broker::config::{Service, Settings};
use serde_json::{Value, json};

async fn inventory(broker: &Broker, service: &str, arguments: Value) -> Value {
    let arguments = with(json!({"service": service, "page_size": 10}), arguments);
    broker.ok("arr_library_inventory", arguments).await
}

fn day(start: &str, end: &str) -> Value {
    json!({"user_id": JELLYFIN_USER, "media_type": "movie", "start_date": start, "end_date": end})
}

fn one_day(media_type: &str) -> Value {
    json!({"media_type": media_type, "start_date": "2024-01-01", "end_date": "2024-01-01"})
}

#[tokio::test]
async fn inventory_projects_allow_listed_fields_only() {
    let cases = [
        (
            "sonarr",
            "/api/v3/series",
            json!({"title": "Example"}),
            "Example",
            json!({"episode_file_count": null, "seasons": null}),
        ),
        (
            "lidarr",
            "/api/v1/artist",
            json!({"artistName": "Band"}),
            "Band",
            json!({"track_file_count": null}),
        ),
    ];
    for (service, path, upstream_item, title, extras) in cases {
        let item = json!({
            "id": 4, "year": 2024, "status": "continuing", "monitored": true,
            "qualityProfileId": 7, "rootFolderPath": "/media", "path": "/media/private",
            "images": [{"url": "must not pass"}], "statistics": "must not pass",
            "movieFile": "must not pass",
        });
        let broker = Broker::start(reply(json!([with(item, upstream_item)]))).await;
        let result = inventory(&broker, service, json!({})).await;
        let expected = json!({
            "id": 4, "title": title, "year": 2024, "status": "continuing", "monitored": true,
            "quality_profile_id": 7, "root_folder_path": "/media", "added": null,
            "genres": null, "size_on_disk_bytes": null, "has_file": null,
        });
        assert_eq!(result["items"], json!([with(expected, extras)]), "{service}");
        let seen = broker.seen();
        assert_eq!(seen[0].path, path);
        assert_eq!(seen[0].header("x-api-key"), Some(format!("{service}-key").as_str()));
        assert!(!seen[0].target.contains("-key"), "{}", seen[0].target);
    }
}

#[tokio::test]
async fn inventory_projects_cleanup_audit_fields_and_rejects_wrong_types() {
    let cases = [
        (
            "sonarr",
            json!({"statistics": {"sizeOnDisk": 12_000_000_000_i64, "episodeFileCount": 3, "percentOfEpisodes": "must not pass"},
                "genres": ["Anime", "Drama"], "added": "2021-05-06T07:08:09Z"}),
            json!({"size_on_disk_bytes": 12_000_000_000_i64, "episode_file_count": 3, "has_file": true,
                "genres": ["Anime", "Drama"], "added": "2021-05-06T07:08:09Z"}),
        ),
        (
            "radarr",
            json!({"movieFile": {"size": 8_000_000_000_i64, "relativePath": "must not pass"}, "hasFile": true,
                "genres": ["Animation"], "added": "2020-01-02T03:04:05Z"}),
            json!({"size_on_disk_bytes": 8_000_000_000_i64, "has_file": true, "genres": ["Animation"],
                "added": "2020-01-02T03:04:05Z"}),
        ),
        (
            "lidarr",
            json!({"statistics": {"sizeOnDisk": 500, "trackFileCount": 9}, "genres": ["Rock"]}),
            json!({"size_on_disk_bytes": 500, "track_file_count": 9, "has_file": true, "genres": ["Rock"]}),
        ),
        (
            "sonarr",
            json!({"statistics": {"sizeOnDisk": 7}}),
            json!({"size_on_disk_bytes": 7, "episode_file_count": null, "has_file": null}),
        ),
        (
            "sonarr",
            json!({"statistics": {"episodeFileCount": 0}}),
            json!({"episode_file_count": 0, "has_file": false}),
        ),
        (
            "radarr",
            json!({"movieFile": {"size": "private"}, "hasFile": 1}),
            json!({"size_on_disk_bytes": null, "has_file": null}),
        ),
        (
            "lidarr",
            json!({"statistics": ["must not pass"]}),
            json!({"size_on_disk_bytes": null, "track_file_count": null, "has_file": null}),
        ),
        (
            "radarr",
            json!({"id": true, "year": 1999.5, "title": 7, "monitored": "yes"}),
            json!({"id": null, "year": null, "title": null, "monitored": null}),
        ),
    ];
    for (service, upstream_item, expected) in cases {
        let row = with(json!({"id": 1, "title": "Item"}), upstream_item.clone());
        let result = inventory(&Broker::start(reply(json!([row]))).await, service, json!({})).await;
        let item = &result["items"][0];
        assert_contains(item, &expected, &format!("{service} {upstream_item}"));
        assert!(!item.to_string().contains("must not pass"), "{item}");
    }
}

#[tokio::test]
async fn inventory_lists_are_capped_and_dropped_when_invalid() {
    let mut genres =
        vec![json!("ok"), json!({"not": "a string"}), json!("x".repeat(1025)), json!(7)];
    genres.extend((0..20).map(|index| json!(format!("genre-{index}"))));
    let seasons: Vec<Value> = (0..70)
        .map(|index| json!({"seasonNumber": index, "monitored": index % 2 == 0, "statistics": "must not pass"}))
        .collect();
    let rows = json!([
        {"id": 1, "genres": genres, "seasons": seasons},
        {"id": 2, "genres": "Anime", "seasons": "junk"},
    ]);
    let result = inventory(&Broker::start(reply(rows)).await, "sonarr", json!({})).await;
    // The cap applies to raw entries first; invalid entries are then dropped.
    let mut expected = vec!["ok".to_owned()];
    expected.extend((0..12).map(|index| format!("genre-{index}")));
    assert_eq!(result["items"][0]["genres"], json!(expected));
    let projected = result["items"][0]["seasons"].as_array().unwrap();
    assert_eq!(projected.len(), 64);
    assert_eq!(
        projected[..2],
        [
            json!({"season_number": 0, "monitored": true}),
            json!({"season_number": 1, "monitored": false})
        ]
    );
    assert_contains(&result["items"][1], &json!({"genres": null, "seasons": null}), "not lists");
}

#[tokio::test]
async fn inventory_search_and_local_pagination() {
    let titles = ["Alpha One", "alpha two", "Beta"];
    let rows: Vec<Value> =
        titles.iter().enumerate().map(|(id, title)| json!({"id": id, "title": title})).collect();
    let broker = Broker::start(reply(json!(rows))).await;
    let page = |page: u32| json!({"page": page, "page_size": 2});
    let first = inventory(&broker, "radarr", page(1)).await;
    assert_eq!(first["items"][0]["title"], titles[0]);
    assert_eq!(first["items"][1]["title"], titles[1]);
    assert_eq!(
        first["pagination"],
        json!({"page": 1, "page_size": 2, "returned_count": 2, "total_count": 3, "has_more": true,
               "upstream_paginated": false, "upstream_truncated": false})
    );
    let last = inventory(&broker, "radarr", page(2)).await;
    assert_eq!(last["items"].as_array().unwrap().len(), 1);
    assert_eq!(last["items"][0]["title"], titles[2]);
    assert_eq!(last["pagination"]["has_more"], false);
    let searched = inventory(&broker, "radarr", json!({"search": " ALPHA "})).await;
    assert_eq!(searched["pagination"]["total_count"], 2);
    assert_eq!(searched["items"][1]["title"], titles[1]);
}

#[tokio::test]
async fn quality_profiles_and_root_folders_are_projected() {
    let profile =
        json!({"id": 1, "name": "HD", "upgradeAllowed": true, "cutoff": 7, "items": ["x"]});
    let broker = Broker::start(reply(json!([profile]))).await;
    assert_eq!(
        broker.ok("arr_quality_profiles", json!({"service": "sonarr"})).await,
        json!({"service": "sonarr", "upstream_truncated": false,
               "items": [{"id": 1, "name": "HD", "upgrade_allowed": true, "cutoff": 7}]})
    );
    assert_eq!(broker.seen()[0].path, "/api/v3/qualityprofile");
    let folder =
        json!({"id": 2, "path": "/tv", "accessible": true, "freeSpace": 5, "totalSpace": 9});
    let broker = Broker::start(reply(json!([folder, "junk"]))).await;
    let folders = broker.ok("arr_root_folders", json!({"service": "lidarr"})).await;
    assert_eq!(
        folders["items"],
        json!([{"id": 2, "path": "/tv", "accessible": true, "free_space_bytes": 5, "total_space_bytes": 9}])
    );
    assert_eq!(broker.seen()[0].path, "/api/v1/rootfolder");
}

#[tokio::test]
async fn search_candidates_are_projected_and_bounded() {
    let film = candidate("radarr");
    let candidates =
        json!([with(film.clone(), json!({"id": 5})), with(film, json!({"junk": true})), "junk"]);
    let broker = Broker::start(write_backend(json!({"candidates": candidates}))).await;
    let arguments = json!({"service": "radarr", "query": "example", "limit": 1});
    assert_eq!(
        broker.ok("arr_search_candidates", arguments).await,
        json!({"service": "radarr", "upstream_truncated": true,
               "items": [{"id": 5, "tmdb_id": 603, "title": "Example Film", "year": 1999,
                          "status": "released", "in_library": true}]})
    );
    let seen = broker.seen();
    assert_eq!(seen[0].path, "/api/v3/movie/lookup");
    assert_eq!(seen[0].query["term"], "example");
    assert_eq!(seen[0].header("x-api-key"), Some("radarr-key"));
    assert!(!seen[0].target.contains("radarr-key"));
}

#[tokio::test]
async fn upstream_failures_are_sanitized() {
    type Configure = fn(&mut Settings);
    type Upstream = Box<dyn Fn(&Seen) -> Response + Send + Sync>;
    let closed_port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    };
    let redirect = |_: &Seen| {
        Response::builder()
            .status(302)
            .header("location", "https://elsewhere.test")
            .body(Body::from("{}"))
            .unwrap()
    };
    let chunked = |_: &Seen| {
        let chunks = (0..3).map(|_| Ok::<_, Infallible>("x".repeat(500)));
        respond(200, Body::from_stream(futures_util::stream::iter(chunks)))
    };
    // A small body that declares an oversized length and then stalls: only the early header
    // check refuses it as oversized, without waiting for bytes that never arrive.
    let declared = |_: &Seen| {
        let body = futures_util::stream::iter([Ok::<_, Infallible>("[]")])
            .chain(futures_util::stream::pending());
        let mut response = respond(200, Body::from_stream(body));
        response.headers_mut().insert("content-length", "5000".parse().unwrap());
        response
    };
    let slow = |_: &Seen| {
        let body = futures_util::stream::once(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, Infallible>("[]")
        });
        respond(200, Body::from_stream(body))
    };
    let small: Configure = |settings| settings.max_response_bytes = 1000;
    let cases: Vec<(&str, Configure, Upstream)> = vec![
        ("HTTP 302", |_| {}, Box::new(redirect)),
        (
            "HTTP 500",
            |_| {},
            Box::new(reply_status(500, json!({"detail": "private upstream detail"}))),
        ),
        ("invalid JSON", |_| {}, Box::new(|_| respond(200, "<html>private</html>"))),
        ("unexpected series response", |_| {}, Box::new(reply(json!({"not": "a list"})))),
        ("unexpected series response", |_| {}, Box::new(reply(json!(vec![json!({}); 10_001])))),
        ("size limit", small, Box::new(|_| respond(200, "private ".repeat(200)))),
        ("size limit", small, Box::new(chunked)),
        ("size limit", small, Box::new(declared)),
        ("timed out", |settings| settings.timeout = Duration::from_millis(10), Box::new(slow)),
    ];
    let mut failures = Vec::new();
    for (message, configure, upstream) in cases {
        let broker = Broker::configured(configure, upstream).await;
        failures.push((
            message,
            broker.err("arr_library_inventory", json!({"service": "sonarr"})).await,
        ));
    }
    let unreachable = Broker::configured(
        |settings| {
            let sonarr = settings.upstreams[Service::Sonarr as usize].as_mut().unwrap();
            sonarr.base_url = closed_port;
        },
        reply(json!([])),
    )
    .await;
    failures.push((
        "request failed",
        unreachable.err("arr_library_inventory", json!({"service": "sonarr"})).await,
    ));
    for (message, error) in failures {
        assert!(error.contains(message), "{message}: {error}");
        assert!(!error.contains("private") && !error.contains("127.0.0.1"), "{message}: {error}");
    }
}

#[tokio::test]
async fn history_request_shape_and_projection() {
    let row = json!({
        "id": 91, "user_id": 7, "friendly_name": "Alice", "rating_key": "opaque-22",
        "media_type": "episode", "title": "Episode", "parent_title": "Season",
        "grandparent_title": "Show", "started": 1_700_000_000, "duration": 120,
        "watched_status": 1, "user": "private name", "ip_address": "10.0.0.9",
    });
    // A misbehaving upstream must never leak another member's or type's rows.
    let other_user = with(row.clone(), json!({"user_id": 8, "title": "Other household member"}));
    let other_type = with(row.clone(), json!({"media_type": "movie", "title": "Not an episode"}));
    let broker = Broker::start(history_reply(json!([row, other_user, other_type]), None)).await;
    let arguments = json!({"media_type": "episode", "start_date": "2024-01-01", "end_date": "2024-01-31",
                           "page_size": 10, "user_id": 7});
    let result = broker.ok("tautulli_play_history", arguments).await;
    assert_eq!(
        result["items"],
        json!([{
            "media_type": "episode", "tautulli_user_id": 7, "user_name": "Alice",
            "tautulli_rating_key": "opaque-22", "tautulli_history_id": 91, "title": "Episode",
            "parent_title": "Season", "grandparent_title": "Show", "played_at": 1_700_000_000,
            "duration_seconds": 120, "completed": true,
        }])
    );
    assert_eq!(result["user_id_filter"], 7);
    let seen = broker.seen();
    let expected = [
        ("cmd", "get_history"),
        ("start", "0"),
        ("length", "10"),
        ("media_type", "episode"),
        ("after", "2024-01-01"),
        ("before", "2024-01-31"),
        ("grouping", "0"),
        ("include_activity", "0"),
        ("user_id", "7"),
    ];
    assert_eq!(seen[0].path, "/api/v2");
    assert_eq!(
        seen[0].query,
        expected.map(|(key, value)| (key.to_owned(), value.to_owned())).into()
    );
    assert_eq!(seen[0].header("x-api-key"), Some("tautulli-key"));
    assert!(!seen[0].target.contains("tautulli-key"));
}

#[tokio::test]
async fn history_completion_domain() {
    let cases = [
        (json!(0), json!(false)),
        (json!(0.25), json!(false)),
        (json!(0.75), json!(false)),
        (json!(1), json!(true)),
        (json!(null), json!(null)),
        (json!("1"), json!(null)),
        (json!(true), json!(null)),
        (json!(2), json!(null)),
    ];
    for (status, completed) in cases {
        let broker = Broker::start(history_reply(json!([{"watched_status": status}]), None)).await;
        let result = broker.ok("tautulli_play_history", one_day("track")).await;
        assert_eq!(result["items"][0]["completed"], completed, "watched_status {status}");
        assert!(!broker.seen()[0].query.contains_key("user_id"));
    }
}

#[tokio::test]
async fn history_requires_a_well_formed_success_envelope() {
    let cases = [
        (json!({"response": {"result": "error", "message": "private detail"}}), "not successful"),
        (json!({"response": {"result": "success"}}), "unexpected history"),
        (
            json!({"response": {"result": "success", "data": {"data": [{}, {}]}}}),
            "unexpected history",
        ),
        (json!([]), "not successful"),
    ];
    for (payload, message) in cases {
        let broker = Broker::start(reply(payload.clone())).await;
        let error = broker
            .err("tautulli_play_history", with(one_day("movie"), json!({"page_size": 1})))
            .await;
        assert!(error.contains(message) && !error.contains("private"), "{payload}: {error}");
    }
}

#[tokio::test]
async fn history_pagination_never_trusts_an_inconsistent_total() {
    // (rows, upstream total, page) -> (has_more, upstream_truncated, upstream_total_count)
    let cases = [
        (1, json!(1), 1, false, false, json!(1)),
        (2, json!(5), 1, true, false, json!(5)),
        (2, json!("many"), 1, true, true, json!(null)),
        (2, json!(1), 2, true, true, json!(null)),
        (1, json!(1), 2, false, false, json!(null)),
    ];
    for (rows, total, page, has_more, truncated, reported) in cases {
        let upstream =
            history_reply(json!(vec![json!({"title": "Movie"}); rows]), Some(total.clone()));
        let arguments = with(one_day("movie"), json!({"page": page, "page_size": 2}));
        let result = Broker::start(upstream).await.ok("tautulli_play_history", arguments).await;
        let expected = json!({"has_more": has_more, "upstream_truncated": truncated,
                              "upstream_total_count": reported, "upstream_paginated": true});
        assert_contains(
            &result["pagination"],
            &expected,
            &format!("{rows} rows of {total}, page {page}"),
        );
    }
}

#[tokio::test]
async fn history_windows_are_bounded_to_31_inclusive_days() {
    let broker = Broker::start(|seen: &Seen| match seen.path.as_str() {
        "/api/v2" => history_reply(json!([]), None)(seen),
        _ => respond(200, "[]"),
    })
    .await;
    for (tool, base) in
        [("tautulli_play_history", one_day("movie")), ("jellyfin_play_history", day("", ""))]
    {
        let arguments = |start: &str, end: &str| {
            with(base.clone(), json!({"start_date": start, "end_date": end}))
        };
        assert_eq!(
            broker.ok(tool, arguments("2024-01-01", "2024-01-31")).await["items"],
            json!([])
        );
        let before = broker.seen().len();
        for (start, end, message) in [
            ("2024-01-01", "2024-02-01", "31 inclusive days"),
            ("2024-01-02", "2024-01-01", "on or after"),
        ] {
            let error = broker.err(tool, arguments(start, end)).await;
            assert!(error.contains(message), "{tool} {start}..{end}: {error}");
        }
        assert_eq!(broker.seen().len(), before, "{tool} must not call upstream");
    }
}

#[tokio::test]
async fn jellyfin_history_iterates_dates_and_projects_only_allowed_fields() {
    let broker = Broker::start(|seen: &Seen| {
        let day = seen.path.split('/').nth(3).unwrap();
        let row = json!({
            "Time": "01:02:03", "Id": format!("item-{day}"), "Name": format!("Title {day}"),
            "Type": "Episode", "Duration": 91.5, "RowId": 7, "Client": "private client",
            "Method": "private method", "Device": "private device",
            "UserName": "private username", "Unknown": "must not pass",
        });
        respond(200, json!([row]).to_string())
    })
    .await;
    let arguments = with(
        day("2024-01-01", "2024-01-02"),
        json!({"media_type": "episode", "timezone_offset": 5.5}),
    );
    let result = broker.ok("jellyfin_play_history", arguments).await;
    let item = |day: &str| {
        json!({"jellyfin_user_id": JELLYFIN_USER, "media_type": "episode",
               "played_at": format!("{day}T01:02:03"), "jellyfin_item_id": format!("item-{day}"),
               "jellyfin_history_id": 7, "title": format!("Title {day}"), "duration_seconds": 91.5})
    };
    assert_eq!(result["items"], json!([item("2024-01-01"), item("2024-01-02")]));
    assert_eq!(result["timezone_offset"], 5.5);
    let seen = broker.seen();
    let paths: Vec<&str> = seen.iter().map(|request| request.path.as_str()).collect();
    assert_eq!(
        paths,
        [
            format!("/user_usage_stats/{JELLYFIN_USER}/2024-01-01/GetItems"),
            format!("/user_usage_stats/{JELLYFIN_USER}/2024-01-02/GetItems")
        ]
    );
    for request in &seen {
        let expected = [("filter", "Episode"), ("timezoneOffset", "5.5")];
        assert_eq!(
            request.query,
            expected.map(|(key, value)| (key.to_owned(), value.to_owned())).into()
        );
        assert_eq!(request.header("x-emby-token"), Some("jellyfin-key"));
        assert_eq!(request.header("x-api-key"), None);
        assert!(!request.target.contains("jellyfin-key"));
    }
}

#[tokio::test]
async fn jellyfin_media_type_filters() {
    for (media_type, filter) in [("movie", "Movie"), ("episode", "Episode"), ("track", "Audio")] {
        let broker = Broker::start(reply(json!([]))).await;
        let arguments = with(day("2024-01-01", "2024-01-01"), json!({"media_type": media_type}));
        broker.ok("jellyfin_play_history", arguments).await;
        let query = &broker.seen()[0].query;
        assert_eq!((query["filter"].as_str(), query["timezoneOffset"].as_str()), (filter, "0"));
    }
}

#[tokio::test]
async fn jellyfin_history_nulls_oversized_and_wrongly_typed_fields() {
    let oversized = "x".repeat(1025);
    let rows = json!([
        {"Id": oversized, "RowId": oversized, "Name": oversized, "Duration": 120, "Time": oversized},
        {"Id": true, "RowId": {"private": "data"}, "Name": 12, "Duration": "120", "Time": null,
         "Client": "must not pass"},
    ]);
    let broker = Broker::start(reply(rows)).await;
    let result = broker.ok("jellyfin_play_history", day("2024-01-01", "2024-01-01")).await;
    let blank = json!({"jellyfin_user_id": JELLYFIN_USER, "media_type": "movie", "played_at": null,
                       "jellyfin_item_id": null, "jellyfin_history_id": null, "title": null,
                       "duration_seconds": null});
    assert_eq!(
        result["items"],
        json!([with(blank.clone(), json!({"duration_seconds": 120})), blank])
    );
}

#[tokio::test]
async fn jellyfin_history_local_pagination() {
    let broker = Broker::start(|seen: &Seen| {
        let day = seen.path.split('/').nth(3).unwrap();
        let rows: Vec<Value> = (0..if day == "2024-01-01" { 2 } else { 1 })
            .map(|index| {
                json!({"Time": "00:00:01", "Id": format!("{day}-{index}"), "RowId": index,
                                "Name": format!("{day}-{index}"), "Duration": index})
            })
            .collect();
        respond(200, json!(rows).to_string())
    })
    .await;
    let arguments = with(day("2024-01-01", "2024-01-02"), json!({"page": 2, "page_size": 2}));
    let result = broker.ok("jellyfin_play_history", arguments).await;
    assert_eq!(result["items"].as_array().unwrap().len(), 1);
    assert_eq!(result["items"][0]["title"], "2024-01-02-0");
    assert_eq!(
        result["pagination"],
        json!({"page": 2, "page_size": 2, "returned_count": 1, "total_count": 3, "has_more": false,
               "upstream_paginated": false, "upstream_truncated": false})
    );
}

#[tokio::test]
async fn jellyfin_history_rejects_malformed_excessive_or_unauthorized_results() {
    let cases = [
        (reply_status(200, json!({"not": "a list"})), "unexpected Jellyfin history"),
        (
            reply_status(200, json!([{"Time": "00:00:00"}, "not an object"])),
            "unexpected Jellyfin history",
        ),
        (reply_status(200, json!(vec![json!({}); 10_001])), "too many Jellyfin history rows"),
        (reply_status(403, json!({"private": "detail"})), "HTTP 403"),
    ];
    for (upstream, message) in cases {
        let error = Broker::start(upstream)
            .await
            .err("jellyfin_play_history", day("2024-01-01", "2024-01-01"))
            .await;
        assert!(error.contains(message) && !error.contains("private"), "{message}: {error}");
    }
}

#[tokio::test]
async fn jellyfin_users_are_projected_to_id_and_name() {
    let alice = json!({"Id": JELLYFIN_USER, "Name": "Alice", "Policy": {"IsAdministrator": "must not pass"},
                       "LastLoginDate": "must not pass", "PrimaryImageTag": "must not pass"});
    let broker = Broker::start(reply(json!([alice, "junk"]))).await;
    assert_eq!(
        broker.ok("jellyfin_users", json!({})).await,
        json!({"items": [{"jellyfin_user_id": JELLYFIN_USER, "name": "Alice"}], "upstream_truncated": false})
    );
    let seen = broker.seen();
    assert_eq!(seen[0].path, "/Users");
    assert_eq!(seen[0].header("x-emby-token"), Some("jellyfin-key"));
    assert_eq!(seen[0].header("x-api-key"), None);
    assert!(!seen[0].target.contains("jellyfin-key"));
    for payload in [json!({"not": "a list"}), json!(vec![json!({}); 10_001])] {
        let error = Broker::start(reply(payload)).await.err("jellyfin_users", json!({})).await;
        assert!(error.contains("unexpected users"), "{error}");
    }
}

#[tokio::test]
async fn season_inventory_aggregates_episode_counts_per_season() {
    let item = json!({"id": 3, "title": "Show", "monitored": true, "path": "/media/private",
                      "seasons": [{"seasonNumber": 1, "monitored": true}, {"seasonNumber": 2, "monitored": false}]});
    let episodes = json!([
        {"seasonNumber": 1, "hasFile": true}, {"seasonNumber": 1, "hasFile": true},
        {"seasonNumber": 1, "hasFile": false}, {"seasonNumber": 2, "hasFile": false},
        {"seasonNumber": "junk", "hasFile": true},
    ]);
    let broker = Broker::start(write_backend(json!({"item": item, "episodes": episodes}))).await;
    assert_eq!(
        broker.ok("arr_season_inventory", json!({"service": "sonarr", "item_id": 3})).await,
        json!({"service": "sonarr", "item": {"id": 3, "title": "Show", "monitored": true},
               "seasons": [
                   {"season_number": 1, "monitored": true, "episode_count": 3, "episode_file_count": 2},
                   {"season_number": 2, "monitored": false, "episode_count": 1, "episode_file_count": 0}]})
    );
    let seen = broker.seen();
    assert!(seen.iter().any(|request| request.path == "/api/v3/series/3"));
    let episodes = seen.iter().find(|request| request.path == "/api/v3/episode").unwrap();
    assert_eq!(episodes.query["seriesId"], "3");
}

#[tokio::test]
async fn album_inventory_projects_file_and_size_details() {
    let albums = json!([
        {"id": 11, "title": "Example Album", "releaseDate": "1994-01-01T00:00:00Z", "monitored": true,
         "statistics": {"trackFileCount": 5, "sizeOnDisk": 40_000_000, "percentOfTracks": "must not pass"},
         "artist": {"artistName": "must not pass"}},
        {"id": 12, "title": "Other", "monitored": false, "statistics": {"trackFileCount": 0, "sizeOnDisk": 0}},
    ]);
    let broker = Broker::start(write_backend(json!({"albums": albums}))).await;
    assert_eq!(
        broker.ok("arr_album_inventory", json!({"service": "lidarr", "artist_id": 42})).await,
        json!({"service": "lidarr", "artist_id": 42, "items": [
            {"album_id": 11, "title": "Example Album", "monitored": true,
             "release_date": "1994-01-01T00:00:00Z", "track_file_count": 5,
             "size_on_disk_bytes": 40_000_000, "has_file": true},
            {"album_id": 12, "title": "Other", "monitored": false, "release_date": null,
             "track_file_count": 0, "size_on_disk_bytes": 0, "has_file": false}]})
    );
    let seen = broker.seen();
    assert_eq!(seen[0].path, "/api/v1/album");
    assert_eq!(seen[0].query["artistId"], "42");
}

fn torrent_broker(
    torrents: Value,
    stats: Value,
    flags: (bool, bool, bool),
) -> impl std::future::Future<Output = Broker> {
    let (login_ok, accept_session, fail_first_get) = flags;
    Broker::configured(
        with_qbit,
        qbit_backend(torrents, stats, login_ok, accept_session, fail_first_get),
    )
}

fn logins(broker: &Broker) -> usize {
    broker.seen().iter().filter(|request| request.path == "/api/v2/auth/login").count()
}

#[tokio::test]
async fn torrent_stats_projects_allow_listed_fields_only() {
    let stats = json!({
        "connection_status": "connected", "dht_nodes": 250,
        "dl_info_speed": 1024, "dl_info_data": 4096, "dl_rate_limit": 0,
        "up_info_speed": 512, "up_info_data": 8192, "up_rate_limit": 100,
        "server_state": "must not pass", "use_subcategories": "must not pass",
    });
    let broker = torrent_broker(json!([]), stats, (true, true, false)).await;
    assert_eq!(
        broker.ok("torrent_client_stats", json!({})).await,
        json!({
            "service": "qbittorrent", "connection_status": "connected", "dht_nodes": 250,
            "download_speed_bytes": 1024, "downloaded_bytes": 4096, "download_rate_limit_bytes": 0,
            "upload_speed_bytes": 512, "uploaded_bytes": 8192, "upload_rate_limit_bytes": 100,
        })
    );
    let seen = broker.seen();
    assert_eq!(logins(&broker), 1, "{seen:?}");
    let stats_call = seen.iter().find(|request| request.path == "/api/v2/transfer/info").unwrap();
    assert_eq!(stats_call.header("cookie"), Some(format!("SID={QBIT_SID}").as_str()));
    assert!(!stats_call.target.contains("secret"), "{}", stats_call.target);
}

#[tokio::test]
async fn torrent_inventory_projects_searches_and_paginates_locally() {
    let other =
        with(torrent_row(), json!({"hash": HASH_B, "name": "Another.Pack", "progress": 0.5}));
    let broker =
        torrent_broker(json!([torrent_row(), other]), json!({}), (true, true, false)).await;
    let first = broker.ok("torrent_client_inventory", json!({"page_size": 1})).await;
    let expected = json!({
        "hash": HASH_A, "name": "Example.Release.2024", "state": "stoppedUP",
        "size_bytes": 4_700_000_000_i64, "progress": 1.0, "ratio": 2.5,
        "seeding_time_seconds": 864_000, "save_path": "/mnt/data/torrents",
        "added_on": 1_700_000_000, "amount_left_bytes": 0,
    });
    assert_eq!(first["items"][0], expected);
    assert_eq!(
        first["pagination"],
        json!({"page": 1, "page_size": 1, "returned_count": 1, "total_count": 2,
               "has_more": true, "upstream_paginated": false, "upstream_truncated": false})
    );
    let searched = broker.ok("torrent_client_inventory", json!({"search": "another"})).await;
    assert_eq!(searched["items"].as_array().unwrap().len(), 1);
    assert_eq!(searched["items"][0]["hash"], HASH_B);
    assert_eq!(searched["pagination"]["total_count"], 1);
    // The upstream is queried once and filtered locally, never by name.
    let seen = broker.seen();
    let mut infos = seen.iter().filter(|request| request.path == "/api/v2/torrents/info");
    assert!(infos.all(|request| request.query.is_empty()), "{seen:?}");
    // The session is established once and reused across both calls.
    assert_eq!(logins(&broker), 1, "{:?}", broker.seen());
}

#[tokio::test]
async fn torrent_inventory_rejects_wrong_typed_fields() {
    let wrong = json!([{
        "hash": 1, "name": true, "state": [], "size": "x", "progress": "x", "ratio": "x",
        "seeding_time": "x", "save_path": 1, "added_on": "x", "amount_left": "x",
    }]);
    let broker = torrent_broker(wrong, json!({}), (true, true, false)).await;
    let result = broker.ok("torrent_client_inventory", json!({})).await;
    assert_eq!(
        result["items"][0],
        json!({
            "hash": null, "name": null, "state": null, "size_bytes": null, "progress": null,
            "ratio": null, "seeding_time_seconds": null, "save_path": null, "added_on": null,
            "amount_left_bytes": null,
        })
    );
}

#[tokio::test]
async fn torrent_check_paths_reports_presence_and_dedupes_hashes() {
    let broker = torrent_broker(json!([torrent_row()]), json!({}), (true, true, false)).await;
    let upper = HASH_A.to_uppercase();
    let arguments = json!({"hashes": [HASH_A, upper, HASH_B, HASH_A]});
    let result = broker.ok("torrent_client_check_paths", arguments).await;
    assert_eq!(
        result,
        json!({
            "service": "qbittorrent", "requested_count": 2, "found_count": 1,
            "items": [
                {"hash": HASH_A, "present": true, "name": "Example.Release.2024",
                 "state": "stoppedUP", "progress": 1.0, "amount_left_bytes": 0,
                 "save_path": "/mnt/data/torrents", "data_complete": true},
                {"hash": HASH_B, "present": false},
            ],
        })
    );
    let seen = broker.seen();
    let info = seen.iter().find(|request| request.path == "/api/v2/torrents/info").unwrap();
    assert_eq!(info.query["hashes"], format!("{HASH_A}|{HASH_B}"));
}

#[tokio::test]
async fn torrent_check_paths_validates_hashes_before_any_upstream_call() {
    let broker = torrent_broker(json!([]), json!({}), (true, true, false)).await;
    let not_hex = "g".repeat(64);
    for hashes in [json!(["not-a-hash"]), json!([not_hex]), json!([HASH_A, "x"])] {
        let error = broker.err("torrent_client_check_paths", json!({"hashes": hashes})).await;
        assert!(error.contains("40 or 64 hexadecimal"), "{hashes}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
}

#[tokio::test]
async fn torrent_session_recovers_once_from_a_rejected_cookie() {
    let broker = torrent_broker(json!([torrent_row()]), json!({}), (true, true, true)).await;
    let result = broker.ok("torrent_client_inventory", json!({})).await;
    assert_eq!(result["items"][0]["hash"], HASH_A);
    assert_eq!(logins(&broker), 2, "{:?}", broker.seen());
}

#[tokio::test]
async fn torrent_session_failures_are_sanitized() {
    // A session that stays rejected is reported once, not retried forever.
    let rejected = torrent_broker(json!([]), json!({}), (true, false, false)).await;
    let error = rejected.err("torrent_client_stats", json!({})).await;
    assert!(error.contains("HTTP 403"), "{error}");
    assert!(!error.contains("secret"), "{error}");
    assert_eq!(logins(&rejected), 2, "{:?}", rejected.seen());
    // A refused login surfaces as an authentication failure without the password.
    let refused = torrent_broker(json!([]), json!({}), (false, true, false)).await;
    let error = refused.err("torrent_client_stats", json!({})).await;
    assert!(error.contains("authentication failed"), "{error}");
    assert!(!error.contains("secret"), "{error}");
    assert_eq!(logins(&refused), 1, "{:?}", refused.seen());
}

#[tokio::test]
async fn tools_for_unconfigured_upstreams_are_clear_parameter_errors() {
    // Radarr stays configured so the arr gate keeps the tools registered;
    // addressing a removed service must fail before any upstream call.
    let broker = Broker::configured(
        |settings| {
            settings.upstreams[Service::Sonarr as usize] = None;
            settings.upstreams[Service::Lidarr as usize] = None;
        },
        reply(json!([])),
    )
    .await;
    let error = broker.err("arr_library_inventory", json!({"service": "sonarr"})).await;
    assert!(error.contains("sonarr is not configured"), "{error}");
    assert!(!error.contains(".test"), "{error}");
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
}
