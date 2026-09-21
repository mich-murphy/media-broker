//! Gated write tools: broker-built bodies, minimal merges, two-phase confirmed deletes.

mod common;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{Broker, MBID, Seen, assert_contains, candidate, open_gates, with, write_backend};
use serde_json::{Value, json};

fn request(service: &str, extra: Value) -> Value {
    let external_id = match service {
        "sonarr" => "81189",
        "radarr" => "603",
        _ => MBID,
    };
    let base = json!({"service": service, "external_id": external_id, "quality_profile_id": 7,
                      "root_folder_path": "/media"});
    with(base, extra)
}

/// A write-enabled broker whose fake upstream differs from the defaults by `overrides`.
async fn backed(overrides: Value) -> Broker {
    Broker::writes(write_backend(overrides)).await
}

/// As [`backed`], with the Sonarr lookup candidate on offer.
async fn sonarr_backed(overrides: Value) -> Broker {
    backed(with(json!({"candidates": [candidate("sonarr")]}), overrides)).await
}

fn methods(seen: &[Seen]) -> Vec<&str> {
    seen.iter().map(|request| request.method.as_str()).collect()
}

fn only<'a>(seen: &'a [Seen], method: &str) -> &'a Seen {
    let mut matching = seen.iter().filter(|request| request.method == method);
    let first = matching.next().unwrap_or_else(|| panic!("no {method} in {:?}", methods(seen)));
    assert!(matching.next().is_none(), "more than one {method}");
    first
}

fn assert_read_only(broker: &Broker) {
    let seen = broker.seen();
    assert!(seen.iter().all(|request| request.method == "GET"), "{:?}", methods(&seen));
}

#[tokio::test]
async fn request_media_adds_a_broker_built_record() {
    let cases = [
        (
            "sonarr",
            "/api/v3/series",
            "tvdb:81189",
            "searchForMissingEpisodes",
            json!({
                "title": "Example Show", "titleSlug": "example-show", "tvdbId": 81189,
                "qualityProfileId": 7, "rootFolderPath": "/media", "seriesType": "anime",
                "monitored": true, "seasonFolder": true,
                "seasons": [{"seasonNumber": 1, "monitored": true}, {"seasonNumber": 2, "monitored": true}],
                "addOptions": {"monitor": "all", "searchForCutoffUnmetEpisodes": false},
            }),
        ),
        (
            "radarr",
            "/api/v3/movie",
            "tmdb:603",
            "searchForMovie",
            json!({
                "title": "Example Film", "titleSlug": "example-film-603", "tmdbId": 603, "year": 1999,
                "qualityProfileId": 7, "rootFolderPath": "/media", "minimumAvailability": "released",
                "monitored": true, "addOptions": {},
            }),
        ),
        (
            "lidarr",
            "/api/v1/artist",
            &format!("lidarr:{MBID}"),
            "searchForMissingAlbums",
            json!({
                "artistName": "Example Band", "foreignArtistId": MBID, "qualityProfileId": 7,
                "metadataProfileId": 1, "rootFolderPath": "/media", "monitored": true,
                "monitorNewItems": "all", "addOptions": {"monitor": "all", "monitored": true},
            }),
        ),
    ];
    for (service, resource, term, search_option, mut expected) in cases {
        // Searching is the default; search_on_add only toggles the search option.
        for (arguments, search) in [(json!({}), true), (json!({"search_on_add": false}), false)] {
            let broker = backed(json!({"candidates": [candidate(service)]})).await;
            let arguments =
                request(service, with(arguments, json!({"root_folder_path": "/media/"})));
            let result = broker.ok("arr_request_media", arguments).await;
            let seen = broker.seen();
            let post = only(&seen, "POST");
            expected["addOptions"][search_option] = json!(search);
            assert_eq!(post.path, resource);
            assert_eq!(post.body, expected, "{service}");
            assert_eq!(post.header("x-api-key"), Some(format!("{service}-key").as_str()));
            assert!(!post.target.contains("-key"));
            let lookup = seen.iter().find(|request| request.path.ends_with("/lookup")).unwrap();
            assert_eq!(lookup.query["term"], term);
            assert_contains(&result["item"], &json!({"id": 42, "in_library": true}), service);
            assert_eq!(result["search_on_add"], search);
            assert_eq!(result["seasons"], Value::Null);
        }
    }
}

/// Arr rejects write bodies without `Content-Type: application/json` (HTTP 415).
#[tokio::test]
async fn writes_send_a_json_content_type_but_reads_do_not() {
    let item = json!({"id": 3, "title": "Show", "monitored": true, "seasons": []});
    let broker = sonarr_backed(json!({"item": item})).await;
    broker.ok("arr_request_media", request("sonarr", json!({}))).await;
    broker.ok("arr_unmonitor_media", json!({"service": "sonarr", "item_id": 3})).await;
    let seen = broker.seen();
    let (reads, writes): (Vec<_>, Vec<_>) =
        seen.iter().partition(|request| request.method == "GET");
    assert_eq!(methods(&seen).iter().filter(|method| **method != "GET").count(), 2);
    assert!(!reads.is_empty());
    assert!(
        writes.iter().all(|request| request.header("content-type") == Some("application/json"))
    );
    assert!(reads.iter().all(|request| request.header("content-type").is_none()));
}

#[tokio::test]
async fn request_media_rejects_unsafe_requests_before_adding() {
    let without_seasons = with(candidate("sonarr"), json!({"seasons": null}));
    let in_library = with(candidate("sonarr"), json!({"id": 9}));
    // (backend overrides on top of the Sonarr candidate, extra arguments, expected message)
    #[rustfmt::skip]
    let cases = [
        (json!({"candidates": [in_library]}), json!({}), "already in the library"),
        (json!({"candidates": [candidate("radarr")]}), json!({}), "no upstream candidate"),
        (json!({"profiles": []}), json!({}), "quality_profile_id"),
        (json!({"folders": []}), json!({}), "root_folder_path"),
        (json!({"folders": [{"path": "/other"}]}), json!({}), "root_folder_path"),
        (json!({"candidates": [without_seasons]}), json!({}), "season"),
        (json!({}), json!({"seasons": [3]}), "do not exist"),
    ];
    for (overrides, arguments, message) in cases {
        let broker = sonarr_backed(overrides).await;
        let error = broker.err("arr_request_media", request("sonarr", arguments)).await;
        assert!(error.contains(message), "{message}: {error}");
        assert!(!broker.seen().is_empty(), "{message}");
        assert_read_only(&broker);
    }
}

#[tokio::test]
async fn request_media_rejects_malformed_identifiers_before_any_upstream_call() {
    let cases = [
        ("sonarr", json!({"external_id": "81189x"}), "external_id"),
        ("sonarr", json!({"external_id": "0"}), "external_id"),
        ("sonarr", json!({"external_id": "007"}), "external_id"),
        ("radarr", json!({"external_id": "2147483648"}), "external_id"),
        ("radarr", json!({"external_id": "-5"}), "external_id"),
        ("radarr", json!({"external_id": "+603"}), "external_id"),
        ("lidarr", json!({"external_id": "not-a-musicbrainz-id"}), "external_id"),
        ("lidarr", json!({"external_id": MBID.replace('-', "")}), "external_id"),
        ("radarr", json!({"seasons": [1]}), "only supported for sonarr"),
    ];
    let broker = backed(json!({})).await;
    for (service, arguments, message) in cases {
        let error = broker.err("arr_request_media", request(service, arguments.clone())).await;
        assert!(error.contains(message), "{service} {arguments}: {error}");
    }
    assert!(broker.seen().is_empty());
}

#[tokio::test]
async fn lidarr_requests_use_the_lowest_metadata_profile() {
    let overrides =
        json!({"candidates": [candidate("lidarr")], "metadata": [{"id": 3}, {"id": 2}]});
    let broker = backed(overrides).await;
    broker.ok("arr_request_media", request("lidarr", json!({}))).await;
    assert_eq!(only(&broker.seen(), "POST").body["metadataProfileId"], 2);
}

#[tokio::test]
async fn request_media_monitors_and_searches_only_selected_seasons() {
    for search in [true, false] {
        let broker = sonarr_backed(json!({})).await;
        let arguments = request("sonarr", json!({"search_on_add": search, "seasons": [1]}));
        let result = broker.ok("arr_request_media", arguments).await;
        let body = only(&broker.seen(), "POST").body.clone();
        assert_eq!(
            body["seasons"],
            json!([{"seasonNumber": 1, "monitored": true}, {"seasonNumber": 2, "monitored": false}])
        );
        // No monitor option: Sonarr derives episode monitoring from the season
        // flags, so an add-search can never touch the unselected seasons.
        assert_eq!(body["addOptions"].get("monitor"), None);
        assert_eq!(body["addOptions"]["searchForMissingEpisodes"], search);
        assert_eq!(result["seasons"], json!([1]));
    }
}

#[tokio::test]
async fn monitoring_toggles_merge_one_flag_and_return_one_projection() {
    for (tool, monitored) in [("arr_unmonitor_media", false), ("arr_monitor_media", true)] {
        let item = json!({"id": 3, "title": "Show", "monitored": !monitored, "seasons": [],
                          "path": "/media/Show", "secret": "upstream-only"});
        let broker = backed(json!({"item": item})).await;
        let result = broker.ok(tool, json!({"service": "sonarr", "item_id": 3})).await;
        let seen = broker.seen();
        let put = only(&seen, "PUT");
        assert_eq!(put.path, "/api/v3/series/3");
        assert_eq!(put.body, with(item, json!({"monitored": monitored})));
        assert_eq!(
            result,
            json!({"service": "sonarr", "item": {"id": 3, "title": "Show", "monitored": monitored}})
        );
    }
}

#[tokio::test]
async fn set_season_monitoring_flips_only_the_requested_seasons() {
    let seasons = |last: bool| {
        json!([{"seasonNumber": 0, "monitored": false, "statistics": {"kept": true}},
               {"seasonNumber": 1, "monitored": true}, {"seasonNumber": 2, "monitored": last}])
    };
    let item = json!({"id": 3, "title": "Show", "monitored": true, "seasons": seasons(true)});
    let broker = backed(json!({"item": item})).await;
    let arguments = |seasons: Value| json!({"service": "sonarr", "item_id": 3, "seasons": seasons, "monitored": false});
    let result = broker.ok("arr_set_season_monitoring", arguments(json!([2]))).await;
    let put = only(&broker.seen(), "PUT").clone();
    assert_eq!(put.path, "/api/v3/series/3");
    assert_eq!(put.body["seasons"], seasons(false));
    assert_eq!(
        result,
        json!({"service": "sonarr", "item": {"id": 3, "title": "Show", "monitored": true},
               "seasons": [{"season_number": 0, "monitored": false}, {"season_number": 1, "monitored": true},
                           {"season_number": 2, "monitored": false}]})
    );
    let error = broker.err("arr_set_season_monitoring", arguments(json!([1, 9]))).await;
    assert!(error.contains("do not exist"), "{error}");
    assert_eq!(broker.seen().iter().filter(|request| request.method == "PUT").count(), 1);
}

#[tokio::test]
async fn set_album_monitored_flips_one_album() {
    for monitored in [true, false] {
        let album = json!({"id": 11, "title": "Example Album", "artistId": 42, "monitored": !monitored,
                           "secret": "upstream-only"});
        let broker = backed(json!({"albums": [album]})).await;
        let arguments = json!({"service": "lidarr", "album_id": 11, "monitored": monitored});
        let result = broker.ok("arr_set_album_monitored", arguments).await;
        let seen = broker.seen();
        let put = only(&seen, "PUT");
        assert_eq!(put.path, "/api/v1/album/11");
        assert_eq!(put.body, with(album, json!({"monitored": monitored})));
        let summary = json!({"album_id": 11, "title": "Example Album", "monitored": monitored});
        assert_eq!(result, json!({"service": "lidarr", "album": summary}));
    }
}

#[tokio::test]
async fn search_item_posts_a_bounded_command() {
    let cases = [
        ("sonarr", "/api/v3/command", json!({"name": "SeriesSearch", "seriesId": 3})),
        ("radarr", "/api/v3/command", json!({"name": "MoviesSearch", "movieIds": [3]})),
        ("lidarr", "/api/v1/command", json!({"name": "ArtistSearch", "artistId": 3})),
    ];
    for (service, path, body) in cases {
        let broker = backed(json!({})).await;
        let result = broker.ok("arr_search_item", json!({"service": service, "item_id": 3})).await;
        let seen = broker.seen();
        assert_eq!((methods(&seen), seen[0].path.as_str()), (vec!["POST"], path));
        assert_eq!(seen[0].body, body);
        let command = json!({"command_id": 9, "name": body["name"], "status": "queued"});
        assert_eq!(result, json!({"service": service, "command": command}));
    }
}

#[tokio::test]
async fn delete_media_is_a_two_phase_confirmed_operation() {
    // Keeping files is the default.
    for (arguments, delete_files, flag) in
        [(json!({"delete_files": true}), true, "true"), (json!({}), false, "false")]
    {
        let item =
            json!({"id": 5, "title": "Old Show", "monitored": true, "path": "/media/Old Show"});
        let broker = backed(json!({"item": item})).await;
        let arguments = with(json!({"service": "sonarr", "item_id": 5}), arguments);
        let summary = json!({"service": "sonarr", "delete_files": delete_files,
                             "item": {"id": 5, "title": "Old Show", "monitored": true}});
        let preview = broker.ok("arr_delete_media", arguments.clone()).await;
        let required = with(summary.clone(), json!({"confirmation_required": true}));
        assert_contains(&preview, &required, "preview");
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let expires = preview["confirmation_expires_at"].as_u64().unwrap();
        assert!((now + 1..=now + 300).contains(&expires), "{expires} vs {now}");
        assert_read_only(&broker);

        let confirmed = with(arguments, json!({"confirmation": preview["confirmation"]}));
        let completed = broker.ok("arr_delete_media", confirmed).await;
        assert_eq!(completed, with(summary, json!({"deleted": true})));
        let seen = broker.seen();
        let delete = only(&seen, "DELETE");
        assert_eq!(delete.path, "/api/v3/series/5");
        assert_eq!(delete.query["deleteFiles"], flag);
        assert_eq!(delete.query["addImportListExclusion"], "false");
        assert_eq!(methods(&seen).last(), Some(&"DELETE"));
    }
}

#[tokio::test]
async fn delete_media_with_album_id_is_a_two_phase_album_delete() {
    let item = json!({"id": 42, "artistName": "Example Band", "monitored": true});
    let broker = backed(json!({"item": item})).await;
    let arguments =
        json!({"service": "lidarr", "item_id": 42, "album_id": 11, "delete_files": true});
    let preview = broker.ok("arr_delete_media", arguments.clone()).await;
    let summary = json!({
        "confirmation_required": true, "delete_files": true,
        "item": {"album_id": 11, "title": "Example Album", "monitored": true},
        "artist": {"id": 42, "title": "Example Band"},
    });
    assert_contains(&preview, &summary, "album preview");
    assert_read_only(&broker);

    // An album confirmation is bound to the exact album delete action.
    let confirmation = json!({"confirmation": preview["confirmation"]});
    for other in
        [json!({"album_id": null}), json!({"album_id": 12}), json!({"delete_files": false})]
    {
        let arguments = with(with(arguments.clone(), confirmation.clone()), other.clone());
        let error = broker.err("arr_delete_media", arguments).await;
        assert!(error.contains("confirmation"), "{other}: {error}");
    }
    assert_read_only(&broker);

    let completed = broker.ok("arr_delete_media", with(arguments, confirmation)).await;
    assert_eq!(completed["deleted"], true);
    let seen = broker.seen();
    let delete = only(&seen, "DELETE");
    assert_eq!(delete.path, "/api/v1/album/11");
    assert_eq!(delete.query["deleteFiles"], "true");
}

#[tokio::test]
async fn delete_album_requires_lidarr_and_a_matching_artist() {
    let album = json!({"id": 11, "title": "Example Album", "artistId": 9, "monitored": true});
    let broker = backed(json!({"albums": [album]})).await;
    let foreign = json!({"service": "sonarr", "item_id": 3, "album_id": 11});
    let error = broker.err("arr_delete_media", foreign).await;
    assert!(error.contains("only supported for lidarr"), "{error}");
    let mismatched = json!({"service": "lidarr", "item_id": 42, "album_id": 11});
    let error = broker.err("arr_delete_media", mismatched).await;
    assert!(error.contains("does not belong"), "{error}");
    assert_read_only(&broker);
}

#[tokio::test]
async fn delete_media_rejects_unbound_tampered_or_expired_confirmation() {
    let backend = || write_backend(json!({"item": {"id": 5, "title": "Old"}}));
    let arguments = json!({"service": "sonarr", "item_id": 5, "delete_files": true});
    let broker = Broker::writes(backend()).await;
    let token = broker.ok("arr_delete_media", arguments.clone()).await["confirmation"]
        .as_str()
        .unwrap()
        .to_owned();
    let cases = [
        json!({"delete_files": false, "confirmation": token}),
        json!({"service": "radarr", "confirmation": token}),
        json!({"item_id": 6, "confirmation": token}),
        json!({"confirmation": format!("{token}tampered")}),
        json!({"confirmation": format!("9{token}")}),
        json!({"confirmation": ""}),
    ];
    for case in cases {
        let error = broker.err("arr_delete_media", with(arguments.clone(), case.clone())).await;
        assert!(error.contains("confirmation is not valid"), "{case}: {error}");
    }
    assert_read_only(&broker);

    // A confirmation minted by one broker is void once its lifetime has elapsed.
    let expiring = |settings: &mut media_broker::config::Settings| {
        open_gates(settings);
        settings.confirmation_ttl = Duration::ZERO;
    };
    let broker = Broker::configured(expiring, backend()).await;
    let preview = broker.ok("arr_delete_media", arguments.clone()).await;
    let confirmed = with(arguments, json!({"confirmation": preview["confirmation"]}));
    let error = broker.err("arr_delete_media", confirmed).await;
    assert!(error.contains("expired"), "{error}");
    assert_read_only(&broker);
}

#[tokio::test]
async fn write_tool_arguments_are_rejected_before_any_upstream_call() {
    // (tool, arguments, field named by the error)
    #[rustfmt::skip]
    let cases = [
        ("arr_request_media", request("radarr", json!({"external_id": "x".repeat(65)})), "external_id"),
        ("arr_request_media", request("radarr", json!({"quality_profile_id": 0})), "quality_profile_id"),
        ("arr_request_media", request("radarr", json!({"root_folder_path": ""})), "root_folder_path"),
        ("arr_request_media", request("radarr", json!({"search_on_add": "yes"})), "search_on_add"),
        ("arr_request_media", request("sonarr", json!({"seasons": []})), "seasons"),
        ("arr_request_media", request("sonarr", json!({"seasons": [1001]})), "seasons"),
        ("arr_unmonitor_media", json!({"service": "sonarr", "item_id": 0}), "item_id"),
        ("arr_monitor_media", json!({"service": "sonarr"}), "item_id"),
        ("arr_search_item", json!({"service": "plex", "item_id": 3}), "service"),
        ("arr_set_season_monitoring", json!({"service": "sonarr", "item_id": 3, "seasons": vec![1; 101], "monitored": true}), "seasons"),
        ("arr_set_season_monitoring", json!({"service": "radarr", "item_id": 3, "seasons": [1], "monitored": true}), "service"),
        ("arr_set_album_monitored", json!({"service": "lidarr", "album_id": 0, "monitored": true}), "album_id"),
        ("arr_delete_media", json!({"service": "sonarr", "item_id": 2_147_483_648_i64}), "item_id"),
        ("arr_delete_media", json!({"service": "lidarr", "item_id": 3, "album_id": 0}), "album_id"),
        ("arr_delete_media", json!({"service": "sonarr", "item_id": 3, "confirmation": "x".repeat(129)}), "confirmation"),
    ];
    let broker = sonarr_backed(json!({})).await;
    for (name, arguments, field) in cases {
        let error = broker.err(name, arguments.clone()).await;
        assert!(error.contains(field), "{name} {arguments}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", methods(&broker.seen()));
}
