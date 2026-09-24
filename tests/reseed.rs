//! The gated qBittorrent reseed: verified-complete starts, kept-data removals,
//! untouched foreign torrents, and replay of interrupted work.

mod common;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{body::Body, response::Response};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use common::{Broker, QBIT_SID, RESEED_TOOL, Seen, respond, with, with_qbit};
use media_broker::config::Settings;
use serde_json::{Value, json};
use sha1::{Digest, Sha1};

const SAVE_PATH: &str = "/data/torrents/music";
const TAG: &str = "media-broker-reseed";

/// A v1 metainfo file whose info dictionary carries `extra` before `pieces`.
fn metainfo(extra: &str) -> (Vec<u8>, String) {
    let info = format!(
        "d6:lengthi32768e4:name5:Album12:piece lengthi16384e{extra}6:pieces40:{}e",
        "p".repeat(40)
    );
    let digest = Sha1::digest(info.as_bytes());
    let hash = digest.iter().fold(String::new(), |hex, byte| hex + &format!("{byte:02x}"));
    let file = format!("d8:announce22:https://tracker.test/a4:info{info}e");
    (file.into_bytes(), hash)
}

fn arguments(file: &[u8]) -> Value {
    json!({"torrent_b64": BASE64.encode(file)})
}

fn preferences() -> Value {
    json!({"incomplete_files_ext": false, "excluded_file_names_enabled": false,
           "use_unwanted_folder": false, "refresh_interval": 0, "proxy_password": "must not pass"})
}

/// One torrent row as qBittorrent 5.1 lists it.
fn row(hash: &str, state: &str, progress: f64, tags: &str) -> Value {
    let left = if progress >= 1.0 { 0 } else { 16384 };
    json!({"hash": hash, "name": "Album", "state": state, "progress": progress,
           "amount_left": left, "size": 32768, "total_size": 32768,
           "save_path": SAVE_PATH, "tags": tags, "tracker": "must not pass"})
}

/// A stateful fake qBittorrent. The first info poll after an add still
/// misses the torrent, the first poll after a recheck reports the stale
/// pre-check state, then `checks` polls report checking before `after` holds.
#[derive(Clone)]
struct Client {
    torrent: Arc<Mutex<Option<Value>>>,
    script: Arc<Mutex<Vec<Value>>>,
    after: Value,
    checks: usize,
    preferences: Value,
    add_answer: &'static str,
    /// Fields of the torrent as first listed after an add.
    added: Value,
}

impl Client {
    fn new(present: Option<Value>, after: Value) -> Self {
        Self {
            torrent: Arc::new(Mutex::new(present)),
            script: Arc::new(Mutex::new(Vec::new())),
            after,
            checks: 2,
            preferences: preferences(),
            add_answer: "Ok.",
            added: json!({}),
        }
    }

    fn backend(self) -> impl Fn(&Seen) -> Response + Send + Sync + 'static {
        let pending_add: Arc<Mutex<Option<Value>>> = Arc::default();
        move |seen| {
            if seen.path == "/api/v2/auth/login" {
                let cookie = format!("SID={QBIT_SID}");
                return Response::builder()
                    .header("set-cookie", cookie)
                    .body(Body::from("Ok."))
                    .unwrap();
            }
            let mut torrent = self.torrent.lock().unwrap();
            let mut script = self.script.lock().unwrap();
            let hash = |seen: &Seen| {
                let form: HashMap<String, String> =
                    url::form_urlencoded::parse(seen.text.as_bytes()).into_owned().collect();
                form.get("hashes").cloned().unwrap_or_default()
            };
            match (seen.method.as_str(), seen.path.as_str()) {
                ("GET", "/api/v2/app/preferences") => respond(200, self.preferences.to_string()),
                ("GET", "/api/v2/torrents/info") => {
                    if let Some(added) = pending_add.lock().unwrap().take() {
                        *torrent = Some(added);
                        return respond(200, "[]");
                    }
                    if !script.is_empty() {
                        let next = script.remove(0);
                        *torrent = Some(with(torrent.clone().unwrap(), next));
                    }
                    let rows: Vec<Value> = torrent.iter().cloned().collect();
                    respond(200, Value::from(rows).to_string())
                }
                ("POST", "/api/v2/torrents/add") => {
                    let (_, info_hash) = metainfo("");
                    let added = with(row(&info_hash, "stoppedDL", 0.0, TAG), self.added.clone());
                    *pending_add.lock().unwrap() = Some(added);
                    respond(200, self.add_answer)
                }
                ("POST", "/api/v2/torrents/recheck") => {
                    assert_eq!(hash(seen), torrent.as_ref().unwrap()["hash"]);
                    // The stale pre-check state is reported once, then checking.
                    let stale = torrent.clone().unwrap();
                    script.push(json!({"state": stale["state"]}));
                    for _ in 0..self.checks {
                        script.push(json!({"state": "checkingDL", "progress": 0.5}));
                    }
                    script.push(self.after.clone());
                    respond(200, Body::empty())
                }
                ("POST", "/api/v2/torrents/start") => {
                    assert_eq!(hash(seen), torrent.as_ref().unwrap()["hash"]);
                    let started = with(torrent.clone().unwrap(), json!({"state": "stalledUP"}));
                    *torrent = Some(started);
                    respond(200, Body::empty())
                }
                ("POST", "/api/v2/torrents/delete") => {
                    assert_eq!(hash(seen), torrent.as_ref().unwrap()["hash"]);
                    *torrent = None;
                    respond(200, Body::empty())
                }
                _ => respond(404, Body::empty()),
            }
        }
    }
}

fn complete() -> Value {
    json!({"state": "stoppedUP", "progress": 1.0, "amount_left": 0})
}

fn incomplete() -> Value {
    json!({"state": "stoppedDL", "progress": 0.25, "amount_left": 24576})
}

fn enable(paths: &[&str]) -> impl FnOnce(&mut Settings) {
    let paths: Vec<String> = paths.iter().map(ToString::to_string).collect();
    move |settings| {
        with_qbit(settings);
        settings.enable_reseeds = true;
        settings.reseed_save_paths = paths;
    }
}

async fn broker(client: Client) -> Broker {
    Broker::configured(enable(&[SAVE_PATH]), client.backend()).await
}

/// The qBittorrent actions posted so far, in order.
fn actions(broker: &Broker) -> Vec<String> {
    let seen = broker.seen();
    let posts = seen.iter().filter(|request| request.method == "POST");
    let actions = posts.filter_map(|request| request.path.strip_prefix("/api/v2/torrents/"));
    actions.map(str::to_owned).collect()
}

/// The named text fields of one multipart body.
fn multipart_fields(body: &str) -> HashMap<String, String> {
    let parts = body.split("\r\n--").filter_map(|part| {
        let (headers, value) = part.split_once("\r\n\r\n")?;
        let name = headers.split("name=\"").nth(1)?.split('"').next()?;
        (!headers.contains("filename=")).then(|| (name.to_owned(), value.to_owned()))
    });
    parts.collect()
}

#[tokio::test]
async fn a_verified_complete_torrent_is_added_stopped_rechecked_then_started() {
    let (file, hash) = metainfo("");
    let broker = broker(Client::new(None, complete())).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(
        result,
        json!({"service": "qbittorrent", "hash": hash, "outcome": "reseeding", "name": "Album",
               "state": "stoppedUP", "progress": 1.0, "size_bytes": 32768,
               "amount_left_bytes": 0, "save_path": SAVE_PATH})
    );
    assert_eq!(actions(&broker), ["add", "recheck", "start"]);
    let seen = broker.seen();
    let add = seen.iter().find(|request| request.path == "/api/v2/torrents/add").unwrap();
    let content_type = add.header("content-type").unwrap();
    assert!(content_type.starts_with("multipart/form-data; boundary="), "{content_type}");
    let fields = multipart_fields(&add.text);
    let expected = [
        ("savepath", SAVE_PATH),
        ("autoTMM", "false"),
        ("useDownloadPath", "false"),
        ("stopped", "true"),
        ("stopCondition", "None"),
        ("skip_checking", "false"),
        ("contentLayout", "Original"),
        ("tags", TAG),
        ("dlLimit", "1"),
    ];
    for (name, value) in expected {
        assert_eq!(fields.get(name).map(String::as_str), Some(value), "{name} in {fields:?}");
    }
    assert_eq!(fields.len(), expected.len(), "{fields:?}");
    let file_part = format!("filename=\"{hash}.torrent\"");
    assert!(add.text.contains(&file_part), "{}", add.text);
    assert!(add.text.contains(std::str::from_utf8(&file).unwrap()));
    assert_eq!(add.header("cookie"), Some(format!("SID={QBIT_SID}").as_str()));
    // Only this torrent is ever addressed, never `all`.
    let info = seen.iter().filter(|request| request.path == "/api/v2/torrents/info");
    assert!(info.clone().all(|request| request.query["hashes"] == hash), "{seen:?}");
    // The torrent is polled into existence before the recheck is sent.
    let before_recheck =
        seen.iter().take_while(|request| request.path != "/api/v2/torrents/recheck");
    let polls = before_recheck.filter(|request| request.path == "/api/v2/torrents/info").count();
    assert_eq!(polls, 3, "one presence check, one miss, one listing: {seen:?}");
}

#[tokio::test]
async fn an_incomplete_recheck_removes_the_torrent_keeping_its_data() {
    let (file, hash) = metainfo("");
    let broker = broker(Client::new(None, incomplete())).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "aborted_incomplete");
    assert_eq!(result["progress"], 0.25);
    assert_eq!(result["amount_left_bytes"], 24576);
    assert_eq!(actions(&broker), ["add", "recheck", "delete"]);
    let seen = broker.seen();
    let delete = seen.iter().find(|request| request.path == "/api/v2/torrents/delete").unwrap();
    assert_eq!(delete.text, format!("hashes={hash}&deleteFiles=false"));
}

#[tokio::test]
async fn a_torrent_with_excluded_files_is_never_counted_complete() {
    let (file, _) = metainfo("");
    let partial = with(complete(), json!({"size": 16384}));
    let broker = broker(Client::new(None, partial)).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "aborted_incomplete");
    assert_eq!(actions(&broker), ["add", "recheck", "delete"]);
}

#[tokio::test]
async fn a_torrent_running_incomplete_after_its_check_is_removed_at_once() {
    // With no files on disk libtorrent skips hashing and may start the
    // torrent before qBittorrent stops it again.
    let (file, _) = metainfo("");
    let running = json!({"state": "downloading", "progress": 0.0, "amount_left": 32768});
    let broker = broker(Client::new(None, running)).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "aborted_incomplete");
    assert_eq!(result["state"], "downloading");
    assert_eq!(actions(&broker), ["add", "recheck", "delete"]);
}

#[tokio::test]
async fn a_check_too_fast_to_observe_settles_after_one_refresh_interval() {
    // Only the stale state and the final state are ever reported.
    let (file, _) = metainfo("");
    let mut client = Client::new(None, json!({"state": "stoppedDL", "progress": 0.0}));
    client.checks = 0;
    let broker = broker(client).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "aborted_incomplete");
}

#[tokio::test]
async fn the_stale_state_right_after_a_recheck_is_not_taken_as_its_result() {
    // A refresh interval long enough that the first poll lands inside it.
    let (file, _) = metainfo("");
    let mut client = Client::new(None, complete());
    client.preferences = with(preferences(), json!({"refresh_interval": 200}));
    let broker = broker(client).await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "reseeding");
    assert_eq!(actions(&broker), ["add", "recheck", "start"]);
}

#[tokio::test]
async fn a_fresh_add_is_always_rechecked_even_if_listed_complete() {
    let (file, _) = metainfo("");
    let mut client = Client::new(None, incomplete());
    client.added = complete();
    let broker = broker(client).await;
    assert_eq!(broker.ok(RESEED_TOOL, arguments(&file)).await["outcome"], "aborted_incomplete");
    assert_eq!(actions(&broker), ["add", "recheck", "delete"]);
}

#[tokio::test]
async fn a_torrent_this_tool_did_not_add_is_left_untouched() {
    let (file, hash) = metainfo("");
    for state in ["stoppedDL", "uploading", "downloading", "checkingDL"] {
        let present = row(&hash, state, 0.5, "music, other");
        let broker = broker(Client::new(Some(present), complete())).await;
        let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
        assert_eq!(result["outcome"], "already_present", "{state}");
        assert_eq!(result["state"], state);
        assert!(actions(&broker).is_empty(), "{state}: {:?}", actions(&broker));
        let seen = broker.seen();
        let preferences = seen.iter().filter(|request| request.path.ends_with("/preferences"));
        assert_eq!(preferences.count(), 0, "{state}: no settings check is needed");
        assert!(!result.to_string().contains("must not pass"), "{result}");
    }
}

#[tokio::test]
async fn replaying_resumes_this_tools_own_unfinished_work() {
    let (file, hash) = metainfo("");
    // (present state, progress, final state after a recheck, expected actions, outcome)
    let cases = [
        ("stalledUP", 1.0, complete(), vec![], "reseeding"),
        ("stoppedUP", 1.0, complete(), vec!["start"], "reseeding"),
        ("stoppedDL", 0.0, complete(), vec!["recheck", "start"], "reseeding"),
        ("missingFiles", 0.0, incomplete(), vec!["recheck", "delete"], "aborted_incomplete"),
        ("checkingDL", 0.5, complete(), vec!["start"], "reseeding"),
        ("downloading", 0.1, complete(), vec!["delete"], "aborted_incomplete"),
    ];
    for (state, progress, after, expected, outcome) in cases {
        let present = row(&hash, state, progress, &format!("music, {TAG}"));
        let client = Client::new(Some(present), after);
        if state == "checkingDL" {
            // The earlier call's recheck is still running: poll it, never restart it.
            client.script.lock().unwrap().extend([json!({"state": "checkingDL"}), complete()]);
        }
        let broker = broker(client).await;
        let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
        assert_eq!(result["outcome"], outcome, "{state}: {result}");
        assert_eq!(actions(&broker), expected, "{state}");
    }
}

#[tokio::test]
async fn a_recheck_outlasting_the_call_reports_checking_and_leaves_it_stopped() {
    let (file, _) = metainfo("");
    let mut client = Client::new(None, complete());
    client.checks = 10_000;
    let broker = Broker::configured(
        |settings| {
            enable(&[SAVE_PATH])(settings);
            settings.reseed_deadline = std::time::Duration::from_millis(100);
        },
        client.backend(),
    )
    .await;
    let result = broker.ok(RESEED_TOOL, arguments(&file)).await;
    assert_eq!(result["outcome"], "checking");
    assert_eq!(result["state"], "checkingDL", "the last state polled, not the pre-check one");
    assert_eq!(actions(&broker), ["add", "recheck"]);
}

#[tokio::test]
async fn client_settings_that_would_change_files_on_disk_are_refused() {
    let (file, _) = metainfo("");
    let cases = [
        (json!({"incomplete_files_ext": true}), "appends an extension"),
        (json!({"incomplete_files_ext": null}), "appends an extension"),
        (json!({"excluded_file_names_enabled": true, "use_unwanted_folder": true}), "unwanted"),
    ];
    for (overrides, message) in cases {
        let mut client = Client::new(None, complete());
        client.preferences = with(preferences(), overrides.clone());
        let broker = broker(client).await;
        let error = broker.err(RESEED_TOOL, arguments(&file)).await;
        assert!(error.contains(message), "{overrides}: {error}");
        assert!(!error.contains("must not pass"), "{error}");
        assert!(actions(&broker).is_empty(), "{overrides}");
    }
    // Excluded names alone only narrow the wanted size, which completeness catches.
    let mut client = Client::new(None, complete());
    client.preferences = with(preferences(), json!({"excluded_file_names_enabled": true}));
    assert_eq!(
        broker(client).await.ok(RESEED_TOOL, arguments(&file)).await["outcome"],
        "reseeding"
    );
}

#[tokio::test]
async fn a_refused_add_is_an_upstream_error() {
    let (file, _) = metainfo("");
    let mut client = Client::new(None, complete());
    client.add_answer = "Fails.";
    let broker = broker(client).await;
    let error = broker.err(RESEED_TOOL, arguments(&file)).await;
    assert!(error.contains("refused the torrent"), "{error}");
    assert_eq!(actions(&broker), ["add"]);
}

#[tokio::test]
async fn save_paths_are_resolved_against_the_exact_allow_list() {
    let (file, _) = metainfo("");
    let two = [SAVE_PATH, "/data/torrents/other"];
    let client = || Client::new(None, complete()).backend();
    let broker = Broker::configured(enable(&two), client()).await;
    let error = broker.err(RESEED_TOOL, arguments(&file)).await;
    assert!(error.contains("save_path is required"), "{error}");
    for path in ["/data/torrents", "/data/torrents/music/../other", "/tmp"] {
        let error =
            broker.err(RESEED_TOOL, with(arguments(&file), json!({"save_path": path}))).await;
        assert!(error.contains("not an allowed reseed save path"), "{path}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
    let chosen = with(arguments(&file), json!({"save_path": "/data/torrents/other/"}));
    assert_eq!(broker.ok(RESEED_TOOL, chosen).await["outcome"], "reseeding");
    let seen = broker.seen();
    let add = seen.iter().find(|request| request.path == "/api/v2/torrents/add").unwrap();
    assert_eq!(multipart_fields(&add.text)["savepath"], "/data/torrents/other");
}

#[tokio::test]
async fn malformed_metainfo_is_rejected_before_any_upstream_call() {
    let (file, _) = metainfo("");
    let (v2, _) = metainfo("12:meta versioni2e");
    let mut trailing = file.clone();
    trailing.extend_from_slice(b"x");
    let deep = format!("d4:info{}e", "l".repeat(10_000));
    let no_pieces = b"d4:infod4:name5:Albumee".to_vec();
    let cases = [
        (json!({"torrent_b64": "not base64!"}), "standard base64"),
        (arguments(b"not bencode"), "well-formed v1"),
        (arguments(&v2), "v2 and hybrid"),
        (arguments(&trailing), "well-formed v1"),
        (arguments(deep.as_bytes()), "well-formed v1"),
        (arguments(&no_pieces), "well-formed v1"),
        (arguments(&file[..file.len() - 1]), "well-formed v1"),
        (arguments(&vec![b'0'; 1_048_577]), "1 MiB"),
        (json!({"torrent_b64": ""}), "torrent_b64"),
        (json!({}), "torrent_b64"),
        (with(arguments(&file), json!({"save_path": ""})), "save_path"),
    ];
    let broker = broker(Client::new(None, complete())).await;
    for (arguments, message) in cases {
        let error = broker.err(RESEED_TOOL, arguments).await;
        assert!(error.contains(message), "{message}: {error}");
    }
    assert!(broker.seen().is_empty(), "{:?}", broker.seen());
}

#[tokio::test]
async fn the_reseed_tool_appears_only_with_qbittorrent_and_its_gate() {
    let client = || Client::new(None, complete()).backend();
    for (qbit, enabled, expected) in
        [(true, true, true), (true, false, false), (false, true, false)]
    {
        let broker = Broker::configured(
            |settings| {
                if qbit {
                    with_qbit(settings);
                }
                settings.enable_reseeds = enabled;
                settings.reseed_save_paths = vec![SAVE_PATH.to_owned()];
            },
            client(),
        )
        .await;
        let tools = broker.tools().await;
        assert_eq!(tools.contains_key(RESEED_TOOL), expected, "qbit={qbit} enabled={enabled}");
        if expected {
            let annotations = &tools[RESEED_TOOL]["annotations"];
            assert_eq!(annotations["readOnlyHint"], false);
            assert_eq!(annotations["destructiveHint"], false);
            assert_eq!(annotations["idempotentHint"], true);
            let schema = &tools[RESEED_TOOL]["inputSchema"];
            assert_eq!(schema["required"], json!(["torrent_b64"]));
            assert_eq!(schema["properties"]["torrent_b64"]["maxLength"], 1_398_104);
            assert_eq!(schema["properties"]["save_path"]["maxLength"], 1024);
        } else {
            let (file, _) = metainfo("");
            let params = json!({"name": RESEED_TOOL, "arguments": arguments(&file)});
            let body = broker.rpc("tools/call", params).await.body;
            assert_eq!(body["result"]["isError"], true, "{body}");
            assert!(broker.seen().is_empty(), "{:?}", broker.seen());
        }
    }
}
