//! Small adapters with bounded responses and explicit projections.
//!
//! Read tools are available when their upstream is configured. Write tools
//! (request, unmonitor, delete, reseed) are gated in the server; deletions
//! additionally require a short-lived signed confirmation bound to the exact
//! action parameters, and reseeds only ever start a torrent whose data a full
//! recheck verified.

use std::{
    collections::{BTreeSet, HashMap},
    fmt::{self, Write as _},
    ops::Range,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, KeyInit, Mac};
use jiff::{Timestamp, ToSpan, civil::Date};
use reqwest::{Method, RequestBuilder, header::CONTENT_TYPE, redirect::Policy};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use crate::config::{Auth, Service, Settings};

const MAX_ITEMS: usize = 10_000;
const MAX_PROJECTED_STRING: usize = 1024;
const MAX_LIST_ENTRIES: usize = 16;
const MAX_SEASON_ENTRIES: usize = 64;
const MAX_REQUEST_BYTES: usize = 262_144;
const MAX_HASHES: usize = 100;
const MAX_BENCODE_DEPTH: usize = 64;
/// The largest accepted metainfo file; a music album's is tens of kilobytes.
pub const MAX_TORRENT_BYTES: usize = 1_048_576;
/// Marks torrents this broker added, so a replay resumes only its own work.
const RESEED_TAG: &str = "media-broker-reseed";
/// Bytes per second; qBittorrent reads 0 as unlimited, so 1 is the floor. It
/// keeps any block from completing in the window where libtorrent may start a
/// torrent with no files on disk before qBittorrent stops it again.
const RESEED_DOWNLOAD_LIMIT: &str = "1";
/// qBittorrent's default state refresh interval, in milliseconds.
const DEFAULT_REFRESH_MS: u64 = 1500;
const DELETE_ACTION: &str = "arr_delete_media";
// Tautulli 2.18.1 emits numeric quarter-step watched statuses; 1 is complete.
const WATCHED_STATUSES: [f64; 5] = [0.0, 0.25, 0.5, 0.75, 1.0];
const FAILED: &str = "upstream request failed";
const OVERSIZED: &str = "upstream response exceeds the configured size limit";
const DATE_RANGE: &str = "end_date must be on or after start_date and within 31 inclusive days";

/// A sanitized failure suitable for returning from an MCP tool.
#[derive(Debug)]
pub enum Error {
    /// A tool argument is outside the broker's safe bounds.
    Parameter(String),
    /// An upstream failed or answered with an unexpected shape.
    Upstream(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (Self::Parameter(message) | Self::Upstream(message)) = self;
        f.write_str(message)
    }
}

type Result<T = Value> = std::result::Result<T, Error>;

fn parameter<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Parameter(message.into()))
}

fn upstream<T>(message: impl Into<String>) -> Result<T> {
    Err(Error::Upstream(message.into()))
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum ArrService {
    Sonarr,
    Radarr,
    Lidarr,
}

impl From<ArrService> for Service {
    fn from(service: ArrService) -> Self {
        match service {
            ArrService::Sonarr => Self::Sonarr,
            ArrService::Radarr => Self::Radarr,
            ArrService::Lidarr => Self::Lidarr,
        }
    }
}

#[derive(Clone, Copy, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MediaType {
    Movie,
    Episode,
    Track,
}

/// Every per-service Arr naming difference as data: the resource path, lookup
/// term prefix, projected and upstream external-id keys, title field, and the
/// description of a valid external id.
struct ArrSpec {
    resource: &'static str,
    lookup_prefix: &'static str,
    external_key: &'static str,
    external_field: &'static str,
    title_field: &'static str,
    id_description: &'static str,
}

const fn arr_spec(fields: [&'static str; 6]) -> ArrSpec {
    let [resource, lookup_prefix, external_key, external_field, title_field, id_description] =
        fields;
    ArrSpec { resource, lookup_prefix, external_key, external_field, title_field, id_description }
}

#[rustfmt::skip]
const ARR_SPECS: [ArrSpec; 3] = [
    arr_spec(["series", "tvdb:", "tvdb_id", "tvdbId", "title", "a positive numeric TVDB identifier"]),
    arr_spec(["movie", "tmdb:", "tmdb_id", "tmdbId", "title", "a positive numeric TMDB identifier"]),
    arr_spec(["artist", "lidarr:", "musicbrainz_id", "foreignArtistId", "artistName", "a MusicBrainz artist UUID"]),
];

impl ArrService {
    const fn spec(self) -> &'static ArrSpec {
        &ARR_SPECS[self as usize]
    }

    fn item_path(self, item_id: u32) -> String {
        format!("/{}/{item_id}", self.spec().resource)
    }

    fn item_summary(self, row: &Value, item_id: u32) -> Value {
        summary(row, "id", self.spec().title_field, item_id)
    }

    /// Catalog ids are decimal strings without leading zeros, capped to int32;
    /// Lidarr takes only the dashed canonical `MusicBrainz` UUID form (any case).
    fn id_valid(self, value: &str) -> bool {
        if self == Self::Lidarr {
            let hex = |group: &str| group.bytes().all(|byte| byte.is_ascii_hexdigit());
            return value.split('-').map(str::len).eq([8, 4, 4, 4, 12])
                && value.split('-').all(hex);
        }
        // `parse` alone would also accept a leading plus sign.
        let digits = value.bytes().all(|byte| byte.is_ascii_digit());
        digits && !value.starts_with('0') && value.parse::<i32>().is_ok()
    }

    fn add_body(
        self,
        candidate: &Value,
        (profile_id, root): (u32, &str),
        metadata_id: Option<i64>,
        search: bool,
        seasons: Option<&[u32]>,
    ) -> Result {
        Ok(match self {
            Self::Sonarr => {
                let kind = text(&candidate["seriesType"]).filter(|kind| !kind.is_empty());
                let mut body = json!({
                    "title": text(&candidate["title"]),
                    "titleSlug": text(&candidate["titleSlug"]),
                    "tvdbId": candidate["tvdbId"].as_i64(),
                    "qualityProfileId": profile_id,
                    "rootFolderPath": root,
                    "seriesType": kind.unwrap_or("standard"),
                    "monitored": true,
                    "seasonFolder": true,
                    "seasons": sonarr_seasons(candidate, seasons)?,
                    "addOptions": {"searchForMissingEpisodes": search, "searchForCutoffUnmetEpisodes": false},
                });
                // With a season selection the monitor option is omitted entirely:
                // Sonarr's legacy path then derives episode monitoring from the
                // submitted season flags, so an add-search only ever touches the
                // selected seasons.
                if seasons.is_none() {
                    body["addOptions"]["monitor"] = json!("all");
                }
                body
            }
            Self::Radarr => json!({
                "title": text(&candidate["title"]),
                "titleSlug": text(&candidate["titleSlug"]),
                "tmdbId": candidate["tmdbId"].as_i64(),
                "year": candidate["year"].as_i64(),
                "qualityProfileId": profile_id,
                "rootFolderPath": root,
                "minimumAvailability": "released",
                "monitored": true,
                "addOptions": {"searchForMovie": search},
            }),
            Self::Lidarr => json!({
                "artistName": text(&candidate["artistName"]),
                "foreignArtistId": text(&candidate["foreignArtistId"]),
                "qualityProfileId": profile_id,
                "metadataProfileId": metadata_id,
                "rootFolderPath": root,
                "monitored": true,
                "monitorNewItems": "all",
                "addOptions": {"monitor": "all", "monitored": true, "searchForMissingAlbums": search},
            }),
        })
    }
}

/// A string no longer than the projected-string limit; anything else is dropped.
fn text(value: &Value) -> Option<&str> {
    let text = value.as_str()?;
    (text.chars().count() <= MAX_PROJECTED_STRING).then_some(text)
}

/// An opaque identifier: an integer or a bounded string.
fn id(value: &Value) -> Value {
    let number = value.as_i64().map(Value::from);
    number.unwrap_or_else(|| json!(text(value)))
}

/// Project a capped list of bounded strings, dropping invalid entries.
fn string_list(value: &Value) -> Option<Vec<&str>> {
    let entries = value.as_array()?.iter().take(MAX_LIST_ENTRIES);
    Some(entries.filter_map(text).collect())
}

/// Project a capped season list to `season_number` and monitored flags.
fn season_summaries(value: &Value) -> Option<Vec<Value>> {
    let rows = value.as_array()?.iter().take(MAX_SEASON_ENTRIES);
    let summary = |row: &Value| {
        let number = row["seasonNumber"].as_i64()?;
        Some(json!({"season_number": number, "monitored": row["monitored"].as_bool()}))
    };
    Some(rows.filter_map(summary).collect())
}

/// Add size and file-count fields from a row's embedded statistics block.
fn file_stats(mut item: Value, row: &Value, kind: &str) -> Value {
    let count = row["statistics"][format!("{kind}FileCount")].as_i64();
    item["size_on_disk_bytes"] = json!(row["statistics"]["sizeOnDisk"].as_i64());
    item[format!("{kind}_file_count")] = json!(count);
    item["has_file"] = json!(count.map(|count| count > 0));
    item
}

/// Summarize one item (`id`, the service's title field) or album (`album_id`,
/// `title`); the requested id stands in when upstream omits the record's own.
fn summary(row: &Value, id_key: &str, title_field: &str, requested_id: u32) -> Value {
    let own_id = row["id"].as_i64().filter(|id| *id != 0);
    json!({
        (id_key): own_id.unwrap_or(requested_id.into()),
        "title": text(&row[title_field]),
        "monitored": row["monitored"].as_bool(),
    })
}

/// Project one lookup or added record to approved keys plus in-library state.
fn lookup_item(row: &Value, spec: &ArrSpec) -> Value {
    let item_id = row["id"].as_i64();
    json!({
        "id": item_id,
        (spec.external_key): id(&row[spec.external_field]),
        "title": text(&row[spec.title_field]),
        "year": row["year"].as_i64(),
        "status": text(&row["status"]),
        "in_library": item_id.is_some_and(|id| id != 0),
    })
}

fn inventory_item(service: ArrService, row: &Value) -> Value {
    let mut item = json!({
        "id": row["id"].as_i64(),
        "title": text(&row[service.spec().title_field]),
        "year": row["year"].as_i64(),
        "status": text(&row["status"]),
        "monitored": row["monitored"].as_bool(),
        "quality_profile_id": row["qualityProfileId"].as_i64(),
        "root_folder_path": text(&row["rootFolderPath"]),
        "added": text(&row["added"]),
        "genres": string_list(&row["genres"]),
    });
    match service {
        ArrService::Sonarr => {
            item["seasons"] = json!(season_summaries(&row["seasons"]));
            file_stats(item, row, "episode")
        }
        ArrService::Radarr => {
            item["size_on_disk_bytes"] = json!(row["movieFile"]["size"].as_i64());
            item["has_file"] = json!(row["hasFile"].as_bool());
            item
        }
        ArrService::Lidarr => file_stats(item, row, "track"),
    }
}

fn sonarr_seasons(candidate: &Value, selection: Option<&[u32]>) -> Result<Vec<Value>> {
    let rows = candidate["seasons"].as_array().into_iter().flatten();
    let numbers: Vec<i64> = rows.filter_map(|row| row["seasonNumber"].as_i64()).collect();
    if numbers.is_empty() {
        return parameter("the lookup candidate has no season metadata");
    }
    let wanted = selection.map(|seasons| seasons.iter().map(|season| i64::from(*season)));
    let wanted: Option<Vec<i64>> = wanted.map(Iterator::collect);
    if wanted.iter().flatten().any(|season| !numbers.contains(season)) {
        return parameter("one or more seasons do not exist on the candidate");
    }
    let selected = |number: &i64| wanted.as_ref().is_none_or(|wanted| wanted.contains(number));
    let season = |number: &i64| json!({"seasonNumber": number, "monitored": selected(number)});
    Ok(numbers.iter().map(season).collect())
}

/// The pagination block of a locally paginated result.
fn local_page(page: u32, page_size: u32, returned: usize, total: usize) -> Value {
    let start = (page as usize - 1) * page_size as usize;
    json!({
        "page": page,
        "page_size": page_size,
        "returned_count": returned,
        "total_count": total,
        "has_more": start + returned < total,
        "upstream_paginated": false,
        "upstream_truncated": false,
    })
}

/// Bound an inclusive window of validated calendar dates to 31 days.
fn date_range(start: Date, end: Date) -> Result<()> {
    if (0..=30).contains(&(end - start).get_days()) {
        return Ok(());
    }
    parameter(DATE_RANGE)
}

/// HTTP client for the allow-listed upstream APIs.
pub struct MediaClient {
    settings: Settings,
    http: reqwest::Client,
    /// The qBittorrent session id, held server-side and never projected.
    qbit_sid: Mutex<Option<String>>,
    /// Serializes reseeds so two calls never drive one torrent at once.
    reseed: Mutex<()>,
}

impl MediaClient {
    /// # Panics
    /// When the TLS backend cannot be initialized.
    pub fn new(settings: Settings) -> Self {
        let builder = reqwest::Client::builder().redirect(Policy::none()).no_proxy();
        let http = builder.build().expect("the HTTP client initializes");
        Self { settings, http, qbit_sid: Mutex::new(None), reseed: Mutex::new(()) }
    }

    /// Address one upstream path; credentials travel in headers, never the URL.
    fn request(
        &self,
        service: impl Into<Service>,
        method: Method,
        path: &str,
    ) -> Result<RequestBuilder> {
        let service = service.into();
        let Some(upstream) = self.settings.upstream(service) else {
            return parameter(format!("{} is not configured on this broker", service.name()));
        };
        let url = format!("{}{}{path}", upstream.base_url, service.api_root());
        let request = self.http.request(method, url);
        Ok(match &upstream.auth {
            Auth::Key(key) => {
                let header = service.key_header().expect("key auth names a header");
                request.header(header, key.expose())
            }
            // Session auth attaches its cookie inside the qBittorrent exchange.
            Auth::Session { .. } => request,
        })
    }

    /// Send one request under a total deadline, returning the status and a
    /// size-capped body without judging the status.
    async fn exchange(&self, request: RequestBuilder) -> Result<(u16, Vec<u8>)> {
        let limit = self.settings.max_response_bytes;
        let exchange = async {
            let Ok(mut response) = request.send().await else {
                return upstream(FAILED);
            };
            let status = response.status().as_u16();
            if response.content_length().unwrap_or(0) > limit as u64 {
                return upstream(OVERSIZED);
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.or_else(|_| upstream(FAILED))? {
                body.extend_from_slice(&chunk);
                if body.len() > limit {
                    return upstream(OVERSIZED);
                }
            }
            Ok((status, body))
        };
        let deadline = tokio::time::timeout(self.settings.timeout, exchange);
        deadline.await.unwrap_or_else(|_| upstream("upstream request timed out"))
    }

    /// One exchange that rejects any HTTP error status.
    async fn fetch(&self, request: RequestBuilder) -> Result<Vec<u8>> {
        let (status, body) = self.exchange(request).await?;
        if status >= 300 {
            return upstream(format!("upstream returned HTTP {status}"));
        }
        Ok(body)
    }

    async fn json(&self, request: RequestBuilder) -> Result {
        let body = self.fetch(request).await?;
        serde_json::from_slice(&body).or_else(|_| upstream("upstream returned invalid JSON"))
    }

    /// Fetch or write one upstream object.
    async fn record(&self, request: RequestBuilder) -> Result {
        let payload = self.json(request).await?;
        if !payload.is_object() {
            return upstream("upstream returned an unexpected response");
        }
        Ok(payload)
    }

    /// Fetch a bounded list of upstream objects, dropping anything else.
    async fn rows(
        &self,
        service: impl Into<Service>,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<Value>> {
        let request = self.request(service, Method::GET, path)?.query(query);
        match self.json(request).await? {
            Value::Array(rows) if rows.len() <= MAX_ITEMS => {
                Ok(rows.into_iter().filter(Value::is_object).collect())
            }
            _ => {
                let resource = path.split('/').nth(1).unwrap_or_default().to_lowercase();
                upstream(format!("upstream returned an unexpected {resource} response"))
            }
        }
    }

    /// Send a broker-built JSON body, refusing unbounded payloads.
    async fn write(&self, service: ArrService, method: Method, path: &str, body: &Value) -> Result {
        let content = body.to_string();
        if content.len() > MAX_REQUEST_BYTES {
            return parameter("request payload exceeds the broker limit");
        }
        let request = self.request(service, method, path)?.body(content);
        self.record(request.header(CONTENT_TYPE, "application/json")).await
    }

    async fn lookup(&self, service: ArrService, term: String) -> Result<Vec<Value>> {
        let path = format!("/{}/lookup", service.spec().resource);
        self.rows(service, &path, &[("term", term)]).await
    }

    /// Fetch one library record.
    async fn item(&self, service: ArrService, item_id: u32) -> Result {
        self.record(self.request(service, Method::GET, &service.item_path(item_id))?).await
    }

    /// Flip the monitored flag of the record at `path`; everything else
    /// round-trips untouched. Returns the updated upstream record.
    async fn set_monitored(&self, service: ArrService, path: &str, monitored: bool) -> Result {
        let mut record = self.record(self.request(service, Method::GET, path)?).await?;
        record["monitored"] = json!(monitored);
        self.write(service, Method::PUT, path, &record).await
    }

    pub async fn inventory(
        &self,
        service: ArrService,
        page: u32,
        page_size: u32,
        search: Option<&str>,
    ) -> Result {
        let path = format!("/{}", service.spec().resource);
        let rows = self.rows(service, &path, &[]).await?;
        let mut items: Vec<Value> = rows.iter().map(|row| inventory_item(service, row)).collect();
        let query = search.unwrap_or_default().trim().to_lowercase();
        let title = |item: &Value| item["title"].as_str().unwrap_or_default().to_lowercase();
        items.retain(|item| title(item).contains(&query));
        let (total, size) = (items.len(), page_size as usize);
        let paged = items.into_iter().skip((page as usize - 1) * size).take(size);
        let items: Vec<Value> = paged.collect();
        let pagination = local_page(page, page_size, items.len(), total);
        Ok(json!({"service": service, "items": items, "pagination": pagination}))
    }

    pub async fn quality_profiles(&self, service: ArrService) -> Result {
        let rows = self.rows(service, "/qualityprofile", &[]).await?;
        let project = |row: &Value| {
            json!({
                "id": row["id"].as_i64(),
                "name": text(&row["name"]),
                "upgrade_allowed": row["upgradeAllowed"].as_bool(),
                "cutoff": row["cutoff"].as_i64(),
            })
        };
        let items: Vec<Value> = rows.iter().map(project).collect();
        Ok(json!({"service": service, "items": items, "upstream_truncated": false}))
    }

    pub async fn root_folders(&self, service: ArrService) -> Result {
        let rows = self.rows(service, "/rootfolder", &[]).await?;
        let project = |row: &Value| {
            json!({
                "id": row["id"].as_i64(),
                "path": text(&row["path"]),
                "accessible": row["accessible"].as_bool(),
                "free_space_bytes": row["freeSpace"].as_i64(),
                "total_space_bytes": row["totalSpace"].as_i64(),
            })
        };
        let items: Vec<Value> = rows.iter().map(project).collect();
        Ok(json!({"service": service, "items": items, "upstream_truncated": false}))
    }

    pub async fn search_candidates(&self, service: ArrService, query: &str, limit: u32) -> Result {
        let rows = self.lookup(service, query.to_owned()).await?;
        let shown = rows.iter().take(limit as usize);
        let items: Vec<Value> = shown.map(|row| lookup_item(row, service.spec())).collect();
        let truncated = rows.len() > limit as usize;
        Ok(json!({"service": service, "items": items, "upstream_truncated": truncated}))
    }

    /// Add one exact catalog item; the broker builds the add body itself.
    pub async fn request_media(
        &self,
        service: ArrService,
        external_id: &str,
        quality_profile_id: u32,
        root_folder_path: &str,
        search_on_add: bool,
        seasons: Option<&[u32]>,
    ) -> Result {
        let spec = service.spec();
        if seasons.is_some() && service != ArrService::Sonarr {
            return parameter("seasons is only supported for sonarr");
        }
        let identifier = external_id.trim();
        if !service.id_valid(identifier) {
            let (expected, name) = (spec.id_description, Service::from(service).name());
            return parameter(format!("external_id must be {expected} for {name}"));
        }
        let rows = self.lookup(service, format!("{}{identifier}", spec.lookup_prefix)).await?;
        let matches = |row: &&Value| {
            let external = &row[spec.external_field];
            let number = external.as_i64().map(|id| id.to_string());
            let found = number.or_else(|| text(external).map(str::to_owned));
            found.is_some_and(|found| found.eq_ignore_ascii_case(identifier))
        };
        let Some(candidate) = rows.iter().find(matches) else {
            return parameter("no upstream candidate matches the external identifier");
        };
        if candidate["id"].as_i64().is_some_and(|id| id != 0) {
            return parameter("the requested media is already in the library");
        }
        let root = self.add_destination(service, quality_profile_id, root_folder_path).await?;
        // Only Lidarr requires a metadata profile when adding an artist.
        let metadata = match service {
            ArrService::Lidarr => Some(self.metadata_profile_id().await?),
            _ => None,
        };
        let destination = (quality_profile_id, root.as_str());
        let body = service.add_body(candidate, destination, metadata, search_on_add, seasons)?;
        let path = format!("/{}", spec.resource);
        let added = self.write(service, Method::POST, &path, &body).await?;
        Ok(json!({
            "service": service,
            "item": lookup_item(&added, spec),
            "search_on_add": search_on_add,
            "seasons": seasons,
        }))
    }

    /// Require a configured quality profile; return the known root folder.
    async fn add_destination(
        &self,
        service: ArrService,
        profile_id: u32,
        root_path: &str,
    ) -> Result<String> {
        let profiles = self.rows(service, "/qualityprofile", &[]).await?;
        let mut known = profiles.iter().map(|row| row["id"].as_i64());
        if !known.any(|id| id == Some(profile_id.into())) {
            return parameter("quality_profile_id is not a configured quality profile");
        }
        let wanted = root_path.trim_end_matches('/');
        let folders = self.rows(service, "/rootfolder", &[]).await?;
        let mut paths = folders.iter().filter_map(|folder| text(&folder["path"]));
        match paths.find(|path| path.trim_end_matches('/') == wanted) {
            Some(path) => Ok(path.to_owned()),
            None => parameter("root_folder_path is not a configured root folder"),
        }
    }

    /// Pick Lidarr's lowest metadata profile id; Lidarr adds require one.
    async fn metadata_profile_id(&self) -> Result<i64> {
        let rows = self.rows(Service::Lidarr, "/metadataprofile", &[]).await?;
        match rows.iter().filter_map(|row| row["id"].as_i64()).min() {
            Some(lowest) => Ok(lowest),
            None => upstream("upstream returned an unexpected metadata profile response"),
        }
    }

    /// Flip one item's monitored flag; everything else is left untouched.
    pub async fn set_monitoring(&self, service: ArrService, id: u32, monitored: bool) -> Result {
        let updated = self.set_monitored(service, &service.item_path(id), monitored).await?;
        Ok(json!({"service": service, "item": service.item_summary(&updated, id)}))
    }

    /// Flip the monitored flag of a set of seasons on one Sonarr series.
    pub async fn set_season_monitoring(
        &self,
        item_id: u32,
        seasons: &[u32],
        monitored: bool,
    ) -> Result {
        let service = ArrService::Sonarr;
        let mut record = self.item(service, item_id).await?;
        let Some(rows) = record["seasons"].as_array_mut() else {
            return upstream("upstream returned an unexpected series response");
        };
        let wanted: BTreeSet<i64> = seasons.iter().map(|season| i64::from(*season)).collect();
        let mut found = BTreeSet::new();
        for row in rows.iter_mut().filter(|row| row.is_object()) {
            let number = row["seasonNumber"].as_i64();
            if let Some(number) = number.filter(|number| wanted.contains(number)) {
                found.insert(number);
                row["monitored"] = json!(monitored);
            }
        }
        if found != wanted {
            return parameter("one or more seasons do not exist on the series");
        }
        let updated =
            self.write(service, Method::PUT, &service.item_path(item_id), &record).await?;
        Ok(json!({
            "service": service,
            "item": service.item_summary(&updated, item_id),
            "seasons": season_summaries(&updated["seasons"]),
        }))
    }

    /// Aggregate per-season monitoring and episode file counts for one series.
    pub async fn season_inventory(&self, item_id: u32) -> Result {
        let service = ArrService::Sonarr;
        let record = self.item(service, item_id).await?;
        let query = [("seriesId", item_id.to_string())];
        let episodes = self.rows(service, "/episode", &query).await?;
        let mut counts: HashMap<i64, (u32, u32)> = HashMap::new();
        for episode in &episodes {
            if let Some(number) = episode["seasonNumber"].as_i64() {
                let (total, files) = counts.entry(number).or_default();
                *total += 1;
                *files += u32::from(episode["hasFile"] == true);
            }
        }
        let mut seasons = season_summaries(&record["seasons"]).unwrap_or_default();
        for season in &mut seasons {
            let number = season["season_number"].as_i64().unwrap_or_default();
            let (total, files) = counts.get(&number).copied().unwrap_or_default();
            season["episode_count"] = json!(total);
            season["episode_file_count"] = json!(files);
        }
        Ok(json!({
            "service": service,
            "item": service.item_summary(&record, item_id),
            "seasons": seasons,
        }))
    }

    /// Project every album of one Lidarr artist with file and size details.
    pub async fn album_inventory(&self, artist_id: u32) -> Result {
        let query = [("artistId", artist_id.to_string())];
        let albums = self.rows(Service::Lidarr, "/album", &query).await?;
        let project = |row: &Value| {
            let album = json!({
                "album_id": row["id"].as_i64(),
                "title": text(&row["title"]),
                "monitored": row["monitored"].as_bool(),
                "release_date": text(&row["releaseDate"]),
            });
            file_stats(album, row, "track")
        };
        let items: Vec<Value> = albums.iter().map(project).collect();
        Ok(json!({"service": ArrService::Lidarr, "artist_id": artist_id, "items": items}))
    }

    /// Flip one Lidarr album's monitored flag; reversible in both directions.
    pub async fn set_album_monitored(&self, album_id: u32, monitored: bool) -> Result {
        let service = ArrService::Lidarr;
        let updated = self.set_monitored(service, &format!("/album/{album_id}"), monitored).await?;
        Ok(json!({"service": service, "album": summary(&updated, "album_id", "title", album_id)}))
    }

    /// Queue an upstream search for one item's monitored missing content.
    pub async fn search_item(&self, service: ArrService, item_id: u32) -> Result {
        let body = match service {
            ArrService::Sonarr => json!({"name": "SeriesSearch", "seriesId": item_id}),
            ArrService::Radarr => json!({"name": "MoviesSearch", "movieIds": [item_id]}),
            ArrService::Lidarr => json!({"name": "ArtistSearch", "artistId": item_id}),
        };
        let queued = self.write(service, Method::POST, "/command", &body).await?;
        let command = json!({
            "command_id": queued["id"].as_i64(),
            "name": text(&queued["name"]),
            "status": text(&queued["status"]),
        });
        Ok(json!({"service": service, "command": command}))
    }

    /// Two-phase delete of one item, or of one Lidarr album while leaving its
    /// artist in place: preview with a bound confirmation, then execute.
    pub async fn delete_media(
        &self,
        service: ArrService,
        item_id: u32,
        delete_files: bool,
        confirmation: Option<&str>,
        album_id: Option<u32>,
    ) -> Result {
        let mut query = vec![("deleteFiles", delete_files.to_string())];
        let (path, mut outcome) = match album_id {
            Some(_) if service != ArrService::Lidarr => {
                return parameter("album_id is only supported for lidarr");
            }
            Some(album_id) => {
                let path = format!("/album/{album_id}");
                let request = self.request(service, Method::GET, &path)?;
                let album = self.record(request).await?;
                if album["artistId"].as_i64() != Some(item_id.into()) {
                    return parameter("album_id does not belong to item_id");
                }
                let artist = self.item(service, item_id).await?;
                let artist = json!({"id": item_id, "title": text(&artist["artistName"])});
                let album = summary(&album, "album_id", "title", album_id);
                (path, json!({"item": album, "artist": artist}))
            }
            None => {
                let item = self.item(service, item_id).await?;
                query.push(("addImportListExclusion", "false".to_owned()));
                (service.item_path(item_id), json!({"item": service.item_summary(&item, item_id)}))
            }
        };
        outcome["service"] = json!(service);
        outcome["delete_files"] = json!(delete_files);
        let action = json!([DELETE_ACTION, service, item_id, album_id, delete_files]).to_string();
        let now = Timestamp::now().as_second();
        if let Some(token) = confirmation {
            self.confirmation_must_match(&action, token, now)?;
            let request = self.request(service, Method::DELETE, &path)?.query(&query);
            self.fetch(request).await?;
            outcome["deleted"] = json!(true);
        } else {
            let expires = now.saturating_add_unsigned(self.settings.confirmation_ttl.as_secs());
            let token = format!("{expires}.{}", self.signature(expires, &action));
            outcome["confirmation_required"] = json!(true);
            outcome["confirmation"] = json!(token);
            outcome["confirmation_expires_at"] = json!(expires);
        }
        Ok(outcome)
    }

    /// Hex HMAC-SHA256, keyed by the bearer token, over an expiry and action.
    fn signature(&self, expires: i64, action: &str) -> String {
        let key = self.settings.bearer_token.expose().as_bytes();
        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
        mac.update(format!("{expires}.{action}").as_bytes());
        let mut hex = String::new();
        for byte in mac.finalize().into_bytes() {
            let _ = write!(hex, "{byte:02x}");
        }
        hex
    }

    /// Require a fresh token signed for exactly this action's parameters.
    fn confirmation_must_match(&self, action: &str, token: &str, now: i64) -> Result<()> {
        let (expires, signature) = token.split_once('.').unwrap_or_default();
        let expires = expires.parse().unwrap_or(i64::MIN);
        let expected = self.signature(expires, action);
        if !bool::from(signature.as_bytes().ct_eq(expected.as_bytes())) {
            return parameter("confirmation is not valid for this action");
        }
        if now >= expires {
            return parameter("confirmation has expired; request a fresh preview");
        }
        Ok(())
    }

    pub async fn history(
        &self,
        media_type: MediaType,
        start: Date,
        end: Date,
        page: u32,
        page_size: u32,
        user_id: Option<u32>,
    ) -> Result {
        date_range(start, end)?;
        let offset = u64::from(page - 1) * u64::from(page_size);
        let request = self
            .request(Service::Tautulli, Method::GET, "")?
            .query(&[("cmd", "get_history")])
            .query(&[("start", offset), ("length", page_size.into())])
            .query(&[("media_type", media_type)])
            // Tautulli's inclusive date bounds are named after/before.
            .query(&[("after", start), ("before", end)])
            // Discrete playback events, not grouped or activity-expanded rows.
            .query(&[("grouping", 0), ("include_activity", 0)])
            .query(&[("user_id", user_id)]);
        let payload = self.json(request).await?;
        let response = &payload["response"];
        if response["result"] != "success" {
            return upstream("upstream history request was not successful");
        }
        let rows: Vec<&Value> = match response["data"]["data"].as_array() {
            Some(rows) if rows.len() <= page_size as usize => {
                rows.iter().filter(|row| row.is_object()).collect()
            }
            _ => return upstream("upstream returned an unexpected history response"),
        };
        // Re-apply the requested filters locally so a misbehaving upstream
        // can never return another household member's rows.
        let kind = json!(media_type);
        let users = user_id.map(|user| [json!(user), json!(user.to_string())]);
        let mut items = Vec::new();
        for row in &rows {
            let other_type = row.get("media_type").is_some_and(|found| *found != kind);
            let other_user = users.as_ref().is_some_and(|users| !users.contains(&row["user_id"]));
            if other_type || other_user {
                continue;
            }
            let status = row["watched_status"].as_f64();
            let known = status.filter(|status| WATCHED_STATUSES.contains(status));
            // The source-prefixed identifiers are approved for household matching.
            items.push(json!({
                "media_type": media_type,
                "tautulli_user_id": id(&row["user_id"]),
                "user_name": text(&row["friendly_name"]),
                "tautulli_rating_key": id(&row["rating_key"]),
                "tautulli_history_id": id(&row["id"]),
                "title": text(&row["title"]),
                "parent_title": text(&row["parent_title"]),
                "grandparent_title": text(&row["grandparent_title"]),
                "played_at": row["started"].as_number(),
                "duration_seconds": row["duration"].as_number(),
                "completed": known.map(|status| status >= 1.0),
            }));
        }
        let received = offset + rows.len() as u64;
        let full_page = rows.len() == page_size as usize;
        // An upstream total smaller than what was already received is never trusted.
        let total = response["data"]["recordsFiltered"].as_u64();
        let total = total.filter(|total| *total >= received);
        Ok(json!({
            "media_type": media_type,
            "start_date": start,
            "end_date": end,
            "user_id_filter": user_id,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "returned_count": items.len(),
                "upstream_total_count": total,
                "has_more": total.map_or(full_page, |total| received < total),
                "upstream_paginated": true,
                "upstream_truncated": total.is_none() && full_page,
            },
            "items": items,
        }))
    }

    /// Return projected Playback Reporting rows, fetched one day at a time.
    #[allow(clippy::too_many_arguments)] // mirrors the tool's argument list
    pub async fn jellyfin_history(
        &self,
        user_id: &str,
        media_type: MediaType,
        start: Date,
        end: Date,
        page: u32,
        page_size: u32,
        timezone_offset: f64,
    ) -> Result {
        date_range(start, end)?;
        let filter = match media_type {
            MediaType::Movie => "Movie",
            MediaType::Episode => "Episode",
            MediaType::Track => "Audio",
        };
        let query =
            [("filter", filter.to_owned()), ("timezoneOffset", timezone_offset.to_string())];
        let size = page_size as usize;
        let window = (page as usize - 1) * size..page as usize * size;
        let mut items = Vec::new();
        let mut total = 0;
        for day in start.series(1.day()).take_while(|day| *day <= end) {
            let path = format!("/user_usage_stats/{user_id}/{day}/GetItems");
            let request = self.request(Service::Jellyfin, Method::GET, &path)?.query(&query);
            let payload = self.json(request).await?;
            let rows = match payload.as_array() {
                Some(rows) if rows.iter().all(Value::is_object) => rows,
                _ => return upstream("upstream returned an unexpected Jellyfin history response"),
            };
            // Count every row for honest local pagination, but retain only the
            // requested page and project it before the day's payload is dropped.
            for row in rows {
                if window.contains(&total) {
                    // Combine the plugin's local time with its requested calendar date.
                    let played_at = text(&row["Time"]).map(|time| format!("{day}T{time}"));
                    let played_at = played_at.filter(|at| at.len() <= MAX_PROJECTED_STRING);
                    items.push(json!({
                        "jellyfin_user_id": user_id,
                        "media_type": media_type,
                        "played_at": played_at,
                        "jellyfin_item_id": id(&row["Id"]),
                        "jellyfin_history_id": id(&row["RowId"]),
                        "title": text(&row["Name"]),
                        "duration_seconds": row["Duration"].as_number(),
                    }));
                }
                total += 1;
                if total > MAX_ITEMS {
                    return upstream("upstream returned too many Jellyfin history rows");
                }
            }
        }
        Ok(json!({
            "jellyfin_user_id": user_id,
            "media_type": media_type,
            "start_date": start,
            "end_date": end,
            "timezone_offset": timezone_offset,
            "pagination": local_page(page, page_size, items.len(), total),
            "items": items,
        }))
    }

    /// Project Jellyfin's user list to id and name pairs for history lookups.
    pub async fn jellyfin_users(&self) -> Result {
        let rows = self.rows(Service::Jellyfin, "/Users", &[]).await?;
        let project =
            |row: &Value| json!({"jellyfin_user_id": text(&row["Id"]), "name": text(&row["Name"])});
        let items: Vec<Value> = rows.iter().map(project).collect();
        Ok(json!({"items": items, "upstream_truncated": false}))
    }

    /// Log in to qBittorrent; the session id stays server-side only.
    ///
    /// The login answer carries the cookie in its headers, so it runs its own
    /// bounded exchange rather than [`MediaClient::fetch`].
    async fn qbit_login(&self) -> Result<String> {
        let Some(qbit) = self.settings.upstream(Service::Qbittorrent) else {
            return parameter("qbittorrent is not configured on this broker");
        };
        let Auth::Session { username, password } = &qbit.auth else {
            return parameter("qbittorrent is not configured on this broker");
        };
        let url = format!("{}{}/auth/login", qbit.base_url, Service::Qbittorrent.api_root());
        let form = [("username", username.as_str()), ("password", password.expose())];
        let body = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
        if body.len() > MAX_REQUEST_BYTES {
            return parameter("qBittorrent credentials exceed the broker limit");
        }
        let request = self
            .http
            .request(Method::POST, url)
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body);
        let limit = self.settings.max_response_bytes;
        let attempt = async {
            let Ok(mut response) = request.send().await else {
                return upstream(FAILED);
            };
            let status = response.status().as_u16();
            let sid =
                response.headers().get_all(reqwest::header::SET_COOKIE).iter().find_map(|value| {
                    let value = value.to_str().ok()?;
                    let sid = value.strip_prefix("SID=")?.split(';').next()?;
                    valid_sid(sid).then(|| sid.to_owned())
                });
            if response.content_length().unwrap_or(0) > limit as u64 {
                return upstream(OVERSIZED);
            }
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await.or_else(|_| upstream(FAILED))? {
                body.extend_from_slice(&chunk);
                if body.len() > limit {
                    return upstream(OVERSIZED);
                }
            }
            Ok((status, sid, body))
        };
        let deadline = tokio::time::timeout(self.settings.timeout, attempt);
        let (status, sid, body) =
            deadline.await.unwrap_or_else(|_| upstream("upstream request timed out"))?;
        let accepted = status < 300 && body.trim_ascii() == b"Ok.";
        let Some(sid) = sid.filter(|_| accepted) else {
            return upstream("upstream authentication failed");
        };
        *self.qbit_sid.lock().await = Some(sid.clone());
        Ok(sid)
    }

    /// GET one qBittorrent path.
    async fn qbit_fetch(&self, path: &str, query: &[(&str, String)]) -> Result<Vec<u8>> {
        self.qbit_exchange(Method::GET, path, |request| request.query(query)).await
    }

    /// POST one broker-built form to a qBittorrent action.
    async fn qbit_post(&self, path: &str, form: &[(&str, &str)]) -> Result<Vec<u8>> {
        let body = url::form_urlencoded::Serializer::new(String::new()).extend_pairs(form).finish();
        let form = |request: RequestBuilder| {
            request.header(CONTENT_TYPE, "application/x-www-form-urlencoded").body(body.clone())
        };
        self.qbit_exchange(Method::POST, path, form).await
    }

    /// One qBittorrent exchange, re-authenticating exactly once on a 403;
    /// `build` completes the request afresh for each attempt.
    async fn qbit_exchange(
        &self,
        method: Method,
        path: &str,
        build: impl Fn(RequestBuilder) -> RequestBuilder,
    ) -> Result<Vec<u8>> {
        for retried in [false, true] {
            let cached = self.qbit_sid.lock().await.clone();
            let sid = match cached {
                Some(sid) => sid,
                None => self.qbit_login().await?,
            };
            let request = self
                .request(Service::Qbittorrent, method.clone(), path)?
                .header(reqwest::header::COOKIE, format!("SID={sid}"));
            let (status, body) = self.exchange(build(request)).await?;
            if status == 403 && !retried {
                *self.qbit_sid.lock().await = None;
                continue;
            }
            if status >= 300 {
                return upstream(format!("upstream returned HTTP {status}"));
            }
            return Ok(body);
        }
        upstream(FAILED)
    }

    /// GET and parse one qBittorrent API path under the pinned version.
    async fn qbit_json(&self, path: &str, query: &[(&str, String)]) -> Result {
        let body = self.qbit_fetch(path, query).await?;
        serde_json::from_slice(&body).or_else(|_| upstream("upstream returned invalid JSON"))
    }

    /// Fetch a bounded list of qBittorrent torrents, dropping anything else.
    async fn qbit_torrents(&self, query: &[(&str, String)]) -> Result<Vec<Value>> {
        match self.qbit_json("/torrents/info", query).await? {
            Value::Array(rows) if rows.len() <= MAX_ITEMS => {
                Ok(rows.into_iter().filter(Value::is_object).collect())
            }
            _ => upstream("upstream returned an unexpected torrents response"),
        }
    }

    /// Project global transfer totals, speeds, and connection status.
    pub async fn torrent_stats(&self) -> Result {
        let payload = self.qbit_json("/transfer/info", &[]).await?;
        if !payload.is_object() {
            return upstream("upstream returned an unexpected transfer response");
        }
        Ok(json!({
            "service": "qbittorrent",
            "connection_status": text(&payload["connection_status"]),
            "dht_nodes": payload["dht_nodes"].as_i64(),
            "download_speed_bytes": payload["dl_info_speed"].as_i64(),
            "downloaded_bytes": payload["dl_info_data"].as_i64(),
            "download_rate_limit_bytes": payload["dl_rate_limit"].as_i64(),
            "upload_speed_bytes": payload["up_info_speed"].as_i64(),
            "uploaded_bytes": payload["up_info_data"].as_i64(),
            "upload_rate_limit_bytes": payload["up_rate_limit"].as_i64(),
        }))
    }

    /// Project a bounded, locally paginated page of torrent swarm state.
    pub async fn torrent_inventory(
        &self,
        page: u32,
        page_size: u32,
        search: Option<&str>,
    ) -> Result {
        let rows = self.qbit_torrents(&[]).await?;
        let mut items: Vec<Value> = rows.iter().map(torrent_item).collect();
        let query = search.unwrap_or_default().trim().to_lowercase();
        let name = |item: &Value| item["name"].as_str().unwrap_or_default().to_lowercase();
        items.retain(|item| name(item).contains(&query));
        let (total, size) = (items.len(), page_size as usize);
        let paged = items.into_iter().skip((page as usize - 1) * size).take(size);
        let items: Vec<Value> = paged.collect();
        let pagination = local_page(page, page_size, items.len(), total);
        Ok(json!({"service": "qbittorrent", "items": items, "pagination": pagination}))
    }

    /// Report which requested info-hashes are present with complete data.
    ///
    /// Completeness is the client's recorded progress. Verifying bytes on
    /// disk requires a recheck, which is a write and out of scope.
    pub async fn torrent_check_paths(&self, hashes: &[String]) -> Result {
        let mut normalized: Vec<String> = Vec::new();
        for hash in hashes {
            let hash = hash.trim().to_lowercase();
            if !normalized.contains(&hash) {
                normalized.push(hash);
            }
        }
        if normalized.is_empty() || normalized.len() > MAX_HASHES {
            return parameter("hashes must contain between 1 and 100 torrent hashes");
        }
        let valid = |hash: &str| {
            matches!(hash.len(), 40 | 64) && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
        };
        if normalized.iter().any(|hash| !valid(hash)) {
            return parameter("torrent hashes must be 40 or 64 hexadecimal characters");
        }
        let query = [("hashes", normalized.join("|"))];
        let rows = self.qbit_torrents(&query).await?;
        let mut found: HashMap<String, &Value> = HashMap::new();
        for row in &rows {
            if let Some(hash) = text(&row["hash"]) {
                let key = hash.to_lowercase();
                if normalized.contains(&key) {
                    found.insert(key, row);
                }
            }
        }
        let items: Vec<Value> = normalized
            .iter()
            .map(|hash| match found.get(hash) {
                Some(row) => path_check_item(row),
                None => json!({"hash": hash, "present": false}),
            })
            .collect();
        Ok(json!({
            "service": "qbittorrent",
            "requested_count": normalized.len(),
            "found_count": found.len(),
            "items": items,
        }))
    }

    /// Fetch the one torrent with this id, if qBittorrent lists it.
    async fn qbit_torrent(&self, hash: &str) -> Result<Option<Value>> {
        let rows = self.qbit_torrents(&[("hashes", hash.to_owned())]).await?;
        Ok(rows.into_iter().find(|row| text(&row["hash"]).is_some_and(|id| id == hash)))
    }

    /// Refuse client settings under which a recheck would change files on
    /// disk, and return how long a state change takes to become visible.
    async fn reseed_preflight(&self) -> Result<Duration> {
        let preferences = self.qbit_json("/app/preferences", &[]).await?;
        if !preferences.is_object() {
            return upstream("upstream returned an unexpected preferences response");
        }
        let off = |name: &str| preferences[name].as_bool() == Some(false);
        if !off("incomplete_files_ext") {
            return parameter(
                "qBittorrent appends an extension to incomplete files, so an incomplete recheck \
                 would rename files on disk; disable it before reseeding",
            );
        }
        if !off("excluded_file_names_enabled") && !off("use_unwanted_folder") {
            return parameter(
                "qBittorrent moves excluded files into an unwanted folder, which would move \
                 files on disk; disable one of those settings before reseeding",
            );
        }
        let refresh = preferences["refresh_interval"].as_u64().unwrap_or(DEFAULT_REFRESH_MS);
        Ok(Duration::from_millis(refresh.min(30_000)) + self.settings.reseed_poll_interval)
    }

    /// Resolve the requested save path against the exact allow-list; a single
    /// allowed path is the default.
    fn reseed_save_path(&self, requested: Option<&str>) -> Result<&str> {
        let allowed = &self.settings.reseed_save_paths;
        match requested.map(|path| path.trim().trim_end_matches('/')) {
            Some(wanted) => match allowed.iter().find(|path| *path == wanted) {
                Some(path) => Ok(path),
                None => parameter("save_path is not an allowed reseed save path"),
            },
            None if allowed.len() == 1 => Ok(&allowed[0]),
            None => parameter("save_path is required when several reseed save paths are allowed"),
        }
    }

    /// Add one metainfo file stopped, tagged, unchecked, and throttled, with
    /// every client default that could move or reshape its files overridden.
    async fn qbit_add(&self, metainfo: &[u8], hash: &str, save_path: &str) -> Result<()> {
        let fields = [
            ("savepath", save_path),
            ("autoTMM", "false"),
            ("useDownloadPath", "false"),
            ("stopped", "true"),
            ("stopCondition", "None"),
            ("skip_checking", "false"),
            ("contentLayout", "Original"),
            ("tags", RESEED_TAG),
            ("dlLimit", RESEED_DOWNLOAD_LIMIT),
        ];
        let mut boundary = format!("media-broker-{hash}");
        while metainfo.windows(boundary.len()).any(|window| window == boundary.as_bytes()) {
            boundary.push('-');
        }
        let mut body = Vec::with_capacity(metainfo.len() + 2048);
        for (name, value) in fields {
            let part = format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            );
            body.extend_from_slice(part.as_bytes());
        }
        let file = format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"torrents\"; \
             filename=\"{hash}.torrent\"\r\nContent-Type: application/x-bittorrent\r\n\r\n"
        );
        body.extend_from_slice(file.as_bytes());
        body.extend_from_slice(metainfo);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let content_type = format!("multipart/form-data; boundary={boundary}");
        let multipart = |request: RequestBuilder| {
            request.header(CONTENT_TYPE, content_type.as_str()).body(body.clone())
        };
        let answer = self.qbit_exchange(Method::POST, "/torrents/add", multipart).await?;
        // "Fails." covers duplicates and a client still starting up alike.
        if answer.trim_ascii() != b"Ok." {
            return upstream("upstream refused the torrent");
        }
        Ok(())
    }

    /// Poll one torrent until it leaves every checking state. A settled state
    /// counts only after a check was seen or `settled_after`, because the
    /// state qBittorrent reports lags a recheck by up to one refresh interval.
    /// `Err` carries the last row seen when the deadline passed first.
    async fn await_check(
        &self,
        hash: &str,
        mut seen_checking: bool,
        settled_after: Instant,
        deadline: Instant,
    ) -> Result<std::result::Result<Value, Value>> {
        loop {
            let Some(row) = self.qbit_torrent(hash).await? else {
                return upstream("the torrent disappeared from the client during its recheck");
            };
            if phase(&row) == Phase::Checking {
                seen_checking = true;
            } else if seen_checking || Instant::now() >= settled_after {
                return Ok(Ok(row));
            }
            let poll = self.settings.reseed_poll_interval;
            if Instant::now() + poll > deadline {
                return Ok(Err(row));
            }
            tokio::time::sleep(poll).await;
        }
    }

    /// Re-add one v1 metainfo file over data already on disk and start seeding
    /// only once a full recheck verifies every piece; otherwise remove it
    /// again, keeping the data. Torrents this tool did not add are never
    /// touched, so a whole batch can be replayed after any interruption.
    pub async fn torrent_reseed(&self, torrent_b64: &str, save_path: Option<&str>) -> Result {
        let Ok(metainfo) = BASE64.decode(torrent_b64.trim()) else {
            return parameter("torrent_b64 must be standard base64");
        };
        if metainfo.len() > MAX_TORRENT_BYTES {
            return parameter("torrent_b64 decodes to more than the 1 MiB metainfo limit");
        }
        let Some(hash) = info_hash(&metainfo) else {
            return parameter(
                "torrent_b64 must hold one well-formed v1 metainfo file; v2 and hybrid \
                 torrents are not supported",
            );
        };
        let save_path = self.reseed_save_path(save_path)?;
        let _serial = self.reseed.lock().await;
        let outcome = |outcome: &str, row: &Value| reseed_item(&hash, outcome, row);
        let present = self.qbit_torrent(&hash).await?;
        if let Some(row) = present.as_ref().filter(|row| !tagged(row)) {
            return Ok(outcome("already_present", row));
        }
        let settle = self.reseed_preflight().await?;
        let deadline = Instant::now() + self.settings.reseed_deadline;
        let fresh = present.is_none();
        let row = if let Some(row) = present {
            row
        } else {
            self.qbit_add(&metainfo, &hash, save_path).await?;
            // The add is asynchronous: the torrent is listed only once
            // libtorrent has loaded it.
            self.await_listed(&hash, deadline).await?
        };
        let checked = match phase(&row) {
            Phase::Seeding if complete(&row) => return Ok(outcome("reseeding", &row)),
            Phase::Seeding | Phase::Downloading => row,
            Phase::Checking => match self.await_check(&hash, true, Instant::now(), deadline).await?
            {
                Ok(row) => row,
                Err(last) => return Ok(outcome("checking", &last)),
            },
            // A stopped torrent from an earlier call was verified only if it
            // is complete: nothing but a recheck raises its progress.
            Phase::Settled if complete(&row) && !fresh => row,
            Phase::Settled => {
                self.qbit_post("/torrents/recheck", &[("hashes", &hash)]).await?;
                let settled_after = Instant::now() + settle;
                match self.await_check(&hash, false, settled_after, deadline).await? {
                    Ok(row) => row,
                    Err(last) => return Ok(outcome("checking", &last)),
                }
            }
        };
        if complete(&checked) {
            if phase(&checked) != Phase::Seeding {
                self.qbit_post("/torrents/start", &[("hashes", &hash)]).await?;
            }
            return Ok(outcome("reseeding", &checked));
        }
        let form = [("hashes", hash.as_str()), ("deleteFiles", "false")];
        self.qbit_post("/torrents/delete", &form).await?;
        Ok(outcome("aborted_incomplete", &checked))
    }

    /// Wait for a just-added torrent to be listed.
    async fn await_listed(&self, hash: &str, deadline: Instant) -> Result {
        loop {
            if let Some(row) = self.qbit_torrent(hash).await? {
                return Ok(row);
            }
            let poll = self.settings.reseed_poll_interval;
            if Instant::now() + poll > deadline {
                return upstream("the added torrent was not listed before the deadline");
            }
            tokio::time::sleep(poll).await;
        }
    }
}

/// qBittorrent session ids are short ASCII tokens; anything else is untrusted.
fn valid_sid(sid: &str) -> bool {
    let token = sid.bytes().all(|byte| byte.is_ascii_alphanumeric() || b"-._~".contains(&byte));
    !sid.is_empty() && sid.len() <= 256 && token
}

/// Project one torrent row to its allow-listed swarm fields.
fn torrent_item(row: &Value) -> Value {
    json!({
        "hash": text(&row["hash"]),
        "name": text(&row["name"]),
        "state": text(&row["state"]),
        "size_bytes": row["size"].as_i64(),
        "progress": row["progress"].as_number(),
        "ratio": row["ratio"].as_number(),
        "seeding_time_seconds": row["seeding_time"].as_i64(),
        "save_path": text(&row["save_path"]),
        "added_on": row["added_on"].as_i64(),
        "amount_left_bytes": row["amount_left"].as_i64(),
    })
}

/// Project one torrent row for the reseed-candidate audit.
fn path_check_item(row: &Value) -> Value {
    let progress = row["progress"].as_f64();
    json!({
        "hash": text(&row["hash"]),
        "present": true,
        "name": text(&row["name"]),
        "state": text(&row["state"]),
        "progress": row["progress"].as_number(),
        "amount_left_bytes": row["amount_left"].as_i64(),
        "save_path": text(&row["save_path"]),
        "data_complete": progress.map(|progress| progress >= 1.0),
    })
}

/// Where a torrent stands for a reseed, from qBittorrent 5.1's state string.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Hashing, queued for a check slot, or moving storage.
    Checking,
    /// Running with the client's recorded data complete.
    Seeding,
    /// Running while incomplete: removed at once, never left to download.
    Downloading,
    /// Stopped, errored, missing files, or unknown.
    Settled,
}

fn phase(row: &Value) -> Phase {
    match row["state"].as_str().unwrap_or_default() {
        "checkingUP" | "checkingDL" | "checkingResumeData" | "moving" => Phase::Checking,
        "uploading" | "stalledUP" | "queuedUP" | "forcedUP" => Phase::Seeding,
        "downloading" | "stalledDL" | "queuedDL" | "forcedDL" | "metaDL" | "forcedMetaDL" => {
            Phase::Downloading
        }
        _ => Phase::Settled,
    }
}

/// Every piece of every file verified: full progress, nothing left, and no
/// file excluded from the wanted size.
fn complete(row: &Value) -> bool {
    let size = row["size"].as_i64();
    row["progress"].as_f64().is_some_and(|progress| progress >= 1.0)
        && row["amount_left"].as_i64() == Some(0)
        && size.is_some_and(|size| size > 0)
        && size == row["total_size"].as_i64()
}

/// Whether this broker added the torrent; qBittorrent joins tags with ", ".
fn tagged(row: &Value) -> bool {
    text(&row["tags"]).is_some_and(|tags| tags.split(',').any(|tag| tag.trim() == RESEED_TAG))
}

/// Project one reseed outcome with the fields a batch report needs.
fn reseed_item(hash: &str, outcome: &str, row: &Value) -> Value {
    json!({
        "service": "qbittorrent",
        "hash": hash,
        "outcome": outcome,
        "name": text(&row["name"]),
        "state": text(&row["state"]),
        "progress": row["progress"].as_number(),
        "size_bytes": row["total_size"].as_i64(),
        "amount_left_bytes": row["amount_left"].as_i64(),
        "save_path": text(&row["save_path"]),
    })
}

/// The end offset of the bencoded value starting at `at`, or `None` when it
/// is malformed, truncated, or nested deeper than [`MAX_BENCODE_DEPTH`].
fn bencode_end(data: &[u8], at: usize, depth: usize) -> Option<usize> {
    match *data.get(at)? {
        b'i' => {
            let end = at + 1 + data.get(at + 1..)?.iter().position(|&byte| byte == b'e')?;
            let digits = data[at + 1..end].strip_prefix(b"-").unwrap_or(&data[at + 1..end]);
            let valid = !digits.is_empty() && digits.iter().all(u8::is_ascii_digit);
            valid.then_some(end + 1)
        }
        b'l' | b'd' if depth < MAX_BENCODE_DEPTH => {
            let dictionary = data[at] == b'd';
            let mut next = at + 1;
            while *data.get(next)? != b'e' {
                // Dictionary keys are byte strings, each followed by its value.
                if dictionary {
                    data[next].is_ascii_digit().then_some(())?;
                    next = bencode_end(data, next, depth + 1)?;
                }
                next = bencode_end(data, next, depth + 1)?;
            }
            Some(next + 1)
        }
        b'0'..=b'9' => {
            let colon = at + data[at..].iter().position(|&byte| byte == b':')?;
            let length: usize = std::str::from_utf8(&data[at..colon]).ok()?.parse().ok()?;
            let end = colon.checked_add(1)?.checked_add(length)?;
            (end <= data.len()).then_some(end)
        }
        _ => None,
    }
}

/// One dictionary entry: the raw key and the byte range of its value.
type Entry<'a> = (&'a [u8], Range<usize>);

/// The entries of the bencoded dictionary starting at `at`, plus the
/// dictionary's end offset.
fn bencode_entries(data: &[u8], at: usize) -> Option<(Vec<Entry<'_>>, usize)> {
    (*data.get(at)? == b'd').then_some(())?;
    let mut entries = Vec::new();
    let mut next = at + 1;
    while *data.get(next)? != b'e' {
        let key_end = data[next].is_ascii_digit().then(|| bencode_end(data, next, 1))??;
        let colon = next + data[next..key_end].iter().position(|&byte| byte == b':')?;
        let value_end = bencode_end(data, key_end, 1)?;
        entries.push((&data[colon + 1..key_end], key_end..value_end));
        next = value_end;
    }
    Some((entries, next + 1))
}

/// The lowercase hex v1 info-hash of one metainfo file: SHA-1 over the exact
/// bytes of its `info` dictionary, which must carry v1 `pieces`. Anything but
/// a single well-formed top-level dictionary is refused.
fn info_hash(metainfo: &[u8]) -> Option<String> {
    let (entries, end) = bencode_entries(metainfo, 0)?;
    (end == metainfo.len()).then_some(())?;
    let (_, info) = entries.into_iter().find(|(key, _)| *key == b"info")?;
    let (fields, _) = bencode_entries(metainfo, info.start)?;
    // qBittorrent identifies v2 and hybrid torrents by their truncated v2
    // hash, so only pure v1 metainfo has an id this function can compute.
    let has = |name: &[u8]| fields.iter().any(|(key, _)| *key == name);
    (has(b"pieces") && !has(b"meta version")).then_some(())?;
    let mut hex = String::with_capacity(40);
    for byte in Sha1::digest(&metainfo[info]) {
        let _ = write!(hex, "{byte:02x}");
    }
    Some(hex)
}
