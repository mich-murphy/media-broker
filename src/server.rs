//! MCP server exposing bounded, projected inventory, history, and gated writes.

use std::{borrow::Cow, ops::Deref, sync::Arc};

use axum::{
    Router,
    extract::{Request, State},
    http::{Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use jiff::civil::Date;
use rmcp::{
    ErrorData, RoleServer, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResponse, CallToolResult, ContentBlock, Implementation,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerConfig, Tool,
        ToolAnnotations,
    },
    service::{MaybeSendFuture, RequestContext},
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use schemars::{JsonSchema, Schema, SchemaGenerator, generate::SchemaSettings, json_schema};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use subtle::ConstantTimeEq;

use crate::{
    adapters::{ArrService, Error, MediaClient, MediaType},
    config::{Secret, Service, Settings},
};

// Argument bounds live in the types, so invalid input is unrepresentable and
// callers see the bounds in the tool schema before calling.

/// An integer within `MIN..=MAX`; `DEFAULT` applies where a tool marks it optional.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(try_from = "i64")]
struct Int<const MIN: u32, const MAX: u32, const DEFAULT: u32 = 0>(u32);

impl<const MIN: u32, const MAX: u32, const DEFAULT: u32> Default for Int<MIN, MAX, DEFAULT> {
    fn default() -> Self {
        Self(DEFAULT)
    }
}

/// A string of `MIN..=MAX` characters.
#[derive(Debug, Deserialize)]
#[serde(try_from = "String")]
struct Text<const MIN: usize, const MAX: usize>(String);

/// Playback Reporting rows are keyed by exactly 32 hex characters.
#[derive(Debug, Deserialize)]
#[serde(try_from = "String")]
struct JellyfinUserId(String);

/// A timezone offset in hours.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(try_from = "f64")]
struct Offset(f64);

/// A selection of 1 to 100 season numbers.
#[derive(Debug, Deserialize)]
#[serde(try_from = "Vec<Int<0, 1000>>")]
struct Seasons(Vec<u32>);

/// A list of 1 to 100 torrent info-hashes.
#[derive(Debug, Deserialize)]
#[serde(try_from = "Vec<Text<1, 64>>")]
struct Hashes(Vec<String>);

/// Bound one argument newtype: `$checked` turns the wire value into the inner
/// value or `None`, `$schema` publishes the same bounds inline, and `Deref`
/// hands the adapters the plain value.
macro_rules! argument {
    (
        [$($generics:tt)*] $type:ty: $wire:ty => $plain:ty, $schema:tt,
        |$value:ident| $checked:expr, $($message:tt)+
    ) => {
        impl<$($generics)*> TryFrom<$wire> for $type {
            type Error = String;

            fn try_from($value: $wire) -> Result<Self, String> {
                $checked.map(Self).ok_or_else(|| format!($($message)+))
            }
        }

        impl<$($generics)*> Deref for $type {
            type Target = $plain;

            fn deref(&self) -> &$plain {
                &self.0
            }
        }

        impl<$($generics)*> JsonSchema for $type {
            fn inline_schema() -> bool {
                true
            }

            fn schema_name() -> Cow<'static, str> {
                stringify!($type).into()
            }

            fn json_schema(_: &mut SchemaGenerator) -> Schema {
                json_schema!($schema)
            }
        }
    };
}

argument!([const MIN: u32, const MAX: u32, const DEFAULT: u32] Int<MIN, MAX, DEFAULT>: i64 => u32,
    {"type": "integer", "minimum": MIN, "maximum": MAX},
    |value| u32::try_from(value).ok().filter(|value| (MIN..=MAX).contains(value)),
    "must be between {MIN} and {MAX}");
argument!([const MIN: usize, const MAX: usize] Text<MIN, MAX>: String => str,
    {"type": "string", "minLength": MIN, "maxLength": MAX},
    |value| (MIN..=MAX).contains(&value.chars().count()).then_some(value),
    "must contain between {MIN} and {MAX} characters");
argument!([] JellyfinUserId: String => str,
    {"type": "string", "minLength": 32, "maxLength": 32, "pattern": "^[0-9a-fA-F]{32}$"},
    |id| (id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit())).then_some(id),
    "must contain exactly 32 hexadecimal characters");
argument!([] Offset: f64 => f64,
    {"type": "number", "minimum": -14, "maximum": 14},
    |hours| (-14.0..=14.0).contains(&hours).then_some(hours),
    "must be between -14 and 14 hours");
argument!([] Seasons: Vec<Int<0, 1000>> => [u32],
    {"type": "array", "minItems": 1, "maxItems": 100,
     "items": {"type": "integer", "minimum": 0, "maximum": 1000}},
    |seasons| (1..=100).contains(&seasons.len())
        .then(|| seasons.iter().map(|season| season.0).collect()),
    "must contain between 1 and 100 seasons");
argument!([] Hashes: Vec<Text<1, 64>> => [String],
    {"type": "array", "minItems": 1, "maxItems": 100,
     "items": {"type": "string", "minLength": 1, "maxLength": 64}},
    |hashes| (1..=100).contains(&hashes.len())
        .then(|| hashes.iter().map(|hash| hash.0.clone()).collect()),
    "must contain between 1 and 100 hashes");

type Page = Int<1, 100_000, 1>;
type PageSize = Int<1, 100, 50>;
type Limit = Int<1, 100, 10>;
type Id = Int<1, { i32::MAX as u32 }>;
type UserId = Int<0, { i32::MAX as u32 }>;
type Search = Text<0, 200>;

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum SonarrOnly {
    Sonarr,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum LidarrOnly {
    Lidarr,
}

const fn yes() -> bool {
    true
}

/// The whole tool surface: a variant's name is the tool name, its doc comment
/// the description, its fields the arguments, and `access` its annotations and
/// gate (`arr`/`sonarr`/`lidarr`/`jellyfin`/`tautulli`/`qbittorrent` need that
/// upstream configured, `write` and `toggle` need requests, `delete` needs
/// deletes).
#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
// Doc comments are the verbatim tool descriptions; a one-value `service`
// argument is validated, then carries no information.
#[allow(clippy::doc_markdown, dead_code)]
enum ToolCall {
    /// Return a bounded page of Sonarr, Radarr, or Lidarr library inventory.
    #[schemars(extend("access" = "arr"))]
    ArrLibraryInventory {
        service: ArrService,
        #[serde(default)]
        page: Page,
        #[serde(default)]
        page_size: PageSize,
        search: Option<Search>,
    },
    /// Return projected quality-profile summaries for one Arr service.
    #[schemars(extend("access" = "arr"))]
    ArrQualityProfiles { service: ArrService },
    /// Return projected root-folder summaries for one Arr service.
    #[schemars(extend("access" = "arr"))]
    ArrRootFolders { service: ArrService },
    /// Search one Arr catalog for requestable media by title or keyword.
    #[schemars(extend("access" = "arr"))]
    ArrSearchCandidates {
        service: ArrService,
        query: Text<1, 200>,
        #[serde(default)]
        limit: Limit,
    },
    /// List Jellyfin users as id/name pairs for jellyfin_play_history.
    #[schemars(extend("access" = "jellyfin"))]
    JellyfinUsers {},
    /// Return sanitized Playback Reporting history over at most 31 days.
    #[schemars(extend("access" = "jellyfin"))]
    JellyfinPlayHistory {
        user_id: JellyfinUserId,
        media_type: MediaType,
        start_date: Date,
        end_date: Date,
        #[serde(default)]
        page: Page,
        #[serde(default)]
        page_size: PageSize,
        #[serde(default)]
        timezone_offset: Offset,
    },
    /// Return sanitized playback history for one media type over at most 31 days.
    #[schemars(extend("access" = "tautulli"))]
    TautulliPlayHistory {
        media_type: MediaType,
        start_date: Date,
        end_date: Date,
        #[serde(default)]
        page: Page,
        #[serde(default)]
        page_size: PageSize,
        user_id: Option<UserId>,
    },
    /// Return per-season monitoring and episode file counts for one series.
    #[schemars(extend("access" = "sonarr"))]
    ArrSeasonInventory { service: SonarrOnly, item_id: Id },
    /// Return per-album monitoring, file, and size details for one artist.
    #[schemars(extend("access" = "lidarr"))]
    ArrAlbumInventory { service: LidarrOnly, artist_id: Id },
    /// Return qBittorrent transfer totals, speeds, and connection status.
    #[schemars(extend("access" = "qbittorrent"))]
    TorrentClientStats {},
    /// Return a bounded page of torrents with swarm state and save paths.
    #[schemars(extend("access" = "qbittorrent"))]
    TorrentClientInventory {
        #[serde(default)]
        page: Page,
        #[serde(default)]
        page_size: PageSize,
        search: Option<Search>,
    },
    /// Report which info-hashes are present with complete recorded data.
    ///
    /// Completeness reflects the client's recorded progress; verifying bytes
    /// on disk requires a recheck, which is a write and out of scope.
    #[schemars(extend("access" = "qbittorrent"))]
    TorrentClientCheckPaths { hashes: Hashes },
    /// Add one exact catalog item; searching for downloads is the default.
    ///
    /// Sonarr accepts an optional seasons list so only those seasons are
    /// monitored and searched. For a selective Lidarr album add: add with
    /// search_on_add=false, then arr_album_inventory, arr_set_album_monitored,
    /// and arr_search_item.
    #[schemars(extend("access" = "write"))]
    ArrRequestMedia {
        service: ArrService,
        external_id: Text<1, 64>,
        quality_profile_id: Id,
        root_folder_path: Text<1, 1024>,
        #[serde(default = "yes")]
        search_on_add: bool,
        seasons: Option<Seasons>,
    },
    /// Stop monitoring one library item, keeping metadata and files.
    #[schemars(extend("access" = "toggle"))]
    ArrUnmonitorMedia { service: ArrService, item_id: Id },
    /// Resume monitoring one library item, reversing arr_unmonitor_media.
    #[schemars(extend("access" = "toggle"))]
    ArrMonitorMedia { service: ArrService, item_id: Id },
    /// Monitor or unmonitor a set of seasons on one series; reversible.
    #[schemars(extend("access" = "toggle"))]
    ArrSetSeasonMonitoring { service: SonarrOnly, item_id: Id, seasons: Seasons, monitored: bool },
    /// Monitor or unmonitor one Lidarr album; reversible in both directions.
    #[schemars(extend("access" = "toggle"))]
    ArrSetAlbumMonitored { service: LidarrOnly, album_id: Id, monitored: bool },
    /// Queue an upstream search for one item's monitored missing content.
    #[schemars(extend("access" = "write"))]
    ArrSearchItem { service: ArrService, item_id: Id },
    /// Delete one library item behind a confirmed two-phase preview.
    ///
    /// For Lidarr, an optional album_id deletes a single album of the artist
    /// identified by item_id instead of the whole artist.
    #[schemars(extend("access" = "delete"))]
    ArrDeleteMedia {
        service: ArrService,
        item_id: Id,
        #[serde(default)]
        delete_files: bool,
        confirmation: Option<Text<0, 128>>,
        album_id: Option<Id>,
    },
}

impl ToolCall {
    /// The dispatch table: one adapter call per tool, given the validated arguments.
    #[rustfmt::skip]
    async fn run(self, client: &MediaClient) -> Result<Value, Error> {
        match self {
            Self::ArrLibraryInventory { service, page, page_size, search } =>
                client.inventory(service, *page, *page_size, search.as_deref()).await,
            Self::ArrQualityProfiles { service } => client.quality_profiles(service).await,
            Self::ArrRootFolders { service } => client.root_folders(service).await,
            Self::ArrSearchCandidates { service, query, limit } =>
                client.search_candidates(service, &query, *limit).await,
            Self::JellyfinUsers {} => client.jellyfin_users().await,
            Self::JellyfinPlayHistory { user_id, media_type, start_date, end_date, page, page_size, timezone_offset } =>
                client.jellyfin_history(&user_id, media_type, start_date, end_date, *page, *page_size,
                    *timezone_offset).await,
            Self::TautulliPlayHistory { media_type, start_date, end_date, page, page_size, user_id } =>
                client.history(media_type, start_date, end_date, *page, *page_size, user_id.as_deref().copied()).await,
            Self::ArrSeasonInventory { item_id, .. } => client.season_inventory(*item_id).await,
            Self::ArrAlbumInventory { artist_id, .. } => client.album_inventory(*artist_id).await,
            Self::TorrentClientStats {} => client.torrent_stats().await,
            Self::TorrentClientInventory { page, page_size, search } =>
                client.torrent_inventory(*page, *page_size, search.as_deref()).await,
            Self::TorrentClientCheckPaths { hashes } => client.torrent_check_paths(&hashes).await,
            Self::ArrRequestMedia { service, external_id, quality_profile_id, root_folder_path, search_on_add, seasons } =>
                client.request_media(service, &external_id, *quality_profile_id, &root_folder_path, search_on_add,
                    seasons.as_deref()).await,
            Self::ArrUnmonitorMedia { service, item_id } => client.set_monitoring(service, *item_id, false).await,
            Self::ArrMonitorMedia { service, item_id } => client.set_monitoring(service, *item_id, true).await,
            Self::ArrSetSeasonMonitoring { item_id, seasons, monitored, .. } =>
                client.set_season_monitoring(*item_id, &seasons, monitored).await,
            Self::ArrSetAlbumMonitored { album_id, monitored, .. } =>
                client.set_album_monitored(*album_id, monitored).await,
            Self::ArrSearchItem { service, item_id } => client.search_item(service, *item_id).await,
            Self::ArrDeleteMedia { service, item_id, delete_files, confirmation, album_id } =>
                client.delete_media(service, *item_id, delete_files, confirmation.as_deref(),
                    album_id.as_deref().copied()).await,
        }
    }
}

/// Split the generated schema of [`ToolCall`] into the tools these settings enable.
///
/// # Panics
/// At startup, when a variant carries no known `access` level.
fn tools(settings: &Settings) -> Vec<Tool> {
    let generator =
        SchemaSettings::draft2020_12().with(|settings| settings.inline_subschemas = true);
    let schema = generator.into_generator().into_root_schema_for::<ToolCall>().to_value();
    let variants = schema["oneOf"].as_array().expect("one schema per tool");
    let configured = |service: Service| settings.upstream(service).is_some();
    let arr =
        configured(Service::Sonarr) || configured(Service::Radarr) || configured(Service::Lidarr);
    let tool = |variant: &Value| {
        let (name, input) = variant["properties"].as_object()?.iter().next()?;
        // (enabled, read-only, destructive, idempotent)
        let (enabled, read_only, destructive, idempotent) = match variant["access"].as_str() {
            Some("arr") => (arr, true, false, true),
            Some("sonarr") => (configured(Service::Sonarr), true, false, true),
            Some("lidarr") => (configured(Service::Lidarr), true, false, true),
            Some("jellyfin") => (configured(Service::Jellyfin), true, false, true),
            Some("tautulli") => (configured(Service::Tautulli), true, false, true),
            Some("qbittorrent") => (configured(Service::Qbittorrent), true, false, true),
            Some("write") => (arr && settings.enable_requests, false, false, false),
            Some("toggle") => (arr && settings.enable_requests, false, false, true),
            Some("delete") => (arr && settings.enable_deletes, false, true, false),
            access => panic!("tool {name} has no known access level: {access:?}"),
        };
        let hints = [read_only, destructive, idempotent, false].map(Some);
        let annotations = ToolAnnotations::from_raw(None, hints[0], hints[1], hints[2], hints[3]);
        let description = variant["description"].as_str()?.to_owned();
        let tool = Tool::new(name.clone(), description, Arc::new(input.as_object()?.clone()));
        Some(enabled.then(|| tool.with_annotations(annotations)))
    };
    let tools = variants.iter().map(|variant| tool(variant).expect("a well-formed tool schema"));
    tools.flatten().collect()
}

#[derive(Clone)]
struct Broker {
    client: Arc<MediaClient>,
    tools: Arc<Vec<Tool>>,
}

impl ServerHandler for Broker {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("media-broker", env!("CARGO_PKG_VERSION")))
            .with_instructions(
                "Projected media inventory and playback history; media requests and \
                 library cleanup are available only where explicitly enabled.",
            )
    }

    fn list_tools(
        &self,
        _: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> impl Future<Output = Result<ListToolsResult, ErrorData>> + MaybeSendFuture + '_ {
        std::future::ready(Ok(ListToolsResult::with_all_items(self.tools.to_vec())))
    }

    /// Argument and upstream failures are sanitized `isError` results.
    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let name = request.name.as_ref();
        let outcome = if self.tools.iter().any(|tool| tool.name == name) {
            let arguments = Value::Object(request.arguments.unwrap_or_default());
            match serde_path_to_error::deserialize::<_, ToolCall>(json!({name: arguments})) {
                Ok(call) => call.run(&self.client).await.map_err(|error| error.to_string()),
                Err(error) => {
                    let path = error.path().to_string();
                    let field = path.strip_prefix(name).unwrap_or(&path).trim_start_matches('.');
                    Err(format!("{field}: {}", error.inner()).trim_start_matches(": ").to_owned())
                }
            }
        } else {
            Err(format!("Unknown tool: {name}"))
        };
        let result = match outcome {
            Ok(content) => CallToolResult::structured(content),
            Err(message) => CallToolResult::error(vec![ContentBlock::text(message)]),
        };
        Ok(result.into())
    }
}

/// Answer unauthenticated liveness probes with a static, data-free body.
///
/// Container health checks carry no credentials, so this one exact route runs
/// ahead of the bearer boundary. It returns nothing but liveness and never
/// touches an upstream.
async fn health(request: Request, next: Next) -> Response {
    if request.method() == Method::GET && request.uri().path() == "/health" {
        let headers =
            [(header::CONTENT_TYPE, "application/json"), (header::CACHE_CONTROL, "no-store")];
        return (StatusCode::OK, headers, json!({"status": "ok"}).to_string()).into_response();
    }
    next.run(request).await
}

/// Require exactly one valid bearer on every HTTP route, including unknown paths.
async fn bearer(State(token): State<Arc<Secret>>, request: Request, next: Next) -> Response {
    let mut values = request.headers().get_all(header::AUTHORIZATION).iter();
    let authorized = match (values.next(), values.next()) {
        (Some(value), None) => {
            value.as_bytes().split_at_checked(7).is_some_and(|(scheme, candidate)| {
                scheme.eq_ignore_ascii_case(b"bearer ")
                    && candidate.ct_eq(token.expose().as_bytes()).into()
            })
        }
        _ => false,
    };
    if authorized {
        return next.run(request).await;
    }
    let headers =
        [(header::WWW_AUTHENTICATE, "Bearer"), (header::CONTENT_TYPE, "application/json")];
    let body = json!({"error": "authentication required"}).to_string();
    (StatusCode::UNAUTHORIZED, headers, body).into_response()
}

/// Build the authenticated Streamable HTTP application.
///
/// # Panics
/// At startup, when the TLS backend or the tool schema is broken.
pub fn router(settings: Settings) -> Router {
    let config = StreamableHttpServerConfig::default()
        .with_legacy_session_mode(false)
        .with_json_response(true)
        .with_sse_keep_alive(None)
        .with_allowed_hosts(settings.allowed_hosts.clone())
        .with_allowed_origins(settings.allowed_origins.clone())
        .enforce_origin_validation(); // an empty allow-list rejects, never disables
    let token = Arc::new(settings.bearer_token.clone());
    let broker =
        Broker { tools: Arc::new(tools(&settings)), client: Arc::new(MediaClient::new(settings)) };
    let service = StreamableHttpService::new(
        move || Ok(broker.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    // The health layer runs outermost so its one exact route answers ahead of
    // the bearer boundary; every other method and path falls through to it.
    Router::new()
        .route_service("/mcp", service)
        .layer(middleware::from_fn_with_state(token, bearer))
        .layer(middleware::from_fn(health))
}
