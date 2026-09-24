//! Configuration and secret-file handling for the media broker.
//!
//! Write tools are opt-in: requests enable brokered adds, monitoring changes,
//! and search triggers, deletes enable the two-phase destructive media
//! removal tool, and reseeds enable the verified-complete qBittorrent reseed
//! into allow-listed save paths. Every upstream is optional, but at least one
//! must be set.

use std::{
    collections::HashMap, fmt, fs, os::unix::fs::PermissionsExt, str::FromStr, time::Duration,
};

use url::Url;

/// Required broker configuration is absent or unsafe.
#[derive(Debug)]
pub struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

type Result<T> = std::result::Result<T, ConfigError>;

macro_rules! bail {
    ($($message:tt)*) => { return Err(ConfigError(format!($($message)*))) };
}

/// A credential that never appears in `Debug` output.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// The six allow-listed upstream APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    Sonarr,
    Radarr,
    Lidarr,
    Tautulli,
    Jellyfin,
    Qbittorrent,
}

/// One row per [`Service`], in declaration order: name, API root.
const SERVICES: [[&str; 2]; 6] = [
    ["sonarr", "/api/v3"],
    ["radarr", "/api/v3"],
    ["lidarr", "/api/v1"],
    ["tautulli", "/api/v2"],
    ["jellyfin", ""],
    ["qbittorrent", "/api/v2"],
];

impl Service {
    pub const ALL: [Self; 6] = [
        Self::Sonarr,
        Self::Radarr,
        Self::Lidarr,
        Self::Tautulli,
        Self::Jellyfin,
        Self::Qbittorrent,
    ];

    #[must_use]
    pub const fn name(self) -> &'static str {
        SERVICES[self as usize][0]
    }

    /// The pinned API version prefix under the base URL.
    #[must_use]
    pub const fn api_root(self) -> &'static str {
        SERVICES[self as usize][1]
    }

    /// The header that carries the API key; keys never travel in URLs.
    /// qBittorrent authenticates with a session cookie instead.
    #[must_use]
    pub const fn key_header(self) -> Option<&'static str> {
        match self {
            Self::Sonarr | Self::Radarr | Self::Lidarr | Self::Tautulli => Some("X-Api-Key"),
            Self::Jellyfin => Some("X-Emby-Token"),
            Self::Qbittorrent => None,
        }
    }
}

/// How the broker authenticates to one upstream.
#[derive(Clone, Debug)]
pub enum Auth {
    /// A static API key sent in the service's key header.
    Key(Secret),
    /// `WebUI` session credentials; the broker logs in and holds the cookie.
    Session { username: String, password: Secret },
}

#[derive(Clone, Debug)]
pub struct Upstream {
    pub base_url: String,
    pub auth: Auth,
}

/// Fully loaded settings.
#[derive(Clone, Debug)]
pub struct Settings {
    pub bearer_token: Secret,
    /// Indexed by `Service`; read through [`Settings::upstream`].
    pub upstreams: [Option<Upstream>; 6],
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub bind_host: String,
    pub port: u16,
    /// Total deadline for one upstream exchange.
    pub timeout: Duration,
    pub max_response_bytes: usize,
    pub enable_requests: bool,
    pub enable_deletes: bool,
    pub enable_reseeds: bool,
    /// Exact qBittorrent save paths a reseed may target; empty unless enabled.
    pub reseed_save_paths: Vec<String>,
    /// Lifetime of a delete confirmation.
    pub confirmation_ttl: Duration,
    /// How often a reseed polls qBittorrent while a recheck runs.
    pub reseed_poll_interval: Duration,
    /// Total time one reseed call may wait for the torrent and its recheck;
    /// a longer check reports `checking` and resumes on replay.
    pub reseed_deadline: Duration,
}

type Env = HashMap<String, String>;

#[derive(Clone, Copy)]
enum Exact {
    Endpoint,
    Host,
    Origin,
}

fn required<'a>(env: &'a Env, name: &str) -> Result<&'a str> {
    match env.get(name).map(|value| value.trim()) {
        Some(value) if !value.is_empty() => Ok(value),
        _ => bail!("missing required configuration: {name}"),
    }
}

/// Read the secret file named by a variable, never logging it.
fn secret(env: &Env, name: &str) -> Result<Secret> {
    let path = required(env, name)?;
    let Some(file) = fs::metadata(path).ok().filter(fs::Metadata::is_file) else {
        bail!("{name} must name a regular secret file");
    };
    if file.permissions().mode() & 0o007 != 0 {
        bail!("{name} must not be world-readable");
    }
    let Ok(content) = fs::read_to_string(path) else { bail!("unable to read {name}") };
    let value = content.trim();
    if value.is_empty() || value.chars().count() > 4096 {
        bail!("{name} is empty or too large");
    }
    Ok(Secret::new(value))
}

/// Validate one exact URL or authority: no wildcards, credentials, query,
/// fragment, or bad port; only an endpoint may carry a path.
fn exact(name: &str, value: &str, kind: Exact) -> Result<String> {
    let (scheme, rest) = match value.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, value),
    };
    let (expected, scheme_fits, path_fits) = match kind {
        Exact::Endpoint => ("http(s) URL", scheme.is_some(), true),
        Exact::Host => ("Host authority", scheme.is_none(), !rest.contains('/')),
        Exact::Origin => ("Origin", scheme.is_some(), !rest.contains('/')),
    };
    let well_formed = scheme.is_none_or(|scheme| matches!(scheme, "http" | "https"))
        && !value.contains(|character: char| character == '*' || character.is_whitespace())
        && Url::parse(&format!("http://{rest}")).is_ok_and(|url| {
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none()
        });
    if !(scheme_fits && path_fits && well_formed) {
        bail!("invalid {name}: expected an exact {expected}");
    }
    Ok(value.trim_end_matches('/').to_owned())
}

/// Parse an explicit boolean; never guess on typos.
fn flag(env: &Env, name: &str) -> Result<bool> {
    match env.get(name).map_or("false", |raw| raw.trim()) {
        raw if raw.eq_ignore_ascii_case("true") => Ok(true),
        raw if raw.eq_ignore_ascii_case("false") => Ok(false),
        _ => bail!("{name} must be true or false"),
    }
}

fn number<N>(env: &Env, name: &str, default: N, low: N, high: N) -> Result<N>
where
    N: Copy + FromStr + PartialOrd + fmt::Display,
{
    let value = match env.get(name).map(|raw| raw.trim().parse()) {
        Some(Ok(value)) => value,
        Some(Err(_)) => bail!("{name} must be a number"),
        None => default,
    };
    if !(low..=high).contains(&value) {
        bail!("{name} must be between {low} and {high}");
    }
    Ok(value)
}

fn csv(env: &Env, name: &str, default: Option<String>, kind: Exact) -> Result<Vec<String>> {
    let raw = match (env.get(name), default) {
        (None, Some(default)) => default,
        (raw, _) => raw.cloned().unwrap_or_default(),
    };
    let values: Vec<&str> =
        raw.split(',').map(str::trim).filter(|value| !value.is_empty()).collect();
    if values.is_empty() {
        bail!("{name} must contain at least one value");
    }
    values.iter().map(|value| exact(name, value, kind)).collect()
}

/// Exact absolute directories without dot segments, wildcards, or control
/// characters; the filesystem root is refused because it allows everything.
fn save_paths(env: &Env, enabled: bool) -> Result<Vec<String>> {
    const NAME: &str = "QBITTORRENT_RESEED_SAVE_PATHS";
    let raw = env.get(NAME).map_or("", |raw| raw.trim());
    if !enabled {
        if !raw.is_empty() {
            bail!("{NAME} requires MEDIA_BROKER_ENABLE_RESEEDS=true");
        }
        return Ok(Vec::new());
    }
    let paths: Vec<&str> = raw.split(',').map(str::trim).filter(|path| !path.is_empty()).collect();
    if paths.is_empty() {
        bail!("{NAME} must contain at least one value when reseeds are enabled");
    }
    let exact_path = |path: &&str| {
        let segments = path.trim_end_matches('/').split('/').skip(1);
        path.starts_with('/')
            && path.trim_end_matches('/').len() > 1
            && !path.contains(|character: char| character == '*' || character.is_control())
            && segments.clone().all(|segment| !matches!(segment, "" | "." | ".."))
    };
    if let Some(path) = paths.iter().find(|path| !exact_path(path)) {
        bail!("invalid {NAME}: {path} is not an exact absolute directory");
    }
    Ok(paths.iter().map(|path| path.trim_end_matches('/').to_owned()).collect())
}

/// Load one optional upstream; a credential without a URL is an error, and
/// inline credentials are forbidden.
fn upstream(env: &Env, service: Service) -> Result<Option<Upstream>> {
    let prefix = service.name().to_uppercase();
    let url_name = format!("{prefix}_URL");
    let url = env.get(&url_name).map_or("", |value| value.trim());
    if service == Service::Qbittorrent {
        if env.get("QBITTORRENT_PASSWORD").is_some_and(|value| !value.is_empty()) {
            bail!("the qBittorrent password must be set with QBITTORRENT_PASSWORD_FILE");
        }
        if url.is_empty() {
            let set = |name: &str| env.get(name).is_some_and(|value| !value.trim().is_empty());
            if set("QBITTORRENT_USERNAME") || set("QBITTORRENT_PASSWORD_FILE") {
                bail!("QBITTORRENT_URL is required when its credentials are set");
            }
            return Ok(None);
        }
        return Ok(Some(Upstream {
            base_url: exact(&url_name, url, Exact::Endpoint)?,
            auth: Auth::Session {
                username: required(env, "QBITTORRENT_USERNAME")?.to_owned(),
                password: secret(env, "QBITTORRENT_PASSWORD_FILE")?,
            },
        }));
    }
    if env.get(&format!("{prefix}_API_KEY")).is_some_and(|key| !key.is_empty()) {
        bail!("upstream API keys must be configured with *_API_KEY_FILE");
    }
    let key_file = format!("{prefix}_API_KEY_FILE");
    if url.is_empty() {
        if env.get(&key_file).is_some_and(|value| !value.trim().is_empty()) {
            bail!("{url_name} is required when {key_file} is set");
        }
        return Ok(None);
    }
    Ok(Some(Upstream {
        base_url: exact(&url_name, url, Exact::Endpoint)?,
        auth: Auth::Key(secret(env, &key_file)?),
    }))
}

impl Settings {
    #[must_use]
    pub fn upstream(&self, service: Service) -> Option<&Upstream> {
        self.upstreams[service as usize].as_ref()
    }

    /// Load all values from the given environment and the secret files it names.
    ///
    /// # Errors
    /// Returns the first absent or unsafe value, naming its variable.
    pub fn load(env: &Env) -> Result<Self> {
        let bind_host = env.get("MEDIA_BROKER_BIND_HOST").map_or("127.0.0.1", |host| host.trim());
        let public = bind_host == "0.0.0.0";
        if !public && !matches!(bind_host, "127.0.0.1" | "::1" | "localhost") {
            bail!("MEDIA_BROKER_BIND_HOST must be loopback or 0.0.0.0");
        }
        if flag(env, "MEDIA_BROKER_ALLOW_PUBLIC_BIND")? != public {
            bail!("MEDIA_BROKER_ALLOW_PUBLIC_BIND=true is required for 0.0.0.0 only");
        }
        let port = number(env, "MEDIA_BROKER_PORT", 8000, 1, u32::from(u16::MAX))?;
        // Loopback binding defaults to its own exact authority; public binding has
        // no safe implicit authority, so both allow-lists must be supplied.
        let authority = match bind_host {
            _ if public => None,
            host if host.contains(':') => Some(format!("[{host}]:{port}")),
            host => Some(format!("{host}:{port}")),
        };
        let bearer_token = secret(env, "MEDIA_BROKER_TOKEN_FILE")?;
        let token = bearer_token.expose();
        let url_safe = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-';
        if !(32..=256).contains(&token.len()) || !token.bytes().all(url_safe) {
            bail!("MEDIA_BROKER_TOKEN_FILE must contain 32-256 URL-safe characters");
        }
        let timeout = number(env, "MEDIA_BROKER_TIMEOUT_SECONDS", 10.0, 0.1, 60.0)?;
        let [sonarr, radarr, lidarr, tautulli, jellyfin, qbittorrent] =
            Service::ALL.map(|service| upstream(env, service));
        let upstreams = [sonarr?, radarr?, lidarr?, tautulli?, jellyfin?, qbittorrent?];
        if upstreams.iter().all(Option::is_none) {
            bail!("at least one upstream must be configured");
        }
        let origin = authority.as_ref().map(|authority| format!("http://{authority}"));
        let enable_reseeds = flag(env, "MEDIA_BROKER_ENABLE_RESEEDS")?;
        if enable_reseeds && upstreams[Service::Qbittorrent as usize].is_none() {
            bail!("MEDIA_BROKER_ENABLE_RESEEDS=true requires QBITTORRENT_URL");
        }
        Ok(Self {
            bearer_token,
            upstreams,
            allowed_hosts: csv(env, "MEDIA_BROKER_ALLOWED_HOSTS", authority, Exact::Host)?,
            allowed_origins: csv(env, "MEDIA_BROKER_ALLOWED_ORIGINS", origin, Exact::Origin)?,
            bind_host: bind_host.to_owned(),
            port: u16::try_from(port).unwrap_or(u16::MAX), // range-checked above
            timeout: Duration::from_secs_f64(timeout),
            max_response_bytes: number(
                env,
                "MEDIA_BROKER_MAX_RESPONSE_BYTES",
                2_000_000,
                1_000,
                50_000_000,
            )?,
            enable_requests: flag(env, "MEDIA_BROKER_ENABLE_REQUESTS")?,
            enable_deletes: flag(env, "MEDIA_BROKER_ENABLE_DELETES")?,
            enable_reseeds,
            reseed_save_paths: save_paths(env, enable_reseeds)?,
            confirmation_ttl: Duration::from_secs(300),
            reseed_poll_interval: Duration::from_secs(1),
            reseed_deadline: Duration::from_secs(45),
        })
    }
}
