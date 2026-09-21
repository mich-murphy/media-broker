//! Configuration and secret-file handling for the media broker.
//!
//! Write tools are opt-in: requests enable brokered adds, monitoring changes,
//! and search triggers, while deletes enable the two-phase destructive media
//! removal tool.

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

/// The five allow-listed upstream APIs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Service {
    Sonarr,
    Radarr,
    Lidarr,
    Tautulli,
    Jellyfin,
}

/// One row per [`Service`], in declaration order: name, API root, key header.
const SERVICES: [[&str; 3]; 5] = [
    ["sonarr", "/api/v3", "X-Api-Key"],
    ["radarr", "/api/v3", "X-Api-Key"],
    ["lidarr", "/api/v1", "X-Api-Key"],
    ["tautulli", "/api/v2", "X-Api-Key"],
    ["jellyfin", "", "X-Emby-Token"],
];

impl Service {
    pub const ALL: [Self; 5] =
        [Self::Sonarr, Self::Radarr, Self::Lidarr, Self::Tautulli, Self::Jellyfin];

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
    #[must_use]
    pub const fn key_header(self) -> &'static str {
        SERVICES[self as usize][2]
    }
}

#[derive(Clone, Debug)]
pub struct Upstream {
    pub base_url: String,
    pub api_key: Secret,
}

/// Fully loaded settings.
#[derive(Clone, Debug)]
pub struct Settings {
    pub bearer_token: Secret,
    /// Indexed by `Service`; read through [`Settings::upstream`].
    pub upstreams: [Upstream; 5],
    pub allowed_hosts: Vec<String>,
    pub allowed_origins: Vec<String>,
    pub bind_host: String,
    pub port: u16,
    /// Total deadline for one upstream exchange.
    pub timeout: Duration,
    pub max_response_bytes: usize,
    pub enable_requests: bool,
    pub enable_deletes: bool,
    /// Lifetime of a delete confirmation.
    pub confirmation_ttl: Duration,
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

/// Load one upstream's URL and secret-file key; inline keys are forbidden.
fn upstream(env: &Env, service: Service) -> Result<Upstream> {
    let prefix = service.name().to_uppercase();
    if env.get(&format!("{prefix}_API_KEY")).is_some_and(|key| !key.is_empty()) {
        bail!("upstream API keys must be configured with *_API_KEY_FILE");
    }
    let url = format!("{prefix}_URL");
    Ok(Upstream {
        base_url: exact(&url, required(env, &url)?, Exact::Endpoint)?,
        api_key: secret(env, &format!("{prefix}_API_KEY_FILE"))?,
    })
}

impl Settings {
    #[must_use]
    pub fn upstream(&self, service: Service) -> &Upstream {
        &self.upstreams[service as usize]
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
        let [sonarr, radarr, lidarr, tautulli, jellyfin] =
            Service::ALL.map(|service| upstream(env, service));
        let origin = authority.as_ref().map(|authority| format!("http://{authority}"));
        Ok(Self {
            bearer_token,
            upstreams: [sonarr?, radarr?, lidarr?, tautulli?, jellyfin?],
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
            confirmation_ttl: Duration::from_secs(300),
        })
    }
}
