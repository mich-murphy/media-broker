//! Configuration loading: exact allow-lists, private secret files, explicit gates.

use std::{collections::HashMap, fs, os::unix::fs::PermissionsExt, path::Path};

use media_broker::config::{Service, Settings};
use tempfile::TempDir;

const TOKEN: &str = "broker-secret-token-0123456789abcdef";

fn secret_file(dir: &Path, name: &str, content: &str, mode: u32) -> String {
    let path = dir.join(name);
    fs::write(&path, content).unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
    path.to_str().unwrap().to_owned()
}

/// A complete, valid environment; overrides replace or (with `None`) unset values.
struct Env {
    dir: TempDir,
    vars: HashMap<String, String>,
}

impl Env {
    fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let token = secret_file(dir.path(), "token", TOKEN, 0o600);
        let mut vars = HashMap::from([("MEDIA_BROKER_TOKEN_FILE".to_owned(), token)]);
        for service in Service::ALL {
            let (name, env) = (service.name(), service.name().to_uppercase());
            let key = secret_file(dir.path(), name, &format!("{name}-key"), 0o600);
            vars.insert(format!("{env}_URL"), format!("http://{name}.test"));
            vars.insert(format!("{env}_API_KEY_FILE"), key);
        }
        Self { dir, vars }
    }

    fn load(&self, overrides: &[(&str, Option<&str>)]) -> Result<Settings, String> {
        let mut vars = self.vars.clone();
        for (name, value) in overrides {
            match value {
                Some(value) => vars.insert((*name).to_owned(), (*value).to_owned()),
                None => vars.remove(*name),
            };
        }
        Settings::load(&vars).map_err(|error| error.to_string())
    }

    fn rejects(&self, overrides: &[(&str, Option<&str>)], message: &str) {
        let error = self.load(overrides).expect_err(message);
        assert!(error.contains(message), "{overrides:?}: {error}");
    }
}

#[test]
fn urls_and_authorities_must_be_exact() {
    const ENDPOINT: &str = "SONARR_URL";
    const HOST: &str = "MEDIA_BROKER_ALLOWED_HOSTS";
    const ORIGIN: &str = "MEDIA_BROKER_ALLOWED_ORIGINS";
    let cases = [
        (ENDPOINT, "https://sonarr.test", true),
        (ENDPOINT, "http://sonarr.test:8989/base/", true),
        (ENDPOINT, "https://user:pass@example.test", false),
        (ENDPOINT, "https://example.test/?x=1", false),
        (ENDPOINT, "https://example.test/#x", false),
        (ENDPOINT, "ftp://example.test", false),
        (ENDPOINT, "example.test:8989", false),
        (ENDPOINT, "http://exa mple.test", false),
        (HOST, "127.0.0.1:8000", true),
        (HOST, "[::1]:8000", true),
        (HOST, "media.example.test", true),
        (HOST, "localhost:*", false),
        (HOST, "::1", false),
        (HOST, "example.test/", false),
        (HOST, "[::1]:8000/path", false),
        (HOST, "http://example.test", false),
        (HOST, "example.test:99999", false),
        (ORIGIN, "https://media.example.test", true),
        (ORIGIN, "http://[::1]:8000", true),
        (ORIGIN, "http://localhost:*", false),
        (ORIGIN, "http://localhost/path", false),
        (ORIGIN, "localhost:8000", false),
        (ORIGIN, "http://u@localhost", false),
    ];
    let env = Env::new();
    for (name, value, accepted) in cases {
        let loaded = env.load(&[(name, Some(value))]);
        if !accepted {
            let error = loaded.expect_err(value);
            assert!(error.contains(name), "{name}={value}: {error}");
            continue;
        }
        let loaded = loaded.unwrap_or_else(|error| panic!("{name}={value}: {error}"));
        let stored = match name {
            ENDPOINT => loaded.upstream(Service::Sonarr).base_url.clone(),
            HOST => loaded.allowed_hosts[0].clone(),
            _ => loaded.allowed_origins[0].clone(),
        };
        assert_eq!(stored, value.trim_end_matches('/'), "{name}={value}");
    }
}

#[test]
fn loopback_binding_defaults_to_its_exact_authority() {
    let env = Env::new();
    for (bind_host, authority) in [(None, "127.0.0.1:8000"), (Some("::1"), "[::1]:8000")] {
        let loaded = env.load(&[("MEDIA_BROKER_BIND_HOST", bind_host)]).unwrap();
        assert_eq!(loaded.allowed_hosts, [authority]);
        assert_eq!(loaded.allowed_origins, [format!("http://{authority}")]);
    }
}

#[test]
fn secrets_are_loaded_from_files_and_never_printed() {
    let loaded = Env::new().load(&[]).unwrap();
    assert_eq!(loaded.bearer_token.expose(), TOKEN);
    assert_eq!(loaded.upstream(Service::Lidarr).api_key.expose(), "lidarr-key");
    let printed = format!("{loaded:?} {loaded:#?}");
    assert!(printed.contains("lidarr.test"), "{printed}");
    assert!(!printed.contains("lidarr-key") && !printed.contains(TOKEN), "{printed}");
}

#[test]
fn public_binding_requires_opt_in_and_explicit_exact_allow_lists() {
    let env = Env::new();
    let public = [
        ("MEDIA_BROKER_BIND_HOST", Some("0.0.0.0")),
        ("MEDIA_BROKER_ALLOWED_HOSTS", Some("media.example.test:8765")),
        ("MEDIA_BROKER_ALLOWED_ORIGINS", Some("https://media.example.test")),
    ];
    env.rejects(&public, "ALLOW_PUBLIC_BIND");
    let opted_in = [&public[..], &[("MEDIA_BROKER_ALLOW_PUBLIC_BIND", Some("true"))]].concat();
    let loaded = env.load(&opted_in).unwrap();
    assert_eq!(loaded.allowed_hosts, ["media.example.test:8765"]);
    let without_hosts = [&opted_in[..], &[("MEDIA_BROKER_ALLOWED_HOSTS", None)]].concat();
    env.rejects(&without_hosts, "ALLOWED_HOSTS");
    let wildcard = [&opted_in[..], &[("MEDIA_BROKER_ALLOWED_HOSTS", Some("x:*"))]].concat();
    env.rejects(&wildcard, "exact Host");
}

#[test]
fn unsafe_configuration_is_rejected() {
    let cases = [
        ("SONARR_API_KEY", Some("inline-secret"), "API_KEY_FILE"),
        ("JELLYFIN_API_KEY", Some("inline-secret"), "API_KEY_FILE"),
        ("RADARR_URL", None, "missing required configuration: RADARR_URL"),
        ("JELLYFIN_URL", None, "missing required configuration: JELLYFIN_URL"),
        ("RADARR_API_KEY_FILE", Some("/nonexistent/key"), "regular secret file"),
        ("MEDIA_BROKER_BIND_HOST", Some("192.168.1.5"), "loopback"),
        ("MEDIA_BROKER_ALLOW_PUBLIC_BIND", Some("true"), "ALLOW_PUBLIC_BIND"),
        ("MEDIA_BROKER_ALLOW_PUBLIC_BIND", Some("yes"), "true or false"),
        ("MEDIA_BROKER_PORT", Some("0"), "PORT must be between"),
        ("MEDIA_BROKER_PORT", Some("65536"), "PORT must be between"),
        ("MEDIA_BROKER_TIMEOUT_SECONDS", Some("soon"), "TIMEOUT_SECONDS must be a number"),
        ("MEDIA_BROKER_TIMEOUT_SECONDS", Some("61"), "TIMEOUT_SECONDS must be between"),
        ("MEDIA_BROKER_TIMEOUT_SECONDS", Some("NaN"), "TIMEOUT_SECONDS must be"),
        ("MEDIA_BROKER_MAX_RESPONSE_BYTES", Some("10"), "MAX_RESPONSE_BYTES must be between"),
        ("MEDIA_BROKER_ALLOWED_HOSTS", Some(" , "), "at least one value"),
    ];
    let env = Env::new();
    for (name, value, message) in cases {
        env.rejects(&[(name, value)], message);
    }
}

#[test]
fn bearer_token_file_must_be_private_and_well_formed() {
    let cases = [
        ("short", 0o600, "32-256"),
        ("broker secret token 0123456789abcdef", 0o600, "32-256"),
        (TOKEN, 0o644, "world-readable"),
        ("", 0o600, "empty or too large"),
    ];
    let env = Env::new();
    for (content, mode, message) in cases {
        let token = secret_file(env.dir.path(), "other-token", content, mode);
        env.rejects(&[("MEDIA_BROKER_TOKEN_FILE", Some(&token))], message);
    }
}

#[test]
fn write_gates_default_to_disabled_and_need_an_explicit_boolean() {
    let env = Env::new();
    let loaded = env.load(&[]).unwrap();
    assert!(!loaded.enable_requests && !loaded.enable_deletes);
    let loaded = env
        .load(&[
            ("MEDIA_BROKER_ENABLE_REQUESTS", Some("true")),
            ("MEDIA_BROKER_ENABLE_DELETES", Some("true")),
        ])
        .unwrap();
    assert!(loaded.enable_requests && loaded.enable_deletes);
    env.rejects(
        &[("MEDIA_BROKER_ENABLE_DELETES", Some("1"))],
        "ENABLE_DELETES must be true or false",
    );
}
