//! Serve the media broker until SIGTERM or ctrl-c.
//!
//! No logger is installed: upstream URLs must never be logged.

use std::{collections::HashMap, error::Error, process::ExitCode, time::Duration};

use media_broker::{config::Settings, server};
use tokio::signal::unix::{SignalKind, signal};

/// Probe the local health route without configuration or credentials; the
/// container HEALTHCHECK runs it because the distroless image has no shell.
async fn healthcheck() -> bool {
    let port = std::env::var("MEDIA_BROKER_PORT")
        .ok()
        .and_then(|port| port.parse::<u16>().ok())
        .unwrap_or(8000);
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(Duration::from_secs(4))
        .build();
    let Ok(client) = client else { return false };
    let url = format!("http://127.0.0.1:{port}/health");
    matches!(client.get(url).send().await, Ok(response) if response.status().is_success())
}

async fn serve() -> Result<(), Box<dyn Error>> {
    let env: HashMap<String, String> = std::env::vars_os()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    let settings = Settings::load(&env)?;
    let address = (settings.bind_host.clone(), settings.port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    let mut terminate = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    };
    axum::serve(listener, server::router(settings)).with_graceful_shutdown(shutdown).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        return if healthcheck().await { ExitCode::SUCCESS } else { ExitCode::FAILURE };
    }
    match serve().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("media-broker: {error}");
            ExitCode::FAILURE
        }
    }
}
