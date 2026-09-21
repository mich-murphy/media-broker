//! Serve the media broker until SIGTERM or ctrl-c.
//!
//! No logger is installed: upstream URLs must never be logged.

use std::{collections::HashMap, error::Error, process::ExitCode};

use media_broker::{config::Settings, server};
use tokio::signal::unix::{SignalKind, signal};

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
    match serve().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("media-broker: {error}");
            ExitCode::FAILURE
        }
    }
}
