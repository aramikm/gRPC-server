use std::sync::Arc;

use opentest::pb::kv_service_server::KvServiceServer;
use opentest::{Config, Database, KvServiceImpl, FILE_DESCRIPTOR_SET};
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.server.log_level.as_str()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let db = Arc::new(Database::open(&config.db)?);
    let service = KvServiceImpl::new(db, &config.server);

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let addr = config.server.address.parse()?;
    tracing::info!(%addr, "starting gRPC server");

    Server::builder()
        .add_service(reflection)
        .add_service(KvServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown_signal())
        .await?;

    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    tracing::info!("shutdown signal received");
}
