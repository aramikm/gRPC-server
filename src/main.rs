use std::sync::Arc;
use std::time::Duration;

use grpcserver::pb::kv_service_server::KvServiceServer;
use grpcserver::{Config, KvServiceImpl, StorageManager, FILE_DESCRIPTOR_SET};
use tonic::transport::Server;
use tracing_subscriber::filter::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.server.log_level.as_str()));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let storage: Arc<dyn grpcserver::Storage> = Arc::new(StorageManager::new(&config).await?);
    let service = KvServiceImpl::new(storage, &config.server);

    tracing::info!(
        "Server started on {} with Kafka enabled={}",
        config.server.address,
        config.kafka.enabled
    );

    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build_v1()?;

    let addr = config.server.address.parse()?;
    tracing::info!(%addr, "starting gRPC server");

    // Wait for shutdown signal
    let shutdown_future = shutdown_signal();

    // Build server with graceful shutdown - this drains in-flight requests
    let server_future = Server::builder()
        .add_service(reflection)
        .add_service(KvServiceServer::new(service))
        .serve_with_shutdown(addr, shutdown_future);

    server_future.await?;

    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let shutdown_timeout = Duration::from_secs(5);

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

    tracing::info!(
        "Shutdown signal received, draining in-flight requests for {}s...",
        shutdown_timeout.as_secs()
    );
    tokio::time::sleep(shutdown_timeout).await;
    tracing::warn!("Shutdown timeout reached, forcing termination");
}
