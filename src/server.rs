use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use prost::Message;
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status};

use crate::config::ServerConfig;
use crate::error::Error;
use crate::pb::{
    kv_service_server::KvService, DataEntry, DeleteRequest, DeleteResponse, GetRequest,
    GetResponse, HealthCheckRequest, HealthCheckResponse, ListRequest, ListResponse, PutRequest,
    PutResponse, StatsRequest, StatsResponse,
};
use crate::storage::Storage;

/// Byte value used to separate namespace from id inside the storage key.
/// Namespace and id are required to be valid UTF-8 strings free of this byte,
/// which makes prefix scans safe across namespaces.
const KEY_SEP: u8 = 0;

pub struct KvServiceImpl {
    storage: Arc<dyn Storage>,
    default_page_size: u32,
    max_page_size: u32,
}

impl KvServiceImpl {
    pub fn new(storage: Arc<dyn Storage>, config: &ServerConfig) -> Self {
        Self {
            storage,
            default_page_size: config.default_page_size,
            max_page_size: config.max_page_size,
        }
    }

    fn make_key(namespace: &str, id: &str) -> std::result::Result<Vec<u8>, Error> {
        validate_field("namespace", namespace)?;
        validate_field("id", id)?;
        let mut k = Vec::with_capacity(namespace.len() + 1 + id.len());
        k.extend_from_slice(namespace.as_bytes());
        k.push(KEY_SEP);
        k.extend_from_slice(id.as_bytes());
        Ok(k)
    }

    fn namespace_prefix(namespace: &str) -> std::result::Result<Vec<u8>, Error> {
        validate_field("namespace", namespace)?;
        let mut p = Vec::with_capacity(namespace.len() + 1);
        p.extend_from_slice(namespace.as_bytes());
        p.push(KEY_SEP);
        Ok(p)
    }

    fn resolve_page_size(&self, requested: u32) -> usize {
        let size = if requested == 0 {
            self.default_page_size
        } else {
            requested.min(self.max_page_size)
        };
        size as usize
    }
}

fn validate_field(field: &str, value: &str) -> std::result::Result<(), Error> {
    if value.is_empty() {
        return Err(Error::InvalidArgument(format!("{field} must not be empty")));
    }
    if value.as_bytes().contains(&KEY_SEP) {
        return Err(Error::InvalidArgument(format!(
            "{field} must not contain nul byte"
        )));
    }
    Ok(())
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn checksum_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

#[tonic::async_trait]
impl KvService for KvServiceImpl {
    async fn put(&self, req: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = req.into_inner();
        let key = KvServiceImpl::make_key(&req.namespace, &req.id)?;

        let now = now_unix_ms();
        let created_at = match self.storage.get(&key).await? {
            Some(bytes) => {
                DataEntry::decode(bytes.as_slice())
                    .map_err(Error::from)?
                    .created_at_unix_ms
            }
            None => now,
        };

        let entry = DataEntry {
            id: req.id,
            namespace: req.namespace,
            checksum: checksum_hex(&req.data),
            data: req.data,
            created_at_unix_ms: created_at,
            updated_at_unix_ms: now,
        };

        let buf = entry.encode_to_vec();
        self.storage.put(&key, &buf).await?;

        Ok(Response::new(PutResponse { entry: Some(entry) }))
    }

    async fn get(&self, req: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = req.into_inner();
        let key = KvServiceImpl::make_key(&req.namespace, &req.id)?;
        match self.storage.get(&key).await? {
            Some(bytes) => {
                let entry = DataEntry::decode(bytes.as_slice()).map_err(Error::from)?;
                Ok(Response::new(GetResponse {
                    found: true,
                    entry: Some(entry),
                }))
            }
            None => Ok(Response::new(GetResponse {
                found: false,
                entry: None,
            })),
        }
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = req.into_inner();
        let key = KvServiceImpl::make_key(&req.namespace, &req.id)?;
        let deleted = self.storage.delete(&key).await?;
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn list(&self, req: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = req.into_inner();
        let prefix = KvServiceImpl::namespace_prefix(&req.namespace)?;
        let page_size = self.resolve_page_size(req.page_size);

        let start_key = if req.page_token.is_empty() {
            None
        } else {
            let decoded = URL_SAFE_NO_PAD
                .decode(req.page_token.as_bytes())
                .map_err(|_| Error::InvalidPageToken)?;
            if !decoded.starts_with(&prefix) {
                return Err(Error::InvalidPageToken.into());
            }
            Some(decoded)
        };

        // Fetch one extra so we can build the next page token without re-reading.
        let mut items = self
            .storage
            .scan_prefix(&prefix, start_key.as_deref(), page_size + 1)
            .await?;

        let next_page_token = if items.len() > page_size {
            let next = items.pop().expect("len > page_size");
            URL_SAFE_NO_PAD.encode(&next.0)
        } else {
            String::new()
        };

        let mut entries = Vec::with_capacity(items.len());
        for (_, v) in items {
            entries.push(DataEntry::decode(v.as_slice()).map_err(Error::from)?);
        }

        Ok(Response::new(ListResponse {
            entries,
            next_page_token,
        }))
    }

    async fn health_check(
        &self,
        _req: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        Ok(Response::new(HealthCheckResponse {
            status: crate::pb::health_check_response::ServingStatus::Serving as i32,
        }))
    }

    async fn get_stats(
        &self,
        _req: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        let stats = self.storage.stats();
        Ok(Response::new(StatsResponse {
            reads_total: stats.reads,
            writes_total: stats.writes,
            deletes_total: stats.deletes,
            list_operations_total: stats.lists,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_key_concatenates_with_separator() {
        let k = KvServiceImpl::make_key("ns", "id").unwrap();
        assert_eq!(k, b"ns\0id");
    }

    #[test]
    fn make_key_rejects_empty() {
        assert!(KvServiceImpl::make_key("", "id").is_err());
        assert!(KvServiceImpl::make_key("ns", "").is_err());
    }

    #[test]
    fn make_key_rejects_nul_byte() {
        assert!(KvServiceImpl::make_key("a\0b", "id").is_err());
        assert!(KvServiceImpl::make_key("ns", "a\0b").is_err());
    }

    #[test]
    fn namespace_prefix_trailing_separator() {
        let p = KvServiceImpl::namespace_prefix("ns").unwrap();
        assert_eq!(p, b"ns\0");
    }

    #[test]
    fn checksum_is_sha256_hex() {
        // sha256("hello") well-known constant
        assert_eq!(
            checksum_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert_ne!(checksum_hex(b"hello"), checksum_hex(b"world"));
    }

    #[test]
    fn resolve_page_size_applies_defaults_and_caps() {
        let cfg = ServerConfig {
            default_page_size: 50,
            max_page_size: 200,
            ..Default::default()
        };
        let tmp = tempfile::TempDir::new().unwrap();
        let db_cfg = crate::config::DatabaseConfig {
            path: tmp.path().join("rocksdb").to_string_lossy().into_owned(),
            ..Default::default()
        };
        let db = Arc::new(crate::RocksDbBackend::open(&db_cfg).unwrap());
        let svc = KvServiceImpl::new(db, &cfg);

        assert_eq!(svc.resolve_page_size(0), 50);
        assert_eq!(svc.resolve_page_size(75), 75);
        assert_eq!(svc.resolve_page_size(5000), 200);
    }
}
