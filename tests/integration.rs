use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use grpcserver::{
    pb::{
        kv_service_client::KvServiceClient, kv_service_server::KvServiceServer, DeleteRequest,
        GetRequest, HealthCheckRequest, ListRequest, PutRequest, StatsRequest,
    },
    Database, DatabaseConfig, KvServiceImpl, ServerConfig,
};
use tempfile::TempDir;
use tokio::time::sleep;
use tonic::transport::{Channel, Server};

/// Spawn an in-process server bound to an ephemeral port. The temp dir backing
/// RocksDB is moved into the server task so that the database is dropped (and
/// flushed) before the directory is removed.
async fn spawn_server() -> KvServiceClient<Channel> {
    spawn_server_with(ServerConfig::default()).await
}

async fn spawn_server_with(server_config: ServerConfig) -> KvServiceClient<Channel> {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("rocksdb").to_string_lossy().into_owned();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    tokio::spawn(async move {
        let db_cfg = DatabaseConfig {
            path,
            ..Default::default()
        };
        let db = Arc::new(Database::open(&db_cfg).unwrap());
        let svc = KvServiceImpl::new(db, &server_config);

        let _ = Server::builder()
            .add_service(KvServiceServer::new(svc))
            .serve(addr)
            .await;

        // Hold `tmp` until the server stops so its files outlive the RocksDB
        // handle. `serve` only returns on shutdown / task cancel; in either
        // case the captured `tmp` drops after the service drops.
        drop(tmp);
    });

    let endpoint = format!("http://{addr}");
    for _ in 0..100 {
        if let Ok(c) = KvServiceClient::connect(endpoint.clone()).await {
            return c;
        }
        sleep(Duration::from_millis(20)).await;
    }
    panic!("server did not start at {endpoint}");
}

fn put_req(ns: &str, id: &str, data: &[u8]) -> PutRequest {
    PutRequest {
        id: id.into(),
        namespace: ns.into(),
        data: data.to_vec(),
    }
}

#[tokio::test]
async fn put_get_roundtrip() {
    let mut c = spawn_server().await;

    let entry = c
        .put(put_req("ns", "k1", b"hello"))
        .await
        .unwrap()
        .into_inner()
        .entry
        .expect("entry");
    assert_eq!(entry.id, "k1");
    assert_eq!(entry.namespace, "ns");
    assert_eq!(entry.data, b"hello");
    // sha256("hello")
    assert_eq!(
        entry.checksum,
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    );

    let got = c
        .get(GetRequest {
            id: "k1".into(),
            namespace: "ns".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(got.found);
    assert_eq!(got.entry.unwrap().data, b"hello");
}

#[tokio::test]
async fn get_missing_returns_not_found_flag() {
    let mut c = spawn_server().await;
    let got = c
        .get(GetRequest {
            id: "nope".into(),
            namespace: "ns".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!got.found);
    assert!(got.entry.is_none());
}

#[tokio::test]
async fn put_preserves_created_at_on_overwrite() {
    let mut c = spawn_server().await;
    let first = c
        .put(put_req("ns", "k1", b"v1"))
        .await
        .unwrap()
        .into_inner()
        .entry
        .unwrap();

    sleep(Duration::from_millis(5)).await;

    let second = c
        .put(put_req("ns", "k1", b"v2"))
        .await
        .unwrap()
        .into_inner()
        .entry
        .unwrap();

    assert_eq!(first.created_at_unix_ms, second.created_at_unix_ms);
    assert!(second.updated_at_unix_ms >= first.updated_at_unix_ms);
    assert_eq!(second.data, b"v2");
}

#[tokio::test]
async fn delete_reports_existed() {
    let mut c = spawn_server().await;
    c.put(put_req("ns", "k1", b"x")).await.unwrap();

    let r1 = c
        .delete(DeleteRequest {
            id: "k1".into(),
            namespace: "ns".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(r1.deleted);

    let r2 = c
        .delete(DeleteRequest {
            id: "k1".into(),
            namespace: "ns".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!r2.deleted);

    let got = c
        .get(GetRequest {
            id: "k1".into(),
            namespace: "ns".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!got.found);
}

#[tokio::test]
async fn list_paginates_across_all_entries_in_order() {
    let mut c = spawn_server().await;

    for i in 0..25 {
        c.put(put_req(
            "ns",
            &format!("k{i:03}"),
            format!("v{i}").as_bytes(),
        ))
        .await
        .unwrap();
    }

    let mut all = Vec::new();
    let mut token = String::new();
    let mut pages = 0;
    loop {
        let resp = c
            .list(ListRequest {
                namespace: "ns".into(),
                page_size: 10,
                page_token: token.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        pages += 1;
        all.extend(resp.entries);
        if resp.next_page_token.is_empty() {
            break;
        }
        token = resp.next_page_token;
        assert!(pages < 10, "pagination did not terminate");
    }

    assert_eq!(all.len(), 25);
    assert_eq!(pages, 3); // 10 + 10 + 5
    for (i, e) in all.iter().enumerate() {
        assert_eq!(e.id, format!("k{i:03}"));
    }
}

#[tokio::test]
async fn list_isolates_namespaces() {
    let mut c = spawn_server().await;
    c.put(put_req("a", "k", b"in-a")).await.unwrap();
    c.put(put_req("b", "k", b"in-b")).await.unwrap();

    let a = c
        .list(ListRequest {
            namespace: "a".into(),
            page_size: 100,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(a.entries.len(), 1);
    assert_eq!(a.entries[0].data, b"in-a");

    let b = c
        .list(ListRequest {
            namespace: "b".into(),
            page_size: 100,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(b.entries.len(), 1);
    assert_eq!(b.entries[0].data, b"in-b");
}

#[tokio::test]
async fn list_applies_default_and_max_page_size() {
    let cfg = ServerConfig {
        default_page_size: 3,
        max_page_size: 5,
        ..Default::default()
    };
    let mut c = spawn_server_with(cfg).await;
    for i in 0..10 {
        c.put(put_req("ns", &format!("k{i:02}"), b"v"))
            .await
            .unwrap();
    }

    // page_size = 0 → default (3)
    let r = c
        .list(ListRequest {
            namespace: "ns".into(),
            page_size: 0,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.entries.len(), 3);

    // page_size > max → capped at 5
    let r = c
        .list(ListRequest {
            namespace: "ns".into(),
            page_size: 1000,
            page_token: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(r.entries.len(), 5);
}

#[tokio::test]
async fn invalid_namespace_or_id_is_rejected() {
    let mut c = spawn_server().await;

    let err = c.put(put_req("", "k", b"v")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = c.put(put_req("ns", "", b"v")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = c.put(put_req("a\0b", "k", b"v")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_rejects_malformed_page_token() {
    let mut c = spawn_server().await;
    let err = c
        .list(ListRequest {
            namespace: "ns".into(),
            page_size: 10,
            page_token: "!!not-base64!!".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn list_rejects_cross_namespace_page_token() {
    let mut c = spawn_server().await;

    // Token from a key in a different namespace.
    let foreign = b"otherns\0key";
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(foreign);

    let err = c
        .list(ListRequest {
            namespace: "myns".into(),
            page_size: 10,
            page_token: token,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn health_check_reports_serving() {
    let mut c = spawn_server().await;
    let r = c
        .health_check(HealthCheckRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        r.status,
        grpcserver::pb::health_check_response::ServingStatus::Serving as i32
    );
}

#[tokio::test]
async fn stats_count_operations() {
    let mut c = spawn_server().await;

    c.put(put_req("ns", "k", b"v")).await.unwrap();
    c.put(put_req("ns", "k", b"v2")).await.unwrap(); // overwrite (read + write)
    c.get(GetRequest {
        id: "k".into(),
        namespace: "ns".into(),
    })
    .await
    .unwrap();

    let s = c.get_stats(StatsRequest {}).await.unwrap().into_inner();
    assert!(s.writes_total >= 2);
    assert!(s.reads_total >= 3); // 2 from put (one is fresh, one is overwrite), 1 explicit get
}

#[tokio::test]
async fn concurrent_puts_all_persist() {
    let mut c = spawn_server().await;

    let mut handles = Vec::new();
    for i in 0..50 {
        let mut cc = c.clone();
        handles.push(tokio::spawn(async move {
            cc.put(put_req("ns", &format!("k{i:03}"), b"v"))
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let mut all = Vec::new();
    let mut token = String::new();
    loop {
        let r = c
            .list(ListRequest {
                namespace: "ns".into(),
                page_size: 100,
                page_token: token,
            })
            .await
            .unwrap()
            .into_inner();
        all.extend(r.entries);
        if r.next_page_token.is_empty() {
            break;
        }
        token = r.next_page_token;
    }
    assert_eq!(all.len(), 50);
}
