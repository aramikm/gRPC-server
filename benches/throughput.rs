//! Throughput + latency benchmark for the gRPC KV service.
//!
//! Connects to an already-running opentest server, seeds a fixed dataset, then
//! drives `Put`, `Get`, and `List` workloads from a pool of concurrent gRPC
//! clients for a fixed wall-clock duration. Reports throughput (req/s) and
//! latency percentiles (mean, p50, p95, p99, p99.9, max) per workload.
//!
//! The server must be running before the benchmark starts. In another shell:
//!     cargo run --release
//!
//! Tunables (env vars; defaults in parentheses):
//!   BENCH_ENDPOINT         (http://127.0.0.1:50051)  server URL
//!   BENCH_NAMESPACE        (bench)  namespace used for all keys
//!   BENCH_DURATION_S       (10)     measurement window per workload
//!   BENCH_WARMUP_S         (2)      warmup window before measurement starts
//!   BENCH_CONCURRENCY      (32)     number of concurrent client workers
//!   BENCH_VALUE_SIZE       (256)    payload size in bytes
//!   BENCH_SEED_COUNT       (10000)  number of entries seeded before reads
//!   BENCH_LIST_PAGE_SIZE   (100)    page_size used by the LIST workload
//!
//! Note: the benchmark does not clean up after itself. For repeatable PUT and
//! LIST numbers, point at a fresh server (or set BENCH_NAMESPACE to a unique
//! value) — leftover keys in the namespace can skew LIST first-page latency.

use std::time::{Duration, Instant};

use opentest::pb::{kv_service_client::KvServiceClient, GetRequest, ListRequest, PutRequest};
use tonic::transport::Channel;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:50051";
const DEFAULT_NAMESPACE: &str = "bench";

#[derive(Clone)]
struct Args {
    endpoint: String,
    namespace: String,
    duration: Duration,
    warmup: Duration,
    concurrency: usize,
    value_size: usize,
    seed_count: usize,
    list_page_size: u32,
}

impl Args {
    fn from_env() -> Self {
        fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> T {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }
        fn env_str(name: &str, default: &str) -> String {
            std::env::var(name).unwrap_or_else(|_| default.to_string())
        }
        Self {
            endpoint: env_str("BENCH_ENDPOINT", DEFAULT_ENDPOINT),
            namespace: env_str("BENCH_NAMESPACE", DEFAULT_NAMESPACE),
            duration: Duration::from_secs(env_parse("BENCH_DURATION_S", 60u64)),
            warmup: Duration::from_secs(env_parse("BENCH_WARMUP_S", 2u64)),
            concurrency: env_parse("BENCH_CONCURRENCY", 32usize),
            value_size: env_parse("BENCH_VALUE_SIZE", 256usize),
            seed_count: env_parse("BENCH_SEED_COUNT", 10_000usize),
            list_page_size: env_parse("BENCH_LIST_PAGE_SIZE", 1000u32),
        }
    }

    fn print(&self) {
        println!("=== opentest throughput benchmark ===");
        println!("  endpoint         {}", self.endpoint);
        println!("  namespace        {}", self.namespace);
        println!("  duration         {}s", self.duration.as_secs());
        println!("  warmup           {}s", self.warmup.as_secs());
        println!("  concurrency      {}", self.concurrency);
        println!("  value_size       {} bytes", self.value_size);
        println!("  seed_count       {}", self.seed_count);
        println!("  list_page_size   {}", self.list_page_size);
        println!();
    }
}

async fn connect(endpoint: &str) -> KvServiceClient<Channel> {
    match KvServiceClient::connect(endpoint.to_string()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to connect to {endpoint}: {e}");
            eprintln!();
            eprintln!("Make sure the opentest server is running. In another shell:");
            eprintln!("    cargo run --release");
            eprintln!();
            eprintln!("Or set BENCH_ENDPOINT to the running server's URL.");
            std::process::exit(1);
        }
    }
}

async fn seed(client: &KvServiceClient<Channel>, namespace: &str, count: usize, value_size: usize) {
    use std::io::Write;
    print!("Seeding {count} entries into namespace '{namespace}'... ");
    std::io::stdout().flush().ok();
    let start = Instant::now();

    let value = vec![0xAB_u8; value_size];
    let workers = 32usize;
    let chunk = count.div_ceil(workers);
    let mut handles = Vec::with_capacity(workers);
    for w in 0..workers {
        let lo = w * chunk;
        let hi = ((w + 1) * chunk).min(count);
        if lo >= hi {
            continue;
        }
        let mut cl = client.clone();
        let v = value.clone();
        let ns = namespace.to_string();
        handles.push(tokio::spawn(async move {
            for i in lo..hi {
                cl.put(PutRequest {
                    id: format!("k{i:08}"),
                    namespace: ns.clone(),
                    data: v.clone(),
                })
                .await
                .expect("seed put");
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    println!("done in {:.2}s", start.elapsed().as_secs_f64());
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() - 1) as f64) * p / 100.0).round() as usize;
    sorted[idx]
}

fn fmt_ns(ns: u64) -> String {
    if ns < 10_000 {
        format!("{ns} ns")
    } else if ns < 10_000_000 {
        format!("{:.2} µs", ns as f64 / 1_000.0)
    } else {
        format!("{:.2} ms", ns as f64 / 1_000_000.0)
    }
}

fn report(name: &str, mut latencies_ns: Vec<u64>, duration: Duration) {
    latencies_ns.sort_unstable();
    let count = latencies_ns.len();
    let rps = if duration.as_secs_f64() > 0.0 {
        count as f64 / duration.as_secs_f64()
    } else {
        0.0
    };
    let mean = if count == 0 {
        0
    } else {
        latencies_ns.iter().sum::<u64>() / count as u64
    };
    println!("[{name}]");
    println!("  Requests       {count}");
    println!("  Duration       {:.2}s", duration.as_secs_f64());
    println!("  Throughput     {:.1} req/s", rps);
    println!("  Mean latency   {}", fmt_ns(mean));
    println!("  p50 latency    {}", fmt_ns(percentile(&latencies_ns, 50.0)));
    println!("  p95 latency    {}", fmt_ns(percentile(&latencies_ns, 95.0)));
    println!("  p99 latency    {}", fmt_ns(percentile(&latencies_ns, 99.0)));
    println!(
        "  p99.9 latency  {}",
        fmt_ns(percentile(&latencies_ns, 99.9))
    );
    println!(
        "  max latency    {}",
        fmt_ns(latencies_ns.last().copied().unwrap_or(0))
    );
    println!();
}

#[derive(Clone, Copy)]
enum Op {
    Put,
    Get,
    List,
}

async fn run_workload(client: &KvServiceClient<Channel>, args: &Args, op: Op) -> Vec<u64> {
    let test_start = Instant::now();
    let measurement_start = test_start + args.warmup;
    let test_end = measurement_start + args.duration;

    let seed_count = args.seed_count.max(1);
    let value = vec![0xCD_u8; args.value_size];

    let mut handles = Vec::with_capacity(args.concurrency);
    for worker_id in 0..args.concurrency {
        let mut cl = client.clone();
        let v = value.clone();
        let ns = args.namespace.clone();
        // For PUT, give each worker a disjoint id range above the seeded range
        // so writes are fresh inserts rather than overwrites of seeded keys.
        let put_offset = args.seed_count + worker_id.saturating_mul(10_000_000);
        let page_size = args.list_page_size;

        handles.push(tokio::spawn(async move {
            let mut latencies: Vec<u64> = Vec::with_capacity(8192);
            let mut counter: usize = 0;
            loop {
                let now = Instant::now();
                if now >= test_end {
                    break;
                }

                let req_start = Instant::now();
                match op {
                    Op::Put => {
                        let id = format!("k{:010}", put_offset + counter);
                        cl.put(PutRequest {
                            id,
                            namespace: ns.clone(),
                            data: v.clone(),
                        })
                        .await
                        .expect("put");
                    }
                    Op::Get => {
                        let id = format!("k{:08}", (counter + worker_id) % seed_count);
                        cl.get(GetRequest {
                            id,
                            namespace: ns.clone(),
                        })
                        .await
                        .expect("get");
                    }
                    Op::List => {
                        cl.list(ListRequest {
                            namespace: ns.clone(),
                            page_size,
                            page_token: String::new(),
                        })
                        .await
                        .expect("list");
                    }
                }
                let req_end = Instant::now();
                counter = counter.wrapping_add(1);
                if req_end >= measurement_start {
                    latencies.push((req_end - req_start).as_nanos() as u64);
                }
            }
            latencies
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        all.extend(h.await.unwrap());
    }
    all
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::from_env();
    args.print();

    let client = connect(&args.endpoint).await;
    seed(&client, &args.namespace, args.seed_count, args.value_size).await;
    println!();

    let phase = |name: &str| {
        println!(
            "Running {name} workload for {}s ({}s warmup + {}s measure)...",
            (args.warmup + args.duration).as_secs(),
            args.warmup.as_secs(),
            args.duration.as_secs()
        );
    };

    phase("PUT");
    let lat = run_workload(&client, &args, Op::Put).await;
    report("PUT (new keys)", lat, args.duration);

    phase("GET");
    let lat = run_workload(&client, &args, Op::Get).await;
    report("GET (seeded keys)", lat, args.duration);

    phase("LIST");
    let lat = run_workload(&client, &args, Op::List).await;
    report(
        &format!("LIST (first page, page_size={})", args.list_page_size),
        lat,
        args.duration,
    );
}
