//! # spn tet server
//!
//! A lightweight utility for development and testing.
//!
//! ## Usage:
//!   1) Run:
//!        WWW_PORTS="8080 8081" WWW_HOST=0.0.0.0 cargo run
//!   2) Try the endpoints:
//!        curl -i http://localhost:8080/                 # root page
//!        curl -i http://localhost:8080/hello            # hello page
//!        curl -i http://localhost:8080/sleep/3          # wait 3s
//!        curl -i http://localhost:8080/xsleep/3/5/2     # wait before/after/during stream
//!        curl -N -v http://localhost:8080/xsleep/1/2/3  # streaming test (with ts)
//!        curl -i http://localhost:8080/large/1048576    # 1MB random data
//!        curl -i http://localhost:8080/error/503        # forced status code
//!        curl -i http://localhost:8080/not-found        # 404 error
//!        curl -i http://localhost:8080/close            # signal connection close
//!
//! ## Features:
//!   - Asynchronous, high-performance HTTP server using Axum and Tokio.
//!   - Lock-free concurrency tracking using Atomic integers (no deadlocks).
//!   - Listen on multiple ports concurrently using lightweight Tokio tasks.
//!   - All pages show: request start time, end time, page path, HTTP status, and elapsed milliseconds.
//!   - Real-time tracking of active requests (concurrency).
//!
//! ## Endpoints:
//!   /                       -> "hello root"
//!   /hello                  -> "hello world"
//!   /sleep/<num>            -> wait <num> seconds (non-blocking)
//!   /xsleep/<x>/<y>/<z>     -> wait x sec before headers, y sec after headers, z sec during body (streaming)
//!   /large/<n>              -> return n bytes of random string (up to 1GB, streaming)
//!   /error/<code>           -> return forced HTTP status <code>
//!   /close                  -> return response with 'Connection: close' header
//!   /*                      -> return 404 Not Found (fallback)
//!
//! ## Environment Variables:
//!   WWW_PORTS: Comma or space separated ports (default: 8080)
//!   WWW_HOST:  Bind address (default: 0.0.0.0)

use axum::{
    body::{Body, Bytes},
    extract::{Path, Request, State},
    http::{header, HeaderName, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use chrono::Local;
use clap::Parser;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, env = "WWW_PORTS", default_value = "8080")]
    ports: String,
    #[arg(long, env = "WWW_HOST", default_value = "0.0.0.0")]
    host: String,
}

struct AppState {
    active_requests: AtomicUsize,
}

#[derive(Clone)]
struct RequestInfo {
    start_dt: String,
    start_inst: Instant,
    client_addr: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let ports: Vec<u16> = args
        .ports
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().expect("Invalid port number"))
        .collect();

    let state = Arc::new(AppState {
        active_requests: AtomicUsize::new(0),
    });

    let app = Router::new()
        .route("/", get(root_handler))
        .route("/hello", get(hello_handler))
        .route("/sleep/{n}", get(sleep_handler))
        .route("/xsleep/{x}/{y}/{z}", get(xsleep_handler))
        .route("/large/{n}", get(large_handler))
        .route("/error/{code}", get(error_handler))
        .route("/close", get(close_handler))
        .fallback(fallback_handler)
        .layer(middleware::from_fn_with_state(state.clone(), logging_middleware))
        .with_state(state);

    let mut join_handles = vec![];
    for port in ports {
        let addr: SocketAddr = format!("{}:{}", args.host, port).parse().expect("Invalid address");
        let app_instance = app.clone();
        println!("Starting server on {}", addr);
        
        let handle = tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
            axum::serve(listener, app_instance.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });
        join_handles.push(handle);
    }

    println!("Endpoints: '/', '/hello', '/sleep/<num>', '/xsleep/<a>/<b>/<c>', '/large/<n>', '/error/<code>'");
    println!("Press Ctrl+C to stop.");

    // Wait for all servers (or Ctrl+C)
    tokio::signal::ctrl_c().await.unwrap();
    println!("\nShutting down...");
}

async fn logging_middleware(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    let start_inst = Instant::now();
    let start_dt = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
    let method = req.method().clone();
    let path = req.uri().to_string();
    let addr = req
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let info = RequestInfo {
        start_dt: start_dt.clone(),
        start_inst,
        client_addr: addr.clone(),
    };
    req.extensions_mut().insert(info);

    let active = state.active_requests.fetch_add(1, Ordering::SeqCst) + 1;
    println!("[{}] START {} {} from {} (threads: {})", start_dt, method, path, addr, active);

    let response = next.run(req).await;

    let elapsed = start_inst.elapsed().as_secs_f64() * 1000.0;
    let end_dt = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
    let status = response.status();
    let active = state.active_requests.fetch_sub(1, Ordering::SeqCst) - 1;

    println!("[{}] END   {} {} status={} ({:.1}ms) (threads: {})", 
        end_dt, method, path, status.as_u16(), elapsed, active);

    response
}

fn render_page(message: &str, req: &Request, status: StatusCode, extra_headers: &[(HeaderName, &str)]) -> String {
    let info = req.extensions().get::<RequestInfo>().unwrap();
    let end_dt = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f%:z").to_string();
    let elapsed_ms = info.start_inst.elapsed().as_secs_f64() * 1000.0;

    let mut body = format!(
        "message: {}\npage: {}\nquery_string: {}\nclient_host: {}\nstatus: {} {}\nstart: {}\nend: {}\nelapsed_ms: {:.1}\n",
        message,
        req.uri().path(),
        req.uri().query().unwrap_or(""),
        info.client_addr,
        status.as_u16(),
        status.canonical_reason().unwrap_or(""),
        info.start_dt,
        end_dt,
        elapsed_ms
    );

    body.push_str("\n--- Request Headers ---\n");
    for (name, value) in req.headers() {
        body.push_str(&format!("{}: {}\n", name, value.to_str().unwrap_or("")));
    }

    body.push_str("\n--- Response Headers ---\n");
    for (name, value) in extra_headers {
        body.push_str(&format!("{}: {}\n", name.as_str(), value));
    }

    body
}

async fn root_handler(req: Request) -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
    let body = render_page("hello root (spn test server)", &req, StatusCode::OK, &headers);
    (StatusCode::OK, headers, body)
}

async fn hello_handler(req: Request) -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
    let body = render_page("hello world", &req, StatusCode::OK, &headers);
    (StatusCode::OK, headers, body)
}

async fn sleep_handler(Path(n): Path<u64>, req: Request) -> impl IntoResponse {
    tokio::time::sleep(Duration::from_secs(n)).await;
    let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
    let body = render_page(&format!("slept {} seconds", n), &req, StatusCode::OK, &headers);
    (StatusCode::OK, headers, body)
}

async fn xsleep_handler(Path((x, y, z)): Path<(u64, u64, u64)>) -> impl IntoResponse {
    tokio::time::sleep(Duration::from_secs(x)).await;
    let stream = async_stream::stream! {
        yield Ok::<_, std::io::Error>(Bytes::from("<html><body>"));
        
        tokio::time::sleep(Duration::from_secs(y)).await;
        yield Ok::<_, std::io::Error>(Bytes::from(format!("Slept {} seconds before headers<br>", x)));
        
        tokio::time::sleep(Duration::from_secs(z)).await;
        yield Ok::<_, std::io::Error>(Bytes::from(format!("Slept {} seconds after headers and {} seconds during body", y, z)));
        
        yield Ok::<_, std::io::Error>(Bytes::from("</body></html>"));
    };

    Response::builder()
        .header(header::CONTENT_TYPE, "text/html")
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn large_handler(Path(n): Path<usize>, req: Request) -> Response {
    let max_size = 1024 * 1024 * 1024; // 1GB
    if n > max_size {
        let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
        let body = render_page(&format!("Error: Requested size {} exceeds 1GB limit", n), &req, StatusCode::BAD_REQUEST, &headers);
        return (StatusCode::BAD_REQUEST, headers, body).into_response();
    }

    let alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".as_bytes();
    let chunk_size = 1024 * 1024;
    let sample_block: Vec<u8> = (0..std::cmp::min(n, chunk_size))
        .map(|_| alphabet[rand::random::<usize>() % alphabet.len()])
        .collect();
    let sample_block = Bytes::from(sample_block);

    let stream = async_stream::stream! {
        let mut remaining = n;
        while remaining > 0 {
            let current_batch = std::cmp::min(remaining, sample_block.len());
            yield Ok::<_, std::io::Error>(sample_block.slice(0..current_batch));
            remaining -= current_batch;
        }
    };

    Response::builder()
        .header(header::CONTENT_TYPE, "text/plain; charset=UTF-8")
        .header(header::CONTENT_LENGTH, n.to_string())
        .body(Body::from_stream(stream))
        .unwrap()
}

async fn error_handler(Path(code): Path<u16>, req: Request) -> impl IntoResponse {
    let status = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST);
    let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
    let body = render_page(&format!("forced error {}", status.as_u16()), &req, status, &headers);
    (status, headers, body)
}

async fn close_handler(req: Request) -> impl IntoResponse {
    let headers = [
        (header::CONTENT_TYPE, "text/plain; charset=UTF-8"),
        (header::CONNECTION, "close"),
    ];
    let body = render_page("connection close requested", &req, StatusCode::OK, &headers);
    (StatusCode::OK, headers, body)
}

async fn fallback_handler(req: Request) -> impl IntoResponse {
    let headers = [(header::CONTENT_TYPE, "text/plain; charset=UTF-8")];
    let body = render_page(&format!("error 404"), &req, StatusCode::NOT_FOUND, &headers);
    (StatusCode::NOT_FOUND, headers, body)
}
