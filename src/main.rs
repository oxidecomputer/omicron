const SERVER_NWORKERS: usize = 4;
const SERVER_BACKLOG: i32 = 64;
// XXX keepalive
const SERVER_CLIENT_REQUEST_TIMEOUT_MS: u64 = 5000;
const SERVER_CLIENT_SHUTDOWN_TIMEOUT_MS: u64 = 5000;
const SERVER_WORKER_SHUTDOWN_TIMEOUT_S: u64 = 10;
const SERVER_WORKER_MAX_CONN: usize = 64;
const SERVER_WORKER_MAX_CONN_CONNECTING: usize = 64;
const SERVER_BIND_ADDRESS: &str = "127.0.0.1:12220";

mod demo;

#[actix_rt::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    // TODO
    // set hostname
    // disable signals
    let server = actix_web::HttpServer::new(|| {
        actix_web::App::new()
            .configure(demo::register_actix_demo)
    })
        .workers(SERVER_NWORKERS)
        .maxconn(SERVER_WORKER_MAX_CONN)
        .maxconnrate(SERVER_WORKER_MAX_CONN_CONNECTING)
        .backlog(SERVER_BACKLOG)
        .client_timeout(SERVER_CLIENT_REQUEST_TIMEOUT_MS)
        .client_shutdown(SERVER_CLIENT_SHUTDOWN_TIMEOUT_MS)
        .shutdown_timeout(SERVER_WORKER_SHUTDOWN_TIMEOUT_S)
        .bind(SERVER_BIND_ADDRESS)?;

    for (addr, scheme) in server.addrs_with_scheme() {
        eprintln!("listening: {}://{}", scheme, addr);
    }

    server.run().await?;

    return Ok(())
}
