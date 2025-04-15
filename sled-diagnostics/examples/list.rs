use sled_diagnostics::logs;

#[tokio::main]
async fn main() {
    let drain = slog::Discard;
    let log = slog::Logger::root(drain, slog::o!());

    let handle = logs::LogsHandle::new(log);
    handle.cleanup_snapshots();

    let zones = logs::LogsHandle::get_zones().unwrap();

    for zone in zones {
        // let zone = "oxz_switch";
        let mut output =
            std::fs::File::create(format!("/tmp/{zone}.zip")).unwrap();
        let handle_clone = handle.clone();
        tokio::task::spawn_blocking(move || {
            handle_clone.get_zone_logs(&zone, &mut output).unwrap()
        })
        .await
        .unwrap();
    }
}
