/*!
 * Shared integration testing facilities
 */

use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use oxide_api_prototype::api_model::ApiIdentityMetadata;
use oxide_api_prototype::ConfigController;
use oxide_api_prototype::ConfigServerController;
use oxide_api_prototype::OxideControllerServer;
use oxide_api_prototype::ServerControllerServer;
use oxide_api_prototype::SimMode;
use slog::Logger;
use std::net::SocketAddr;
use std::path::Path;
use uuid::Uuid;

const SERVER_CONTROLLER_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";

pub struct ControlPlaneTestContext {
    pub external_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub server: OxideControllerServer,
    server_controller: ServerControllerServer,
    logctx: LogContext,
}

impl ControlPlaneTestContext {
    pub async fn teardown(self) {
        self.server.http_server_external.close();
        self.server.http_server_internal.close();
        // XXX can we (/ how do we?) wait for these things to shut down?
        // self.server.wait_for_finish().await.unwrap();
        // self.server_controller.teardown().await;
        self.server_controller.http_server.close();
        self.logctx.cleanup_successful();
    }
}

pub async fn test_setup(test_name: &str) -> ControlPlaneTestContext {
    /*
     * We load as much configuration as we can from the test suite configuration
     * file.  In practice, TestContext requires that the TCP port be 0 and that
     * if the log will go to a file then the path must be the sentinel value
     * "UNUSED".  (See TestContext::new() for details.)  Given these
     * restrictions, it may seem barely worth reading a config file at all.
     * However, users can change the logging level and local IP if they want,
     * and as we add more configuration options, we expect many of those can be
     * usefully configured (and reconfigured) for the test suite.
     */
    let config_file_path = Path::new("tests/config.test.toml");
    let config = ConfigController::from_file(config_file_path)
        .expect("failed to load config.test.toml");
    let logctx = LogContext::new(test_name, &config.log);
    let rack_id = Uuid::parse_str(RACK_UUID).unwrap();

    let server = OxideControllerServer::start(&config, &rack_id, &logctx.log)
        .await
        .unwrap();
    let testctx_external = ClientTestContext::new(
        server.http_server_external.local_addr(),
        logctx.log.new(o!("component" => "external client test context")),
    );
    let testctx_internal = ClientTestContext::new(
        server.http_server_internal.local_addr(),
        logctx.log.new(o!("component" => "internal client test context")),
    );

    /* Set up a single server controller. */
    let sc_id = Uuid::parse_str(SERVER_CONTROLLER_UUID).unwrap();
    let sc = start_server_controller(
        logctx.log.new(o!(
            "component" => "ServerControllerServer",
            "server" => sc_id.to_string(),
        )),
        server.http_server_internal.local_addr(),
        sc_id,
    )
    .await
    .unwrap();

    ControlPlaneTestContext {
        server: server,
        external_client: testctx_external,
        internal_client: testctx_internal,
        server_controller: sc,
        logctx: logctx,
    }
}

/*
 * XXX might the commonization be simpler if we provide:
 * - a common function that waits for a server to stop and produces a sane error
 * - a common function that takes two Tasks and waits for them both and produces
 *   a single unified error
 * (the first of these currently appears in the run_server() functions for both
 * controller and server_controller, and the second appears in server_controller
 * but could appear both there and below, in the test suite)
 */
pub async fn start_server_controller(
    log: Logger,
    controller_address: SocketAddr,
    id: Uuid,
) -> Result<ServerControllerServer, String> {
    let config = ConfigServerController {
        id,
        sim_mode: SimMode::Explicit,
        controller_address,
        dropshot: ConfigDropshot {
            bind_address: SocketAddr::new("127.0.0.1".parse().unwrap(), 0),
        },
        /* TODO-cleanup this is unused */
        log: ConfigLogging::StderrTerminal {
            level: ConfigLoggingLevel::Debug,
        },
    };

    ServerControllerServer::start(&config, &log).await
}

/** Returns whether the two identity metadata objects are identical. */
pub fn identity_eq(ident1: &ApiIdentityMetadata, ident2: &ApiIdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}
