/*!
 * Shared integration testing facilities
 */

use dropshot::test_util::ClientTestContext;
use dropshot::test_util::LogContext;
use dropshot::test_util::TestContext;
use dropshot::ConfigDropshot;
use oxide_api_prototype::api_model::ApiIdentityMetadata;
use oxide_api_prototype::sc_api;
use oxide_api_prototype::ControllerClient;
use oxide_api_prototype::ControllerServerConfig;
use oxide_api_prototype::OxideControllerServer;
use oxide_api_prototype::ServerController;
use oxide_api_prototype::SimMode;
/* XXX reveals this is really an implementation detail */
use oxide_api_prototype::ApiServerStartupInfo;
use slog::Logger;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use uuid::Uuid;

const SERVER_CONTROLLER_UUID: &str = "b6d65341-167c-41df-9b5c-41cded99c229";
const RACK_UUID: &str = "c19a698f-c6f9-4a17-ae30-20d711b8f7dc";

pub struct ControlPlaneTestContext {
    pub external_client: ClientTestContext,
    pub internal_client: ClientTestContext,
    pub server_controller: TestContext,
    pub server: OxideControllerServer,
    logctx: LogContext,
}

impl ControlPlaneTestContext {
    pub async fn teardown(self) {
        self.server.http_server_external.close();
        self.server.http_server_internal.close();
        // XXX can we (/ how do we?) wait for the thing to shut down?
        // self.server.wait_for_finish().await.unwrap();
        self.server_controller.teardown().await;
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
    let config = ControllerServerConfig::from_file(config_file_path)
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
            "component" => "server_controller",
            "server" => sc_id.to_string(),
        )),
        server.http_server_internal.local_addr(),
        sc_id.clone(),
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

/* XXX most of this is copied from run_server_controller_api_server() */
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
) -> Result<TestContext, String> {
    /*
     * XXX use of "component" is overloaded -- caller sets it to
     * "server_controller"
     */
    let dropshot_log = log.new(o!("component" => "dropshot"));
    let sc_log = log.new(o!(
        "component" => "server_controller",
        "server" => id.to_string(),
    ));

    let client_log = log.new(o!("component" => "controller_client"));
    let controller_client =
        Arc::new(ControllerClient::new(controller_address.clone(), client_log));

    let sc = Arc::new(ServerController::new_simulated_with_id(
        &id,
        SimMode::Explicit,
        sc_log,
        Arc::clone(&controller_client),
    ));

    let config_dropshot = ConfigDropshot {
        bind_address: SocketAddr::new("127.0.0.1".parse().unwrap(), 0),
    };
    let api = sc_api();

    let rv = TestContext::new(api, sc, &config_dropshot, None, dropshot_log);

    /*
     * TODO this could happen continuously until it succeeds, but it should be
     * bounded (unlike the case when run as a standalone executable).
     */
    controller_client
        .notify_server_online(id.clone(), ApiServerStartupInfo {
            sc_address: rv.server.local_addr(),
        })
        .await
        .unwrap();
    Ok(rv)
}

/** Returns whether the two identity metadata objects are identical. */
pub fn identity_eq(ident1: &ApiIdentityMetadata, ident2: &ApiIdentityMetadata) {
    assert_eq!(ident1.id, ident2.id);
    assert_eq!(ident1.name, ident2.name);
    assert_eq!(ident1.description, ident2.description);
    assert_eq!(ident1.time_created, ident2.time_created);
    assert_eq!(ident1.time_modified, ident2.time_modified);
}
