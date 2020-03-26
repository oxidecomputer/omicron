/*!
 * Shared automated testing facilities
 */

use httpapi::test_util::ClientTestContext;
use oxide_api_prototype::ApiServer;
use oxide_api_prototype::ApiServerConfig;
use std::path::Path;
use tokio::task::JoinHandle;

/**
 * ApiTestContext encapsulates several pieces needed for these basic tests.
 * Essentially, any common setup code (and components, like an HTTP server and
 * client) ought to be included in this struct.
 * XXX TODO-cleanup This duplicates a lot of the code in
 * lib/httpapi/tests/test_demo.rs.  These are essentially two different
 * consumers of the `httpapi` crate and it shouldn't take so much boilerplate to
 * set up a test context.
 */
pub struct ApiTestContext {
    /** client-side testing context */
    pub client_testctx: ClientTestContext,
    /** handle to the HTTP server under test */
    api_server: ApiServer,
    /** handle to the task that's running the HTTP server */
    api_server_task: JoinHandle<Result<(), hyper::error::Error>>,
}

impl ApiTestContext {
    /**
     * Set up a `ApiTestContext` for running tests against the API server.  This
     * binds to a hardcoded IP address and port, starts the server, and
     * instantiates a client.  The results are encapsulated in the
     * `ApiTestContext` struct.
     */
    pub async fn new(test_name: &str) -> ApiTestContext {
        /*
         * We load as much configuration as we can from the test suite
         * configuration file.  However, we override the TCP port for the server
         * to 0, indicating that we wish to bind to any available port.  This is
         * necessary because we'll run multiple servers concurrently, so there's
         * no one port that we could reasonably pick.
         * XXX TODO-cleanup consider just removing "test-suite" mode and doing
         * it here?  This could more closely mirror the DemoTestContext, clean
         * up its own log file, etc.
         */
        let config_file_path = Path::new("tests/config.test.toml");
        let mut config = ApiServerConfig::from_file(config_file_path)
            .expect("failed to load config.test.toml");
        config.bind_address.set_port(0);

        let mut api_server = ApiServer::new(&config).unwrap();
        let api_server_task = api_server.http_server.run();

        let server_addr = api_server.http_server.local_addr();
        let client_log = api_server.log.new(o!(
            "http_client" => "test suite",
            "test_name" => test_name.to_string(),
        ));
        let client_testctx = ClientTestContext::new(server_addr, client_log);

        ApiTestContext {
            client_testctx,
            api_server,
            api_server_task,
        }
    }

    /**
     * Tear down facilities that were set up during the test.  Currently, this
     * shuts down the test HTTP server and waits for it to exit gracefully.
     */
    pub async fn teardown(self) {
        self.api_server.http_server.close();
        let join_result =
            self.api_server_task.await.expect("failed to join on test server");
        join_result.expect("server closed with an error");
    }
}
