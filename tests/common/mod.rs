/*!
 * Shared automated testing facilities
 */

use dropshot::test_util::log_file_for_test;
use dropshot::test_util::ClientTestContext;
use dropshot::ConfigLogging;
use oxide_api_prototype::ApiServer;
use oxide_api_prototype::ApiServerConfig;
use std::fs;
use std::path::Path;
use tokio::task::JoinHandle;

/**
 * ApiTestContext encapsulates several pieces needed for these basic tests.
 * Essentially, any common setup code (and components, like an HTTP server and
 * client) ought to be included in this struct.
 * TODO-cleanup This duplicates a lot of the code in
 * lib/dropshot/tests/test_demo.rs.  These are essentially two different
 * consumers of the `dropshot` crate and it shouldn't take so much boilerplate
 * to set up a test context.
 */
pub struct ApiTestContext {
    /** client-side testing context */
    pub client_testctx: ClientTestContext,
    /** handle to the HTTP server under test */
    api_server: ApiServer,
    /** handle to the task that's running the HTTP server */
    api_server_task: JoinHandle<Result<(), hyper::error::Error>>,
    /** path to the per-test log file */
    log_path: Option<String>,
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
         * no one port that we could reasonably pick.  We also override the file
         * path to provide a unique filename.
         *
         * Given these overrides, it's barely worth reading a config file at
         * all.  However, users can change the logging level and local IP if
         * they want, and as we add more configuration options, we expect many
         * of those can be usefully configured (and reconfigured) for the test
         * suite.
         */
        let config_file_path = Path::new("tests/config.test.toml");
        let mut config = ApiServerConfig::from_file(config_file_path)
            .expect("failed to load config.test.toml");
        config.dropshot.bind_address.set_port(0);
        let mut log_path = None;
        if let ConfigLogging::File {
            level,
            path: _,
            if_exists,
        } = config.log
        {
            let new_path = log_file_for_test(test_name);
            let new_path_str = new_path.as_path().display().to_string();
            config.log = ConfigLogging::File {
                level,
                path: new_path_str.clone(),
                if_exists,
            };
            log_path = Some(new_path_str);
        }

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
            log_path,
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
        if let Some(log_path) = self.log_path {
            fs::remove_file(log_path).unwrap();
        }
    }
}
