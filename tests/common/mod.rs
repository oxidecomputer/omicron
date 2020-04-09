/*!
 * Shared automated testing facilities
 */

use dropshot::test_util::TestContext;
use oxide_api_prototype::ApiServerConfig;
use std::path::Path;

/**
 * Set up a `TestContext` for running tests against the API server.
 */
pub async fn test_setup(test_name: &str) -> TestContext {
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
    let config = ApiServerConfig::from_file(config_file_path)
        .expect("failed to load config.test.toml");
    let api = oxide_api_prototype::dropshot_api();
    let apictx = oxide_api_prototype::ApiContext::new();
    oxide_api_prototype::populate_initial_data(&apictx).await;
    TestContext::new(test_name, api, apictx, &config.dropshot, &config.log)
        .await
}
