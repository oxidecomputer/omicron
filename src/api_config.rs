/*!
 * Interfaces for parsing configuration files and working with API server
 * configuration.
 */

use dropshot::ConfigLogging;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;

/**
 * Represents configuration for the whole API server.
 */
#[derive(Debug, Deserialize)]
pub struct ApiServerConfig {
    /** IP address and TCP port to which to bind for accepting connections. */
    pub bind_address: SocketAddr,
    /** Server-wide logging configuration. */
    pub log: ConfigLogging,
}

impl ApiServerConfig {
    /**
     * Load an `ApiServerConfig` from the given TOML file.  The format is
     * described in the README.  This config object can then be used to create a
     * new `ApiServer`.
     */
    pub fn from_file(path: &Path) -> Result<ApiServerConfig, String> {
        let file_read = std::fs::read_to_string(path);
        let file_contents = file_read.map_err(|error| {
            format!("read \"{}\": {}", path.display(), error)
        })?;
        let config_parsed: ApiServerConfig = toml::from_str(&file_contents)
            .map_err(|error| {
                format!("parse \"{}\": {}", path.display(), error)
            })?;
        Ok(config_parsed)
    }
}

#[cfg(test)]
mod test {
    use super::ApiServerConfig;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;

    /*
     * Chunks of valid config file.  These are put together with invalid chunks
     * in the test suite to construct complete config files that will only fail
     * on the known invalid chunk.
     */
    const CONFIG_VALID_LOG: &str = r##"
            [log]
            level = "critical"
            mode = "stderr-terminal"
        "##;

    /**
     * Generates a temporary filesystem path unique for the given label.
     */
    fn temp_path(label: &str) -> PathBuf {
        let arg0str = std::env::args().next().expect("expected process arg0");
        let arg0 = Path::new(&arg0str)
            .file_name()
            .expect("expected arg0 filename")
            .to_str()
            .expect("expected arg0 filename to be valid Unicode");
        let pid = std::process::id();
        let mut pathbuf = std::env::temp_dir();
        pathbuf.push(format!("{}.{}.{}", arg0, pid, label));
        pathbuf
    }

    /**
     * Load an ApiServerConfig with the given string `contents`.  To exercise
     * the full path, this function writes the contents to a file first, then
     * loads the config from that file, then removes the file.  `label` is used
     * as a unique string for the filename and error messages.  It should be
     * unique for each test.
     */
    fn read_config(
        label: &str,
        contents: &str,
    ) -> Result<ApiServerConfig, String> {
        let pathbuf = temp_path(label);
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path.display());
        fs::write(path, contents).expect("write to tempfile failed");

        let result = ApiServerConfig::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    /*
     * Totally bogus config files (nonexistent, bad TOML syntax)
     */

    #[test]
    fn test_config_nonexistent() {
        let error = ApiServerConfig::from_file(Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        assert!(error
            .starts_with("read \"/nonexistent\": No such file or directory"));
    }

    #[test]
    fn test_config_bad_toml() {
        let error =
            read_config("bad_toml", "foo =").expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": unexpected eof"));
    }

    /*
     * Empty config (special case of a missing required field, but worth calling
     * out explicitly)
     */

    #[test]
    fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error.contains("\": missing field"));
    }

    /*
     * Bad values for "bind_address"
     */

    #[test]
    fn test_config_bad_bind_address_port_too_small() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "127.0.0.1:-3"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_port_too_small", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[test]
    fn test_config_bad_bind_address_port_too_large() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "127.0.0.1:65536"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_port_too_large", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[test]
    fn test_config_bad_bind_address_garbage() {
        let bad_config = format!(
            "{}{}",
            r###"
            bind_address = "foobar"
            "###,
            CONFIG_VALID_LOG
        );
        let error = read_config("bad_bind_address_garbage", &bad_config)
            .expect_err("expected failure");
        assert!(error.starts_with("parse \""));
        assert!(error
            .contains("\": invalid IP address syntax for key `bind_address`"));
    }

    #[tokio::test]
    async fn test_config_bind_address() {
        let client = hyper::Client::new();
        let bind_ip_str = "127.0.0.1";
        let bind_port: u16 = 12221;

        /*
         * This helper constructs a GET HTTP request to
         * http://$bind_ip_str:$port/, where $port is the argument to the
         * closure.
         */
        let cons_request = |port: u16| {
            let uri = hyper::Uri::builder()
                .scheme("http")
                .authority(format!("{}:{}", bind_ip_str, port).as_str())
                .path_and_query("/")
                .build()
                .unwrap();
            hyper::Request::builder()
                .method(http::method::Method::GET)
                .uri(&uri)
                .body(hyper::Body::empty())
                .unwrap()
        };

        /*
         * Make sure there is not currently a server running on our expected
         * port so that when we subsequently create a server and run it we know
         * we're getting the one we configured.
         */
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());

        /*
         * Now start a server with our configuration and make the request again.
         * This should succeed in terms of making the request.  (The request
         * itself might fail with a 400-level or 500-level response code -- we
         * don't want to depend on too much from the ApiServer here -- but we
         * should have successfully made the request.)
         */
        let config_text = format!(
            "bind_address = \"{}:{}\"\n{}",
            bind_ip_str, bind_port, CONFIG_VALID_LOG
        );
        let config = read_config("bind_address", &config_text).unwrap();
        let mut server = super::super::ApiServer::new(&config, false).unwrap();
        let task = server.http_server.run();
        client.request(cons_request(bind_port)).await.unwrap();
        server.http_server.close();
        task.await.unwrap().unwrap();

        /*
         * Make another request to make sure it fails now that we've shut down
         * the server.
         */
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());

        /*
         * Start a server on another TCP port and make sure we can reach that
         * one (and NOT the one we just shut down).
         */
        let config_text = format!(
            "bind_address = \"{}:{}\"\n{}",
            bind_ip_str,
            bind_port + 1,
            CONFIG_VALID_LOG
        );
        let config = read_config("bind_address", &config_text).unwrap();
        let mut server = super::super::ApiServer::new(&config, false).unwrap();
        let task = server.http_server.run();
        client.request(cons_request(bind_port + 1)).await.unwrap();
        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());
        server.http_server.close();
        task.await.unwrap().unwrap();

        let error = client.request(cons_request(bind_port)).await.unwrap_err();
        assert!(error.is_connect());
        let error =
            client.request(cons_request(bind_port + 1)).await.unwrap_err();
        assert!(error.is_connect());
    }
}
