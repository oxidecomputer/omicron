/*!
 * Interfaces for parsing configuration files and working with API server
 * configuration.
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;

/**
 * Represents configuration for the whole API server.
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ApiServerConfig {
    /** Dropshot configuration */
    pub dropshot: ConfigDropshot,
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
    use dropshot::ConfigDropshot;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;
    use std::fs;
    use std::net::SocketAddr;
    use std::path::Path;
    use std::path::PathBuf;

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
     * Success case.  We don't need to retest semantics for either ConfigLogging
     * or ConfigDropshot because those are both tested within Dropshot.  If we
     * add new configuration sections of our own, we will want to test those
     * here (both syntax and semantics).
     */
    #[test]
    fn test_valid() {
        let config = read_config(
            "valid",
            r##"
            [dropshot]
            bind_address = "10.1.2.3:4567"
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            "##,
        )
        .unwrap();
        assert_eq!(config, ApiServerConfig {
            dropshot: ConfigDropshot {
                bind_address: "10.1.2.3:4567".parse::<SocketAddr>().unwrap(),
            },
            log: ConfigLogging::File {
                level: ConfigLoggingLevel::Debug,
                if_exists: ConfigLoggingIfExists::Fail,
                path: "/nonexistent/path".to_string()
            }
        });
    }
}
