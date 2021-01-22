/*!
 * Interfaces for parsing configuration files and working with OXC server
 * configuration
 */

use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::path::{Path, PathBuf};

/**
 * Configuration for an OXC server
 */
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ConfigController {
    /** Dropshot configuration for external API server */
    pub dropshot_external: ConfigDropshot,
    /** Dropshot configuration for internal API server */
    pub dropshot_internal: ConfigDropshot,
    /** Server-wide logging configuration. */
    pub log: ConfigLogging,
}

#[derive(Debug)]
pub struct LoadError {
    path: PathBuf,
    kind: LoadErrorKind,
}
#[derive(Debug)]
pub enum LoadErrorKind {
    Io(std::io::Error),
    Parse(toml::de::Error),
}

impl From<(PathBuf, std::io::Error)> for LoadError {
    fn from((path, err): (PathBuf, std::io::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Io(err) }
    }
}

impl From<(PathBuf, toml::de::Error)> for LoadError {
    fn from((path, err): (PathBuf, toml::de::Error)) -> Self {
        LoadError { path, kind: LoadErrorKind::Parse(err) }
    }
}

impl std::error::Error for LoadError {}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.kind {
            LoadErrorKind::Io(e) => {
                write!(f, "read \"{}\": {}", self.path.display(), e)
            }
            LoadErrorKind::Parse(e) => {
                write!(f, "parse \"{}\": {}", self.path.display(), e)
            }
        }
    }
}

impl std::cmp::PartialEq<std::io::Error> for LoadError {
    fn eq(&self, other: &std::io::Error) -> bool {
        if let LoadErrorKind::Io(e) = &self.kind {
            e.kind() == other.kind()
        } else {
            false
        }
    }
}

impl ConfigController {
    /**
     * Load a `ConfigController` from the given TOML file
     *
     * This config object can then be used to create a new `OxideController`.
     * The format is described in the README.
     */
    pub fn from_file(path: &Path) -> Result<ConfigController, LoadError> {
        let file_contents = std::fs::read_to_string(path)
            .map_err(|e| (path.to_path_buf(), e))?;
        let config_parsed: ConfigController = toml::from_str(&file_contents)
            .map_err(|e| (path.to_path_buf(), e))?;
        Ok(config_parsed)
    }
}

#[cfg(test)]
mod test {
    use super::ConfigController;
    use super::{LoadError, LoadErrorKind};
    use dropshot::ConfigDropshot;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;
    use libc;
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
     * Load a ConfigController with the given string `contents`.  To exercise
     * the full path, this function writes the contents to a file first, then
     * loads the config from that file, then removes the file.  `label` is used
     * as a unique string for the filename and error messages.  It should be
     * unique for each test.
     */
    fn read_config(
        label: &str,
        contents: &str,
    ) -> Result<ConfigController, LoadError> {
        let pathbuf = temp_path(label);
        let path = pathbuf.as_path();
        eprintln!("writing test config {}", path.display());
        fs::write(path, contents).expect("write to tempfile failed");

        let result = ConfigController::from_file(path);
        fs::remove_file(path).expect("failed to remove temporary file");
        eprintln!("{:?}", result);
        result
    }

    /*
     * Totally bogus config files (nonexistent, bad TOML syntax)
     */

    #[test]
    fn test_config_nonexistent() {
        let error = ConfigController::from_file(Path::new("/nonexistent"))
            .expect_err("expected config to fail from /nonexistent");
        let expected = std::io::Error::from_raw_os_error(libc::ENOENT);
        assert_eq!(error, expected);
    }

    #[test]
    fn test_config_bad_toml() {
        let error =
            read_config("bad_toml", "foo =").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.line_col(), Some((0, 5)));
            assert_eq!(
                error.to_string(),
                "unexpected eof encountered at line 1 column 6"
            );
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
    }

    /*
     * Empty config (special case of a missing required field, but worth calling
     * out explicitly)
     */

    #[test]
    fn test_config_empty() {
        let error = read_config("empty", "").expect_err("expected failure");
        if let LoadErrorKind::Parse(error) = &error.kind {
            assert_eq!(error.line_col(), None);
            assert_eq!(error.to_string(), "missing field `dropshot_external`");
        } else {
            panic!(
                "Got an unexpected error, expected Parse but got {:?}",
                error
            );
        }
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
            [dropshot_external]
            bind_address = "10.1.2.3:4567"
            request_body_max_bytes = 1024
            [dropshot_internal]
            bind_address = "10.1.2.3:4568"
            request_body_max_bytes = 1024
            [log]
            mode = "file"
            level = "debug"
            path = "/nonexistent/path"
            if_exists = "fail"
            "##,
        )
        .unwrap();

        assert_eq!(
            config,
            ConfigController {
                dropshot_external: ConfigDropshot {
                    bind_address: "10.1.2.3:4567"
                        .parse::<SocketAddr>()
                        .unwrap(),
                    ..Default::default()
                },
                dropshot_internal: ConfigDropshot {
                    bind_address: "10.1.2.3:4568"
                        .parse::<SocketAddr>()
                        .unwrap(),
                    ..Default::default()
                },
                log: ConfigLogging::File {
                    level: ConfigLoggingLevel::Debug,
                    if_exists: ConfigLoggingIfExists::Fail,
                    path: "/nonexistent/path".to_string()
                }
            }
        );
    }
}
