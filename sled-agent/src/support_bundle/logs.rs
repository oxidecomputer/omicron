use std::collections::BTreeSet;

use camino::Utf8PathBuf;
use dropshot::{Body, HttpError};
use range_requests::PotentialRange;
use sled_diagnostics::{SledDiagnosticsLogError, SledDiagnosticsLogs};
use slog::Logger;
use slog_error_chain::InlineErrorChain;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to join tokio task: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Logs(#[from] SledDiagnosticsLogError),

    #[error("Path \"{0}\" not found")]
    NotFound(Utf8PathBuf),

    #[error("Directory \"{0}\" is not a valid log path")]
    InvalidLogDir(Utf8PathBuf),

    #[error("Path \"{0}\" should not be the final component")]
    MissingFilename(Utf8PathBuf),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Range(#[from] range_requests::Error),

    #[error("Not a file")]
    NotAFile,
}

fn err_str(err: &dyn std::error::Error) -> String {
    InlineErrorChain::new(err).to_string()
}

impl From<Error> for HttpError {
    fn from(err: Error) -> Self {
        match err {
            Error::InvalidLogDir(_) => {
                HttpError::for_bad_request(None, "Invalid log dir".to_string())
            }
            Error::NotAFile => {
                HttpError::for_bad_request(None, "Not a file".to_string())
            }
            Error::NotFound(path) => HttpError::for_not_found(
                None,
                format!("Log file {path} not found"),
            ),
            err => HttpError::for_internal_error(err_str(&err)),
        }
    }
}

pub struct SupportBundleLogs<'a> {
    log: &'a Logger,
}

impl<'a> SupportBundleLogs<'a> {
    pub fn new(log: &'a Logger) -> Self {
        Self { log }
    }

    pub async fn get_index(&self) -> Result<SledDiagnosticsLogs, Error> {
        tokio::task::spawn_blocking(move || sled_diagnostics::find_logs())
            .await
            .map_err(Error::Join)?
            .map_err(Error::Logs)
    }

    /// Get a log file from disk preforming normalization and validation along
    /// the way.
    pub async fn get_log_file(
        &self,
        req: &str,
        range: Option<PotentialRange>,
    ) -> Result<http::Response<Body>, Error> {
        let valid_dirs =
            tokio::task::spawn_blocking(|| sled_diagnostics::valid_log_dirs())
                .await
                .map_err(Error::Join)?
                .map_err(Error::Logs)?;

        let normalized = normalize_path(req);
        let valid_path = validate_log_dir(&valid_dirs, normalized)?;
        let mut file = match tokio::fs::File::open(&valid_path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(valid_path));
            }
            Err(e) => return Err(e.into()),
        };
        let metadata = file.metadata().await?;

        if !metadata.is_file() {
            return Err(Error::NotAFile);
        }

        const CONTENT_TYPE: http::HeaderValue =
            http::HeaderValue::from_static("text/plain");
        let content_type = Some(CONTENT_TYPE);

        if let Some(range) = range {
            // If this has a range request, we need to validate the range
            // and put bounds on the part of the file we're reading.
            let range = match range.parse(metadata.len()) {
                Ok(range) => range,
                Err(err_response) => return Ok(err_response),
            };

            info!(&self.log, "SupportBundle GET log file (ranged)";
                "file" => %valid_path,
                "start" => range.start(),
                "limit" => range.content_length().get(),
            );

            file.seek(std::io::SeekFrom::Start(range.start())).await?;
            let limit = range.content_length().get();
            return Ok(range_requests::make_get_response(
                Some(range),
                metadata.len(),
                content_type,
                ReaderStream::new(file.take(limit)),
            )?);
        } else {
            info!(&self.log, "SupportBundle GET log file";
                "file" => %valid_path,
            );

            Ok(range_requests::make_get_response(
                None,
                metadata.len(),
                content_type,
                // The take here is load bearing as we want to cap continously
                // growing log files.
                ReaderStream::new(file.take(metadata.len())),
            )?)
        }
    }
}

/// Given a path represented as a `str` return a normalized `Utf8PathBuf` that
/// starts with "/" and removes "..", "."  components.
fn normalize_path(path: &str) -> Utf8PathBuf {
    let path = Utf8PathBuf::from(format!("/{path}"));
    let mut normalized = Utf8PathBuf::new();

    for component in path.components() {
        match component {
            camino::Utf8Component::RootDir => normalized.push("/"),
            camino::Utf8Component::Normal(c) => normalized.push(c),
            // Toss the rest out rather than popping off the stack. Consumers
            // should be passing full paths.
            _ => (),
        }
    }

    normalized
}

fn validate_log_dir(
    allowed_dirs: &BTreeSet<Utf8PathBuf>,
    path: Utf8PathBuf,
) -> Result<Utf8PathBuf, Error> {
    let directory =
        path.parent().ok_or(Error::MissingFilename(path.clone()))?;
    match allowed_dirs.contains(directory) {
        true => Ok(path.clone()),
        false => Err(Error::InvalidLogDir(path)),
    }
}

#[cfg(test)]
mod test {
    use crate::support_bundle::logs::*;
    use camino::Utf8PathBuf;

    #[test]
    fn test_normalize_path() {
        assert_eq!(
            normalize_path("missing/root/dir"),
            Utf8PathBuf::from(r"/missing/root/dir")
        );

        assert_eq!(
            normalize_path("/etc/passwd"),
            Utf8PathBuf::from(r"/etc/passwd")
        );

        assert_eq!(
            normalize_path("../../etc/shadow"),
            Utf8PathBuf::from(r"/etc/shadow")
        );

        assert_eq!(
            normalize_path("./../secret/file"),
            Utf8PathBuf::from(r"/secret/file")
        );

        assert_eq!(
            normalize_path("/find/the/./../secret/file"),
            Utf8PathBuf::from(r"/find/the/secret/file")
        );
    }

    #[test]
    fn test_validate_log_dir() {
        let mut valid_dirs = BTreeSet::new();
        valid_dirs.insert(Utf8PathBuf::from(r"/zpool/int/cockroachdb"));
        valid_dirs.insert(Utf8PathBuf::from(r"/var/svc/log"));

        // NB: We are using match/panic rather than assert statements due to
        // some of the conents of `Error` not implementing `PartialEq`.

        // Valid log path
        let path = Utf8PathBuf::from(r"/zpool/int/cockroachdb/stderr.log");
        let is_valid = validate_log_dir(&valid_dirs, path.clone()).unwrap();
        assert_eq!(path, is_valid);

        // Invalid log path consisting of just the root directory
        let path = Utf8PathBuf::from(r"/");
        match validate_log_dir(&valid_dirs, path.clone()) {
            Err(Error::MissingFilename(_)) => (),
            val => panic!("validating {path} should be Error::MissingFilename but found {val:?}"),
        }

        // Invalid log path consisting of just a file name
        let path = Utf8PathBuf::from(r"stderr.log");
        match validate_log_dir(&valid_dirs, path.clone()) {
            Err(Error::InvalidLogDir(_)) => (),
            val => panic!("validating {path} should be Error::InvalidLogDir but found {val:?}"),
        }

        // Invalid log path outside of the acceptable directories
        let path = Utf8PathBuf::from(r"/zpool/ext/cockroachdb/stderr.log");
        match validate_log_dir(&valid_dirs, path.clone()) {
            Err(Error::InvalidLogDir(_)) => (),
            val => panic!("validating {path} should be Error::InvalidLogDir but found {val:?}"),
        }
    }
}
