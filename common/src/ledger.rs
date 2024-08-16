// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to help reading/writing json files from/to multiple paths

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{de::DeserializeOwned, Serialize};
use slog::{debug, error, info, warn, Logger};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize JSON to file {path}: {err}")]
    JsonSerialize { path: Utf8PathBuf, err: serde_json::error::Error },

    #[error("Cannot deserialize JSON from file {path}: {err}")]
    JsonDeserialize { path: Utf8PathBuf, err: serde_json::error::Error },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Not found in storage")]
    NotFound,

    #[error("Failed to write the ledger to storage (tried to access: {failed_paths:?})")]
    FailedToWrite { failed_paths: Vec<(Utf8PathBuf, Error)> },
}

impl Error {
    fn io_path(path: &Utf8Path, err: std::io::Error) -> Self {
        Self::Io { message: format!("Error accessing {}", path), err }
    }
}

impl From<Error> for crate::api::external::Error {
    fn from(err: Error) -> Self {
        crate::api::external::Error::InternalError {
            internal_message: err.to_string(),
        }
    }
}

// TODO: .json EXPECTORATE test?
//
// ... yes, but maybe not here? Seems like we gotta know the type of "T" to pull
// this off.

/// Manage the serialization and deserialization of a ledger of information.
///
/// This structure is intended to help with serialization and deserialization
/// of configuration information to both M.2s.
#[derive(Debug)]
pub struct Ledger<T> {
    log: Logger,
    ledger: T,
    paths: Vec<Utf8PathBuf>,
}

impl<T: Ledgerable> Ledger<T> {
    /// Creates a ledger with a new initial value, ready to be written to
    /// `paths.`
    pub fn new_with(log: &Logger, paths: Vec<Utf8PathBuf>, default: T) -> Self {
        Self { log: log.clone(), ledger: default, paths }
    }

    /// Reads the ledger from any of the provided `paths`.
    ///
    /// Returns the ledger with the highest generation number if it
    /// exists, otherwise returns `None`.
    pub async fn new(log: &Logger, paths: Vec<Utf8PathBuf>) -> Option<Self> {
        // Read the ledgers from storage
        if let Some(ledger) = Self::read(log, &paths).await {
            Some(Self { log: log.clone(), ledger, paths })
        } else {
            None
        }
    }

    async fn read(log: &Logger, paths: &Vec<Utf8PathBuf>) -> Option<T> {
        // Read all the ledgers that we can.
        let mut ledgers = vec![];
        for path in paths.iter() {
            match T::read_from(log, &path).await {
                Ok(ledger) => ledgers.push(ledger),
                Err(err) => {
                    debug!(log, "Failed to read ledger: {err}"; "path" => %path)
                }
            }
        }

        // Return the ledger with the highest generation number.
        let ledger = ledgers.into_iter().reduce(|prior, ledger| {
            if ledger.is_newer_than(&prior) {
                ledger
            } else {
                prior
            }
        });
        ledger
    }

    pub fn data(&self) -> &T {
        &self.ledger
    }

    pub fn data_mut(&mut self) -> &mut T {
        &mut self.ledger
    }

    pub fn into_inner(self) -> T {
        self.ledger
    }

    /// Writes the ledger back to all config directories.
    ///
    /// Succeeds if at least one of the writes succeeds.
    pub async fn commit(&mut self) -> Result<(), Error> {
        // Bump the generation number any time we want to commit the ledger.
        self.ledger.generation_bump();

        let mut failed_paths = vec![];
        let mut one_successful_write = false;
        for path in self.paths.iter() {
            if let Err(e) = self.atomic_write(&path).await {
                warn!(self.log, "Failed to write ledger"; "path" => ?path, "err" => ?e);
                failed_paths.push((path.to_path_buf(), e));
            } else {
                one_successful_write = true;
            }
        }

        if !one_successful_write {
            error!(self.log, "No successful writes to ledger");
            return Err(Error::FailedToWrite { failed_paths });
        }
        Ok(())
    }

    // Atomically serialize and write the ledger to storage.
    //
    // We accomplish this by first writing to a temporary file, then
    // renaming to the target location.
    async fn atomic_write(&self, path: &Utf8Path) -> Result<(), Error> {
        let mut tmp_path = path.to_path_buf();
        let tmp_filename = format!(
            ".{}.tmp",
            tmp_path.file_name().expect("Should have file name")
        );
        tmp_path.set_file_name(tmp_filename);

        self.ledger.write_to(&self.log, &tmp_path).await?;

        tokio::fs::rename(&tmp_path, &path)
            .await
            .map_err(|err| Error::io_path(&path, err))?;

        Ok(())
    }
}

#[async_trait]
pub trait Ledgerable: DeserializeOwned + Serialize + Send + Sync {
    /// Returns true if [Self] is newer than `other`.
    fn is_newer_than(&self, other: &Self) -> bool;

    /// Increments the gneration number.
    fn generation_bump(&mut self);

    /// Reads from `path` as a json-serialized version of `Self`.
    async fn read_from(log: &Logger, path: &Utf8Path) -> Result<Self, Error> {
        if path.exists() {
            info!(log, "Reading ledger from {}", path);
            <Self as Ledgerable>::deserialize(
                &tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|err| Error::io_path(&path, err))?,
            )
            .map_err(|err| Error::JsonDeserialize {
                path: path.to_path_buf(),
                err,
            })
        } else {
            info!(log, "No ledger in {path}");
            Err(Error::NotFound)
        }
    }

    /// Writes to `path` as a json-serialized version of `Self`.
    async fn write_to(
        &self,
        log: &Logger,
        path: &Utf8Path,
    ) -> Result<(), Error> {
        info!(log, "Writing ledger to {}", path);
        let as_str = serde_json::to_string(&self).map_err(|err| {
            Error::JsonSerialize { path: path.to_path_buf(), err }
        })?;
        tokio::fs::write(&path, as_str)
            .await
            .map_err(|err| Error::io_path(&path, err))?;
        Ok(())
    }

    fn deserialize(s: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(s)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    pub use dropshot::test_util::LogContext;
    use dropshot::ConfigLogging;
    use dropshot::ConfigLoggingIfExists;
    use dropshot::ConfigLoggingLevel;

    // Copied from `omicron-test-utils` to avoid a circular dependency where
    // `omicron-common` depends on `omicron-test-utils` which depends on
    // `omicron-common`.
    /// Set up a [`dropshot::test_util::LogContext`] appropriate for a test named
    /// `test_name`
    ///
    /// This function is currently only used by unit tests.  (We want the dead code
    /// warning if it's removed from unit tests, but not during a normal build.)
    pub fn test_setup_log(test_name: &str) -> LogContext {
        let log_config = ConfigLogging::File {
            level: ConfigLoggingLevel::Trace,
            path: "UNUSED".into(),
            if_exists: ConfigLoggingIfExists::Fail,
        };

        LogContext::new(test_name, &log_config)
    }

    #[derive(Serialize, serde::Deserialize, Default, Eq, PartialEq, Debug)]
    struct Data {
        generation: u64,
        contents: String,
    }

    impl Ledgerable for Data {
        fn is_newer_than(&self, other: &Self) -> bool {
            self.generation > other.generation
        }

        fn generation_bump(&mut self) {
            self.generation = self.generation + 1;
        }
    }

    #[tokio::test]
    async fn test_create_default_ledger() {
        let logctx = test_setup_log("create_default_ledger");
        let log = &logctx.log;

        let config_dir = camino_tempfile::Utf8TempDir::new().unwrap();
        let ledger = Ledger::new_with(
            &log,
            vec![config_dir.path().to_path_buf()],
            Data::default(),
        );

        // Since we haven't previously stored anything, expect to read a default
        // value.
        assert_eq!(ledger.data(), &Data::default());

        let ledger =
            Ledger::<Data>::new(&log, vec![config_dir.path().to_path_buf()])
                .await;
        assert!(ledger.is_none());

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_create_ledger_reads_from_storage() {
        let logctx = test_setup_log("create_ledger_reads_from_storage");
        let log = &logctx.log;

        let config_dir = camino_tempfile::Utf8TempDir::new().unwrap();
        let config_path = config_dir.path().join("ledger.json");

        // Create the ledger within a configuration directory
        let mut ledger =
            Ledger::new_with(&log, vec![config_path.clone()], Data::default());
        ledger.data_mut().contents = "new contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");
        assert!(config_path.exists());

        drop(ledger);

        // Re-create the ledger, observe the new contents.
        let ledger =
            Ledger::<Data>::new(&log, vec![config_path.clone()]).await.unwrap();

        assert_eq!(ledger.data().contents, "new contents");
        assert_eq!(ledger.data().generation, 1);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_create_ledger_reads_latest_from_storage() {
        let logctx = test_setup_log("create_ledger_reads_latest_from_storage");
        let log = &logctx.log;

        // Create the ledger, initialize contents.
        let config_dirs = [
            camino_tempfile::Utf8TempDir::new().unwrap(),
            camino_tempfile::Utf8TempDir::new().unwrap(),
        ];
        let config_paths = config_dirs
            .iter()
            .map(|d| d.path().join("ledger.json"))
            .collect::<Vec<_>>();

        let mut ledger =
            Ledger::new_with(&log, config_paths.clone(), Data::default());
        ledger.data_mut().contents = "new contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        assert!(config_paths[0].exists());
        assert!(config_paths[1].exists());

        drop(ledger);

        // Let's write again, but only using one of the two config dirs.
        let mut ledger = Ledger::<Data>::new(&log, config_paths[..1].to_vec())
            .await
            .expect("Failed to read ledger");
        ledger.data_mut().contents = "even newer contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        drop(ledger);

        // Re-create the ledger (using both config dirs), observe the newest contents.
        let ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to read ledger");

        assert_eq!(ledger.data().contents, "even newer contents");
        assert_eq!(ledger.data().generation, 2);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_commit_handles_write_failures() {
        let logctx = test_setup_log("create_commit_handles_write_failures");
        let log = &logctx.log;

        // Create the ledger, initialize contents.
        let mut config_dirs = vec![
            camino_tempfile::Utf8TempDir::new().unwrap(),
            camino_tempfile::Utf8TempDir::new().unwrap(),
        ];
        let config_paths = config_dirs
            .iter()
            .map(|d| d.path().join("ledger.json"))
            .collect::<Vec<_>>();

        let mut ledger =
            Ledger::new_with(&log, config_paths.clone(), Data::default());
        ledger.data_mut().contents = "written to both configs".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        assert!(config_paths[0].exists());
        assert!(config_paths[1].exists());

        drop(ledger);

        // Remove one of the config directories, try again.
        //
        // We should still be able to read and write the ledger.
        config_dirs.remove(1);
        assert!(config_paths[0].exists());
        assert!(!config_paths[1].exists());

        let mut ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to read ledger");

        assert_eq!(ledger.data().contents, "written to both configs");
        assert_eq!(ledger.data().generation, 1);
        ledger.data_mut().contents = "written to one config".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        drop(ledger);

        // We can still parse the ledger from a single path
        let ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to read ledger");
        assert_eq!(ledger.data().contents, "written to one config");
        assert_eq!(ledger.data().generation, 2);

        drop(ledger);

        // Remove the last config directory, try again.
        //
        // We should not be able to write the ledger.
        drop(config_dirs);
        assert!(!config_paths[0].exists());
        assert!(!config_paths[1].exists());

        let ledger = Ledger::<Data>::new(&log, config_paths.clone()).await;
        assert!(ledger.is_none());

        let mut ledger =
            Ledger::new_with(&log, config_paths.clone(), Data::default());
        assert_eq!(ledger.data(), &Data::default());
        let err = ledger.commit().await.unwrap_err();
        assert!(
            matches!(err, Error::FailedToWrite { .. }),
            "Unexpected error: {err}"
        );

        logctx.cleanup_successful();
    }
}
