// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to help reading/writing toml files from/to multiple paths

use async_trait::async_trait;
use camino::{Utf8Path, Utf8PathBuf};
use serde::{de::DeserializeOwned, Serialize};
use slog::Logger;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot serialize TOML to file {path}: {err}")]
    TomlSerialize { path: Utf8PathBuf, err: toml::ser::Error },

    #[error("Cannot deserialize TOML from file {path}: {err}")]
    TomlDeserialize { path: Utf8PathBuf, err: toml::de::Error },

    #[error("Failed to perform I/O: {message}: {err}")]
    Io {
        message: String,
        #[source]
        err: std::io::Error,
    },

    #[error("Failed to write the ledger to storage")]
    FailedToAccessStorage,
}

impl Error {
    fn io_path(path: &Utf8Path, err: std::io::Error) -> Self {
        Self::Io { message: format!("Error accessing {path}"), err }
    }
}

impl From<Error> for omicron_common::api::external::Error {
    fn from(err: Error) -> Self {
        omicron_common::api::external::Error::InternalError {
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
pub struct Ledger<T> {
    log: Logger,
    ledger: T,
    paths: Vec<Utf8PathBuf>,
}

impl<T: Ledgerable> Ledger<T> {
    /// Reads the ledger from any of the provided `paths`.
    ///
    /// Returns the following, in order:
    /// - The ledger with the highest generation number
    /// - If none exists, returns a default ledger
    pub async fn new(
        log: &Logger,
        paths: Vec<Utf8PathBuf>,
    ) -> Result<Self, Error> {
        // Read all the ledgers that we can.
        let mut ledgers = vec![];
        for path in paths.iter() {
            if let Ok(ledger) = T::read_from(log, &path).await {
                ledgers.push(ledger);
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

        // If we can't read either ledger, start a new one.
        let ledger = ledger.unwrap_or_else(|| T::default());

        Ok(Self { log: log.clone(), ledger, paths })
    }

    pub fn data(&self) -> &T {
        &self.ledger
    }

    pub fn data_mut(&mut self) -> &mut T {
        &mut self.ledger
    }

    /// Writes the ledger back to all config directories.
    ///
    /// Succeeds if at least one of the writes succeeds.
    pub async fn commit(&mut self) -> Result<(), Error> {
        // Bump the generation number any time we want to commit the ledger.
        self.ledger.generation_bump();

        let mut one_successful_write = false;
        for path in self.paths.iter() {
            if let Err(e) = self.atomic_write(&path).await {
                warn!(self.log, "Failed to write to {path}: {e}");
            } else {
                one_successful_write = true;
            }
        }

        if !one_successful_write {
            return Err(Error::FailedToAccessStorage);
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
pub trait Ledgerable:
    Default + DeserializeOwned + Serialize + Send + Sync
{
    /// Returns true if [Self] is newer than `other`.
    fn is_newer_than(&self, other: &Self) -> bool;

    /// Increments the gneration number.
    fn generation_bump(&mut self);

    /// Reads from `path` as a toml-serialized version of `Self`.
    async fn read_from(log: &Logger, path: &Utf8Path) -> Result<Self, Error> {
        if path.exists() {
            debug!(log, "Reading ledger from {path}");
            toml::from_str(
                &tokio::fs::read_to_string(&path)
                    .await
                    .map_err(|err| Error::io_path(&path, err))?,
            )
            .map_err(|err| Error::TomlDeserialize {
                path: path.to_path_buf(),
                err,
            })
        } else {
            debug!(log, "No ledger in {path}");
            Ok(Self::default())
        }
    }

    /// Writes to `path` as a toml-serialized version of `Self`.
    async fn write_to(
        &self,
        log: &Logger,
        path: &Utf8Path,
    ) -> Result<(), Error> {
        debug!(log, "Writing ledger to {path}");
        let serialized =
            toml::Value::try_from(&self).expect("Cannot serialize ledger");
        let as_str = toml::to_string(&serialized).map_err(|err| {
            Error::TomlSerialize { path: path.to_path_buf(), err }
        })?;
        tokio::fs::write(&path, as_str)
            .await
            .map_err(|err| Error::io_path(&path, err))?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use omicron_test_utils::dev::test_setup_log;

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
        let ledger =
            Ledger::<Data>::new(&log, vec![config_dir.path().to_path_buf()])
                .await
                .expect("Failed to create ledger");

        // Since we haven't previously stored anything, expect to read a default
        // value.
        assert_eq!(ledger.data(), &Data::default());

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_create_ledger_reads_from_storage() {
        let logctx = test_setup_log("create_ledger_reads_from_storage");
        let log = &logctx.log;

        let config_dir = camino_tempfile::Utf8TempDir::new().unwrap();
        let config_path = config_dir.path().join("ledger.toml");

        // Create the ledger within a configuration directory
        let mut ledger = Ledger::<Data>::new(&log, vec![config_path.clone()])
            .await
            .expect("Failed to create ledger");
        ledger.data_mut().contents = "new contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");
        assert!(config_path.exists());

        drop(ledger);

        // Re-create the ledger, observe the new contents.
        let ledger = Ledger::<Data>::new(&log, vec![config_path.clone()])
            .await
            .expect("Failed to create ledger");

        assert_eq!(ledger.data().contents, "new contents");
        assert_eq!(ledger.data().generation, 1);

        logctx.cleanup_successful();
    }

    #[tokio::test]
    async fn test_create_ledger_reads_latest_from_storage() {
        let logctx = test_setup_log("create_ledger_reads_latest_from_storage");
        let log = &logctx.log;

        // Create the ledger, initialize contents.
        let config_dirs = vec![
            camino_tempfile::Utf8TempDir::new().unwrap(),
            camino_tempfile::Utf8TempDir::new().unwrap(),
        ];
        let config_paths = config_dirs
            .iter()
            .map(|d| d.path().join("ledger.toml"))
            .collect::<Vec<_>>();

        let mut ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to create ledger");
        ledger.data_mut().contents = "new contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        assert!(config_paths[0].exists());
        assert!(config_paths[1].exists());

        drop(ledger);

        // Let's write again, but only using one of the two config dirs.
        let mut ledger = Ledger::<Data>::new(&log, config_paths[..=1].to_vec())
            .await
            .expect("Failed to create ledger");
        ledger.data_mut().contents = "even newer contents".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        drop(ledger);

        // Re-create the ledger (using both config dirs), observe the newest contents.
        let ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to create ledger");

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
            .map(|d| d.path().join("ledger.toml"))
            .collect::<Vec<_>>();

        let mut ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to create ledger");
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
            .expect("Failed to create ledger");

        assert_eq!(ledger.data().contents, "written to both configs");
        assert_eq!(ledger.data().generation, 1);
        ledger.data_mut().contents = "written to one config".to_string();
        ledger.commit().await.expect("Failed to write ledger");

        drop(ledger);

        // We can still parse the ledger from a single path
        let ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to create ledger");
        assert_eq!(ledger.data().contents, "written to one config");
        assert_eq!(ledger.data().generation, 2);

        drop(ledger);

        // Remove the last config directory, try again.
        //
        // We should not be able to write the ledger.
        drop(config_dirs);
        assert!(!config_paths[0].exists());
        assert!(!config_paths[1].exists());

        let mut ledger = Ledger::<Data>::new(&log, config_paths.clone())
            .await
            .expect("Failed to create ledger");

        assert_eq!(ledger.data(), &Data::default());
        let err = ledger.commit().await.unwrap_err();
        assert!(
            matches!(err, Error::FailedToAccessStorage),
            "Unexpected error: {err}"
        );

        logctx.cleanup_successful();
    }
}
