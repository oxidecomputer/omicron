// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functions related to management of measurement corpus
use camino::{Utf8Path, Utf8PathBuf};
use sled_agent_config_reconciler::InternalDisksReceiver;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum MeasurementError {
    #[error("Missing INSTALL dataset")]
    MissingInstallSet,
    #[error("io error at {path}")]
    Io { path: Utf8PathBuf, err: std::io::Error },
    #[error("Missing boot disk")]
    MissingBootDisk,
}

/// Access the measurements in the install directory
pub async fn sled_new_measurement_paths(
    receiver: &InternalDisksReceiver,
) -> Result<Vec<Utf8PathBuf>, MeasurementError> {
    let mut all = vec![];

    let mut dirs: Vec<_> = receiver
        .current()
        .all_install_datasets()
        .map(|p| p.join("measurements"))
        .collect();

    if dirs.is_empty() {
        return Err(MeasurementError::MissingInstallSet);
    }

    // We don't have an install dataset for our automated deployment
    // testing. Instead, rely on the files getting copied/packaged.
    let testing_corpus_path =
        Utf8Path::new("/opt/oxide/sled-agent/pkg/testing-measurements");

    if testing_corpus_path.is_dir() {
        dirs.push(testing_corpus_path.into());
    }

    for dir in dirs {
        match dir.read_dir_utf8() {
            Ok(iter) => {
                for entry in iter {
                    let entry = entry.map_err(|err| MeasurementError::Io {
                        path: dir.clone(),
                        err,
                    })?;
                    all.push(entry.path().into());
                }
            }
            // We purposely skip over errors here in case the
            // directory is missing. This will just end up as
            // an empty corpus set.
            Err(_) => {}
        }
    }

    Ok(all)
}
