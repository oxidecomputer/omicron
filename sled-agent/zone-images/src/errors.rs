// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use camino::Utf8Path;
use std::{io, sync::Arc};
use thiserror::Error;

pub(crate) fn zone_manifest_not_found(path: &Utf8Path) -> String {
    format!("zone manifest not found at `{}`", path)
}

/// An `io::Error` wrapper that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
pub(crate) struct ArcIoError(pub Arc<io::Error>);

impl ArcIoError {
    pub(crate) fn new(error: io::Error) -> Self {
        Self(Arc::new(error))
    }
}

/// Testing aid.
impl PartialEq for ArcIoError {
    fn eq(&self, other: &Self) -> bool {
        // Simply comparing io::ErrorKind is good enough for tests.
        self.0.kind() == other.0.kind()
    }
}

/// A `serde_json::Error` that implements `Clone` and `PartialEq`.
#[derive(Clone, Debug, Error)]
#[error(transparent)]
pub(crate) struct ArcSerdeJsonError(pub Arc<serde_json::Error>);

impl ArcSerdeJsonError {
    pub(crate) fn new(error: serde_json::Error) -> Self {
        Self(Arc::new(error))
    }
}

/// Testing aid.
impl PartialEq for ArcSerdeJsonError {
    fn eq(&self, other: &Self) -> bool {
        // Simply comparing line/column/category is good enough for tests.
        self.0.line() == other.0.line()
            && self.0.column() == other.0.column()
            && self.0.classify() == other.0.classify()
    }
}
