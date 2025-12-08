// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to support bundles.

pub use sled_agent_types_migrations::latest::support_bundle::*;

// The final name of the bundle, as it is stored within the dedicated
// datasets.
//
// The full path is of the form:
//
// /pool/ext/$(POOL_UUID)/crypt/$(DATASET_TYPE)/$(BUNDLE_UUID)/bundle.zip
//                              |               | This is a per-bundle nested dataset
//                              | This is a Debug dataset
//
// NOTE: The DebugCollector has been explicitly configured to ignore these
// files, so they are not removed. If the files used here change in the future,
// DebugCollector should also be updated.
pub const BUNDLE_FILE_NAME: &str = "bundle.zip";
pub const BUNDLE_TMP_FILE_NAME: &str = "bundle.zip.tmp";

/// When a bundle is destroyed, and it returns a 404 due to
/// the bundle not being found, what error message do we expect?
///
/// This helps distinguish from other errors which might cause a 404 to
/// propagate.
pub const NESTED_DATASET_NOT_FOUND: &str = "Nested dataset not found";
