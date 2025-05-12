// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to support bundles.

// The final name of the bundle, as it is stored within the dedicated
// datasets.
//
// The full path is of the form:
//
// /pool/ext/$(POOL_UUID)/crypt/$(DATASET_TYPE)/$(BUNDLE_UUID)/bundle.zip
//                              |               | This is a per-bundle nested dataset
//                              | This is a Debug dataset
//
// NOTE: The "DumpSetupWorker" has been explicitly configured to ignore these files, so they are
// not removed. If the files used here change in the future, DumpSetupWorker should also be
// updated.
pub const BUNDLE_FILE_NAME: &str = "bundle.zip";
pub const BUNDLE_TMP_FILE_NAME_SUFFIX: &str = "bundle.zip.tmp";
