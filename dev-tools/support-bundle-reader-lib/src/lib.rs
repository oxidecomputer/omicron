// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities to inspect support bundles

mod bundle_accessor;
mod dashboard;
mod index;

// TODO: Not all this needs to be external

pub use bundle_accessor::AsyncZipFile;
pub use bundle_accessor::BoxedFileAccessor;
pub use bundle_accessor::FileAccessor;
pub use bundle_accessor::InternalApiAccess;
pub use bundle_accessor::LocalFileAccess;
pub use bundle_accessor::SupportBundleAccessor;
pub use dashboard::SupportBundleDashboard;
pub use dashboard::run_dashboard;
pub use index::SupportBundleIndex;
