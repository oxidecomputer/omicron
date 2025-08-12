// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! APIs, both internal and external.

pub mod external;
pub mod internal;

/// API versioning header name used across Omicron APIs.
pub const VERSION_HEADER: http::HeaderName =
    http::HeaderName::from_static("api-version");
