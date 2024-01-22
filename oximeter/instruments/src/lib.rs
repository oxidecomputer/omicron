// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! General-purpose types for instrumenting code to producer oximeter metrics.

// Copyright 2023 Oxide Computer Company

#[cfg(feature = "http-instruments")]
pub mod http;

#[cfg(all(
    any(feature = "link", feature = "virtual-machine"),
    target_os = "illumos"
))]
pub mod kstat;
