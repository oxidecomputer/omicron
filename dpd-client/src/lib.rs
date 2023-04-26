// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2023 Oxide Computer Company

#![allow(clippy::redundant_closure_call)]
#![allow(clippy::needless_lifetimes)]
#![allow(clippy::match_single_binding)]
#![allow(clippy::clone_on_copy)]
#![allow(clippy::unnecessary_to_owned)]
// The progenitor-generated API for dpd currently incorporates a type from
// oximeter, which includes a docstring that has a doc-test in it.
// That test passes for code that lives in omicron, but fails for code imported
// by omicron.
#![allow(rustdoc::broken_intra_doc_links)]

use slog::Logger;

include!(concat!(env!("OUT_DIR"), "/dpd-client.rs"));

/// State maintained by a [`Client`].
#[derive(Clone, Debug)]
pub struct ClientState {
    /// An arbitrary tag used to identify a client, for controlling things like
    /// per-client settings.
    pub tag: String,
    /// Used for logging requests and responses.
    pub log: Logger,
}
