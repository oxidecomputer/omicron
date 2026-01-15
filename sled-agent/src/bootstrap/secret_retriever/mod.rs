// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Key retrieval mechanisms for use by [`key_manager::KeyManager`]

mod global;
mod hardcoded;
mod lrtq;
mod tq;
mod tq_or_lrtq;

pub use global::GlobalSecretRetriever;
pub use tq_or_lrtq::TqOrLrtqSecretRetriever;
