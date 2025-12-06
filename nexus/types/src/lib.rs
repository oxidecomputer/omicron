// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Low-level crate defining types used in nexus's internal and external APIs.
//!
//! This crate is conceptually still a part of `nexus` and is not intended to
//! be usable in any other context. The decision of what lives in this crate is
//! primarily driven by what types the database model needs to know about, to
//! allow the model to be built as an independent crate from the bulk of nexus
//! to shorten compilation times.
//!
//! That this crate includes `identity` (traits related to database resources
//! and assets`) and `{internal_api,external_api}::params` (types defining
//! requests to create or update resources) should be unsurprising. That it also
//! contains `external_api::views` is somewhat surprising: why should the
//! database model know about views? The existence (and inclusion) of
//! `external_api::shared` is the main driver: We have types that are used both
//! as parameters and as views, and we don't really want to duplicate them.
//!
//! We could move `external_api::views` and back to the top-level `nexus` crate,
//! but then the views would be split between that and `external_api::shared` in
//! this crate. We could also consider some way of eliminating
//! `external_api::shared` entirely, but all attempts to do so thus far have
//! appeared to be outright worse than leaving things as they are now.
//!
//! This means we have to define `From` (and related) conversions between
//! params/views and model types in the `db-model` crate due to Rust orphan
//! rules, so our model layer knows about our views. That seems to be a
//! relatively minor offense, so it's the way we leave things for now.

pub mod authn;
pub mod deployment;
pub mod external_api;
pub mod fm;
pub mod identity;
pub mod internal_api;
pub mod inventory;
pub mod multicast;
pub mod quiesce;
pub mod silo;
