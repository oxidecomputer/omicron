// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Functional code for the latest versions of types.
//!
//! This module contains inherent methods, trait implementations (e.g.,
//! `Display`, `FromStr`), and other functional code attached to versioned
//! types. Per RFD 619, such code must be implemented on the latest versions
//! of each type and live in this module.
//!
//! Within this module, types are referred to using `latest::` identifiers.

macro_rules! path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "Name or ID of the "]
            #[doc = $name]
            pub $param: NameOrId,
        }
    };
}

macro_rules! id_path_param {
    ($struct:ident, $param:ident, $name:tt) => {
        id_path_param!($struct, $param, $name, Uuid);
    };

    ($struct:ident, $param:ident, $name:tt, $uuid_type:ident) => {
        #[derive(Serialize, Deserialize, JsonSchema)]
        pub struct $struct {
            #[doc = "ID of the "]
            #[doc = $name]
            #[schemars(with = "Uuid")]
            pub $param: $uuid_type,
        }
    };
}

pub(crate) use id_path_param;
pub(crate) use path_param;

mod alert;
mod disk;
mod hardware;
mod instance;
pub(crate) mod multicast;
mod networking;
mod physical_disk;
mod policy;
mod saml;
mod silo;
mod sled;
mod switch;
mod user;
