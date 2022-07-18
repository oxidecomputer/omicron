// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Fixed (hardcoded) data that gets inserted into the database programmatically
//! either when the rack is set up or when Nexus starts up.

use lazy_static::lazy_static;

pub mod role_assignment;
pub mod role_builtin;
pub mod silo;
pub mod silo_user;
pub mod user_builtin;

lazy_static! {
    /* See nexus-authn for where this uuid comes from. */
    pub static ref FLEET_ID: uuid::Uuid =
        "001de000-1334-4000-8000-000000000000"
            .parse()
            .expect("invalid uuid for builtin fleet id");
}

#[cfg(test)]
fn assert_valid_uuid(id: &uuid::Uuid) {
    match id.get_version() {
        Some(uuid::Version::Random) => (),
        _ => panic!("invalid v4 uuid: {:?}", id),
    };

    match id.get_variant() {
        uuid::Variant::RFC4122 => (),
        _ => panic!("unexpected variant in uuid: {:?}", id),
    };
}

#[cfg(test)]
mod test {
    use super::assert_valid_uuid;
    use super::FLEET_ID;

    #[test]
    fn test_builtin_fleet_id_is_valid() {
        assert_valid_uuid(&FLEET_ID);
    }
}
