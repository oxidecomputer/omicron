// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Fixed (hardcoded) data that gets inserted into the database programmatically
//! either when the rack is set up or when Nexus starts up.

// Here's a proposed convention for choosing uuids that we hardcode into
// Omicron.
//
//   001de000-05e4-4000-8000-000000000000
//   ^^^^^^^^ ^^^^ ^    ^
//       +-----|---|----|-------------------- prefix used for all reserved uuids
//             |   |    |                     (looks a bit like "oxide")
//             +---|----|-------------------- says what kind of resource it is
//                                            (see below)
//                 +----|-------------------- v4
//                      +-------------------- variant 1 (most common for v4)
//
// This way, the uuids stand out a bit.  It's not clear if this convention will
// be very useful, but it beats a random uuid.  (Is it safe to do this?  Well,
// these are valid v4 uuids, and they're as unlikely to collide with a future
// uuid as any random uuid is.)
//
// The specific kinds of resources to which we've assigned uuids:
//
//    UUID PREFIX     RESOURCE
//    001de000-05e4   built-in users ("05e4" looks a bit like "user")
//    001de000-1334   built-in fleet ("1334" looks like the "leet" in "fleet")
//    001de000-5110   built-in silo ("5110" looks like "silo")
//    001de000-4401   built-in services project
//    001de000-074c   built-in services vpc
//    001de000-c470   built-in services vpc subnets
//    001de000-all0   singleton ID for source IP allowlist ("all0" is like "allow")
//    001de000-7768   singleton ID for webhook probe event ('wh' for 'webhook'
//                    is ascii 0x77 0x68).

use std::sync::LazyLock;

pub mod allow_list;
pub mod project;
pub mod role_assignment;
pub mod role_builtin;
pub mod silo;
pub mod silo_user;
pub mod user_builtin;
pub mod vpc;
pub mod vpc_firewall_rule;
pub mod vpc_subnet;

/* See above for where this uuid comes from. */
pub static FLEET_ID: LazyLock<uuid::Uuid> = LazyLock::new(|| {
    "001de000-1334-4000-8000-000000000000"
        .parse()
        .expect("invalid uuid for builtin fleet id")
});

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
    use super::FLEET_ID;
    use super::allow_list::USER_FACING_SERVICES_ALLOW_LIST_ID;
    use super::assert_valid_uuid;

    #[test]
    fn test_builtin_fleet_id_is_valid() {
        assert_valid_uuid(&FLEET_ID);
    }

    #[test]
    fn test_allowlist_id_is_valid() {
        assert_valid_uuid(&USER_FACING_SERVICES_ALLOW_LIST_ID);
    }
}
