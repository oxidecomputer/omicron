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

pub mod silo {
    lazy_static::lazy_static! {
        pub static ref SILO_ID: uuid::Uuid =
            "001de000-5110-4000-8000-000000000000"
                .parse()
                .expect("invalid uuid for builtin silo id");
    }
}

pub mod silo_user {
    lazy_static::lazy_static! {
        pub static ref USER_TEST_PRIVILEGED_ID: uuid::Uuid =
            // "4007" looks a bit like "root".
            "001de000-05e4-4000-8000-000000004007"
                .parse()
                .expect("invalid uuid for user test privileged");

        pub static ref USER_TEST_UNPRIVILEGED_ID: uuid::Uuid =
            // 60001 is the decimal uid for "nobody" on Helios.
            "001de000-05e4-4000-8000-000000060001"
                .parse()
                .expect("invalid uuid for user test unprivileged");
    }
}

pub mod user_builtin {
    lazy_static::lazy_static! {
        /// Internal user used for seeding initial database data
        // NOTE: This uuid is duplicated in dbinit.sql.
        pub static ref USER_DB_INIT_ID: uuid::Uuid =
                // "0001" is the first possible user that wouldn't be confused with
                // 0, or root.
                "001de000-05e4-4000-8000-000000000001"
                    .parse()
                    .expect("invalid uuid for built-in user");

        /// Internal user for performing operations to manage the
        /// provisioning of services across the fleet.
        pub static ref USER_SERVICE_BALANCER_ID: uuid::Uuid =
                "001de000-05e4-4000-8000-00000000bac3"
                    .parse()
                    .expect("invalid uuid for built-in user");

        /// Internal user used by Nexus when handling internal API requests
        pub static ref USER_INTERNAL_API_ID: uuid::Uuid =
                "001de000-05e4-4000-8000-000000000002"
                    .parse()
                    .expect("invalid uuid for built-in user");

        /// Internal user used by Nexus to read privileged control plane data
        pub static ref USER_INTERNAL_READ_ID: uuid::Uuid =
                // "4ead" looks like "read
                "001de000-05e4-4000-8000-000000004ead"
                    .parse()
                    .expect("invalid uuid for built-in user");

        /// Internal user used by Nexus when recovering sagas
        pub static ref USER_SAGA_RECOVERY_ID: uuid::Uuid =
                // "3a8a" looks a bit like "saga"
                "001de000-05e4-4000-8000-000000003a8a"
                    .parse()
                    .expect("invalid uuid for built-in user");

        /// Internal user used by Nexus when authenticating external requests
        pub static ref USER_EXTERNAL_AUTHN_ID: uuid::Uuid =
                "001de000-05e4-4000-8000-000000000003"
                    .parse()
                    .expect("invalid uuid for built-in user");
    }
}
