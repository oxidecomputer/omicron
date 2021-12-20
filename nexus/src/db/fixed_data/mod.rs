// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//! Fixed (hardcoded) data that gets inserted into the database programmatically
//! either when the rack is set up or when Nexus starts up.

//
// Here's a proposed convention for choosing uuids that we hardcode into
// Omicron.
//
//   001de000-05e4-4000-8000-000000000000
//   ^^^^^^^^ ^^^^ ^    ^
//       +-----|---|----|-------------------- prefix used for all reserved uuids
//             |   |    |                     (looks a bit like "oxide")
//             +---|----|-------------------- says what kind of resource it is
//                 |    |                     ("05e4" looks a bit like "user")
//                 +----|-------------------- v4
//                      +-------------------- variant 1 (most common for v4)
//
// This way, the uuids stand out a bit.  It's not clear if this convention will
// be very useful, but it beats a random uuid.  (Is it safe to do this?  Well,
// these are valid v4 uuids, and they're as unlikely to collide with a future
// uuid as any random uuid is.)
//

pub mod role_builtin;
pub mod user_builtin;
