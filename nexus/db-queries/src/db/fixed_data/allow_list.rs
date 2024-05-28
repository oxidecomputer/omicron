// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2024 Oxide Computer Company

//! Fixed data for source IP allowlist implementation.

/// UUID of singleton source IP allowlist.
pub static USER_FACING_SERVICES_ALLOW_LIST_ID: uuid::Uuid =
    uuid::uuid!("001de000-a110-4000-8000-000000000000");
