// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::table;

table! {
    key_shares(epoch) {
        epoch -> Integer,
        share -> Text,
        share_digest -> Binary,
        committed -> Bool,
    }
}

table! {
    encrypted_root_secrets(epoch) {
        epoch -> Integer,
        salt -> Binary,
        secret -> Binary,
        tag -> Binary,

    }
}
