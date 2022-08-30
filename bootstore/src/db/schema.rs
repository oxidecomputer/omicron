// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::table;

table! {
    key_share_prepares(epoch) {
        epoch -> Integer,
        share -> Text,
        share_digest -> Binary,
    }
}

table! {
    key_share_commits(epoch) {
        epoch -> Integer,
        share_digest -> Binary,
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
