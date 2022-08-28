// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use diesel::table;

table! {
    key_share_prepare(epoch) {
        epoch -> Integer,
        share -> Text,
    }
}

table! {
    key_share_commit(epoch) {
        epoch -> Integer,
    }
}

table! {
    encrypted_root_secret(epoch) {
        epoch -> Integer,
        salt -> Binary,
        secret -> Binary,
        tag -> Binary,

    }
}
