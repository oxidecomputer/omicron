// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use lazy_static::lazy_static;

lazy_static! {
    pub static ref SILO_ID: uuid::Uuid = "001de000-5110-4000-8000-000000000000"
        .parse()
        .expect("invalid uuid for builtin silo id");
}
