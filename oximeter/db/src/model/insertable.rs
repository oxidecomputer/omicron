// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
//
// Copyright 2024 Oxide Computer Company

//! Methods for inserting data into a table via the native interface.

use oximeter::AuthzScope;
use oximeter::TimeseriesDescription;
use oximeter::TimeseriesSchema;
use oximeter::Units;
use std::num::NonZeroU8;
use crate::native::block::Block;
use crate::native::Error;

pub trait ToBlock: Sized {
    fn to_block(items: &[Self]) -> Result<Block, Error>;
}

impl ToBlock for TimeseriesSchema {
}

#[test]
fn foo() {
    let items = vec![String::from("foo")];
    Inserter::insert(items);
}
