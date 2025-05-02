// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_client::types::SpComponentCaboose;
use hubtools::CabooseError;

pub mod sp_test_state;
pub mod step_through;
pub mod test_artifacts;
pub mod updates;

pub fn cabooses_equal(c1: &SpComponentCaboose, c2: &hubtools::Caboose) -> bool {
    let (Ok(name), Ok(board), Ok(version), Ok(git_commit)) =
        (c2.name(), c2.board(), c2.version(), c2.git_commit())
    else {
        return false;
    };

    if c1.name.as_bytes() != name
        || c1.board.as_bytes() != board
        || c1.version.as_bytes() != version
        || c1.git_commit.as_bytes() != git_commit
    {
        return false;
    }

    match (&c1.sign, c2.sign()) {
        (Some(c1sign), Ok(c2sign)) => c1sign.as_bytes() == c2sign,
        (None, Err(CabooseError::MissingTag { .. })) => true,
        (Some(_), Err(CabooseError::MissingTag { .. }))
        | (_, Err(CabooseError::TlvcReadError(..)))
        | (None, Ok(_)) => false,
    }
}
