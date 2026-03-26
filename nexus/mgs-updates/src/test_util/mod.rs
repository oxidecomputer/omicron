// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_client::types::SpComponentCaboose;
use hubtools::CabooseError;

pub mod host_phase_2_test_state;
pub mod sp_test_state;
pub mod step_through;
pub mod test_artifacts;
pub mod updates;

/// Returns whether caboose `c1` (reported by an SP via MGS) matches caboose
/// `c2` (constructed or read via `hubtools`)
pub fn cabooses_equal(c1: &SpComponentCaboose, c2: &hubtools::Caboose) -> bool {
    let SpComponentCaboose {
        board,
        // epoch is currently ignored.  `hubtools::Caboose` does not expose it
        // so we cannot check it.
        epoch: _epoch,
        git_commit,
        name,
        sign,
        version,
    } = c1;

    let (Ok(c2name), Ok(c2board), Ok(c2version), Ok(c2git_commit)) =
        (c2.name(), c2.board(), c2.version(), c2.git_commit())
    else {
        return false;
    };

    if name.as_bytes() != c2name
        || board.as_bytes() != c2board
        || version.as_bytes() != c2version
        || git_commit.as_bytes() != c2git_commit
    {
        return false;
    }

    match (&sign, c2.sign()) {
        (Some(c1sign), Ok(c2sign)) => c1sign.as_bytes() == c2sign,
        (None, Err(CabooseError::MissingTag { .. })) => true,
        (Some(_), Err(CabooseError::MissingTag { .. }))
        | (_, Err(CabooseError::TlvcReadError(..)))
        | (None, Ok(_)) => false,
    }
}
