// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::SitrepBuilder;
use crate::analysis_input::Input;

pub fn analyze(
    _input: &Input,
    _builder: &mut SitrepBuilder<'_>,
) -> anyhow::Result<()> {
    anyhow::bail!("FM analysis is not yet implemented")
}
