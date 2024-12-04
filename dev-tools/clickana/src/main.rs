// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::Result;
use clickana::Clickana;

mod clickana;

fn main() -> Result<()> {
    let terminal = ratatui::init();
    let app_result = Clickana::new().run(terminal);
    ratatui::restore();
    app_result
}