// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Custom tui widgets

use tui::style::Color;

mod animated_logo;
mod box_connector;
mod fade;
mod popup;
mod rack;

pub use animated_logo::{Logo, LogoState, LOGO_HEIGHT, LOGO_WIDTH};
pub use box_connector::{BoxConnector, BoxConnectorKind};
pub use fade::Fade;
pub use popup::{ButtonText, Popup};
pub use rack::Rack;
