// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Implementation of the [`crate::FsmState`] for coordinating reconfigurations

use crate::{FsmCtx, FsmState};

/// The [`FsmState`] for coordinating a reconfiguration
pub struct Coordinating {}
