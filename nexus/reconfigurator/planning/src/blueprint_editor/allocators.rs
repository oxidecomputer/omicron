// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Blueprint planner resource allocation

mod external_networking;

pub use self::external_networking::ExternalNetworkingAllocator;
pub use self::external_networking::ExternalNetworkingChoice;
pub use self::external_networking::ExternalNetworkingError;
pub use self::external_networking::ExternalSnatNetworkingChoice;
