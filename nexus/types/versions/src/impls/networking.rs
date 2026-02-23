// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::latest;
use oxnet::IpNet;

impl From<IpNet> for latest::networking::AddressLotBlockCreate {
    fn from(ipnet: IpNet) -> Self {
        match ipnet {
            IpNet::V4(v4) => Self {
                first_address: v4.first_addr().into(),
                last_address: v4.last_addr().into(),
            },
            IpNet::V6(v6) => Self {
                first_address: v6.first_addr().into(),
                last_address: v6.last_addr().into(),
            },
        }
    }
}
