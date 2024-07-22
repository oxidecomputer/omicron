// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Types related to descriptions of Omicron-managed zones.

use std::net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV6};

use omicron_common::{
    api::{
        external::Generation,
        internal::shared::{NetworkInterface, SourceNatConfig},
    },
    zpool_name::ZpoolName,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

