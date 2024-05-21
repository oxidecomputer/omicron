// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Symbolic representations of omicron zones and there networking resources

use super::{Enumerable, SymbolicId};
use serde::{Deserialize, Serialize};

/// A symbolic representation of external network resources mapped to an
/// individual omicron zone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OmicronZoneNetworkResources {
    external_ips: Vec<OmicronZoneExternalIp>,
    zone_nics: Vec<OmicronZoneNic>,
}

/// A symbolic representation of an `OmicronZoneExternalIpUuid`
///
/// These map to floating IPs and source NAT configs
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct OmicronZoneExternalIpUuid {
    symbolic_id: SymbolicId,
}

impl OmicronZoneExternalIpUuid {
    pub fn new(symbolic_id: SymbolicId) -> OmicronZoneExternalIpUuid {
        OmicronZoneExternalIpUuid { symbolic_id }
    }
}

impl Enumerable for OmicronZoneExternalIpUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// Symbolic representation of an external ip for a zone
///
/// We don't actually bother representing the underlying floating IPs or SNAT
/// configs. Instead we just generate consistent ones when reifying the the
/// symbolic types to concrete types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OmicronZoneExternalIp {
    Floating(OmicronZoneExternalIpUuid),
    Snat(OmicronZoneExternalIpUuid),
}

/// A symbolic representation of an `OmicronZoneNic`
///
/// We don't bother tracking all the details. We'll generate
/// consistent details of the concrete type during reification.
/// Symbolic representation of an OmicronZoneUuid
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct OmicronZoneNic {
    symbolic_id: SymbolicId,
}

impl OmicronZoneNic {
    pub fn new(symbolic_id: SymbolicId) -> OmicronZoneNic {
        OmicronZoneNic { symbolic_id }
    }
}

impl Enumerable for OmicronZoneNic {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// Symbolic representation of an OmicronZoneUuid
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord,
)]
pub struct OmicronZoneUuid {
    symbolic_id: SymbolicId,
}

impl Enumerable for OmicronZoneUuid {
    fn symbolic_id(&self) -> SymbolicId {
        self.symbolic_id
    }
}

/// An abstract representation of a zone type
///
/// This should be updated when blueprint generation supports more types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ZoneType {
    BoundaryNtp,
    Clickhouse,
    ClickhouseKeeper,
    CockroachDb,
    Crucible,
    CruciblePantry,
    ExternalDns,
    InternalDns,
    InternalNtp,
    Nexus,
    Oximeter,
}
