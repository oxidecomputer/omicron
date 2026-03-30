// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Support bundle request types and data selection

use nexus_types::fm::ereport::EreportFilters;
use nexus_types::support_bundle::{
    BundleData, BundleDataCategory, BundleDataSelection, SledSelection,
};

use omicron_uuid_kinds::SledUuid;
use std::num::NonZeroU64;

/// We use "/var/tmp" to use Nexus' filesystem for temporary storage,
/// rather than "/tmp", which would keep this collected data in-memory.
pub const TEMPDIR: &str = "/var/tmp";

/// The size of piece of a support bundle to transfer to the sled agent
/// within a single streaming request.
pub const CHUNK_SIZE: NonZeroU64 = NonZeroU64::new(1024 * 1024 * 1024).unwrap();

/// Specifies the data to be collected within the Support Bundle.
#[derive(Clone)]
pub struct BundleRequest {
    /// The size of chunks to use when transferring a bundle from Nexus
    /// to a sled agent.
    ///
    /// Typically, this is CHUNK_SIZE, but can be modified for testing.
    pub transfer_chunk_size: NonZeroU64,

    /// The set of data to be included within this bundle.
    ///
    /// Maps each category to its filter. If a category is not in the map,
    /// it is excluded from the bundle.
    pub data_selection: BundleDataSelection,
}

impl BundleRequest {
    pub fn all() -> Self {
        Self {
            transfer_chunk_size: CHUNK_SIZE,
            data_selection: BundleDataSelection::all(),
        }
    }

    pub fn include_reconfigurator_data(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::Reconfigurator)
    }

    pub fn include_host_info(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::HostInfo)
    }

    pub fn include_sled_host_info(&self, id: SledUuid) -> bool {
        match self.data_selection.get(BundleDataCategory::HostInfo) {
            Some(BundleData::HostInfo(SledSelection::All)) => true,
            Some(BundleData::HostInfo(SledSelection::Specific(sleds))) => {
                sleds.contains(&id)
            }
            _ => false,
        }
    }

    pub fn get_ereport_filters(&self) -> Option<&EreportFilters> {
        match self.data_selection.get(BundleDataCategory::Ereports) {
            Some(BundleData::Ereports(filters)) => Some(filters),
            _ => None,
        }
    }

    pub fn include_sled_cubby_info(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::SledCubbyInfo)
    }

    pub fn include_sp_dumps(&self) -> bool {
        self.data_selection.contains(BundleDataCategory::SpDumps)
    }
}
