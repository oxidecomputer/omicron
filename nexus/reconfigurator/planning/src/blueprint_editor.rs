// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! High-level facilities for editing Blueprints
//!
//! See crate-level documentation for details.

mod allocators;
mod sled_editor;

pub use allocators::BlueprintResourceAllocatorInputError;
pub use allocators::ExternalNetworkingError;
pub use sled_editor::DatasetsEditError;
pub use sled_editor::DisksEditError;
pub use sled_editor::MultipleDatasetsOfKind;
pub use sled_editor::SledEditError;
pub use sled_editor::SledInputError;
pub use sled_editor::ZonesEditError;

pub(crate) use allocators::BlueprintResourceAllocator;
pub(crate) use allocators::ExternalNetworkingChoice;
pub(crate) use allocators::ExternalSnatNetworkingChoice;
pub(crate) use sled_editor::DiskExpungeDetails;
pub(crate) use sled_editor::EditedSled;
pub(crate) use sled_editor::SledEditor;
