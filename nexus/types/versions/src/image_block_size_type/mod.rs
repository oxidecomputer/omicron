// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Version `IMAGE_BLOCK_SIZE_TYPE` of the Nexus external API.
//!
//! The `Image` view type's `block_size` field changed from a plain `ByteCount`
//! to the constrained `BlockSize` newtype, so that the generated spec and docs
//! reflect the valid block sizes (512, 2048, or 4096).

pub mod image;
