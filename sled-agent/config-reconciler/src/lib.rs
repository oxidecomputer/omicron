// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Machinery for sled-agent to reconcile available hardware resources with the
//! Nexus-provided `OmicronSledConfig` describing the set of disks, datasets,
//! and Omicron zones that should be in service.
//!
//! The initial entry point to this system is [`ConfigReconcilerHandle::new()`].
//! This should be called early in sled-agent startup. Later during the
//! sled-agent start process, once sled-agent has gotten its top-level
//! configuration (either from RSS, if going through rack setup, or by reading
//! from the internal disks, if cold booting), sled-agent should call
//! [`ConfigReconcilerHandle::spawn_reconciliation_task()`]. These two calls
//! will spawn several tokio tasks that run for the duration of the sled-agent
//! process:
//!
//! * A task for managing internal disks (in the `internal_disks` module of this
//!   crate). This task takes raw disks as input over a watch channel, and emits
//!   managed internal disks as output on another watch channel. Sled-agent
//!   components that care about internal disks can get access to this output
//!   channel via [`ConfigReconcilerHandle::internal_disks_rx()`]. On Gimlet and
//!   Cosmo, "internal" disks have the M.2 form factor; much existing code will
//!   refer to them as `m2` disks, but the form factor may change in future
//!   sleds. The term "internal disks" is for "the disks that are not easily
//!   swappable and can be managed before trust quorum has unlocked"; we use
//!   them to store ledgers, etc.
//! * A task for serializing ZFS operations on datasets (in the
//!   `dataset_serialization_task` module of this crate). This task takes
//!   requests over an `mpsc` channel. This channel is not exposed directly;
//!   instead, various operations that go through this task are implemented as
//!   methods on `ConfigReconcilerHandle` (e.g.,
//!   [`ConfigReconcilerHandle::nested_dataset_ensure_mounted()]).
//! * A task for managing the ledgered `OmicronSledConfig` for this sled. This
//!   task takes requests over an `mpsc` channel, and also emits the state of
//!   the currently-ledgered config over a watch channel. This watch channel is
//!   not directly exposed outside this crate, but is indirectly accessible via
//!   specific operations like [`ConfigReconcilerHandle::inventory()`].
//! * A task that performs reconciliation of all of the above tasks' outputs and
//!   the current `OmicronSledConfig`. This task does the actual work to manage
//!   external (in Gimlet and Cosmo, U.2 form factor) disks, datasets on
//!   external disks, and Omicron service zones. It emits its results on a watch
//!   channel. This watch channel is not exposed directly, but specific views of
//!   it are available via methods like
//!   [`ConfigReconcilerHandle::timesync_status()`] and
//!   [`ConfigReconcilerHandle::inventory()`].

// TODO-cleanup Remove once we've filled in all the `unimplemented!()`s that
// will make use of various arguments and fields.
#![allow(dead_code)]

mod dataset_serialization_task;
mod disks_common;
mod dump_setup_task;
mod handle;
mod internal_disks;
mod ledger;
mod raw_disks;
mod reconciler_task;
mod sled_agent_facilities;

// TODO-cleanup Make this private once the reconciler uses it instead of
// sled-agent proper.
pub mod dump_setup;

pub use dataset_serialization_task::DatasetTaskError;
pub use dataset_serialization_task::NestedDatasetDestroyError;
pub use dataset_serialization_task::NestedDatasetEnsureError;
pub use dataset_serialization_task::NestedDatasetListError;
pub use dataset_serialization_task::NestedDatasetMountError;
pub use handle::AvailableDatasetsReceiver;
pub use handle::ConfigReconcilerHandle;
pub use handle::ConfigReconcilerSpawnToken;
pub use handle::InventoryError;
pub use handle::ReconcilerInventory;
pub use handle::TimeSyncConfig;
pub use internal_disks::InternalDisks;
pub use internal_disks::InternalDisksReceiver;
pub use ledger::LedgerArtifactConfigError;
pub use ledger::LedgerNewConfigError;
pub use ledger::LedgerTaskError;
pub use raw_disks::RawDisksSender;
pub use reconciler_task::CurrentlyManagedZpools;
pub use reconciler_task::CurrentlyManagedZpoolsReceiver;
pub use reconciler_task::TimeSyncError;
pub use reconciler_task::TimeSyncStatus;
pub use sled_agent_facilities::SledAgentArtifactStore;
pub use sled_agent_facilities::SledAgentFacilities;
