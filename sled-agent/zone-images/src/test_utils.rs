// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::sync::Arc;

use camino::Utf8Path;
use omicron_common::disk::DiskIdentity;
use omicron_uuid_kinds::InternalZpoolUuid;
use sled_agent_config_reconciler::InternalDiskDetails;
use sled_agent_config_reconciler::InternalDisksReceiver;
use sled_storage::config::MountConfig;

pub(crate) fn make_internal_disks_rx(
    root: &Utf8Path,
    boot_zpool: InternalZpoolUuid,
    other_zpools: &[InternalZpoolUuid],
) -> InternalDisksReceiver {
    let fake_from_zpool = |zpool: InternalZpoolUuid, is_boot_disk: bool| {
        let identity = DiskIdentity {
            vendor: "sled-agent-zone-images-test".to_string(),
            model: "fake-disk".to_string(),
            serial: zpool.to_string(),
        };
        InternalDiskDetails::fake_details(
            identity,
            zpool,
            is_boot_disk,
            None,
            None,
        )
    };
    let mount_config = MountConfig {
        root: root.to_path_buf(),
        synthetic_disk_root: root.to_path_buf(),
    };
    InternalDisksReceiver::fake_static(
        Arc::new(mount_config),
        std::iter::once(fake_from_zpool(boot_zpool, true)).chain(
            other_zpools
                .iter()
                .copied()
                .map(|pool| fake_from_zpool(pool, false)),
        ),
    )
}
