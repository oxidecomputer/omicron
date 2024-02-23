// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! A background thread for managing the VMM reservoir

use illumos_utils::vmm_reservoir;
use omicron_common::api::external::ByteCount;
use slog::Logger;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use tokio::sync::{oneshot, watch};

use sled_hardware::HardwareManager;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to create reservoir: {0}")]
    Reservoir(#[from] vmm_reservoir::Error),

    #[error("Invalid reservoir configuration: {0}")]
    ReservoirConfig(String),

    #[error("VmmReservoirManager is currently busy")]
    Busy,

    #[error("VmmReservoirManager has shutdown")]
    Shutdown,

    #[error(
        "Communication error with VmmReservoirManager: ReplySenderDropped"
    )]
    ReplySenderDropped,
}

#[derive(Debug, Clone, Copy)]
pub enum ReservoirMode {
    Size(u32),
    Percentage(u8),
}

impl ReservoirMode {
    /// Return a configuration of the VMM reservoir as either a percentage of
    /// DRAM or as an exact size in MiB.
    ///
    /// Panic upon invalid configuration
    pub fn from_config(
        percentage: Option<u8>,
        size_mb: Option<u32>,
    ) -> Option<ReservoirMode> {
        match (percentage, size_mb) {
            (None, None) => None,
            (Some(p), None) => Some(ReservoirMode::Percentage(p)),
            (None, Some(mb)) => Some(ReservoirMode::Size(mb)),
            (Some(_), Some(_)) => panic!(
                "only one of vmm_reservoir_percentage and \
                vmm_reservoir_size_mb is allowed"
            ),
        }
    }
}

/// A message sent from [`VmmReservoirManagerHandle`] to [`VmmReservoirManager`]
enum ReservoirManagerMsg {
    SetReservoirSize {
        mode: ReservoirMode,
        reply_tx: oneshot::Sender<Result<(), Error>>,
    },
}

/// A mechanism to interact with the [`VmmReservoirManager`]
pub struct VmmReservoirManagerHandle {
    reservoir_size: Arc<AtomicU64>,
    tx: flume::Sender<ReservoirManagerMsg>,
    watch_rx: watch::Receiver<Option<ByteCount>>,
    _manager_handle: thread::JoinHandle<()>,
}

impl VmmReservoirManagerHandle {
    /// Returns the last-set size of the reservoir
    pub fn reservoir_size(&self) -> ByteCount {
        self.reservoir_size.load(Ordering::SeqCst).try_into().unwrap()
    }

    /// Return a [`tokio::sync::watch::Receiver`] whose value changes when the
    /// reservoir size is updated.
    #[allow(unused)]
    pub fn watcher(&self) -> watch::Receiver<Option<ByteCount>> {
        self.watch_rx.clone()
    }

    /// Tell the [`VmmReservoirManager`] to set the reservoir size and wait for
    /// a response.
    ///
    /// Returns an error if the allocation fails or if the manager is currently
    /// busy handling another request.
    //
    // It's anticipated this will be used to change the reservation
    #[allow(unused)]
    pub async fn set_reservoir_size(
        &self,
        mode: ReservoirMode,
    ) -> Result<(), Error> {
        let (tx, rx) = oneshot::channel();
        let msg = ReservoirManagerMsg::SetReservoirSize { mode, reply_tx: tx };
        if let Err(e) = self.tx.try_send(msg) {
            return Err(match e {
                flume::TrySendError::Full(e) => Error::Busy,
                flume::TrySendError::Disconnected(e) => Error::Shutdown,
            });
        }
        rx.await.map_err(|_| Error::ReplySenderDropped)?
    }
}

/// Manage the VMM reservoir in a background thread
pub struct VmmReservoirManager {
    reservoir_size: Arc<AtomicU64>,
    rx: flume::Receiver<ReservoirManagerMsg>,
    watch_tx: watch::Sender<Option<ByteCount>>,
    log: Logger,
}

impl VmmReservoirManager {
    pub fn spawn(
        log: &Logger,
        hardware_manager: HardwareManager,
        reservoir_mode: Option<ReservoirMode>,
    ) -> VmmReservoirManagerHandle {
        let log = log.new(o!("component" => "VmmReservoirManager"));
        // We use a rendevous channel to only allow one request at a time.
        // Resizing a reservoir may block the thread for up to two minutes, so
        // we want to ensure it is complete before allowing another call.
        let (tx, rx) = flume::bounded(0);
        let reservoir_size = Arc::new(AtomicU64::new(0));
        let (watch_tx, watch_rx) = watch::channel(None);
        let manager = VmmReservoirManager {
            reservoir_size: reservoir_size.clone(),
            rx,
            watch_tx,
            log,
        };
        let _manager_handle = thread::spawn(move || {
            manager.run(hardware_manager, reservoir_mode)
        });
        VmmReservoirManagerHandle {
            reservoir_size,
            tx,
            watch_rx,
            _manager_handle,
        }
    }

    fn run(
        self,
        hardware_manager: HardwareManager,
        reservoir_mode: Option<ReservoirMode>,
    ) {
        match reservoir_mode {
            None => warn!(self.log, "Not using VMM reservoir"),
            Some(ReservoirMode::Size(0))
            | Some(ReservoirMode::Percentage(0)) => {
                warn!(
                    self.log,
                    "Not using VMM reservoir (size 0 bytes requested)"
                )
            }
            Some(mode) => {
                if let Err(e) = self.set_reservoir_size(&hardware_manager, mode)
                {
                    error!(self.log, "Failed to setup VMM reservoir: {e}");
                }
            }
        }

        while let Ok(msg) = self.rx.recv() {
            let ReservoirManagerMsg::SetReservoirSize { mode, reply_tx } = msg;
            match self.set_reservoir_size(&hardware_manager, mode) {
                Ok(()) => {
                    let _ = reply_tx.send(Ok(()));
                }
                Err(e) => {
                    error!(self.log, "Failed to setup VMM reservoir: {e}");
                    let _ = reply_tx.send(Err(e));
                }
            }
        }
    }

    /// Sets the VMM reservoir to the requested percentage of usable physical
    /// RAM or to a size in MiB. Either mode will round down to the nearest
    /// aligned size required by the control plane.
    fn set_reservoir_size(
        &self,
        hardware: &sled_hardware::HardwareManager,
        mode: ReservoirMode,
    ) -> Result<(), Error> {
        let hardware_physical_ram_bytes = hardware.usable_physical_ram_bytes();
        let req_bytes = match mode {
            ReservoirMode::Size(mb) => {
                let bytes = ByteCount::from_mebibytes_u32(mb).to_bytes();
                if bytes > hardware_physical_ram_bytes {
                    return Err(Error::ReservoirConfig(format!(
                        "cannot specify a reservoir of {bytes} bytes when \
                        physical memory is {hardware_physical_ram_bytes} bytes",
                    )));
                }
                bytes
            }
            ReservoirMode::Percentage(percent) => {
                if !matches!(percent, 1..=99) {
                    return Err(Error::ReservoirConfig(format!(
                        "VMM reservoir percentage of {} must be between 0 and \
                        100",
                        percent
                    )));
                };
                (hardware_physical_ram_bytes as f64 * (percent as f64 / 100.0))
                    .floor() as u64
            }
        };

        let req_bytes_aligned = vmm_reservoir::align_reservoir_size(req_bytes);

        if req_bytes_aligned == 0 {
            warn!(
                self.log,
                "Requested reservoir size of {} bytes < minimum aligned size \
                of {} bytes",
                req_bytes,
                vmm_reservoir::RESERVOIR_SZ_ALIGN
            );
            return Ok(());
        }

        // The max ByteCount value is i64::MAX, which is ~8 million TiB.
        // As this value is either a percentage of DRAM or a size in MiB
        // represented as a u32, constructing this should always work.
        let reservoir_size = ByteCount::try_from(req_bytes_aligned).unwrap();
        if let ReservoirMode::Percentage(percent) = mode {
            info!(
                self.log,
                "{}% of {} physical ram = {} bytes)",
                percent,
                hardware_physical_ram_bytes,
                req_bytes,
            );
        }
        info!(self.log, "Setting reservoir size to {reservoir_size} bytes");
        vmm_reservoir::ReservoirControl::set(reservoir_size)?;

        self.reservoir_size.store(reservoir_size.to_bytes(), Ordering::SeqCst);
        self.watch_tx.send_replace(Some(reservoir_size));
        info!(
            self.log,
            "Finished setting reservoir size to {reservoir_size} bytes"
        );

        Ok(())
    }
}
