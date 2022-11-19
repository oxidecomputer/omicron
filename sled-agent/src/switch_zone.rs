// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Special-case software for handling for the switch zone.
//!
//! Most zones are launched via the ServiceManager, which is controlled by the
//! sled agent. This switch zone, however, must be launched before the sled
//! agent is running, and before the underlay network is operational.

use anyhow::anyhow;
use crate::illumos::dladm::{Dladm, Etherstub};
use crate::illumos::link::{Link, VnicAllocator};
use crate::illumos::running_zone::{InstalledZone, RunningZone};
use crate::params::DendriteAsic;
use crate::params::ServiceType;
use crate::params::ServiceZoneRequest;
use crate::params::ZoneType;
use crate::smf_helper::SmfHelper;
use omicron_common::address::DENDRITE_PORT;
use omicron_common::address::MGS_PORT;
use slog::Logger;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

struct Task {
    // A signal for the initializer task to terminate
    exit_tx: oneshot::Sender<()>,
    // A task repeatedly trying to initialize the zone
    initializer: JoinHandle<()>,
}

// Describes the Switch Zone state.
enum SwitchZone {
    // The switch zone is not currently running.
    Disabled,
    // The Zone is still initializing - it may be awaiting the initialization
    // of certain links.
    Initializing {
        // The request for the zone
        request: ServiceZoneRequest,
        // A background task
        worker: Option<Task>,
    },
    // The Zone is currently running.
    Running(RunningZone),
}

struct SwitchZoneManagerInner {
    log: Logger,
    switch_zone: Mutex<SwitchZone>,
    vnic_allocator: VnicAllocator<Etherstub>,
    stub_scrimlet: Option<bool>,
}

/// A wrapper around the Switch Zone, with methods to enable or disable it.
#[derive(Clone)]
pub struct SwitchZoneManager {
    inner: Arc<SwitchZoneManagerInner>,
}

impl SwitchZoneManager {
    pub fn new(log: &Logger, etherstub: Etherstub, stub_scrimlet: Option<bool>) -> Self {
        Self {
            inner: Arc::new(SwitchZoneManagerInner {
                log: log.clone(),
                switch_zone: Mutex::new(SwitchZone::Disabled),
                vnic_allocator: VnicAllocator::new("Switch", etherstub),
                stub_scrimlet,
            })
        }
    }

    pub async fn ensure_scrimlet_services_active(&self) {
        info!(self.inner.log, "Ensuring scrimlet services (enabling services)");

        let services = match self.inner.stub_scrimlet {
            Some(_) => {
                vec![ServiceType::Dendrite { asic: DendriteAsic::TofinoStub }]
            }
            None => {
                vec![
                    ServiceType::ManagementGatewayService,
                    ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic },
                    ServiceType::Tfport { pkt_source: "tfpkt0".to_string() },
                ]
            }
        };

        let request = ServiceZoneRequest {
            id: Uuid::new_v4(),
            zone_type: ZoneType::Switch,
            addresses: vec![],
            // TODO TODO TODO TODO????
//            addresses: vec![self.inner.switch_ip()],
            gz_addresses: vec![],
            services,
        };

        self.ensure_switch(Some(request)).await;
    }

    pub async fn ensure_scrimlet_services_deactive(&self) {
        info!(self.inner.log, "Ensuring scrimlet services (disabling services)");
        self.ensure_switch(None).await;
    }

    // TODO: We don't need to use a ServiceZoneRequest here?
    // We could provide a more switch-specific type?
    async fn ensure_switch(&self, request: Option<ServiceZoneRequest>) {
        let log = &self.inner.log;
        let mut switch_zone = self.inner.switch_zone.lock().await;

        match (&mut *switch_zone, request) {
            (SwitchZone::Disabled, Some(request)) => {
                info!(log, "Enabling switch zone (new)");
                let mgr = self.clone();
                let (exit_tx, exit_rx) = oneshot::channel();
                *switch_zone = SwitchZone::Initializing {
                    request,
                    worker: Some(Task {
                        exit_tx,
                        initializer: tokio::task::spawn(async move {
                            mgr.initialize_zone_loop(exit_rx).await
                        }),
                    }),
                };
            }
            (SwitchZone::Initializing { .. }, Some(_)) => {
                info!(log, "Enabling switch zone (already underway)");
            }
            (SwitchZone::Running(_), Some(_)) => {
                info!(log, "Enabling switch zone (already complete)");
            }
            (SwitchZone::Disabled, None) => {
                info!(log, "Disabling switch zone (already complete)");
            }
            (SwitchZone::Initializing { worker, .. }, None) => {
                info!(log, "Disabling switch zone (was initializing)");
                let worker = worker
                    .take()
                    .expect("Initializing without background task");
                // If this succeeds, we told the background task to exit
                // successfully. If it fails, the background task already
                // exited.
                let _ = worker.exit_tx.send(());
                worker
                    .initializer
                    .await
                    .expect("Switch initializer task panicked");
                *switch_zone = SwitchZone::Disabled;
            }
            (SwitchZone::Running(_), None) => {
                info!(log, "Disabling switch zone (was running)");
                *switch_zone = SwitchZone::Disabled;
            }
        }
    }

    // Body of a tokio task responsible for running until the switch zone is
    // inititalized, or it has been told to stop.
    async fn initialize_zone_loop(&self, mut exit_rx: oneshot::Receiver<()>) {
        loop {
            {
                let mut switch_zone = self.inner.switch_zone.lock().await;
                match &*switch_zone {
                    SwitchZone::Initializing { request, .. } => {
                        match self.initialize_zone(&request).await {
                            Ok(zone) => {
                                *switch_zone = SwitchZone::Running(zone);
                                return;
                            }
                            Err(err) => {
                                warn!(
                                    self.inner.log,
                                    "Failed to initialize switch zone: {err}"
                                );
                            }
                        }
                    }
                    _ => return,
                }
            }

            tokio::select! {
                // If we've been told to stop trying, bail.
                _ = &mut exit_rx => return,

                // Poll for the device every second - this timeout is somewhat
                // arbitrary, but we probably don't want to use backoff here.
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => (),
            };
        }
    }

    // Check the services intended to run in the zone to determine whether any
    // physical devices need to be mapped into the zone when it is created.
    fn devices_needed(
        req: &ServiceZoneRequest,
    ) -> Result<Vec<String>, anyhow::Error> {
        let mut devices = vec![];
        for svc in &req.services {
            match svc {
                ServiceType::Dendrite { asic: DendriteAsic::TofinoAsic } => {
                    // When running on a real sidecar, we need the /dev/tofino
                    // device to talk to the tofino ASIC.
                    devices.push("/dev/tofino".to_string());
                }
                _ => (),
            }
        }

        for dev in &devices {
            if !Path::new(dev).exists() {
                return Err(anyhow!("Missing device: {dev}"));
            }
        }
        Ok(devices)
    }

    // Check the services intended to run in the zone to determine whether any
    // physical links or vnics need to be mapped into the zone when it is created.
    //
    // NOTE: This function is implemented to return the first link found, under
    // the assumption that "at most one" would be necessary.
    fn link_needed(
        &self,
        req: &ServiceZoneRequest,
    ) -> Result<Option<Link>, anyhow::Error> {
        for svc in &req.services {
            match svc {
                ServiceType::Tfport { pkt_source } => {
                    // The tfport service requires a MAC device to/from which sidecar
                    // packets may be multiplexed.  If the link isn't present, don't
                    // bother trying to start the zone.
                    match Dladm::verify_link(pkt_source) {
                        Ok(link) => {
                            return Ok(Some(link));
                        }
                        Err(_) => {
                            return Err(anyhow!("Missing link: {pkt_source}"));
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(None)
    }

    // Check the services intended to run in the zone to determine whether any
    // additional privileges need to be enabled for the zone.
    fn privs_needed(req: &ServiceZoneRequest) -> Vec<String> {
        let mut needed = Vec::new();
        for svc in &req.services {
            if let ServiceType::Tfport { .. } = svc {
                needed.push("default".to_string());
                needed.push("sys_dl_config".to_string());
            }
        }
        needed
    }

    async fn initialize_zone(
        &self,
        request: &ServiceZoneRequest,
    ) -> Result<RunningZone, anyhow::Error> {
        let device_names = Self::devices_needed(request)?;
        let link = self.link_needed(request)?;
        let limit_priv = Self::privs_needed(request);

        let devices: Vec<zone::Device> = device_names
            .iter()
            .map(|d| zone::Device { name: d.to_string() })
            .collect();

        let installed_zone = InstalledZone::install(
            &self.inner.log,
            &self.inner.vnic_allocator,
            &request.zone_type.to_string(),
            // unique_name=
            None,
            // dataset=
            &[],
            &devices,
            // opte_ports=
            vec![],
            link,
            limit_priv,
        )
        .await?;

        let running_zone = RunningZone::boot(installed_zone).await?;

        /*
        for addr in &request.addresses {
            info!(
                self.inner.log,
                "Ensuring address {} exists",
                addr.to_string()
            );
            let addr_request =
                AddressRequest::new_static(IpAddr::V6(*addr), None);
            running_zone.ensure_address(addr_request).await?;
            info!(
                self.inner.log,
                "Ensuring address {} exists - OK",
                addr.to_string()
            );
        }
        */

        /*
        let gateway = self.inner.underlay_address;
        running_zone.add_default_route(gateway).await.map_err(|err| {
            Error::ZoneCommand { intent: "Adding Route".to_string(), err }
        })?;
        */

        for service in &request.services {
            // TODO: Related to
            // https://github.com/oxidecomputer/omicron/pull/1124 , should we
            // avoid importing this manifest?
            debug!(self.inner.log, "importing manifest");

            let smfh = SmfHelper::new(&running_zone, service);
            smfh.import_manifest()?;

            match &service {
                // TODO: Clean this up?
                ServiceType::Nexus { .. } => panic!("NO"),
                ServiceType::InternalDns { .. } => panic!("NO"),
                ServiceType::Oximeter => panic!("NO"),
                ServiceType::ManagementGatewayService => {
                    info!(self.inner.log, "Setting up MGS service");

                    let address = request.addresses[0];
                    smfh.setprop("config/id", request.id)?;
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, MGS_PORT),
                    )?;
                    smfh.refresh()?;
                }
                ServiceType::Dendrite { asic } => {
                    info!(self.inner.log, "Setting up dendrite service");

                    let address = request.addresses[0];
                    smfh.setprop(
                        "config/address",
                        &format!("[{}]:{}", address, DENDRITE_PORT,),
                    )?;
                    match *asic {
                        DendriteAsic::TofinoAsic => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/sidecar_config.toml",
                        )?,
                        DendriteAsic::TofinoStub => smfh.setprop(
                            "config/port_config",
                            "/opt/oxide/dendrite/misc/model_config.toml",
                        )?,
                        DendriteAsic::Softnpu => {}
                    };
                    smfh.refresh()?;
                }
                ServiceType::Tfport { pkt_source } => {
                    info!(self.inner.log, "Setting up tfport service");

                    smfh.setprop("config/pkt_source", pkt_source)?;
                    let address = request.addresses[0];
                    smfh.setprop("config/host", &format!("[{}]", address))?;
                    smfh.setprop("config/port", &format!("{}", DENDRITE_PORT))?;
                    smfh.refresh()?;
                }
            }

            debug!(self.inner.log, "enabling service");
            smfh.enable()?;
        }
        Ok(running_zone)
    }


}
