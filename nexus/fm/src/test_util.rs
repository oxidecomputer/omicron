// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::analysis_input::{Builder, Input, InvalidInputs};
use crate::builder::SitrepBuilderRng;
use chrono::DateTime;
use chrono::Utc;
use iddqd::IdOrdMap;
use nexus_db_model::EreporterRestart;
use nexus_reconfigurator_planning::example;
use nexus_types::fm::ereport::{
    Ena, Ereport, EreportData, EreportId, Reporter,
};
use nexus_types::fm::{Sitrep, SitrepVersion};
use nexus_types::in_service_disk::InServiceDisk;
use nexus_types::inventory;
use nexus_types::observed_saga::ObservedSaga;
use omicron_test_utils::dev;
use omicron_uuid_kinds::EreporterRestartKind;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use omicron_uuid_kinds::RackUuid;
use rand::rngs::StdRng;
use std::sync::Arc;
use std::sync::Mutex;
use typed_rng::TypedUuidRng;

pub struct FmTest {
    pub reporters: SimReporters,
    pub sitrep_rng: SitrepBuilderRng,
    pub system_builder: example::ExampleSystemBuilder,
    pub rack_id: RackUuid,
    pub log: slog::Logger,
}

impl FmTest {
    pub fn new_with_logctx(test_name: &str) -> (Self, dev::LogContext) {
        let logctx = dev::test_setup_log(test_name);
        (Self::new(test_name, &logctx.log), logctx)
    }

    pub fn new(test_name: &str, log: &slog::Logger) -> Self {
        let rack_id = TypedUuidRng::from_seed(test_name, "rack-id").next();
        let log = log.new(slog::o!("sim_rack_id" => rack_id.to_string()));
        let example_system_builder =
            example::ExampleSystemBuilder::new(&log, test_name);
        let reporters = SimReporters::new(
            test_name,
            log.new(slog::o!("component" => "sim-reporters")),
            rack_id,
        );
        Self {
            reporters,
            sitrep_rng: SitrepBuilderRng::from_seed(test_name),
            system_builder: example_system_builder,
            rack_id,
            log,
        }
    }

    /// Returns an analysis [`Input`] [`Builder`] pre-loaded with the simulated
    /// reporter restarts this harness has handed out so far.
    // TODO(eliza): eventually it would be nice if the inventory collection and
    // in-service-disks were generated from the `ExampleSystemBuilder`
    // somehow...
    pub fn input_builder(
        &self,
        parent_sitrep: Option<Arc<(SitrepVersion, Sitrep)>>,
        inv: Arc<inventory::Collection>,
        in_service_disks: Arc<IdOrdMap<InServiceDisk>>,
        observed_sagas: Arc<IdOrdMap<ObservedSaga>>,
    ) -> Result<Builder, InvalidInputs> {
        let mut builder = Input::builder(parent_sitrep, inv)?
            .in_service_disks(in_service_disks)
            .observed_sagas(observed_sagas);
        builder.add_ereporter_restarts(
            self.reporters.ereporter_restarts().iter().cloned(),
        );
        Ok(builder)
    }
}

pub struct SimReporters {
    log: slog::Logger,
    parent: StdRng,
    collector_id_rng: TypedUuidRng<OmicronZoneKind>,
    reporters: iddqd::IdOrdMap<Arc<ReporterShared>>,
    rack_id: RackUuid,
}

impl SimReporters {
    fn new(test_name: &str, log: slog::Logger, rack_id: RackUuid) -> Self {
        let mut parent = typed_rng::from_seed(test_name, "sim-reporters");
        // TODO(eliza): would be more realistic to pick something from the
        // example system's omicron zones, but these UUIDs are only used for
        // debugging purposes...
        let collector_id_rng =
            TypedUuidRng::from_parent_rng(&mut parent, "collector-ids");
        Self {
            parent,
            collector_id_rng,
            log,
            reporters: iddqd::IdOrdMap::new(),
            rack_id,
        }
    }

    /// Returns the restarts known to have written ereports to the database,
    /// derived from the ereports each simulated reporter has produced.
    ///
    /// A restart session appears here only once it has emitted at least one
    /// ereport: until then it has never been persisted, so the diagnosis
    /// engines must not see it. Each entry's `time_first_seen` and
    /// `time_latest_ereport_received` mirror the write-time bookkeeping that
    /// the real `ereports_insert` query performs.
    pub fn ereporter_restarts(
        &self,
    ) -> IdOrdMap<Arc<nexus_db_model::EreporterRestart>> {
        let mut restarts = IdOrdMap::new();
        for shared in &self.reporters {
            for restart in shared.restart_entries() {
                restarts
                    .insert_unique(Arc::new(restart))
                    .expect("simulated restart IDs must be unique");
            }
        }
        restarts
    }

    pub fn reporter(&mut self, reporter: Reporter) -> SimReporter {
        let collector_id = self.collector_id_rng.next();
        let mut restart_id_rng = TypedUuidRng::from_parent_rng(
            &mut self.parent,
            ("restart_id", reporter),
        );
        let restart_id = restart_id_rng.next();
        let shared = Arc::new(ReporterShared {
            rack_id: self.rack_id,
            reporter,
            restarts: Mutex::new(vec![Restart {
                restart_id,
                // Timestamps for this restart are determined only once we
                // actually produce the first simulated ereport.
                timestamps: None,
            }]),
        });
        self.reporters
            .insert_unique(shared.clone())
            // TODO(eliza): if we moved *all* the reporter state (i.e. the
            // current ENA tracking and such) into `ReporterShared`, we could
            // instead make `SimReporters::reporter` just give you the existing
            // one...
            .expect("this simulated reporter already exists!");
        SimReporter {
            shared,
            ena: Ena(0x1),
            restart_id_rng,
            collector_id,
            log: self.log.new(slog::o!("reporter" => reporter.to_string())),
        }
    }
}

pub struct SimReporter {
    shared: Arc<ReporterShared>,
    ena: Ena,
    restart_id_rng: TypedUuidRng<EreporterRestartKind>,

    // TODO(eliza): this is not super realistic, as it will give a new "nexus"
    // to each reporter...but the DEs don't actually care who collected the
    // ereport, and we just need something to put in there.
    collector_id: OmicronZoneUuid,

    log: slog::Logger,
}

#[derive(Debug)]
struct ReporterShared {
    rack_id: RackUuid,
    reporter: Reporter,
    restarts: Mutex<Vec<Restart>>,
}

impl iddqd::IdOrdItem for ReporterShared {
    type Key<'a> = &'a Reporter;

    fn key(&self) -> Self::Key<'_> {
        &self.reporter
    }

    iddqd::id_upcast!();
}

/// A simulated restart in a reporter location's restart history.
#[derive(Debug)]
struct Restart {
    restart_id: EreporterRestartUuid,
    /// `None` until this restart emits its first ereport. When this is `None`,
    /// the [`SimReporters::ereporter_restarts`] method on `SimReporters` does
    /// not include this restart in its output, as we have not yet produced any
    /// ereports for this restart, and the real ereport table in CRDB would not
    /// contain an entry for the restart yet.
    timestamps: Option<ReporterTimestamps>,
}

#[derive(Copy, Clone, Debug)]
struct ReporterTimestamps {
    time_first_seen: DateTime<Utc>,
    time_latest_ereport_received: DateTime<Utc>,
}

impl ReporterShared {
    fn restart_entries(&self) -> Vec<EreporterRestart> {
        let (reporter, slot_type, slot) = match self.reporter {
            Reporter::HostOs { slot, .. } => (
                nexus_db_model::EreporterType::Host,
                nexus_db_model::SpType::Sled,
                slot.map(nexus_db_model::SpMgsSlot::from),
            ),
            Reporter::Sp { slot, sp_type, .. } => (
                nexus_db_model::EreporterType::Sp,
                sp_type.into(),
                Some(slot.into()),
            ),
        };
        let restarts = self.restarts.lock().unwrap();
        restarts
            .iter()
            .filter_map(|restart| {
                let ReporterTimestamps {
                    time_first_seen,
                    time_latest_ereport_received,
                } = restart.timestamps?;
                Some(EreporterRestart {
                    id: restart.restart_id.into(),
                    time_first_seen,
                    reporter,
                    slot_type,
                    slot,
                    rack_id: self.rack_id.into(),
                    time_latest_ereport_received,
                })
            })
            .collect()
    }
}

impl SimReporter {
    #[track_caller]
    pub fn parse_ereport(
        &mut self,
        now: chrono::DateTime<Utc>,
        json: &str,
    ) -> Ereport {
        self.mk_ereport(
            now,
            json.parse().expect("must be called with valid ereport JSON"),
        )
    }

    pub fn mk_ereport(
        &mut self,
        now: chrono::DateTime<Utc>,
        json: serde_json::Map<String, serde_json::Value>,
    ) -> Ereport {
        self.ena.0 += 1;
        let id = {
            let mut restarts = self.shared.restarts.lock().unwrap();
            let restart = restarts.last_mut().expect(
                "a reporter always has a current restart, this is a bug in \
                the test framework",
            );
            match &mut restart.timestamps {
                // Update the timestamps if they are already tracked for this
                // restart...
                Some(ReporterTimestamps {
                    ref mut time_latest_ereport_received,
                    ref mut time_first_seen,
                }) => {
                    *time_latest_ereport_received =
                        (*time_latest_ereport_received).max(now);
                    *time_first_seen = (*time_first_seen).min(now);
                }
                // ...or set them, creating the restart entry if it hasn't
                // already been observed.
                None => {
                    restart.timestamps = Some(ReporterTimestamps {
                        time_first_seen: now,
                        time_latest_ereport_received: now,
                    });
                }
            }
            EreportId { ena: self.ena, restart_id: restart.restart_id }
        };
        mk_ereport(
            &self.log,
            self.shared.reporter,
            id,
            self.collector_id,
            now,
            json,
        )
    }

    /// Simulate this reporter restarting, resetting its ENAs and generating a
    /// new restart ID.
    // TODO(eliza): This should return a data loss report perhaps?
    pub fn restart(&mut self) {
        self.ena = Ena(0x1);
        let restart_id = self.restart_id_rng.next();
        self.shared
            .restarts
            .lock()
            .unwrap()
            .push(Restart { restart_id, timestamps: None });

        slog::info!(
            &self.log,
            "simulating a reporter restart";
            "restart_id" => %restart_id,
        );
    }
}

pub fn mk_ereport(
    log: &slog::Logger,
    reporter: Reporter,
    id: EreportId,
    collector_id: OmicronZoneUuid,
    time_collected: chrono::DateTime<Utc>,
    json: serde_json::Map<String, serde_json::Value>,
) -> Ereport {
    let (_ena, data) = match reporter {
        Reporter::Sp { .. } => {
            let raw = ereport_types::Ereport { ena: id.ena, data: json };
            EreportData::from_sp_ereport(log, id.restart_id, raw)
        }
        Reporter::HostOs { .. } => {
            todo!(
                "eliza: when we get around to actually ingesting host ereport \
                 JSON, figure out what the field names for serial and part \
                 numbers would be!",
            );
        }
    };
    slog::info!(
        &log,
        "simulating an ereport: {}", id;
        "ereport_id" => %id,
        "ereport_class" => ?data.class,
        "serial_number" => ?data.serial_number,
        "part_number" => ?data.part_number,
    );
    Ereport::new(id, time_collected, collector_id, data, reporter)
}
