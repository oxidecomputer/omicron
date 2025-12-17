// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::builder::SitrepBuilderRng;
use chrono::Utc;
use nexus_reconfigurator_planning::example;
use nexus_types::fm::ereport::{
    Ena, Ereport, EreportData, EreportId, Reporter,
};
use omicron_test_utils::dev;
use omicron_uuid_kinds::EreporterRestartKind;
use omicron_uuid_kinds::EreporterRestartUuid;
use omicron_uuid_kinds::OmicronZoneKind;
use omicron_uuid_kinds::OmicronZoneUuid;
use rand::rngs::StdRng;
use slog::Logger;
use typed_rng::TypedUuidRng;

pub struct FmTest {
    pub logctx: dev::LogContext,
    pub reporters: SimReporters,
    pub sitrep_rng: SitrepBuilderRng,
    pub system_builder: example::ExampleSystemBuilder,
}

impl FmTest {
    pub fn new(test_name: &str) -> Self {
        let logctx = dev::test_setup_log(test_name);
        let example_system_builder =
            example::ExampleSystemBuilder::new(&logctx.log, test_name);
        let reporters = SimReporters::new(
            test_name,
            logctx.log.new(slog::o!("component" => "sim-reporters")),
        );
        Self {
            logctx,
            reporters,
            sitrep_rng: SitrepBuilderRng::from_seed(test_name),
            system_builder: example_system_builder,
        }
    }
}

pub struct SimReporters {
    log: slog::Logger,
    parent: StdRng,
    collector_id_rng: TypedUuidRng<OmicronZoneKind>,
}

impl SimReporters {
    fn new(test_name: &str, log: slog::Logger) -> Self {
        let mut parent = typed_rng::from_seed(test_name, "sim-reporters");
        // TODO(eliza): would be more realistic to pick something from the
        // example system's omicron zones, but these UUIDs are only used for
        // debugging purposes...
        let collector_id_rng =
            TypedUuidRng::from_parent_rng(&mut parent, "collector-ids");
        Self { parent, collector_id_rng, log }
    }

    pub fn reporter(&mut self, reporter: Reporter) -> SimReporter {
        let collector_id = self.collector_id_rng.next();
        let mut restart_id_rng = TypedUuidRng::from_parent_rng(
            &mut self.parent,
            ("restart_id", reporter),
        );
        let restart_id = restart_id_rng.next();
        SimReporter {
            reporter,
            restart_id,
            ena: Ena(0x1),
            restart_id_rng,
            collector_id,
            log: self.log.new(slog::o!("reporter" => reporter.to_string())),
        }
    }
}

pub struct SimReporter {
    reporter: Reporter,
    restart_id: EreporterRestartUuid,
    ena: Ena,
    restart_id_rng: TypedUuidRng<EreporterRestartKind>,

    // TODO(eliza): this is not super realistic, as it will give a new "nexus"
    // to each reporter...but the DEs don't actually care who collected the
    // ereport, and we just need something to put in there.
    collector_id: OmicronZoneUuid,

    log: slog::Logger,
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
        mk_ereport(
            &self.log,
            self.reporter,
            EreportId { ena: self.ena, restart_id: self.restart_id },
            self.collector_id,
            now,
            json,
        )
    }

    pub fn restart(&mut self) {
        self.ena = Ena(0x1);
        self.restart_id = self.restart_id_rng.next();
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
    let data = match reporter {
        Reporter::Sp { .. } => {
            let raw = ereport_types::Ereport { ena: id.ena, data: json };
            EreportData::from_sp_ereport(
                log,
                id.restart_id,
                raw,
                time_collected,
                collector_id,
            )
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
        "simulating an ereport: {}", data.id;
        "ereport_id" => %data.id,
        "ereport_class" => ?data.class,
        "serial_number" => ?data.serial_number,
        "part_number" => ?data.part_number,
    );
    Ereport { reporter, data }
}
