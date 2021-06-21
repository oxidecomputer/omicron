/*!
 * Facilities for mapping Rust types to database types
 *
 * See omicron-common/src/model_db.rs.
 */

use chrono::DateTime;
use chrono::Utc;
use omicron_common::model::ApiDiskCreateParams;
use omicron_common::model::ApiDiskRuntimeState;
use omicron_common::model::ApiDiskState;
use omicron_common::model::ApiIdentityMetadataCreateParams;
use omicron_common::model::ApiInstanceCreateParams;
use omicron_common::model::ApiInstanceRuntimeState;
use omicron_common::model::ApiInstanceState;
use omicron_common::model::ApiProjectCreateParams;
use omicron_common::model::OximeterAssignment;
use omicron_common::model::OximeterInfo;
use omicron_common::model::ProducerEndpoint;

use super::sql::SqlSerialize;
use super::sql::SqlValueSet;

impl SqlSerialize for ApiIdentityMetadataCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("name", &self.name);
        output.set("description", &self.description);
        output.set("time_deleted", &(None as Option<DateTime<Utc>>));
    }
}

impl SqlSerialize for ApiProjectCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output)
    }
}

impl SqlSerialize for ApiInstanceCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("ncpus", &self.ncpus);
        output.set("memory", &self.memory);
        output.set("hostname", &self.hostname);
    }
}

impl SqlSerialize for ApiInstanceState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("instance_state", &self.label());
    }
}

impl SqlSerialize for ApiInstanceRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.run_state.sql_serialize(output);
        output.set("active_server_id", &self.sled_uuid);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
    }
}

impl SqlSerialize for ApiDiskCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
        output.set("size_bytes", &self.size);
        output.set("origin_snapshot", &self.snapshot_id);
    }
}

impl SqlSerialize for ApiDiskRuntimeState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.disk_state.sql_serialize(output);
        output.set("state_generation", &self.gen);
        output.set("time_state_updated", &self.time_updated);
    }
}

impl SqlSerialize for ApiDiskState {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        let attach_id = &self.attached_instance_id().map(|id| *id);
        output.set("attach_instance_id", attach_id);
        output.set("disk_state", &self.label());
    }
}

impl SqlSerialize for OximeterInfo {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("id", &self.collector_id);
        output.set("ip", &self.address.ip());
        output.set("port", &i32::from(self.address.port()));
    }
}

impl SqlSerialize for ProducerEndpoint {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("id", &self.id);
        output.set("ip", &self.address.ip());
        output.set("port", &i32::from(self.address.port()));
        output.set("interval", &self.interval.as_secs_f64());
        output.set("route", &self.base_route);
    }
}

impl SqlSerialize for OximeterAssignment {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("oximeter_id", &self.oximeter_id);
        output.set("producer_id", &self.producer_id);
    }
}
