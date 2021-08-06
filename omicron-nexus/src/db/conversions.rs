/*!
 * Facilities for mapping Rust types to database types
 *
 * See omicron-common/src/model_db.rs.
 */

use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::VpcCreateParams;
use omicron_common::api::internal::nexus::OximeterAssignment;
use omicron_common::api::internal::nexus::OximeterInfo;
use omicron_common::api::internal::nexus::ProducerEndpoint;

use super::sql::SqlSerialize;
use super::sql::SqlValueSet;

impl SqlSerialize for IdentityMetadataCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        output.set("name", &self.name);
        output.set("description", &self.description);
        output.set("time_deleted", &(None as Option<DateTime<Utc>>));
    }
}

impl SqlSerialize for VpcCreateParams {
    fn sql_serialize(&self, output: &mut SqlValueSet) {
        self.identity.sql_serialize(output);
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
