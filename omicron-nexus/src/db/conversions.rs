/*!
 * Facilities for mapping Rust types to database types
 *
 * See omicron-common/src/model_db.rs.
 */

use chrono::DateTime;
use chrono::Utc;
use omicron_common::api::external::IdentityMetadataCreateParams;
use omicron_common::api::external::VpcCreateParams;

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
        output.set("dns_name", &self.dns_name);
    }
}
