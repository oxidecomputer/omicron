use crate::{SqlU32, impl_enum_type};
use chrono::DateTime;
use chrono::Utc;
use ipnetwork::IpNetwork;
use nexus_db_schema::schema::bfd_session;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    BfdModeEnum:

    #[derive(
        Clone,
        Copy,
        Debug,
        AsExpression,
        FromSqlRow,
        PartialEq,
        Serialize,
        Deserialize
    )]
    pub enum BfdMode;

    SingleHop => b"single_hop"
    MultiHop => b"multi_hop"
);

#[derive(
    Queryable, Insertable, Selectable, Clone, Debug, Serialize, Deserialize,
)]
#[diesel(table_name = bfd_session)]
pub struct BfdSession {
    pub id: Uuid,
    pub local: Option<IpNetwork>,
    pub remote: IpNetwork,
    pub detection_threshold: SqlU32,
    pub required_rx: SqlU32,
    pub switch: String,
    pub mode: BfdMode,
    pub time_created: DateTime<Utc>,
    pub time_modified: DateTime<Utc>,
    pub time_deleted: Option<DateTime<Utc>>,
}

impl From<omicron_common::api::external::BfdMode> for BfdMode {
    fn from(value: omicron_common::api::external::BfdMode) -> Self {
        match value {
            omicron_common::api::external::BfdMode::SingleHop => {
                BfdMode::SingleHop
            }
            omicron_common::api::external::BfdMode::MultiHop => {
                BfdMode::MultiHop
            }
        }
    }
}
