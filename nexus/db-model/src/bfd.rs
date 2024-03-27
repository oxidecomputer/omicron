use crate::schema::bfd_session;
use crate::{impl_enum_type, SqlU32};
use chrono::DateTime;
use chrono::Utc;
use ipnetwork::IpNetwork;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

impl_enum_type!(
    #[derive(SqlType, Debug, Clone, Copy)]
    #[diesel(postgres_type(name = "bfd_mode", schema = "public"))]
    pub struct BfdModeEnum;

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
    #[diesel(sql_type = BfdModeEnum)]
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
