use super::impl_enum_type;
use nexus_types::external_api::params;

impl_enum_type!(
    #[derive(Clone, SqlType, Debug, QueryId)]
    #[diesel(postgres_type(name = "quota_resource_kind"))]
    pub struct QuotaResourceTypeEnum;

    #[derive(Copy, Clone, Debug, AsExpression, FromSqlRow, PartialEq, Serialize, Deserialize)]
    #[diesel(sql_type = QuotaResourceTypeEnum)]
    pub enum QuotaResourceKind;

    Cpu => b"Cpu",
    Memory => b"Memory",
    Storage => b"Storage",
);

impl From<params::QuotaResourceKind> for QuotaResourceKind {
    fn from(k: params::QuotaResourceKind) -> Self {
        match k {
            params::QuotaResourceKind::Cpu => QuotaResourceKind::Cpu,
            params::QuotaResourceKind::Memory => QuotaResourceKind::Memory,
            params::QuotaResourceKind::Storage => QuotaResourceKind::Storage,
        }
    }
}

impl From<QuotaResourceKind> for params::QuotaResourceKind {
    fn from(value: QuotaResourceKind) -> Self {
        match value {
            QuotaResourceKind::Cpu => params::QuotaResourceKind::Cpu,
            QuotaResourceKind::Memory => params::QuotaResourceKind::Memory,
            QuotaResourceKind::Storage => params::QuotaResourceKind::Storage,
        }
    }
}
