use db_macros::Asset;
use uuid::Uuid;

#[derive(Queryable, Insertable, Debug, Clone, Selectable, Asset)]
#[diesel(table_name = quota)]
pub struct Quota {
    #[diesel(embed)]
    identity: QuotaIdentity,

    pub silo_id: Uuid,

    pub resource_type: QuotaResourceKind,
    pub limit: i64,
}

impl Quota {
    pub fn new(
        silo_id: Uuid,
        resource_type: QuotaResourceKind,
        limit: i64,
    ) -> Self {
        Self {
            identity: QuotaIdentity::new(Uuid::new_v4()),
            silo_id,
            resource_type,
            limit,
        }
    }
}
