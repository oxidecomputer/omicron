use omicron_uuid_kinds::{PropolisUuid, SledUuid};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VmmAndSledIds {
    pub vmm_id: PropolisUuid,
    pub sled_id: SledUuid,
}
