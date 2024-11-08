use crate::key::Key;
use anyhow::Result;
use aws_lc_rs::rand::SystemRandom;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::num::NonZeroU64;
use tough::editor::signed::SignedRole;
use tough::schema::{KeyHolder, RoleKeys, RoleType, Root};

pub(crate) async fn new_root(
    keys: Vec<Key>,
    expires: DateTime<Utc>,
) -> Result<SignedRole<Root>> {
    let mut root = Root {
        spec_version: "1.0.0".to_string(),
        consistent_snapshot: true,
        version: NonZeroU64::new(1).unwrap(),
        expires,
        keys: HashMap::new(),
        roles: HashMap::new(),
        _extra: HashMap::new(),
    };
    for key in &keys {
        let key = key.as_tuf_key()?;
        root.keys.insert(key.key_id()?, key);
    }
    for kind in [
        RoleType::Root,
        RoleType::Snapshot,
        RoleType::Targets,
        RoleType::Timestamp,
    ] {
        root.roles.insert(
            kind,
            RoleKeys {
                keyids: root.keys.keys().cloned().collect(),
                threshold: NonZeroU64::new(1).unwrap(),
                _extra: HashMap::new(),
            },
        );
    }

    let keys = crate::key::boxed_keys(keys);
    Ok(SignedRole::new(
        root.clone(),
        &KeyHolder::Root(root),
        &keys,
        &SystemRandom::new(),
    )
    .await?)
}
