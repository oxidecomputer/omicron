use std::collections::HashMap;
use std::sync::Arc;

use crate::v1::types::FieldSet;

const CACHE_SIZE: usize = 8192;

pub struct FieldSetCache {
    fieldsets: HashMap<Box<str>, Arc<FieldSet>>,
}

impl FieldSetCache {
    pub fn new() -> Self {
        Self { fieldsets: HashMap::new() }
    }

    pub(crate) fn get(
        &mut self,
        key: &str,
    ) -> Result<Arc<FieldSet>, serde_json::Error> {
        if let Some(fieldset) = self.fieldsets.get(key) {
            Ok(Arc::clone(fieldset))
        } else {
            let fieldset = Arc::new(serde_json::from_str::<FieldSet>(&key)?);
            if self.fieldsets.len() >= CACHE_SIZE {
                self.fieldsets.clear();
            }
            self.fieldsets.insert(Box::from(key), Arc::clone(&fieldset));
            Ok(fieldset)
        }
    }
}
