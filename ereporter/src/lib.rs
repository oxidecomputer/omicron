// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use chrono::DateTime;
use chrono::Utc;
use std::collections::HashMap;
use tokio::sync::mpsc;

mod buffer;
pub mod server;

#[must_use = "an `EreportBuilder` does nothing unless `submit()` is called"]
pub struct EreportBuilder {
    data: EreportData,
    permit: mpsc::OwnedPermit<EreportData>,
}

impl EreportBuilder {
    /// Reserve capacity for at least `facts` facts.
    pub fn reserve(&mut self, facts: usize) {
        self.data.facts.reserve(facts);
    }

    pub fn time_created(
        &mut self,
        time_created: chrono::DateTime<Utc>,
    ) -> &mut Self {
        self.data.time_created = time_created;
        self
    }

    pub fn fact(
        &mut self,
        name: impl ToString,
        value: impl ToString,
    ) -> Option<String> {
        self.data.facts.insert(name.to_string(), value.to_string())
    }

    pub fn facts<K, V>(
        &mut self,
        facts: impl Iterator<Item = (impl ToString, impl ToString)>,
    ) {
        self.data
            .facts
            .extend(facts.map(|(k, v)| (k.to_string(), v.to_string())))
    }

    pub fn submit(self) {
        self.permit.send(self.data);
    }
}

/// A reporter handle used to generate ereports.
#[derive(Clone, Debug)]
pub struct Reporter(pub(crate) tokio::sync::mpsc::Sender<EreportData>);

impl Reporter {
    /// Begin constructing a new ereport, returning an [`EreportBuilder`].
    pub async fn report(
        &mut self,
        class: impl ToString,
    ) -> Result<EreportBuilder, ()> {
        let time_created = Utc::now();
        let permit = self.0.clone().reserve_owned().await.map_err(|_| ())?;
        Ok(EreportBuilder {
            data: EreportData {
                class: class.to_string(),
                time_created,
                facts: HashMap::new(),
            },
            permit,
        })
    }
}

/// An ereport.
pub(crate) struct EreportData {
    pub(crate) class: String,
    pub(crate) facts: HashMap<String, String>,
    pub(crate) time_created: DateTime<Utc>,
}
