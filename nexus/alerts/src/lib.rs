// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use nexus_db_model::AlertClass;
pub use omicron_uuid_kinds::AlertUuid;

use nexus_auth::context::OpContext;
use omicron_common::api::external::Error;
use schemars::SchemaGenerator;
use schemars::{JsonSchema, schema::Schema};
use serde::Serialize;
use std::borrow::Cow;
use std::collections::BTreeMap;

pub mod alerts;

/// Trait implemented by types that represent the data payload of an alert.
pub trait Alert: Serialize + JsonSchema {
    /// The event's event class.
    const CLASS: EventClass;
    /// The version number of the event's payload.
    const VERSION: u32;
}

/// The interface for publishing alerts.
///
/// This is represented as a separate trait, so that code in crates downstream
/// of `omicron-nexus` can publish alerts using the `Nexus` struct's
/// `alert_publish` method, without depending on `nexus` itself. This
/// allows code in crates that are dependencies of `omicron-nexus` to publish
/// alerts.
#[allow(async_fn_in_trait)]
pub trait PublishAlert {
    async fn publish_event<A: Alert>(
        &self,
        opctx: &OpContext,
        id: AlertUuid,
        alert: A,
    ) -> Result<nexus_db_model::Alert, Error>;
}

#[derive(Default)]
pub struct AlertSchemaRegistry {
    schemas: BTreeMap<AlertClass, RegisteredSchema>,
}

#[derive(Default)]
pub struct RegisteredSchema(BTreeMap<u32, SchemaVersion>);

pub struct SchemaVersion {
    gen_schema: Box<
        dyn Fn(&mut schemars::SchemaGenerator) -> Schema
            + Send
            + Sync
            + 'static,
    >,
    name: String,
    schema_id: Cow<'static, str>,
}

impl SchemaVersion {
    fn for_alert<A: Alert>() -> Self {
        Self {
            gen_schema: Box::new(|generator| A::json_schema(generator)),
            name: A::name(),
            schema_id: A::schema_id(),
        }
    }
}

impl AlertSchemaRegistry {
    pub fn register<A: Alert>(&mut self) {
        let class = E::CLASS;
        let version = E::VERSION;

        if self
            .schemas
            .entry(class)
            .or_default()
            .0
            .insert(version, SchemaVersion::for_alert::<A>())
            .is_some()
        {
            panic!(
                "Attempted to register two alert class schemas for {class} v{version}!"
            );
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema_versions_for(
        &self,
        class: EventClass,
    ) -> Option<&BTreeMap<u32, SchemaVersion>> {
        Some(&self.schemas.get(&class)?.0)
    }

    pub fn schema_for(
        &self,
        class: EventClass,
        version: u32,
    ) -> Option<&SchemaVersion> {
        self.schema_versions_for(class)?.get(&version)
    }
}

impl JsonSchema for AlertSchemaRegistry {
    fn schema_name() -> String {
        "Oxide Alerts".to_string()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> Schema {
        todo!("eliza figure this out")
        // for (class, alert) in self.schemas {

        // }
    }
}
