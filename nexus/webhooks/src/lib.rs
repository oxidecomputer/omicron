// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

pub use nexus_db_model::WebhookEventClass as EventClass;
pub use omicron_uuid_kinds::WebhookEventUuid as EventUuid;

use nexus_auth::context::OpContext;
use omicron_common::api::external::Error;
use schemars::{JsonSchema, schema::Schema};
use serde::Serialize;
use std::collections::BTreeMap;

pub mod events;

/// Trait implemented by types that represent the payload of a webhook event.
pub trait Event: Serialize + JsonSchema {
    /// The event's event class.
    const CLASS: EventClass;
    /// The version number of the event's payload.
    const VERSION: u32;
}

/// The interface for publishing webhook events.
///
/// This is represented as a separate trait, so that code in crates downstream
/// of `omicron-nexus` can publish events using the `Nexus` struct's
/// `webhook_event_publish` method, without depending on `nexus` itself. This
/// allows code in crates that are dependencies of `omicron-nexus` to publish
/// events.

#[allow(async_fn_in_trait)]
pub trait PublishEvent {
    async fn publish_event<E: Event>(
        &self,
        opctx: &OpContext,
        id: EventUuid,
        event: E,
    ) -> Result<nexus_db_model::WebhookEvent, Error>;
}

#[derive(Default)]
pub struct EventSchemaRegistry {
    schemas: BTreeMap<EventClass, BTreeMap<u32, Schema>>,
}

impl EventSchemaRegistry {
    pub fn register<E: Event>(&mut self) {
        let class = E::CLASS;
        let version = E::VERSION;
        let mut schema_gen = schemars::SchemaGenerator::new(Default::default());
        let schema = E::json_schema(&mut schema_gen);

        if self
            .schemas
            .entry(class)
            .or_insert_with(BTreeMap::new)
            .insert(version, schema)
            .is_some()
        {
            panic!(
                "Attempted to register two event class schemas for {class} v{version}!"
            );
        }
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema_versions_for(
        &self,
        class: EventClass,
    ) -> Option<&BTreeMap<u32, Schema>> {
        self.schemas.get(&class)
    }

    pub fn schema_for(
        &self,
        class: EventClass,
        version: u32,
    ) -> Option<&Schema> {
        self.schema_versions_for(class)?.get(&version)
    }
}
