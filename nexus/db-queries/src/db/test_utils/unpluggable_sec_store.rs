// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Test-only helper function for detaching storage.

use crate::db;
use async_trait::async_trait;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// A thin wrapper around [`db::CockroachDbSecStore`] that lets
/// us "unplug" access to the writable backend.
#[derive(Debug)]
pub struct UnpluggableCockroachDbSecStore {
    // If "true", we avoid actually writing to the underlying datastore.
    unplugged: AtomicBool,
    sec_store: db::CockroachDbSecStore,
}

impl UnpluggableCockroachDbSecStore {
    pub fn new(
        sec_id: db::SecId,
        sec_generation: String,
        datastore: Arc<db::DataStore>,
        log: slog::Logger,
    ) -> Self {
        Self {
            unplugged: AtomicBool::new(false),
            sec_store: db::CockroachDbSecStore::new(
                sec_id,
                sec_generation,
                datastore,
                log,
            ),
        }
    }

    /// Attaches or detaches the storage subsystem.
    ///
    /// The [`steno::SecStore`] interface is still available,
    /// but all operations will silently be dropped if the
    /// datastore is "unplugged".
    pub fn set_unplug(&self, unplugged: bool) {
        self.unplugged.store(unplugged, Ordering::SeqCst)
    }
}

#[async_trait]
impl steno::SecStore for UnpluggableCockroachDbSecStore {
    async fn saga_create(
        &self,
        create_params: steno::SagaCreateParams,
    ) -> Result<(), anyhow::Error> {
        if self.unplugged.load(Ordering::SeqCst) {
            // TODO: This is a little odd - we're admittedly lying within
            // the context of the test - but there is no error propagation
            // for the other methods, so it's at least consistent.
            //
            // It would likely be more correct to return an error here, if
            // the other methods on this trait also could return errors.
            return Ok(());
        }
        self.sec_store.saga_create(create_params).await
    }

    async fn record_event(&self, event: steno::SagaNodeEvent) {
        if self.unplugged.load(Ordering::SeqCst) {
            return;
        }
        self.sec_store.record_event(event).await
    }

    async fn saga_update(
        &self,
        id: steno::SagaId,
        update: steno::SagaCachedState,
    ) {
        if self.unplugged.load(Ordering::SeqCst) {
            return;
        }
        self.sec_store.saga_update(id, update).await
    }
}
