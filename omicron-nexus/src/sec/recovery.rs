/*!
 * Handles recovery of sagas
 * XXX The updating of saga rows needs to happen too, especially when done.
 */

use super::log;
use crate::db;
use crate::db::schema;
use crate::db::sql::Table;
use crate::db::sql_operations::sql_paginate;
use omicron_common::error::ApiError;
use std::future::Future;
use std::sync::Arc;

/* XXX TODO-doc */
/* This is regrettably desugared due to rust-lang/rust#63033. */
pub fn list_sagas<'a, 'b, 'c>(
    pool: &'a db::Pool,
    sec_id: &'b log::SecId,
) -> impl Future<Output = Result<Vec<log::Saga>, ApiError>> + 'c
where
    'a: 'c,
    'b: 'c,
{
    /*
     * For now, we do the simplest thing: we fetch all the sagas that the
     * caller's going to need before returning any of them.  This is easier to
     * implement than, say, using a channel or some other stream.  In principle
     * we're giving up some opportunity for parallelism.  The caller could be
     * going off and fetching the saga log for the first sagas that we find
     * while we're still listing later sagas.  Doing that properly would require
     * concurrency limits to prevent overload or starvation of other database
     * consumers.  Anyway, having Nexus come up is really only blocked on this
     * step, not recovering all the individual sagas.
     */
    async move {
        let client = pool.acquire().await?;

        let mut sql = db::sql::SqlString::new();
        let saga_state_done = log::SagaState::Done;
        let p1 = sql.next_param(&saga_state_done);
        let p2 = sql.next_param(sec_id);
        sql.push_str(&format!("saga_state != {} AND current_sec = {}", p1, p2));

        sql_paginate::<
            schema::LookupGenericByUniqueId,
            schema::Saga,
            <schema::Saga as Table>::ModelType,
        >(&client, (), schema::Saga::ALL_COLUMNS, sql)
        .await
    }
}

/* This is regrettably desugared due to rust-lang/rust#63033. */
pub fn load_saga_log<'a, 'b, 'c>(
    pool: &'a db::Pool,
    saga: &'b log::Saga,
    sink: Arc<dyn steno::SagaLogSink>,
) -> impl Future<Output = Result<steno::SagaLog, ApiError>> + 'c
where
    'a: 'c,
    'b: 'c,
{
    async move {
        let client = pool.acquire().await?;
        let mut extra_sql = db::sql::SqlString::new();
        extra_sql.push_str("TRUE");
        let log_records = sql_paginate::<
            schema::LookupSagaNodeEvent,
            schema::SagaNodeEvent,
            <schema::SagaNodeEvent as Table>::ModelType,
        >(
            &client,
            (&saga.id.0,),
            schema::SagaNodeEvent::ALL_COLUMNS,
            extra_sql,
        )
        .await?;
        let events = log_records
            .into_iter()
            .map(steno::SagaNodeEvent::from)
            .collect::<Vec<steno::SagaNodeEvent>>();

        let log_serialized = steno::SagaLogSerialized {
            saga_id: saga.id,
            creator: saga.creator.0.to_string(),
            params: saga.saga_params.clone(),
            events,
        };

        steno::SagaLog::load_raw(log_serialized, sink).map_err(|error| {
            ApiError::internal_error(&format!(
                "failed to load log for saga \"{}\": {:#}",
                saga.id, error,
            ))
        })
    }
}
