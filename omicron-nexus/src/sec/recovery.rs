/*!
 * Handles recovery of sagas
 * XXX consumer should not create new sagas until listing sagas is complete
 * because they might try to recover the one they started
 */

use super::log;
use crate::db;
use crate::db::schema;
use crate::db::sql::Table;
use crate::db::sql_operations::sql_paginate;
use omicron_common::error::ApiError;
use slog::Logger;
use std::future::Future;

/* XXX TODO-doc */
/* This is regrettably desugared due to rust-lang/rust#63033. */
pub fn list_sagas<'a, 'b, 'c>(
    pool: &'a db::Pool,
    log: Logger,
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
    let log = log.new(o!("sec_id" => sec_id.0.to_string()));
    async move {
        let client = pool.acquire().await?;
        debug!(log, "saga list: start");

        let mut sql = db::sql::SqlString::new();
        let saga_state_done = log::SagaState::Done;
        let p1 = sql.next_param(&saga_state_done);
        let p2 = sql.next_param(sec_id);
        sql.push_str(&format!("saga_state != {} AND current_sec = {}", p1, p2));

        let result = sql_paginate::<
            schema::LookupGenericByUniqueId,
            schema::Saga,
            <schema::Saga as Table>::ModelType,
        >(&client, (), schema::Saga::ALL_COLUMNS, sql)
        .await;
        match &result {
            Ok(s) => info!(log, "saga list: succeeded"; "nsagas" => s.len()),
            Err(error) => error!(log, "saga list: failed"; "error" => #%error),
        };
        result
    }
}
