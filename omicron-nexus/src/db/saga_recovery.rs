/*!
 * Handles recovery of sagas
 */

use crate::db;
use crate::db::schema;
use crate::db::sql::Table;
use crate::db::sql_operations::sql_paginate;
use omicron_common::api::ApiError;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::BackoffError;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use steno::SagaTemplateGeneric;

/**
 * Starts an asynchronous task to recover sagas (as after a crash or restart)
 *
 * More specifically, this task queries the database to list all uncompleted
 * sagas that are assigned to SEC `sec_id` and for each one:
 *
 * * finds the appropriate template in `templates`
 * * loads the saga log from the database
 * * uses `sec_client.saga_resume()` to prepare to resume execution of the saga
 *   using the persistent saga log
 * * resumes execution of each saga
 */
pub fn recover<T>(
    log: slog::Logger,
    sec_id: db::SecId,
    uctx: Arc<T>,
    pool: Arc<db::Pool>,
    sec_client: Arc<steno::SecClient>,
    templates: &'static BTreeMap<
        &'static str,
        Arc<dyn steno::SagaTemplateGeneric<T>>,
    >,
) -> tokio::task::JoinHandle<()>
where
    T: Send + Sync + fmt::Debug + 'static,
{
    tokio::spawn(async move {
        info!(&log, "start saga recovery");

        /*
         * We perform the initial list of sagas using a standard retry policy.
         * We treat all errors as transient because there's nothing we can do
         * about any of them except try forever.  As a result, we never expect
         * an error from the overall operation.
         * TODO-monitoring we definitely want a way to raise a big red flag if
         * saga recovery is not completing.
         * TODO-robustness It would be better to retry the individual database
         * operations within this operation than retrying the overall operation.
         * As this is written today, if the listing requires a bunch of pages
         * and the operation fails partway through, we'll re-fetch all the pages
         * we successfully fetched before.  If the database is overloaded and
         * only N% of requests are completing, the probability of this operation
         * succeeding decreases considerably as the number of separate queries
         * (pages) goes up.  We'd be much more likely to finish the overall
         * operation if we didn't throw away the results we did get each time.
         */
        let found_sagas = retry_notify(
            internal_service_policy(),
            || async {
                list_unfinished_sagas(&log, &pool, &sec_id)
                    .await
                    .map_err(BackoffError::Transient)
            },
            |error, duration| {
                warn!(
                    &log,
                    "failed to list sagas (will retry after {:?}): {:#}",
                    duration,
                    error
                )
            },
        )
        .await
        .unwrap();

        info!(&log, "listed sagas ({} total)", found_sagas.len());

        for saga in found_sagas {
            /*
             * TODO-robustness We should put this into a retry loop.  We may
             * also want to take any failed sagas and put them at the end of the
             * queue.  It shouldn't really matter, in that the transient
             * failures here are likely to affect recovery of all sagas.
             * However, it's conceivable we misclassify a permanent failure as a
             * transient failure, or that a transient failure is more likely to
             * affect some sagas than others (e.g, data on a different node, or
             * it has a larger log that rqeuires more queries).  To avoid one
             * bad saga ruining the rest, we should try to recover the rest
             * before we go back to one that's failed.
             */
            /* TODO-debug want visibility into "abandoned" sagas */
            let saga_id = saga.id;
            if let Err(error) =
                recover_saga(&log, &uctx, &pool, &sec_client, templates, saga)
                    .await
            {
                warn!(&log, "failed to recover saga {}: {:#}", saga_id, error);
            }
        }
    })
}

/**
 * Queries the database to return a list of uncompleted sagas assigned to SEC
 * `sec_id`
 */
async fn list_unfinished_sagas(
    log: &slog::Logger,
    pool: &db::Pool,
    sec_id: &db::SecId,
) -> Result<Vec<db::saga_types::Saga>, ApiError> {
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
    trace!(&log, "listing sagas");
    let client = pool.acquire().await?;

    let mut sql = db::sql::SqlString::new();
    let saga_state_done = steno::SagaCachedState::Done.to_string();
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

/**
 * Recovers an individual saga
 *
 * This function loads the saga log and uses `sec_client` to resume execution.
 */
async fn recover_saga<T>(
    log: &slog::Logger,
    uctx: &Arc<T>,
    pool: &db::Pool,
    sec_client: &steno::SecClient,
    templates: &BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<T>>>,
    saga: db::saga_types::Saga,
) -> Result<(), ApiError>
where
    T: Send + Sync + fmt::Debug + 'static,
{
    let saga_id = saga.id;
    let template_name = saga.template_name.as_str();
    trace!(log, "recovering saga: start";
        "saga_id" => saga.id.to_string(),
        "template_name" => template_name,
    );
    let template = templates.get(template_name).ok_or_else(|| {
        ApiError::internal_error(&format!(
            "saga {} uses unknown template {:?}",
            saga.id, template_name,
        ))
    })?;
    trace!(log, "recovering saga: found template";
        "saga_id" => ?saga.id,
        "template_name" => template_name
    );
    let log_events = load_saga_log(pool, &saga).await?;
    trace!(log, "recovering saga: loaded log";
        "saga_id" => ?saga.id,
        "template_name" => template_name
    );
    let _ = sec_client
        .saga_resume(
            saga.id,
            Arc::clone(uctx),
            Arc::clone(template),
            saga.template_name,
            saga.saga_params,
            log_events,
        )
        .await
        .map_err(|error| {
            /*
             * TODO-robustness We want to differentiate between retryable and
             * not here
             */
            ApiError::internal_error(&format!(
                "failed to resume saga: {:#}",
                error
            ))
        })?;
    sec_client.saga_start(saga_id).await.map_err(|error| {
        ApiError::internal_error(&format!("failed to start saga: {:#}", error))
    })?;
    info!(log, "recovering saga: done"; "saga_id" => ?saga.id);
    Ok(())
}

/**
 * Queries the database to load the full log for the specified saga
 */
pub async fn load_saga_log(
    pool: &db::Pool,
    saga: &db::saga_types::Saga,
) -> Result<Vec<steno::SagaNodeEvent>, ApiError> {
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

    Ok(log_records
        .iter()
        .map(steno::SagaNodeEvent::from)
        .collect::<Vec<steno::SagaNodeEvent>>())
}
