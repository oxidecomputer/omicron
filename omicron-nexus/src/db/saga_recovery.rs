/*!
 * Handles recovery of sagas
 */

use crate::db;
use diesel::{ExpressionMethods, QueryDsl, RunQueryDsl};
use omicron_common::api::external::Error;
use omicron_common::api::external::LookupType;
use omicron_common::api::external::ResourceType;
use omicron_common::backoff::internal_service_policy;
use omicron_common::backoff::retry_notify;
use omicron_common::backoff::BackoffError;
use std::collections::BTreeMap;
use std::convert::TryFrom;
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
) -> Result<Vec<db::saga_types::Saga>, Error> {
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
    use db::diesel_schema::saga::dsl;
    let conn = pool.acquire_diesel().await?;

    // TODO: ... do we want this to be paginated? Why?
    // The old impl used pagination, but then proceeded to read everything
    // anyway?
    // (we could that too with a synthetic pagparams + loop, I guess?)
    dsl::saga
        .filter(dsl::saga_state.ne(steno::SagaCachedState::Done.to_string()))
        .filter(dsl::current_sec.eq(sec_id))
        .load(&*conn)
        .map_err(|e| {
            Error::from_diesel(
                e,
                ResourceType::SagaDbg,
                LookupType::ById(sec_id.0),
            )
        })
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
) -> Result<(), Error>
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
        Error::internal_error(&format!(
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
            Error::internal_error(&format!(
                "failed to resume saga: {:#}",
                error
            ))
        })?;
    sec_client.saga_start(saga_id).await.map_err(|error| {
        Error::internal_error(&format!("failed to start saga: {:#}", error))
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
) -> Result<Vec<steno::SagaNodeEvent>, Error> {
    use db::diesel_schema::saganodeevent::dsl;
    let conn = pool.acquire_diesel().await?;

    // TODO: Again, should this be paginated?
    // We proceed to read everything anyway.
    let log_records: Vec<steno::SagaNodeEvent> = dsl::saganodeevent
        .filter(dsl::saga_id.eq(saga.id))
        // Load DB SagaNodeEvent type from the database.
        .load::<db::saga_types::SagaNodeEvent>(&*conn)
        .map_err(|e| {
            Error::from_diesel(
                e,
                ResourceType::SagaDbg,
                LookupType::ById(saga.id.0),
            )
        })?
        .into_iter()
        // Parse the DB type into the steno type.
        .map(|db_event| steno::SagaNodeEvent::try_from(db_event))
        .collect::<Result<_, Error>>()?;

    Ok(log_records)
}
