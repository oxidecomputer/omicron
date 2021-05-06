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
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use steno::SagaTemplateGeneric;

pub fn recover<T>(
    log: slog::Logger,
    sec_id: log::SecId,
    uctx: Arc<T>,
    pool: Arc<db::Pool>,
    sec_client: Arc<steno::SecClient>,
    templates: BTreeMap<&'static str, Arc<dyn steno::SagaTemplateGeneric<T>>>,
) -> tokio::task::JoinHandle<()>
where
    T: Send + Sync + fmt::Debug + 'static,
{
    tokio::spawn(async move {
        info!(&log, "start saga recovery");
        /* TODO-robustness retry loop */
        let found_sagas = match list_sagas(&pool, &sec_id).await {
            Ok(found) => {
                info!(&log, "listed sagas ({} total)", found.len());
                found
            }
            Err(error) => {
                error!(&log, "failed to list sagas: {:#}", error);
                Vec::new()
            }
        };

        for saga in found_sagas {
            /* TODO-robustness retry loop */
            /* TODO-debug want visibility into "abandoned" sagas */
            let saga_id = saga.id;
            if let Err(error) =
                recover_saga(&log, &uctx, &pool, &sec_client, &templates, saga)
                    .await
            {
                warn!(&log, "failed to recover saga {}: {:#}", saga_id, error);
            }
        }
    })
}

/* XXX TODO-doc */
/* This is regrettably desugared due to rust-lang/rust#63033. */
fn list_sagas<'a, 'b, 'c>(
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
}

async fn recover_saga<T>(
    log: &slog::Logger,
    uctx: &Arc<T>,
    pool: &db::Pool,
    sec_client: &steno::SecClient,
    templates: &BTreeMap<&'static str, Arc<dyn SagaTemplateGeneric<T>>>,
    saga: log::Saga,
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
            /* XXX We want to differentiate between retryable and not here */
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

/* This is regrettably desugared due to rust-lang/rust#63033. */
pub fn load_saga_log<'a, 'b, 'c>(
    pool: &'a db::Pool,
    saga: &'b log::Saga,
) -> impl Future<Output = Result<Vec<steno::SagaNodeEvent>, ApiError>> + 'c
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

        Ok(log_records
            .iter()
            .map(steno::SagaNodeEvent::from)
            .collect::<Vec<steno::SagaNodeEvent>>())
    }
}
