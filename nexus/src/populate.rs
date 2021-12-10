//! Nexus startup task to load hardcoded data into the database

use crate::context::OpContext;
use crate::db::DataStore;
use omicron_common::backoff;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PopulateStatus {
    NotDone,
    Done,
    Failed(String),
}

pub fn populate_start(
    opctx: OpContext,
    datastore: Arc<DataStore>,
) -> tokio::sync::watch::Receiver<PopulateStatus> {
    let (tx, rx) = tokio::sync::watch::channel(PopulateStatus::NotDone);

    tokio::spawn(async move {
        let result = populate(&opctx, &datastore).await;
        if let Err(error) = tx.send(match result {
            Ok(()) => PopulateStatus::Done,
            Err(message) => PopulateStatus::Failed(message),
        }) {
            error!(opctx.log, "nobody waiting for populate: {:#}", error)
        }
    });

    rx
}

async fn populate(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), String> {
    populate_users(opctx, &datastore).await?;
    populate_roles(opctx, &datastore).await
}

async fn populate_users(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), String> {
    let db_result = backoff::retry_notify(
        backoff::internal_service_policy(),
        || async {
            datastore.load_builtin_users(&opctx).await.map_err(|error| {
                use omicron_common::api::external::Error;
                match &error {
                    Error::ServiceUnavailable { .. } => {
                        backoff::BackoffError::Transient(error)
                    }
                    _ => backoff::BackoffError::Permanent(error),
                }
            })
        },
        |error, delay| {
            warn!(
                opctx.log,
                "failed to load built-in users; will retry in {:?}", delay;
                "error_message" => ?error,
            );
        },
    )
    .await;

    if let Err(error) = &db_result {
        /*
         * TODO-autonomy this should raise an alert, bump a counter, or raise
         * some other red flag that something is wrong.  (This should be
         * unlikely in practice.)
         */
        error!(opctx.log,
            "gave up trying to load built-in users";
            "error_message" => ?error
        );
    }

    db_result.map_err(|error| error.to_string())
}

// XXX commonize
async fn populate_roles(
    opctx: &OpContext,
    datastore: &DataStore,
) -> Result<(), String> {
    let db_result = backoff::retry_notify(
        backoff::internal_service_policy(),
        || async {
            datastore.load_builtin_roles(&opctx).await.map_err(
                |error| {
                    use omicron_common::api::external::Error;
                    match &error {
                        Error::ServiceUnavailable { .. } => {
                            backoff::BackoffError::Transient(error)
                        }
                        _ => backoff::BackoffError::Permanent(error),
                    }
                },
            )
        },
        |error, delay| {
            warn!(
                opctx.log,
                "failed to load built-in roles; will retry in {:?}", delay;
                "error_message" => ?error,
            );
        },
    )
    .await;

    if let Err(error) = &db_result {
        /*
         * TODO-autonomy this should raise an alert, bump a counter, or raise
         * some other red flag that something is wrong.  (This should be
         * unlikely in practice.)
         */
        error!(opctx.log,
            "gave up trying to load built-in roles";
            "error_message" => ?error
        );
    }

    db_result.map_err(|error| error.to_string())
}
