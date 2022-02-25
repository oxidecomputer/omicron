//! Nexus startup task to load hardcoded data into the database

use crate::context::OpContext;
use crate::db::DataStore;
use futures::future::BoxFuture;
use futures::FutureExt;
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
    use omicron_common::api::external::Error;
    struct Populator<'a> {
        name: &'static str,
        func: &'a (dyn Fn() -> BoxFuture<'a, Result<(), Error>> + Send + Sync),
    }

    let populate_users = || {
        async { datastore.load_builtin_users(opctx).await.map(|_| ()) }.boxed()
    };
    let populate_roles = || {
        async { datastore.load_builtin_roles(opctx).await.map(|_| ()) }.boxed()
    };
    let populate_role_asgns = || {
        async { datastore.load_builtin_role_asgns(opctx).await.map(|_| ()) }
            .boxed()
    };
    let populate_silos = || {
        async { datastore.load_builtin_silos(opctx).await.map(|_| ()) }.boxed()
    };
    let populators = [
        Populator { name: "users", func: &populate_users },
        Populator { name: "roles", func: &populate_roles },
        Populator { name: "role assignments", func: &populate_role_asgns },
        Populator { name: "silos", func: &populate_silos },
    ];

    for p in populators {
        let db_result = backoff::retry_notify(
            backoff::internal_service_policy(),
            || async {
                (p.func)().await.map_err(|error| match &error {
                    Error::ServiceUnavailable { .. } => {
                        backoff::BackoffError::Transient(error)
                    }
                    _ => backoff::BackoffError::Permanent(error),
                })
            },
            |error, delay| {
                warn!(
                    opctx.log,
                    "failed to populate built-in {}; will retry in {:?}",
                    p.name,
                    delay;
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
                "gave up trying to populate built-in {}", p.name;
                "error_message" => ?error
            );
        }

        db_result.map_err(|error| error.to_string())?;
    }

    Ok(())
}
