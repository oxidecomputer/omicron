//! Nexus startup task to load hardcoded data into the database

use crate::context::OpContext;
use crate::db::DataStore;
use omicron_common::backoff;
use std::sync::Arc;
use tokio::sync::Mutex;

// This exists only for debugging today.  We could include more information
// here, like how many attempts we've made, when we started, etc.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataPopulateStatus {
    Unstarted,
    InProgress,
    Done,
    Failed(String),
}

impl Default for DataPopulateStatus {
    fn default() -> Self {
        DataPopulateStatus::Unstarted
    }
}

pub fn populate_start(
    opctx: OpContext,
    datastore: Arc<DataStore>,
    result: Arc<Mutex<DataPopulateStatus>>,
) {
    tokio::spawn(populate(opctx, datastore, result));
}

async fn populate(
    opctx: OpContext,
    datastore: Arc<DataStore>,
    result: Arc<Mutex<DataPopulateStatus>>,
) {
    {
        let mut status = result.lock().await;
        assert_eq!(*status, DataPopulateStatus::Unstarted);
        *status = DataPopulateStatus::InProgress;
    }

    let db_result = backoff::retry_notify(
        backoff::internal_service_policy(),
        || async {
            datastore.load_predefined_users(&opctx).await.map_err(|error| {
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
                "failed to load predefined users; will retry in {:?}", delay;
                "error_message" => ?error,
            );
        },
    )
    .await;

    let mut status = result.lock().await;
    assert_eq!(*status, DataPopulateStatus::InProgress);
    if let Err(error) = db_result {
        /*
         * TODO-autonomy this should raise an alert, bump a counter, or raise
         * some other red flag that something is wrong.  (This should be
         * unlikely in practice.)
         */
        error!(opctx.log,
            "gave up trying to load predefined users";
            "error_message" => ?error
        );
        *status = DataPopulateStatus::Failed(error.to_string());
    } else {
        *status = DataPopulateStatus::Done;
    }
}
