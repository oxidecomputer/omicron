//! Nexus startup task to load hardcoded data into the database

use crate::context::OpContext;
use crate::db::DataStore;
use std::sync::Arc;
use tokio::sync::Mutex;

// This exists only for debugging today.  We could include more information
// here, like how many attempts we've made, when we started, etc.
#[derive(Debug, Eq, PartialEq)]
pub enum DataPopulateStatus {
    Unstarted,
    InProgress,
    Done,
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

    /*
     * XXX Use exponential backoff?
     */
    datastore.load_predefined_users(&opctx).await;

    {
        let mut status = result.lock().await;
        assert_eq!(*status, DataPopulateStatus::InProgress);
        *status = DataPopulateStatus::Done;
    }
}
