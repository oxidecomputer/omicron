/*!
 * Handles recovery of sagas
 * XXX consumer should not create new sagas until recovery is complete because
 * they might try to recover the one they started
 */

use super::log;
use crate::db;
use slog::Logger;
use std::sync::Arc;

pub struct RecoverSagas {
    sec_id: log::SecId,
    channel_rx: tokio::sync::mpsc::Receiver<Result<log::Saga, anyhow::Error>>,
    task: tokio::task::JoinHandle<()>,
}

impl RecoverSagas {
    fn new(
        pool: Arc<db::Pool>,
        log: Logger,
        sec_id: &log::SecId,
    ) -> RecoverSagas {
        /* There is no particular need for buffering here. */
        let (channel_tx, channel_rx) = tokio::sync::mpsc::channel(1);
        let log = log.new(o!("sec_id" => sec_id.0.to_string()));
        let task_impl = RecoverSagasImpl {
            log,
            pool,
            sec_id: *sec_id,
            channel_tx,
            nfound: 0,
        };
        let task = tokio::spawn(task_impl.recover());

        RecoverSagas { sec_id: *sec_id, channel_rx, task }
    }
}

/* XXX impl Stream for Recoverer? */

struct RecoverSagasImpl {
    log: Logger,
    pool: Arc<db::Pool>,
    sec_id: log::SecId,
    channel_tx: tokio::sync::mpsc::Sender<Result<log::Saga, anyhow::Error>>,
    nfound: usize,
}

impl RecoverSagasImpl {
    /*
     * Take ownership so that the `self` (and especially the channel) goes out
     * of scope when we're done.
     */
    async fn recover(self) {
        let log = &self.log;
        debug!(log, "saga list: start");
        let maybe_error = self.do_recover().await;
        debug!(log, "saga list: found sagas"; "nsagas" => self.nfound);
        if let Err(error) = maybe_error {
            error!(log,
                "saga list: failed";
                "nfound" => self.nfound,
                "error" => #%error,
            );
            self.channel_tx.send(Err(error)).await;
        } else {
            info!(log, "saga list: succeeded"; "nsagas" => self.nfound);
        }
    }

    async fn do_recover(&self) -> Result<(), anyhow::Error> {
        let client = self.pool.acquire().await?;
        todo!(); // XXX
    }
}
