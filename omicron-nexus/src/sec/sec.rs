/*!
 * Saga Execution Coordinator (SEC)
 * XXX consider breaking up the channel
 * XXX consider moving recovery into a separate Future but not a separate Task
 */

use super::recovery;
use crate::db;
use crate::sec;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use omicron_common::error::ApiError;
use omicron_common::model;
use slog::Logger;
use std::any::type_name;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use steno::SagaLogSink;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use uuid::Uuid;

enum SagaLoadState {
    Unloaded,
    Running {
        task: tokio::task::JoinHandle<()>,
        exec: Arc<dyn steno::SagaExecManager>,
        waiter: Option<oneshot::Sender<steno::SagaResult>>,
    },
    Done {
        result: steno::SagaResult,
    },
    Abandoned {
        time: DateTime<Utc>,
        reason: anyhow::Error,
    },
}

impl fmt::Debug for SagaLoadState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SagaLoadState::Unloaded => f.write_str("unloaded"),
            SagaLoadState::Running { .. } => f.write_str("running"),
            SagaLoadState::Done { .. } => f.write_str("done"),
            SagaLoadState::Abandoned { .. } => f.write_str("abandoned"),
        }
    }
}

enum SecMsg<T> {
    RecoverStart {
        saga_ctx: Arc<T>,
    },

    SagaRecovered {
        saga_id: steno::SagaId,
        load_state: SagaLoadState,
    },

    SagaRecoveryDone,

    SagaAdd {
        saga_id: steno::SagaId,
        exec: Arc<dyn steno::SagaExecManager>,
        notify_done: oneshot::Sender<steno::SagaResult>,
    },

    SagaDone {
        saga_id: steno::SagaId,
    },

    SagasList {
        /* TODO-cleanup This ought to be a DataPageParams. */
        marker: Option<Uuid>,
        limit: NonZeroU32,
        result_channel: oneshot::Sender<
            Result<Vec<Result<model::ApiSagaView, ApiError>>, ApiError>,
        >,
    },

    SagaGet {
        saga_id: Uuid,
        result_channel: oneshot::Sender<Result<model::ApiSagaView, ApiError>>,
    },

    Shutdown,
}

impl<T> fmt::Debug for SecMsg<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}::", type_name::<Self>()))?;
        match self {
            SecMsg::RecoverStart { .. } => f.write_str("RecoverStart"),
            SecMsg::SagaRecovered { saga_id, load_state } => {
                f.write_fmt(format_args!(
                    "SagaRecovered {{ saga_id = {:?}, load_state = {:?} }}",
                    saga_id, load_state,
                ))
            }
            SecMsg::SagaRecoveryDone => f.write_str("SagaRecoveryDone"),
            SecMsg::SagaAdd { saga_id, .. } => f.write_fmt(format_args!(
                "SagaAdd {{ saga_id = {:?} }}",
                saga_id
            )),
            SecMsg::SagaDone { saga_id } => {
                f.write_fmt(format_args!("Done {{ saga_id = {:?} }}", saga_id))
            }
            SecMsg::Shutdown => f.write_str("Shutdown"),
            SecMsg::SagasList { marker, .. } => f.write_fmt(format_args!(
                "SagasList {{ marker = {:?} }}",
                marker
            )),
            SecMsg::SagaGet { saga_id, .. } => {
                f.write_fmt(format_args!("SagaGet {{ id = {:?} }}", saga_id))
            }
        }
    }
}

pub struct SagaExecCoordinatorHandle<T> {
    log: Logger,
    main_task: tokio::task::JoinHandle<()>,
    msg_tx: mpsc::Sender<SecMsg<T>>,
    recover_permit: Option<mpsc::OwnedPermit<SecMsg<T>>>,
    shutdown_permit: Option<mpsc::OwnedPermit<SecMsg<T>>>,
}

pub struct RecoveryToken<T> {
    permit: mpsc::OwnedPermit<SecMsg<T>>,
}

impl<T> SagaExecCoordinatorHandle<T> {
    /* XXX TODO-cleanup definitely want a builder here */
    pub fn new(
        pool: Arc<db::Pool>,
        log: Logger,
        sec_id: &sec::log::SecId,
        sink: Arc<dyn SagaLogSink>,
        /* XXX should the string be an enum? */
        templates: BTreeMap<
            &'static str,
            Arc<dyn steno::SagaTemplateGeneric<T>>,
        >,
    ) -> SagaExecCoordinatorHandle<T>
    where
        T: Send + Sync + 'static,
    {
        let hdl_log = log.new(o!());
        let (msg_tx, msg_rx) = mpsc::channel(3); // XXX
        let mut sec = SagaExecCoordinator {
            sec_id: *sec_id,
            log,
            pool,
            sagas: BTreeMap::new(),
            msg_rx,
            msg_tx: msg_tx.clone(),
            sink,
            templates: Arc::new(templates),
            recovered: RecoverState::NotStarted,
        };

        let main_task = tokio::spawn(async move { sec.run_sec().await });
        let recover_permit = Some(msg_tx.clone().try_reserve_owned().unwrap());
        let shutdown_permit = Some(msg_tx.clone().try_reserve_owned().unwrap());
        SagaExecCoordinatorHandle {
            log: hdl_log,
            main_task,
            msg_tx,
            recover_permit,
            shutdown_permit,
        }
    }

    /*
     * XXX TODO-cleanup This is really stupid. It's here because of a circular
     * reference between Nexus and SEC, and our desire to use permits so that
     * start_recovery() cannot fail.
     */
    pub fn prepare_recovery(&mut self) -> RecoveryToken<T> {
        RecoveryToken {
            permit: self
                .recover_permit
                .take()
                .expect("cannot start recovery twice"),
        }
    }

    pub fn start_recovery(&self, t: RecoveryToken<T>, saga_ctx: Arc<T>)
    where
        T: Send + Sync + 'static,
    {
        t.permit.send(SecMsg::RecoverStart { saga_ctx });
    }

    pub async fn saga_run(
        &self,
        saga_id: steno::SagaId,
        exec: Arc<dyn steno::SagaExecManager>,
    ) -> steno::SagaResult {
        let (notify_done, wait_done) = oneshot::channel();
        let secmsg = SecMsg::SagaAdd { saga_id, notify_done, exec };
        self.must_send(secmsg).await;
        wait_done.await.unwrap()
    }

    pub async fn sagas_list_page(
        &self,
        pagparams: &model::DataPageParams<'_, Uuid>,
    ) -> Result<Vec<Result<model::ApiSagaView, ApiError>>, ApiError> {
        let (tx, rx) = oneshot::channel();
        let marker = pagparams.marker.map(|u| *u);
        let limit = pagparams.limit;
        assert_eq!(pagparams.direction, model::PaginationOrder::Ascending);
        let secmsg = SecMsg::SagasList { marker, limit, result_channel: tx };
        self.must_send(secmsg).await;
        rx.await.unwrap()
    }

    pub async fn saga_get(
        &self,
        saga_id: &Uuid,
    ) -> Result<model::ApiSagaView, ApiError> {
        let (tx, rx) = oneshot::channel();
        let secmsg = SecMsg::SagaGet { saga_id: *saga_id, result_channel: tx };
        self.must_send(secmsg).await;
        rx.await.unwrap()
    }

    async fn must_send(&self, msg: SecMsg<T>) {
        /*
         * It should never be possible for the SEC to shutdown while we're still
         * around.
         */
        self.msg_tx
            .send(msg)
            .await
            .unwrap_or_else(|e| panic!("channel closed unexpectedly: {:#}", e));
    }
}

impl<T> Drop for SagaExecCoordinatorHandle<T> {
    fn drop(&mut self) {
        warn!(&self.log, "initiating shutdown");
        let permit = self.shutdown_permit.take().expect("dropped twice");
        permit.send(SecMsg::Shutdown);
    }
}

enum RecoverState {
    NotStarted,
    SagasListed(Vec<sec::log::Saga>),
    Running(tokio::task::JoinHandle<()>),
    Done,
}

impl fmt::Debug for RecoverState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RecoverState::NotStarted => "NotStarted",
            RecoverState::SagasListed(_) => "SagasListed",
            RecoverState::Running(_) => "Running",
            RecoverState::Done => "Done",
        })
    }
}

struct SagaExecCoordinator<T> {
    sec_id: sec::log::SecId,
    log: Logger,
    pool: Arc<db::Pool>,
    sagas: BTreeMap<steno::SagaId, SagaLoadState>,
    msg_rx: mpsc::Receiver<SecMsg<T>>,
    msg_tx: mpsc::Sender<SecMsg<T>>,
    sink: Arc<dyn SagaLogSink>,
    templates:
        Arc<BTreeMap<&'static str, Arc<dyn steno::SagaTemplateGeneric<T>>>>,
    recovered: RecoverState,
}

impl<T> SagaExecCoordinator<T>
where
    T: Send + Sync + 'static,
{
    async fn run_sec(&mut self) {
        /* XXX How do we report asynchronous errors? */
        let log = &self.log;
        info!(log, "starting saga execution coordinator");
        info!(log, "searching for existing sagas");

        // XXX replace unwrap with retry loop, log on error
        // XXX comment about it being important that we finish this before
        // recovery
        let sagas =
            recovery::list_sagas(&self.pool, &self.sec_id).await.unwrap();
        info!(log, "found sagas"; "nsagas" => sagas.len());

        for saga in &sagas {
            assert!(self
                .sagas
                .insert(saga.id, SagaLoadState::Unloaded)
                .is_none());
        }

        assert!(matches!(self.recovered, RecoverState::NotStarted));
        self.recovered = RecoverState::SagasListed(sagas);

        while let Some(msg) = self.msg_rx.recv().await {
            trace!(&self.log, "received message"; "message" => ?msg);
            match msg {
                SecMsg::RecoverStart { saga_ctx } => {
                    assert!(matches!(
                        self.recovered,
                        RecoverState::SagasListed(_)
                    ));
                    self.msg_recover_start(saga_ctx);
                    assert!(matches!(self.recovered, RecoverState::Running(_)));
                }
                SecMsg::SagaRecovered { saga_id, load_state } => {
                    assert!(matches!(self.recovered, RecoverState::Running(_)));
                    self.msg_saga_recovered(saga_id, load_state);
                }
                SecMsg::SagaRecoveryDone => {
                    assert!(matches!(self.recovered, RecoverState::Running(_)));
                    self.msg_saga_recovery_done().await;
                }
                SecMsg::SagaAdd { saga_id, exec, notify_done } => {
                    self.msg_saga_add(saga_id, exec, notify_done);
                }
                SecMsg::SagaDone { saga_id } => {
                    self.msg_saga_done(saga_id).await;
                }
                SecMsg::Shutdown => {
                    todo!(); // XXX
                }
                SecMsg::SagasList { marker, limit, result_channel } => {
                    self.msg_sagas_list_page(marker, limit, result_channel);
                }
                SecMsg::SagaGet { saga_id, result_channel } => {
                    self.msg_saga_get(saga_id, result_channel);
                }
            }
        }

        info!(&self.log, "shutting down");
    }

    fn msg_recover_start(&mut self, saga_ctx: Arc<T>) {
        /*
         * It's a little tricky to satisfy the borrow checker here without an
         * extra copy.  We expect `self.recovered` here to be
         * `SagasListed(Vec<...>)`.  If so, we want to take that Vec, pass it to
         * an async function, run that in another task, and replace
         * `self.recovered` with a new variant `Running` that contains the task.
         * But we can't move the Vec out of the enum while it's in `self`, and
         * we can't put the appropriate variant into `self` until after we've
         * moved the Vec.  To work around this, we use "NotStarted" as a dummy
         * value that we can stick into "self" just for this instant.  (We could
         * add a real variant for this case, but it's annoying to muddy up all
         * the other consumers of this enum with this one small edge case.)
         * TODO-cleanup Is there a better way?
         */
        let dummy = RecoverState::NotStarted;
        let recov = std::mem::replace(&mut self.recovered, dummy);

        let sagas = match recov {
            RecoverState::SagasListed(sagas) => sagas,
            other => {
                /* Put things back the way we found it. */
                self.recovered = other;
                panic!(
                    "started recovery in unexpected state: {:?}",
                    self.recovered
                );
            }
        };

        let recover = Recover {
            log: self.log.new(o!()),
            tx: self.msg_tx.clone(),
            pool: Arc::clone(&self.pool),
            sec_id: self.sec_id,
            saga_ctx,
            sink: Arc::clone(&self.sink),
            sagas,
            templates: Arc::clone(&self.templates),
        };

        let task = tokio::spawn(recover_sagas(recover));
        self.recovered = RecoverState::Running(task);
    }

    fn msg_saga_recovered(
        &mut self,
        saga_id: steno::SagaId,
        load_state: SagaLoadState,
    ) {
        assert!(!matches!(load_state, SagaLoadState::Unloaded));
        let previous = self.sagas.insert(saga_id, load_state);
        assert!(matches!(previous, Some(SagaLoadState::Unloaded)));
    }

    fn msg_saga_add(
        &mut self,
        saga_id: steno::SagaId,
        exec: Arc<dyn steno::SagaExecManager>,
        notify_done: oneshot::Sender<steno::SagaResult>,
    ) {
        let tx = self.msg_tx.clone();
        let mut load_state = exec_saga(saga_id, exec, tx);
        match load_state {
            SagaLoadState::Running { waiter: ref mut w @ None, .. } => {
                w.replace(notify_done)
            }
            _ => {
                panic!("unexpected load state from exec_saga: {:?}", load_state)
            }
        };
        assert!(self.sagas.insert(saga_id, load_state).is_none());
    }

    async fn msg_saga_done(&mut self, saga_id: steno::SagaId) {
        let load_state = self.sagas.remove(&saga_id);
        /* XXX TODO-cleanup */
        let (waiter, result1, result2) = match load_state {
            Some(SagaLoadState::Running { task, exec, waiter }) => {
                /*
                 * This should always be very quick, as this task should have
                 * finished as soon as it sent the SagaDone message.
                 */
                task.await.expect("failed to wait for saga task");
                (waiter, exec.result(), exec.result())
            }
            _ => {
                panic!(
                    "unexpected load state when saga done: {:?}",
                    load_state
                );
            }
        };

        info!(
            &self.log,
            "saga done";
            "saga_id" => saga_id.to_string(),
            "result" => ?result1,
        );

        if let Some(w) = waiter {
            w.send(result1).unwrap_or_else(|_| {
                panic!("waiter gone before we could send message")
            });
        }

        self.sagas.insert(saga_id, SagaLoadState::Done { result: result2 });
    }

    async fn msg_saga_recovery_done(&mut self) {
        match &mut self.recovered {
            RecoverState::Running(ref mut task) => {
                /*
                 * This should always be very quick, as this task should have
                 * finished as soon as it sent the SagaDone message.
                 */
                task.await.expect("failed to wait for task");
            }
            other => {
                panic!("unexpected RecoveryDone message in state {:?}", other)
            }
        }

        self.recovered = RecoverState::Done;
    }

    fn msg_sagas_list_page(
        &mut self,
        marker: Option<Uuid>,
        limit: NonZeroU32,
        result_channel: oneshot::Sender<
            Result<Vec<Result<model::ApiSagaView, ApiError>>, ApiError>,
        >,
    ) {
        let marker = marker.map(|u| steno::SagaId(u));
        let pagparams = model::DataPageParams {
            marker: marker.as_ref(),
            direction: model::PaginationOrder::Ascending,
            limit,
        };
        let list = omicron_common::collection::collection_page_as_iter(
            &self.sagas,
            &pagparams,
        )
        .map(|(saga_id, load_state)| self.saga_view(saga_id, load_state))
        .collect::<Vec<Result<model::ApiSagaView, ApiError>>>();
        result_channel.send(Ok(list)).unwrap();
    }

    fn msg_saga_get(
        &mut self,
        saga_id: Uuid,
        result_channel: oneshot::Sender<Result<model::ApiSagaView, ApiError>>,
    ) {
        let id = steno::SagaId(saga_id);
        let rv = self
            .sagas
            .get(&id)
            .ok_or_else(|| {
                ApiError::not_found_by_id(
                    model::ApiResourceType::SagaDbg,
                    &saga_id,
                )
            })
            .and_then(|load_state| self.saga_view(&id, load_state));
        result_channel.send(rv).unwrap();
    }

    fn saga_view(
        &self,
        saga_id: &steno::SagaId,
        load_state: &SagaLoadState,
    ) -> Result<model::ApiSagaView, ApiError> {
        // XXX all over this function
        let time: DateTime<Utc> = "2021-04-28T02:18:25Z".parse().unwrap();
        let state = match load_state {
            SagaLoadState::Unloaded => model::ApiSagaStateView::Unloaded,
            SagaLoadState::Running { .. } => model::ApiSagaStateView::Running,
            SagaLoadState::Done { result } => match &result.kind {
                Ok(_) => model::ApiSagaStateView::Done {
                    failed: false,
                    error_node_name: None,
                    error_info: None,
                },
                Err(error) => model::ApiSagaStateView::Done {
                    failed: true,
                    error_node_name: Some(error.error_node_name.clone()),
                    error_info: Some(error.error_source.clone()),
                },
            },
            SagaLoadState::Abandoned { time, reason } => {
                model::ApiSagaStateView::Abandoned {
                    time_abandoned: *time,
                    reason: format!("{:#}", reason),
                }
            }
        };
        Ok(model::ApiSagaView {
            id: saga_id.0,
            template: model::ApiSagaTemplateView {
                name: "unknown".to_owned(), // XXX
                nodes: vec![],              // XXX
            },
            state,
            identity: model::ApiIdentityMetadata {
                id: saga_id.0,
                name: model::ApiName::try_from("unused".to_owned()).unwrap(),
                description: "unused".to_owned(),
                time_created: time,
                time_modified: time,
            },
        })
    }
}

struct Recover<T> {
    log: Logger,
    tx: mpsc::Sender<SecMsg<T>>,
    pool: Arc<db::Pool>,
    sec_id: sec::log::SecId,
    saga_ctx: Arc<T>,
    sink: Arc<dyn steno::SagaLogSink>,
    sagas: Vec<sec::log::Saga>,
    templates:
        Arc<BTreeMap<&'static str, Arc<dyn steno::SagaTemplateGeneric<T>>>>,
}

/*
 * TODO See dtolnay/thiserror#98.  It would be nice to avoid explicitly needing
 * to put the source into these messages.
 */
#[derive(Error, Debug)]
enum RecoverError {
    #[error(
        "failed to recover saga \"{saga_id}\": database error: {source:#}"
    )]
    DatabaseError { saga_id: steno::SagaId, source: ApiError },

    #[error(
        "failed to recover saga \"{saga_id}\": \
         saga uses unknown template: \"{template_name}\""
    )]
    MissingTemplate { saga_id: steno::SagaId, template_name: String },

    #[error("failed to recover saga \"{saga_id}\": log error: {source:#}")]
    LogError { saga_id: steno::SagaId, source: anyhow::Error },
}

impl RecoverError {
    fn retryable(&self) -> bool {
        match self {
            RecoverError::DatabaseError { source, .. } => source.retryable(),
            RecoverError::MissingTemplate { .. } => false,
            RecoverError::LogError { .. } => false,
        }
    }
}

async fn recover_sagas<T: Send + Sync + 'static>(recover: Recover<T>) {
    for saga in &recover.sagas {
        let log = recover.log.new(o!("saga_id" => saga.id.to_string()));
        debug!(log, "recover saga");

        let load_state = match recover_saga(&recover, saga).await {
            Ok(s) => {
                info!(&log, "recovered saga");
                s
            }
            Err(error) if error.retryable() => {
                // XXX sleep and retry -- if appropriate!
                // XXX is this the best way to put the error in the log?
                warn!(&log, "failed to recover saga {:#}", error);
                panic!("failed to recover saga: {:#}", error);
            }
            Err(error) => {
                assert!(!error.retryable());
                // XXX is this the best way to put the error in the log?
                error!(&log, "failed to recover saga {:#}", error);
                SagaLoadState::Abandoned {
                    time: Utc::now(),
                    reason: anyhow!(error).context("abandoned at recovery"),
                }
            }
        };

        // XXX can we use a permit here instead?
        let msg = SecMsg::SagaRecovered { saga_id: saga.id, load_state };
        recover.tx.send(msg).await.unwrap_or_else(|error| {
            panic!("channel closed unexpectedly: {:#}", error);
        });
    }

    recover.tx.send(SecMsg::SagaRecoveryDone).await.unwrap_or_else(|error| {
        panic!("channel closed unexpectedly: {:#}", error);
    });
}

async fn recover_saga<T: Send + Sync + 'static>(
    recover: &Recover<T>,
    saga: &sec::log::Saga,
) -> Result<SagaLoadState, RecoverError> {
    // XXX bail unless current_sec == us
    let saga_id = saga.id;
    let template = recover
        .templates
        .get(saga.template_name.as_str())
        .ok_or_else(|| RecoverError::MissingTemplate {
            saga_id,
            template_name: saga.template_name.clone(),
        })?;

    let sglog = recovery::load_saga_log(
        &recover.pool,
        &saga,
        Arc::clone(&recover.sink),
    )
    .await
    .map_err(|source| RecoverError::DatabaseError { saga_id, source })?;

    let exec = Arc::clone(template)
        .recover(
            sglog,
            recover.sec_id.0.to_string().as_str(),
            Arc::clone(&recover.saga_ctx),
            Arc::clone(&recover.sink),
        )
        .map_err(|source| RecoverError::LogError { saga_id, source })?;

    Ok(exec_saga(saga_id, exec, recover.tx.clone()))
}

fn exec_saga<T: Send + Sync + 'static>(
    saga_id: steno::SagaId,
    exec: Arc<dyn steno::SagaExecManager>,
    tx: mpsc::Sender<SecMsg<T>>,
) -> SagaLoadState {
    let exec_clone = Arc::clone(&exec);
    let task = tokio::spawn(async move {
        exec_clone.run().await;
        tx.send(SecMsg::SagaDone { saga_id }).await.unwrap_or_else(|e| {
            // XXX
            panic!("channel closed unexpectedly: {:#}", e);
        });
    });
    SagaLoadState::Running { task: task, exec: exec, waiter: None }
}
