use crate::buffer;
use crate::EreportData;
use dropshot::{
    ConfigLogging, EmptyScanParams, HttpError, HttpResponseDeleted,
    HttpResponseOk, PaginationParams, Query, RequestContext, ResultsPage,
    WhichPage,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use slog::Logger;
use std::collections::VecDeque;
use std::net::SocketAddr;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct ServerStarter {
    config: Config,
    ctx: ServerContext,
    ereports: mpsc::Receiver<EreportData>,
    requests: mpsc::Receiver<buffer::ServerReq>,
}

pub struct RunningServer {
    // Handle to the buffer task.
    buffer_task: tokio::task::JoinHandle<()>,
    // TODO(eliza): hang onto the running dropshot stuff.
}

pub struct ReporterIdentity {
    /// The UUID of the reporter endpoint.
    pub id: Uuid,
    /// The socket address on which to bind the reporter endpoint.
    pub addr: SocketAddr,
}

pub struct Config {
    pub reporter: ReporterIdentity,
    /// The address at which we attempt to register as a producer.
    ///
    /// If the address is not provided, the address of Nexus will be resolved
    /// using internal DNS, based on the local address of the server being
    /// configured.
    pub registration_address: Option<SocketAddr>,
    /// The maximum size of Dropshot requests.
    pub request_body_max_bytes: usize,
    /// The maximum number of ereports to buffer before exerting backpressure on producers.
    pub buffer_capacity: usize,
    pub request_channel_capacity: usize,
    /// The logging configuration or actual logger used to emit logs.
    pub log: LogConfig,
}

/// Either configuration for building a logger, or an actual logger already
/// instantiated.
///
/// This can be used to start a [`Server`] with a new logger or a child of a
/// parent logger if desired.
#[derive(Debug, Clone)]
pub enum LogConfig {
    /// Configuration for building a new logger.
    Config(ConfigLogging),
    /// An explicit logger to use.
    Logger(Logger),
}

#[derive(Clone)]
struct ServerContext {
    tx: mpsc::Sender<buffer::ServerReq>,
}

struct EreporterApiImpl;

impl ServerStarter {
    pub fn new(config: Config) -> (crate::Reporter, Self) {
        let (ereport_tx, ereports) = mpsc::channel(config.buffer_capacity);
        let (tx, requests) = mpsc::channel(128);
        let this =
            Self { config, ereports, ctx: ServerContext { tx }, requests };
        (crate::Reporter(ereport_tx), this)
    }

    pub async fn start(self) -> anyhow::Result<RunningServer> {
        let Self { config, ctx, ereports, requests } = self;
        let log = todo!("eliza: log config");
        // TODO:
        // 1. discover nexus
        // 2. register server and recover sequence number
        let seq = todo!("eliza: discover sequence number");
        // 3. spawn buffer task
        let buffer_task = tokio::spawn(
            crate::buffer::Buffer {
                seq,
                buf: VecDeque::with_capacity(config.buffer_capacity),
                log,
                id: config.reporter.id,
                ereports,
                requests,
            }
            .run(),
        );
        // 4. spawn dropshot server
        todo!("eliza: dropshot server");

        Ok(RunningServer { buffer_task })
    }
}

impl ereporter_api::EreporterApi for EreporterApiImpl {
    type Context = ServerContext;

    async fn ereports_list(
        reqctx: RequestContext<Self::Context>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<ereporter_api::Ereport>>, HttpError>
    {
        let ctx = reqctx.context();

        let pagination = query.into_inner();
        let limit = reqctx.page_limit(&pagination)?.get() as usize;

        let start_seq = match pagination.page {
            WhichPage::First(..) => None,
            WhichPage::Next(seq) => Some(seq),
        };

        slog::debug!(
            reqctx.log,
            "received ereport list request";
            "start_seq" => ?start_seq,
            "limit" => limit,
        );
        let (tx, rx) = oneshot::channel();
        ctx.tx
            .send(buffer::ServerReq::List { start_seq, limit, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        let list = rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })?;
        let page = ResultsPage::new(
            list,
            &EmptyScanParams {},
            |ereport: &ereporter_api::Ereport, _| ereport.seq,
        )?;
        Ok(HttpResponseOk(page))
    }

    async fn ereports_truncate(
        reqctx: RequestContext<Self::Context>,
        path: dropshot::Path<ereporter_api::SeqPathParam>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let ctx = reqctx.context();
        let ereporter_api::SeqPathParam { seq } = path.into_inner();
        slog::debug!(reqctx.log, "received ereport truncate request"; "seq" => ?seq);
        let (tx, rx) = oneshot::channel();
        ctx.tx
            .send(buffer::ServerReq::TruncateTo { seq, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })??;
        Ok(HttpResponseDeleted())
    }
}
