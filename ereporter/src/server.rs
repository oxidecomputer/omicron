use crate::buffer;
use crate::registry::ReporterRegistry;
use dropshot::{
    ConfigDropshot, EmptyScanParams, HttpError, HttpResponseDeleted,
    HttpResponseOk, HttpResponseUpdatedNoContent, PaginationParams, Path,
    Query, RequestContext, ResultsPage, TypedBody, WhichPage,
};
use omicron_common::api::external::Error;
use omicron_common::api::external::Generation;
use std::net::SocketAddr;
use tokio::sync::oneshot;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    pub server_address: SocketAddr,
    // /// The maximum size of Dropshot requests.
    pub request_body_max_bytes: usize,
}

pub struct RunningServer {
    _server: dropshot::HttpServer<ReporterRegistry>,
}

impl ReporterRegistry {
    pub fn start_server(
        &self,
        config: Config,
    ) -> anyhow::Result<RunningServer> {
        let log = &self.0.log;

        let server = {
            let dropshot_cfg = ConfigDropshot {
                bind_address: config.server_address,
                request_body_max_bytes: config.request_body_max_bytes,
                default_handler_task_mode: dropshot::HandlerTaskMode::Detached,
                log_headers: vec![],
            };
            let log = log.new(slog::o!("component" => "dropshot"));
            let api = ereporter_api::ereporter_api_mod::api_description::<
                EreporterApiImpl,
            >()?;
            dropshot::HttpServerStarter::new(
                &dropshot_cfg,
                api,
                self.clone(),
                &log,
            )
            .map_err(|e| {
                anyhow::anyhow!("could not start dropshot server: {e}")
            })?
            .start()
        };

        Ok(RunningServer { _server: server })
    }
}

struct EreporterApiImpl;
impl ereporter_api::EreporterApi for EreporterApiImpl {
    type Context = ReporterRegistry;

    async fn ereports_list(
        reqctx: RequestContext<Self::Context>,
        path: Path<ereporter_api::ReporterId>,
        query: Query<PaginationParams<EmptyScanParams, Generation>>,
    ) -> Result<HttpResponseOk<ResultsPage<ereporter_api::Entry>>, HttpError>
    {
        let registry = reqctx.context();
        let pagination = query.into_inner();
        let limit = reqctx.page_limit(&pagination)?.get() as usize;
        let ereporter_api::ReporterId { id, generation } = path.into_inner();

        let start_seq = match pagination.page {
            WhichPage::First(..) => None,
            WhichPage::Next(seq) => Some(seq),
        };

        slog::debug!(
            reqctx.log,
            "received ereport list request";
            "reporter_id" => %id,
            "reporter_gen" => %generation,
            "start_seq" => ?start_seq,
            "limit" => limit,
        );
        let worker =
            registry.get_worker(&id).ok_or(HttpError::for_not_found(
                Some("NO_REPORTER".to_string()),
                format!("no reporter with ID {id}"),
            ))?;
        let (tx, rx) = oneshot::channel();
        worker
            .send(buffer::ServerReq::List { generation, start_seq, limit, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        let list = rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })??;
        let page = ResultsPage::new(
            list,
            &EmptyScanParams {},
            |entry: &ereporter_api::Entry, _| entry.seq,
        )?;
        Ok(HttpResponseOk(page))
    }

    async fn ereports_acknowledge(
        reqctx: RequestContext<Self::Context>,
        path: Path<ereporter_api::AcknowledgePathParams>,
    ) -> Result<HttpResponseDeleted, HttpError> {
        let registry = reqctx.context();
        let ereporter_api::AcknowledgePathParams {
            reporter: ereporter_api::ReporterId { id, generation },
            seq,
        } = path.into_inner();
        slog::debug!(
            reqctx.log,
            "received ereport acknowledge request";
            "reporter_id" => %id,
            "reporter_generation" => %generation,
            "seq" => ?seq,
        );

        let worker =
            registry.get_worker(&id).ok_or(HttpError::for_not_found(
                Some("NO_REPORTER".to_string()),
                format!("no reporter with ID {id}"),
            ))?;
        let (tx, rx) = oneshot::channel();
        worker
            .send(buffer::ServerReq::Ack { seq, generation, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })??;
        Ok(HttpResponseDeleted())
    }

    async fn ereporter_put_generation(
        reqctx: RequestContext<Self::Context>,
        path: Path<ereporter_api::PutGenerationPathParams>,
        body: TypedBody<ereporter_api::PutGenerationBody>,
    ) -> Result<HttpResponseUpdatedNoContent, HttpError> {
        let registry = reqctx.context();
        let ereporter_api::PutGenerationPathParams { id } = path.into_inner();
        let ereporter_api::PutGenerationBody { generation, generation_id } =
            body.into_inner();
        slog::debug!(
            reqctx.log,
            "received ereporter register request";
            "reporter_id" => %id,
            "reporter_generation" => %generation,
            "reporter_generation_id" => %generation_id,
        );

        let worker =
            registry.get_worker(&id).ok_or(HttpError::for_not_found(
                Some("NO_REPORTER".to_string()),
                format!("no reporter with ID {id}"),
            ))?;
        let (tx, rx) = oneshot::channel();
        worker
            .send(buffer::ServerReq::Register { generation, generation_id, tx })
            .await
            .map_err(|_| Error::internal_error("server shutting down"))?;
        rx.await.map_err(|_| {
            // This shouldn't happen!
            Error::internal_error("buffer canceled request rudely!")
        })??;
        Ok(HttpResponseUpdatedNoContent())
    }
}
