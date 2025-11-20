// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::context::ServerContext;
use dropshot::HttpError;
use dropshot::HttpResponseOk;
use dropshot::RequestContext;
use slog::error;
use slog::info;
use slog_error_chain::InlineErrorChain;
use std::sync::Arc;
use update_admin_api::*;
use update_admin_types::SmfServicesInMaintenance;

type UpdateAdminApiDescription = dropshot::ApiDescription<Arc<ServerContext>>;

pub fn api() -> UpdateAdminApiDescription {
    update_admin_api_mod::api_description::<UpdateAdminImpl>()
        .expect("registered entrypoints")
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateAdminError {
    #[error("failed to execute pilot within switch zone")]
    ExecutePilot(#[source] std::io::Error),
}

impl From<UpdateAdminError> for HttpError {
    fn from(err: UpdateAdminError) -> Self {
        // All errors are currently treated as 500s
        HttpError::for_internal_error(InlineErrorChain::new(&err).to_string())
    }
}

enum UpdateAdminImpl {}

impl UpdateAdminImpl {
    async fn smf_services_in_maintenance_get(
        ctx: &ServerContext,
    ) -> Result<SmfServicesInMaintenance, UpdateAdminError> {
        let log = ctx.log();
        info!(log, "listing SMF services in maintenance");

        let output = tokio::process::Command::new("/usr/bin/pilot")
            .args(["host", "exec", "-c", "'svcs -xZ'", "0-31"])
            .output()
            .await
            .map_err(UpdateAdminError::ExecutePilot)?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let result = parse_services_in_maintenance_result(&stdout);
        info!(log, "parse_services_in_maintenance_result"; "result" => ?result);
        result
    }
}

impl UpdateAdminApi for UpdateAdminImpl {
    type Context = Arc<ServerContext>;

    async fn smf_services_in_maintenance(
        rqctx: RequestContext<Self::Context>,
    ) -> Result<HttpResponseOk<SmfServicesInMaintenance>, HttpError> {
        let ctx = rqctx.context();
        let response = Self::smf_services_in_maintenance_get(ctx).await?;
        Ok(HttpResponseOk(response))
    }
}

fn parse_services_in_maintenance_result(
    stdout: &str,
) -> Result<SmfServicesInMaintenance, UpdateAdminError> {
    Ok(SmfServicesInMaintenance { services_by_sled: stdout.to_string() })
}
