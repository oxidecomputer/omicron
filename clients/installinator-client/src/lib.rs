// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for installinator to make API requests.

progenitor::generate_api!(
    spec = "../../openapi/installinator/installinator-1.0.0-c0ed87.json",
    interface = Positional,
    inner_type = slog::Logger,
    derives = [schemars::JsonSchema],
    crates = {
        "omicron-uuid-kinds" = "*",
    },
    replace = {
        Duration = std::time::Duration,
        EventReportForGenericSpec = oxide_update_engine_types::events::EventReport<oxide_update_engine_types::spec::GenericSpec>,
        ProgressEventForGenericSpec = oxide_update_engine_types::events::ProgressEvent<oxide_update_engine_types::spec::GenericSpec>,
        StepEventForGenericSpec = oxide_update_engine_types::events::StepEvent<oxide_update_engine_types::spec::GenericSpec>,
    }
);

impl progenitor::progenitor_client::ClientHooks<slog::Logger> for Client {
    async fn pre<E>(
        &self,
        request: &mut reqwest::Request,
        _info: &progenitor::progenitor_client::OperationInfo,
    ) -> Result<(), progenitor::progenitor_client::Error<E>> {
        slog::debug!(self.inner(), "client request";
            "method" => %request.method(),
            "uri" => %request.url(),
            "body" => ?&request.body(),
        );
        Ok(())
    }

    async fn post<E>(
        &self,
        result: &reqwest::Result<reqwest::Response>,
        _info: &progenitor::progenitor_client::OperationInfo,
    ) -> Result<(), progenitor::progenitor_client::Error<E>> {
        slog::debug!(self.inner(), "client response"; "result" => ?result);
        Ok(())
    }
}

/// A type alias for errors returned by this crate.
pub type ClientError = crate::Error<crate::types::Error>;
