// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Interface for making API requests to an Omicron NTP admin server

progenitor::generate_api!(
    spec = "../../openapi/ntp-admin/ntp-admin-latest.json",
    interface = Positional,
    inner_type = slog::Logger,
    derives = [schemars::JsonSchema],
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
