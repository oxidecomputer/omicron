// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Module containing types for updating RoT Bootloaders via MGS.

use super::MgsClients;
use crate::SpComponentUpdateHelper;
use crate::common_sp_update::PrecheckError;
use crate::common_sp_update::PrecheckStatus;
use futures::future::BoxFuture;
use nexus_types::deployment::PendingMgsUpdate;

type GatewayClientError = gateway_client::Error<gateway_client::types::Error>;

pub struct ReconfiguratorRotBootloaderUpdater;
impl SpComponentUpdateHelper for ReconfiguratorRotBootloaderUpdater {
    /// Checks if the component is already updated or ready for update
    fn precheck<'a>(
        &'a self,
        _log: &'a slog::Logger,
        _mgs_clients: &'a mut MgsClients,
        _update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<PrecheckStatus, PrecheckError>> {
        // TODO-K: To be completed in a follow up PR
        todo!()
    }

    /// Attempts once to perform any post-update actions (e.g., reset the
    /// device)
    fn post_update<'a>(
        &'a self,
        _log: &'a slog::Logger,
        _mgs_clients: &'a mut MgsClients,
        _update: &'a PendingMgsUpdate,
    ) -> BoxFuture<'a, Result<(), GatewayClientError>> {
        // TODO-K: To be completed in a follow up PR
        todo!()
    }
}
