// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Reconciler for maintaining internal copy of DNS configuration

// XXX-dap move all of this into datastore?

use dns_service_client::types::DnsConfigParams;
use dns_service_client::types::DnsConfigZone;
use nexus_db_model::DnsGroup;
use nexus_db_model::DnsVersion;
use nexus_db_queries::context::OpContext;
use nexus_db_queries::db::DataStore;
use omicron_common::api::external::DataPageParams;
use omicron_common::api::external::Error;
use omicron_common::api::external::InternalContext;
use omicron_common::bail_unless;
use slog::debug;
use std::num::NonZeroU32;
use std::sync::Arc;
use tokio::sync::watch;

pub struct DnsConfigReconciler {
    config_tx: watch::Sender<Option<DnsConfigParams>>,
    config_rx: watch::Receiver<Option<DnsConfigParams>>,
    datastore: Arc<DataStore>,
    dns_group: DnsGroup,
}

// XXX-dap some thoughts on the generalizeable interface
// accept in constructor a channel on which it receives messages to "go"
// that message contains a channel it can use to send reports back (e.g.,
// progress)

impl DnsConfigReconciler {
    pub fn new(
        datastore: Arc<DataStore>,
        dns_group: DnsGroup,
    ) -> DnsConfigReconciler {
        let (config_tx, config_rx) = watch::channel(None);
        DnsConfigReconciler { config_tx, config_rx, datastore, dns_group }
    }
}
