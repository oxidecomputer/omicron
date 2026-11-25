// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Shared configuration utilities for omicron-dev and jbor.

use anyhow::Context;
use camino::Utf8PathBuf;
use futures::StreamExt;
use libc::SIGINT;
use nexus_config::NexusConfig;
use signal_hook_tokio::Signals;
use std::fs;

pub trait Configurable {
    fn nexus_config(&self) -> &Utf8PathBuf;
}

pub fn start_stream() -> futures::stream::Fuse<signal_hook_tokio::SignalsInfo> {
    let signals = Signals::new(&[SIGINT]).expect("failed to wait for SIGINT");
    signals.fuse()
}

pub fn read_config(
    args: &dyn Configurable,
) -> Result<NexusConfig, anyhow::Error> {
    let config_str = fs::read_to_string(&args.nexus_config())?;
    let mut config: NexusConfig = toml::from_str(&config_str)
        .context(format!("parsing config: {}", args.nexus_config().as_str()))?;
    config.pkg.log = dropshot::ConfigLogging::File {
        // See LogContext::new(),
        path: "UNUSED".to_string().into(),
        level: dropshot::ConfigLoggingLevel::Trace,
        if_exists: dropshot::ConfigLoggingIfExists::Fail,
    };
    Ok(config)
}
