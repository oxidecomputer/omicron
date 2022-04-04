// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

// Required nightly features for `usdt`
#![cfg_attr(target_os = "macos", feature(asm_sym))]
#![feature(asm)]

//! This crate provides UDP-based communication across the Oxide management
//! switch to a collection of SPs.
//!
//! The primary entry point is [`Communicator`].

mod communicator;
mod management_switch;
mod recv_handler;

pub use usdt::register_probes;

pub mod error;

pub use communicator::Communicator;
pub use communicator::FuturesUnorderedImpl;
pub use management_switch::SpIdentifier;
pub use management_switch::SpType;
pub use recv_handler::SerialConsoleChunk;
pub use recv_handler::SerialConsoleContents;

// TODO these will remain public for a while, but eventually will be removed
// altogther; currently these provide a way to hard-code the rack topology,
// which is not what we want.
pub use management_switch::KnownSp;
pub use management_switch::KnownSps;
