// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Copyright 2022 Oxide Computer Company

// Required nightly features for `usdt`
#![cfg_attr(target_os = "macos", feature(asm_sym))]

//! This crate provides UDP-based communication across the Oxide management
//! switch to a collection of SPs.
//!
//! The primary entry point is [`Communicator`].

mod communicator;
mod management_switch;
mod single_sp;
mod timeout;

pub use usdt::register_probes;

pub mod error;

pub use communicator::Communicator;
pub use communicator::FuturesUnorderedImpl;
pub use management_switch::LocationConfig;
pub use management_switch::LocationDeterminationConfig;
pub use management_switch::SpIdentifier;
pub use management_switch::SpType;
pub use management_switch::SwitchConfig;
pub use management_switch::SwitchPortConfig;
pub use single_sp::AttachedSerialConsole;
pub use single_sp::AttachedSerialConsoleRecv;
pub use single_sp::AttachedSerialConsoleSend;
pub use single_sp::SingleSp;
pub use single_sp::DISCOVERY_MULTICAST_ADDR;
pub use timeout::Elapsed;
pub use timeout::Timeout;
