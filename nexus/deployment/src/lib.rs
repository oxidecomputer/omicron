// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Omicron deployment management
//!
//! "Deployment management" here refers broadly to managing the lifecycle of
//! software components.  That includes deployment, undeployment, upgrade,
//! bringing into service, and removing from service.  It includes
//! dynamically-deployed components (like most Omicron zones, like Nexus and
//! CockroachDB) as well as components that are tied to fixed physical hardware
//! (like the host operating system and device firmware).  This system will
//! potentially manage configuration, too.  See RFD 418 for background and a
//! survey of considerations here.
//!
//! The basic idea is that you have:
//!
//! * **fleet policy**: describes things like how many CockroachDB nodes there
//!   should be, how many Nexus nodes there should be, the target system version
//!   that all software should be running, which sleds are currently in service,
//!   etc.
//!
//! * **inventory [collections]**: describes what software is currently running
//!   on which hardware components, including versions and configuration.  This
//!   includes all control-plane-managed software, including device firmware,
//!   host operating system, Omicron zones, etc.
//!
//! * **[deployment] plan**: describes what software _should_ be running on
//!   which hardware components, including versions and configuration.  Like
//!   inventory collections, the plan covers all control-plane-managed software.
//!   Plans must be specific enough that multiple Nexus instances can attempt to
//!   realize the same plan concurrently without stomping on each other.  (For
//!   example, it's not specific enough to say "there should be one more
//!   CockroachDB node" or even "there should be six CockroachDB nodes" because
//!   two Nexus instances might _both_ decide to provision a new node and then
//!   we'd have too many.)  Plans must also be incremental enough that any
//!   execution of them should not break the system.  For example, if between
//!   two consecutive plans the version of every CockroachDB node changed, then
//!   concurrent plan execution might try to update them all at once, bringing
//!   the whole Cockroach cluster down.  In this case, we need to use a sequence
//!   of plans that each only updates one node at a time to ensure that the
//!   system keeps working.
//!
//! In terms of carrying it out, here's the basic idea:
//!
//! Here's the basic idea:
//!
//! ```ignore
//! The Planner
//!
//!     fleet policy     (latest inventory)   (latest plan)
//!              \               |               /            
//!               \              |              /
//!                +----------+  |  +----------/
//!                           |  |  |
//!                           v  v  v
//!
//!                          "planner"
//!                      (background task)
//!                              |
//!                              v                 no
//!                     is a new plan necessary? ------> done
//!                              |
//!                              | yes
//!                              v
//!                     generate a new plan
//!                              |
//!                              |
//!                              v
//!                     commit plan to database
//!                              |
//!                              |
//!                              v
//!                            done
//!
//!
//! The Executor (better name?)
//!
//!     latest committed plan       latest inventory
//!                      |             |  
//!                      |             |
//!                      +----+   +----+
//!                           |   |
//!                           v   v
//!
//!                         "executor"
//!                      (background task)
//!                             |
//!                             v
//!                     determine actions needed
//!                     take actions
//! ```
//!
//! In more detail:
//!
//! There is a relatively static **deployment policy** that specifies things
//! like the number of each type of service we expect to have (e.g., 5
//! CockroachDB nodes, 3 Nexus instances, etc.), the target version (for
//! either each software or the whole fleet), and similar parameters.  Some of
//! these could be modified by operators or support (e.g., an upgrade might
//! update the target version), but we don't expect that to happen often.
//!
//! An **inventory** background task periodically reaches out to various
//! components (mainly Sled Agent and the Management Gateway Service) to
//! assemble a list of all of the hardware and software components in the
//! system, including their versions and key configuration.  This list is
//! called a _collection_.
//!
//! A **deployment configuration** describes:
//!
//!   * for each fixed software component (i.e., tied to a specific hardware
//!     device), what version and configuration it should be running
//!
//!   * for each dynamically-deployed software component (e.g., most Omicron
//!     zones), how many of each version should be running on which sleds --
//!     and with what configuration
//!
//!   * similar information about other software-like components (e.g.,
//!     database schema)
//!
//! A "deployment configuration" is a sort of generalization of an "update
//! plan".
//!
//! At any given time, the system has exactly one _goal_ deployment
//! configuration.  The deployment system is always attempting to make reality
//! match this configuration.  The system can be aware of more than one
//! deployment configuration, including past ones, later ones, those generated
//! by Oxide support, etc.
//!
//! Note that there are many constraints on deployment configurations,
//! like: if a service S exists in the plan that depends on version V of
//! dependency D, there should be some minimum number of instances of version
//! V of dependency D in the plan.  Also, deployment configurations should
//! usually only change one thing relative to the previous configuration.
//! That's because during execution (below), the assumption is that all Nexus
//! instances may attempt to execute parts of the configuration at the same
//! time.  Say we want to update all five CockroachDB nodes.  If we changed
//! all of their versions from one configuration to the next, then some Nexus
//! instances might attempt to _remove_ all the old ones before any Nexus
//! instances deployed the new ones -- oops!  To carry out this change, we'd
//! create separate deployment configurations for each step -- deploy one new
//! node, remove an old one, repeat, etc.
//!
//! A **planner** background task periodically evaluates whether the current
//! target deployment configuration is consistent with the current deployment
//! policy.  If not, the task generates a new deployment configuration
//! consistent with the current deployment policy and attempts to make that the
//! new target.  (Multiple Nexus instances may try to do this concurrently.
//! CockraochDB's strong consistency ensures that only one can win.  The other
//! Nexus instances must go back to evaluating the winning target deployment
//! configuration before trying to change it again.
//!
//! An **execution** background task periodically evaluates whether the state
//! reflected in the latest inventory collection is consistent with the current
//! target deployment configuration.  If not, executes operations to bring
//! reality into line with the deployment configuration.  This means
//! provisioning new zones, removing old zones, adding instances to DNS,
//! removing instances from DNS, carrying out firmware updates, etc.
