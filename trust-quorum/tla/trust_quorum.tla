---- MODULE trust_quorum ----
(* Model the behavior of the trust quorum protocol.

   We are primarily interested in testing message distribution and
   reconfiguration under failure conditions to ensure that key shares get
   distributed appropriately and commits can only occur when enough prepare
   messages have been acknowledged to a coordinator.

   We explicitly ignore all crypto behavior and assume that shares are
   distributed along with prepare messages and rack secrets can be reconstructed
   with enough prepares.

   Some design choices:
    * Provide easy enabling conditions for actions and put them
      in top level variables - See "NexusOp" and "NodeOp"
    * Minimize state
    * Store all messages ever sent and gate receipt on state of nexus
      and nodes, and auxiliary variables. This is much easier than
      retrying on reboots, and is much less code.
    * When it doesn't matter which node is selected, use CHOOSE or
      hardcode. Symmetry sets are tricky and they don't work with
      liveness checking. We avoid them and instead manually reduce
      the state space.
*)

EXTENDS Integers, FiniteSets, TLC, TLCExt

CONSTANTS
    (* The set of trust quorum nodes *)
    n1,n2,n3,n4,n5,
    NODES,
    (* A way to model an `Option<T>` *)
    NONE,
    (* The maximum number of reconfigurations *)
    MAX_EPOCH,
    MAX_CRASHES_PER_NODE,
    MAX_NEXUS_CRASHES,
    CRASHSET

VARIABLES nexusOp, nodeOp, nexus, nodes, msgs, crashed

vars == <<nexusOp, nodeOp, nexus, nodes, msgs, crashed>>

-------------------------------------------------------------------------------
(* Type Definitions

   These aren't really any different from other definitions except:
    * We can use them to build up our type invariant `TypeOK`
    * They give us some centralized structure that is useful for
      action and operator definitions
    * They are all set constructors so that instances of a given type are an
      element in the set.
*)

(* All configurations. We define them this way rather than random selection
   from a group of nodes because that explodes the state space. *)
AllConfigs == <<
  {n1, n2, n3},
  {n1, n2, n3, n4, n5},
  {n2, n3, n4, n5}
>>

Epoch == 1..MAX_EPOCH

(* The set of all possible trust quorum configuration*)
Configuration == [
   epoch: Epoch,
   members: SUBSET NODES,
   coordinator: NODES
]

(* What is Nexus doing right now *)
NexusOp == {
   "Idle",
   "Preparing",
   "Committing",
   "Aborting"
}

 \* All fields represent the current configuration
 \* All state is persistent, and survives restarts
NexusState == [
   config: Configuration,
   is_committed: BOOLEAN,
   is_aborted: BOOLEAN,
   prepared_nodes: SUBSET NODES,
   committed_nodes: SUBSET NODES,
   last_committed_config: Configuration \union {NONE}
]

\* Mostly in-memory state. On reboot, a node starts in
\* "Uncommitted" \/ "Unlocked" \/  "Expunged"
NodeOp == {
  \* The node is unlocked in the current committed config waiting for requests
  "Idle",
  \* Does not yet have a committed configuration
  "Uncommitted",
  \* Rebooted and unlocking with its last committed config
  "Unlock",
  \* Rebooted and unlocking while collecting shares for the current config
  \* so it can reconstruct its own share and commit this config.
  "UnlockComputeShare",
  \* The node was part of a prior configuration, but has been expunged.
  \* It is no longer eligibile to be a member of a new configuration.
  \* We actually ensure this staticallly with our choice of configurations
  \* in the model state.
  "Expunged",
  \* Coordinating a reconfiguration by collecting shares for the last
  \* committed config
  "CoordinateCollectShares",
  \* Coordinating a reconfiguration by sending out prepare messages and
  \* waiting for acks.
  "CoordinatePrepare",
  \* This node did not receive a prepare message for the currently committed
  \* configuration and is retrieving the configuration from another node
  "GetConfig",
  \* This node did not receive a prepare message for the currently committed
  \* configuration and is retrieving shares *after* getting the configuration
  \* via `GetConfig`. When it has `K` shares it will recompute its own
  \* share and then commit the configuration. We assume key rotation
  \* occurs on commit, as it's always possible in a healthy system
  \* to retrieve k shares and we only model this during unlock to shrink
  \* the state space. We may revisit this.
  "ComputeShare"
}

NodeState == [
  config: Configuration,
  is_committed: BOOLEAN,
  is_aborted: BOOLEAN,
  last_committed_config: Configuration \union {NONE},

  (* We don't actually collect real shares, but we pretend to learn them them
     via `Prepare` messages or reconstruction after retrieving replies to K
     `get_share` messages*)
   has_share: BOOLEAN,

   \* Operation specific state below
   \* Acknowledgements specific to the current `op`

   acks: SUBSET NODES
]

(* Which nodes are currently down *)
Crashed == [
   nodes: SUBSET NODES,

   (* We use this to limit the number of crashes. *)
   counts: [NODES -> Nat]
]

(* All possible in messages sent between nexus and nodes and nodes themselves.
   Messages are removed when received, or dropped by an action. *)
Msgs ==
  (* Start a reconfiguration at a coordinator *)
  [type: {"reconfigure"}, config: Configuration] \union

  (* A prepare msg sent to a node from a coordinator. A prepare contains both a
  configuration and implicitly a node's key share.*)
  [type: {"prepare"}, to: NODES, config: Configuration] \union

  (* A prepare ack from a node to a coordinator*)
  [type: {"prepare_ack"}, from: NODES, epoch: Epoch] \union

  (* Send a commit to a node from Nexus *)
  [type: {"commit"}, to: NODES, epoch: Epoch] \union

  (* Send a commit ack to nexus from a node *)
  [type: {"commit_ack"}, from: NODES, epoch: Epoch] \union

  (* When nexus is committing, it tries to reach nodes which have not acked
     their prepares by sending them a "prepare_and_commit" message.

     This messsage includes the set of nodes that have prepared so that the
     node receiving this message can pull the current configuration from one
     of those nodes. Note that in this model the `Configuration` is also sent
     directly from Nexus with the `reconfigure` message. However, in real
     code, the actual configuration is generated by a coordinator and includes
     encrypted data for prior rack secrets. Therefore we model that message
     passing behavior in this spec.
  *)
  [type: {"prepare_and_commit"},
   epoch: Epoch,
   to: NODES,
   prepared: SUBSET NODES] \union

  (* Retrieve a configuration for a given epoch from a node.
     Nodes only respond if this is the current configuration and the
     requesting node is a member of the configuration.
  *)
  [type: {"get_config"},
   epoch: Epoch,
   to: NODES,
   from: NODES] \union

  (* Retrieve a key share for a given epoch from a node.
     Nodes only respond if this is the current configuration and the
     requesting node is a member of the configuration.
  *)
  [type: {"get_share"},
   epoch: Epoch,
   to: NODES,
   from: NODES] \union

  (* A configuration returned in response to a `get_config` *)
  [type: {"config"},
   config: Configuration,
   to: NODES,
   from: NODES] \union

  (* A key share returned in response to a `get_share` *)
  [type: {"share"},
   epoch: Epoch,
   to: NODES,
   from: NODES] \union

  (* Inform a node that it is no longer a part of the trust quorum as of
     the given epoch *)
  [type: {"expunged"}, epoch: Epoch, to: NODES, from: NODES] \union

  (* This is an "implicit commit" in a reply message that comes from another
     node when that node has moved on to a later configuration and the
     requesting node is still part of the new configuration, but unaware of it.

    As a result, a requesting node may have to retrieve a configuration and key
    shares to recompute its share if it never received a prepare message for
    this epoch.
  *)
  [type: {"commit_advance"},
   config:
   Configuration,
   to: NODES,
   from: NODES]



-------------------------------------------------------------------------------
(* Operators *)

(* Receive a message given a validity check operator

   Normally, we'd use `\E \in msgs` to receive a message. However, as it turns
   out, this explodes the search space. Since we keep all messages ever sent,
   even if only one message will actually be valid, it is still very expensive
   to use `\E \in msgs`. This is because TLC takes *every* message and attempts
   in parallel to see if it is valid. If it is not valid, the state space in
   coverage will not expand, but the check will still be done for every message.

   If there are a few valid messages, then TLC will expand the actual state
   space of valid states, essentially creating one new path for each valid
   message. These paths are all equivalent for our purposes. Receivers don't
   care which message they receive first, but whether they have received enough
   messages. In any case, even if there were a few concurrency bugs that could
   possibly be exposed via use of `\E \in msgs`, it would still be too expensive
   to use. 

   Therefore, we instead filter down the set of valid messages, and choose one
   one from the set. The `valid(_)` operator must ensure that the same message
   will not get chosen again, generally by looking at the current accumulator
   called `acks` or changing a `NodeOp`. Unfortunately, using `CHOOSE` does
   always pick the first element in the set that matches deterministically,
   and isn't actually randomly choosing. So we'll always retrieve from nodes in
   order. Fortunately, the ordering of the receives themselves is ordered via
   `\E id in NODES`, which randomizes things for us already. This seems like a
   reasonable trade off, as the TLC run time improves so much we can actually
   iterate and possibly add liveness properties to check.
*)
Recv(valid(_)) ==
  LET ms == {m \in msgs: valid(m)} IN
  IF ms = {} THEN
    NONE
  ELSE
    CHOOSE m \in ms: TRUE

(* The number of nodes (K) required to reconstruct a secret for a configuration.
*)
Threshold(Membership) == Cardinality(Membership) \div 2 + 1

(* The number of nodes (Z) in addition to (K) for Nexus to decide to commit *)
CommitSafetyParam == 1

CanConstructShare(acked, config) ==
  Cardinality(acked) >= Threshold(config.members)

CommittedOrAborted ==
  nexus.is_committed \/ nexus.is_aborted

LastCommittedConfig(id) ==
  IF nodes[id] # NONE THEN
    nodes[id].last_committed_config
  ELSE
    NONE

CollectingShares(id) ==
    /\ id \notin crashed.nodes
    /\ \/ nodeOp[id] = "Unlock"
       \/ nodeOp[id] = "UnlockComputeShare"
       \/ nodeOp[id] = "CoordinateCollectShares"
       \/ nodeOp[id] = "ComputeShare"

UpAndUnlocked(id) ==
    /\ id \notin crashed.nodes
    \* Can't receive from nexus before unlock
    /\ \/ nodeOp[id] = "Uncommitted"
       \/ nodeOp[id] = "Idle"
       \/ nodeOp[id] = "CoordinatePrepare"
       \/ nodeOp[id] = "CoordinateCollectShares"
       \/ nodeOp[id] = "ComputeShare" 

ChooseCoordinator(members) ==
    IF nexus.last_committed_config = NONE THEN
       CHOOSE x \in members: TRUE
    ELSE
       CHOOSE x \in members:
         /\ x \in nexus.last_committed_config.members
         /\ nodes[x].last_committed_config = nexus.last_committed_config

NeedsToGetConfig(id, e) ==
  /\ \/ nodes[id] = NONE
     \/ /\ nodes[id] # NONE
        /\ e > nodes[id].config.epoch
  /\ nodeOp[id] # "GetConfig"

NeedsToComputeShare(id, e) ==
  /\ nodes[id] # NONE
  /\ e = nodes[id].config.epoch
  /\ ~nodes[id].has_share
  /\ nodeOp[id] # "ComputeShare"

ReadyToCommit(id, e) ==
  /\ nodes[id] # NONE
  /\ e = nodes[id].config.epoch
  /\ nodes[id].has_share

CurrentNexusRequest(type, id, m) ==
    /\ m.type = type
    /\ m.to = id
    /\ m.epoch = nexus.config.epoch
    /\ ~nexus.is_aborted
    /\ nexusOp = "Committing"

BcastPrepare(destinations, config) ==
  {[type |-> "prepare", to |-> x, config |-> config]: x \in destinations}

BcastGetShare(destinations, from, epoch) ==
  {[type |-> "get_share",
    to |-> x,
    from |-> from,
    epoch |-> epoch]: x \in destinations}

BcastCommit(destinations, epoch) ==
  {[type |-> "commit", to |-> x, epoch |-> epoch]: x \in destinations}

BcastPrepareAndCommit(destinations, prepared, epoch) ==
    {[type |-> "prepare_and_commit",
      to |-> x,
      prepared |-> prepared,
      epoch |-> epoch] :x \in destinations}

BcastGetConfig(destinations, from, epoch) ==
  {[type |-> "get_config",
    to |-> x,
    from |-> from,
    epoch |-> epoch]: x \in destinations}

-------------------------------------------------------------------------------
(* TLC Constraints
   These are state predicates used to limit the number of model states in TLC.
*)

MaxEpochConstraint == nexus.config.epoch < MAX_EPOCH

-------------------------------------------------------------------------------
(* Invariants *)

TypeOK ==
  /\ nexusOp \in NexusOp
  /\ nexus \in NexusState
  /\ nodes \in [NODES -> NodeState \union {NONE}]
  /\ nodeOp \in [NODES -> NodeOp]
  /\ msgs \subseteq Msgs
  /\ crashed \in Crashed

NexusCoordinatorIsPartOfConfig ==
  nexus.config.coordinator \in nexus.config.members

NodeConfigsMatchNexus ==
  \A id \in DOMAIN nodes:
    \/ nodes[id] = NONE
    \/ /\ nodes[id] # NONE
       /\ \/ nexus.config.epoch > nodes[id].config.epoch
          \/ nodes[id].config = nexus.config

NodesThatHaveAckedCommitsHaveActuallyCommitted ==
  \A id \in nexus.committed_nodes: nodes[id].is_committed

LastCommittedConfigLessThanEqualConfig ==
  \A id \in DOMAIN nodes:
    \/ nodes[id] = NONE
    \/ /\ nodes[id] # NONE
       /\ nodes[id].last_committed_config = NONE
    \/ /\ nodes[id] # NONE
       /\ nodes[id].last_committed_config # NONE
       /\ nodes[id].last_committed_config.epoch <= nodes[id].config.epoch

CommittedNodesHaveShares ==
  \A id \in DOMAIN nodes:
    \/ nodes[id] = NONE
    \/ /\ nodes[id] # NONE
    \/ /\ nodes[id] # NONE
       /\ \/ /\ nodes[id].is_committed
             /\  nodes[id].has_share
          \/ ~nodes[id].is_committed


-----------------------------------------------------------------------------
(* Temporal properties *)
AllNodesUnlock == <>[](\A id \in nexus.config.members: nodeOp[id] # "Unlock") 

-----------------------------------------------------------------------------
(* The initial state predicate that sets the initial state of the system. *)

Init ==
  LET config == [epoch |-> 1,
                members |-> AllConfigs[1],
                coordinator |-> n1]
  IN
  /\ nexusOp = "Preparing"
  /\ nexus = [config |-> config,
              is_committed |-> FALSE,
              is_aborted |-> FALSE,
              prepared_nodes |-> {},
              committed_nodes |-> {},
              last_committed_config |-> NONE]
  /\ nodes = [n \in NODES |-> NONE]
  /\ nodeOp = [n \in NODES |-> "Uncommitted"]
  /\ msgs = {[type |-> "reconfigure", config |-> config]}
  /\ crashed = [nodes |-> {}, counts |-> [x \in NODES |-> 0]]

------------------------------------------------------------------------------
(* State transition functions

  All states transition functions are actions, as they are boolean valued and
  use the priming operator (').
*)

NexusStartReconfiguration ==
  /\ nexusOp = "Idle"
  /\ LET e == nexus.config.epoch + 1
         m == AllConfigs[e]
         c == ChooseCoordinator(m)
         conf == [epoch |-> e,
                  members |-> m,
                  coordinator |-> c]
     IN
       /\ nexusOp' = "Preparing"
       /\ nexus' = [config |-> conf,
                    is_committed |-> FALSE,
                    is_aborted |-> FALSE,
                    prepared_nodes |-> {},
                    committed_nodes |-> {},
                    last_committed_config |-> nexus.last_committed_config]
       /\ msgs' = msgs \union {[type |-> "reconfigure", config |-> conf]}
  /\ UNCHANGED <<nodes, crashed, nodeOp>>


RecvInitialConfig(id) ==
  LET valid(m) == /\ m.type = "reconfigure"
                  /\ id = m.config.coordinator
                  /\ m.config.epoch = nexus.config.epoch
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ UpAndUnlocked(id)
    /\ ~CommittedOrAborted
    /\ nodes[id] = NONE
    /\ nodes' = [nodes EXCEPT ![id] = [config |-> m.config,
                                       last_committed_config |-> NONE,
                                       is_committed |-> FALSE,
                                       is_aborted |-> FALSE,
                                       has_share |-> TRUE,
                                       acks |-> {id}]]
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "CoordinatePrepare"]
    /\ msgs' = UNION {msgs, BcastPrepare(m.config.members \ {id}, m.config)}
    /\ UNCHANGED <<nexusOp, nexus, crashed>>

RecvReconfigure(id) ==
  LET valid(m) == /\ m.type = "reconfigure"
                  /\ id = m.config.coordinator
                  /\ m.config.epoch = nexus.config.epoch
                  /\ nodes[id] # NONE
                  /\ m.config.epoch > nodes[id].config.epoch
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ UpAndUnlocked(id)
    /\ ~CommittedOrAborted
    /\ LET to == nodes[id].last_committed_config.members \ {id}
           lce == nodes[id].last_committed_config.epoch
           node == nodes[id]
       IN
      (* There is no committed configuration so just start preparing By choice
         of coordinators we assume that a coordinator is a committed member
         of the old and new group, and so the `last_committed_config` of the
         coordinator matches that of nexus. *)
      /\ \/ /\ node.last_committed_config = NONE
            /\ nodes' = [nodes EXCEPT ![id].config = m.config,
                                      ![id].is_committed = FALSE,
                                      ![id].is_aborted = FALSE,
                                      ![id].has_share = TRUE,
                                      ![id].acks = {id}]
            /\ nodeOp' = [nodeOp EXCEPT ![id] = "CoordinatePrepare"]
            /\ msgs' = UNION {msgs, BcastPrepare(m.config.members \ {id},
                                                 m.config)}

         \* Need to collect prior shares to recompute last rack secret
         \/ /\ node.last_committed_config # NONE
            /\ nodes' = [nodes EXCEPT ![id].config = m.config,
                                      ![id].is_committed = FALSE,
                                      ![id].is_aborted = FALSE,
                                      ![id].has_share = FALSE,
                                      ![id].acks = {id}]
            /\ nodeOp' = [nodeOp EXCEPT ![id] = "CoordinateCollectShares"]
            /\ msgs' = UNION {msgs, BcastGetShare(to, id, lce)}
      /\ UNCHANGED <<nexusOp, nexus, crashed>>


RecvShareWhileUnlocking(id) ==
  LET valid(m) == /\ m.type = "share"
                  /\ m.to = id
                  /\ nodeOp[id] = "Unlock"
                  /\ m.epoch = nodes[id].last_committed_config.epoch
                  /\ m.from \notin nodes[id].acks
                  /\ m.from \notin crashed.nodes

      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ LET acked == nodes[id].acks \union {m.from} IN
       IF CanConstructShare(acked, nodes[id].last_committed_config) THEN
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
         /\ nodes' = [nodes EXCEPT  ![id].acks = {}]
       ELSE
         /\ nodes' = [nodes EXCEPT ![id].acks = acked]
         /\ UNCHANGED nodeOp
    /\ UNCHANGED <<nexusOp, nexus, msgs, crashed>>

RecvShareWhileUnlockingComputeShare(id) ==
  LET valid(m) == /\ m.type = "share"
                  /\ m.to = id
                  /\ m.epoch = nodes[id].config.epoch
                  /\ m.from \notin nodes[id].acks
                  /\ m.from \notin crashed.nodes
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ nodeOp[id] = "UnlockComputeShare"
    /\ LET acked == nodes[id].acks \union {m.from} IN
       IF CanConstructShare(acked, nodes[id].config) THEN
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
         /\ nodes' = [nodes EXCEPT ![id].has_share = TRUE,
                                   ![id].is_committed = TRUE,
                                   ![id].acks = {},
                                   ![id].last_committed_config = nodes[id].config]
       ELSE
         /\ nodes' = [nodes EXCEPT ![id].acks = acked]
         /\ UNCHANGED nodeOp
    /\ UNCHANGED <<nexusOp, nexus, msgs, crashed>>

RecvShareWhileCoordinating(id) ==
  LET valid(m) == /\ m.type = "share"
                  /\ m.to = id
                  /\ nodeOp[id] = "CoordinateCollectShares"
                  /\ id = nodes[id].config.coordinator
                  /\ m.epoch = nodes[id].last_committed_config.epoch
                  /\ m.from \notin nodes[id].acks
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ LET acked == nodes[id].acks \union {m.from}
           to == nodes[id].config.members \ {id}
       IN
       IF CanConstructShare(acked, nodes[id].last_committed_config) THEN
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "CoordinatePrepare"]
         /\ nodes' = [nodes EXCEPT ![id].has_share = TRUE,
                                   ![id].acks = {id}]
         /\ msgs' = UNION {msgs, BcastPrepare(to, nodes[id].config)}
       ELSE
         /\ nodes' = [nodes EXCEPT ![id].acks = acked]
         /\ UNCHANGED <<nodeOp, msgs>>
    /\ UNCHANGED <<nexusOp, nexus, crashed>>

RecvShareWhileComputingShare(id) ==
  LET valid(m) == /\ m.type = "share"
                  /\ m.to = id
                  /\ nodeOp[id] = "ComputeShare"
                  /\ m.epoch = nodes[id].config.epoch
                  /\ m.from \notin nodes[id].acks
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ LET acked == UNION {nodes[id].acks, {m.from}} IN
       IF CanConstructShare(acked, nodes[id].config) THEN
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
         /\ nodes' = [nodes EXCEPT ![id].has_share = TRUE,
                                   ![id].is_committed = TRUE,
                                   ![id].last_committed_config
                                     = nodes[id].config,
                                   ![id].acks = {}]
       ELSE
         /\ nodes' = [nodes EXCEPT![id].acks = acked]
         /\ UNCHANGED nodeOp
    /\ UNCHANGED <<nexusOp, nexus, crashed, msgs>>

RecvPrepareAck(id) ==
  LET valid(m) == /\ m.type = "prepare_ack"
                  /\ nodeOp[id] = "CoordinatePrepare"
                  /\ m.epoch = nodes[id].config.epoch
                  /\ m.from \notin nodes[id].acks
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ nodes' = [nodes EXCEPT ![id].acks = @ \union {m.from}]
    /\ UNCHANGED <<nexusOp, nexus, nodeOp, crashed, msgs>>

(* We directly intuit that if nexus is up, and the coordinator is up, then
   polling the state of acked prepares from the coordinator will always succeed.
   We short circuit that polling message here, which can cause state explosion
   if used indiscriminately and just look at the coordinator. *)
NexusCommit ==
  LET e == nexus.config.epoch
      c == nexus.config.coordinator
      acked == nodes[c].acks
      needed_p == nexus.config.members \ acked
      commits == BcastCommit(acked, e)
      preps == BcastPrepareAndCommit(needed_p, acked, e)
  IN
  /\ nexusOp = "Preparing"
  /\ c \notin crashed.nodes
  /\ nodeOp[c] = "CoordinatePrepare"
  /\ nodes[c].config.epoch = nexus.config.epoch
  /\ Cardinality(acked) >= Threshold(nexus.config.members) + CommitSafetyParam
  /\ nexusOp' = "Committing"
  /\ nexus' = [nexus EXCEPT !.is_committed = TRUE,
                            !.prepared_nodes = acked,
                            !.last_committed_config = nexus.config]
  /\ msgs' = UNION {msgs, commits, preps}
  /\ UNCHANGED <<nodes, nodeOp, crashed>>

RecvPrepare(id) ==
  LET valid(m) == /\ m.type = "prepare"
                  (* We don't drop messages. But it's useful to exercise paths
                     where they would be dropped. If the sending node has
                     already seen a commit from nexus, then we can assume this
                     message was lost. This will allow us to exercise the full
                     `PrepareAndCommit" path as well as the full unlock path. It's
                     strictly necessary for the latter, because otherwise a node
                     that restarts will see a prepare from an earlier coordination
                     and commit without trying to unlock in the old committed
                     configuration or going straight to "Uncommitted" if there
                     is no old committed configuration. We can't use a `retry_ct`
                     here, because then if the coordinator hasn't committed and
                     all the other nodes transiently reboot, we'd mask the retries
                     that we get for free by keeping all the messages ever sent
                     in `msgs`.
   
                    This is one of the few places where explict retries would be
                    handy, but the whole spec would have to change again to support
                    that. *)
                  /\ nodeOp[m.config.coordinator] = "CoordinatePrepare"
                  /\ m.to = id
                  /\ \/ /\ nodes[id] = NONE
                     \/ /\ nodes[id] # NONE
                        /\ m.config.epoch > nodes[id].config.epoch
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ nodeOp[id] # "Expunged"
    /\ nodes' = [nodes EXCEPT ![id] = [config |-> m.config,
                                       last_committed_config
                                         |-> LastCommittedConfig(id),
                                       is_committed |-> FALSE,
                                       is_aborted |-> FALSE,
                                       has_share |-> TRUE,
                                       acks |-> {}]]
    /\ msgs' = UNION {msgs, {[type |-> "prepare_ack",
                              epoch |-> m.config.epoch,
                               from |-> id]}}
    \* If we're still unlocking, then continue to do that, otherwise
    \* transition to idle.
    /\ IF \/ nodeOp[id] = "Unlock" \/ nodeOp[id] = "Uncommitted" THEN
         UNCHANGED nodeOp
       ELSE
         nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
    /\ UNCHANGED <<nexusOp, nexus, crashed>>

RecvPrepareAndCommitMissingConfig(id) ==
  LET valid(m) == /\ CurrentNexusRequest("prepare_and_commit", id, m)
                  /\ NeedsToGetConfig(id, m.epoch)
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ UpAndUnlocked(id)
    /\ nodeOp[id] # "GetConfig"
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "GetConfig"]
    /\ msgs' = UNION {msgs, BcastGetConfig(m.prepared, id, m.epoch)}
    /\ UNCHANGED <<nodes, nexusOp, nexus, crashed>>

RecvPrepareAndCommitMissingShare(id) ==
  LET valid(m) == /\ CurrentNexusRequest("prepare_and_commit", id, m)
                  /\ NeedsToComputeShare(id, m.epoch)
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    \* Can't receive from nexus before unlock
    /\ \/ nodeOp[id] = "Uncommitted"
       \/ nodeOp[id] = "Idle"
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "ComputeShare"]
    /\ nodes' = [nodes EXCEPT ![id].acks = {}]
    /\ msgs' = UNION {msgs, BcastGetShare(nodes[id].config.members \ {id},
                                          id,
                                          m.epoch)}
    /\ UNCHANGED <<nexusOp, nexus, crashed>>

RecvPrepareAndCommitAlreadyPrepared(id) ==
  LET valid(m) ==  /\ CurrentNexusRequest("prepare_and_commit", id, m)
                   /\ ReadyToCommit(id, m.epoch)
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ UpAndUnlocked(id)
    /\ nodes' = [nodes EXCEPT ![id].is_committed = TRUE]
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
    /\ msgs' = UNION {msgs, {[type |-> "commit_ack",
                              epoch |-> m.epoch,
                              from |-> id]}}
    /\ UNCHANGED <<nexusOp, nexus, crashed>>

RecvConfig(id) ==
  LET valid(m) == /\ m.type = "config"
                  /\ m.to = id
                  /\ m.from \notin crashed.nodes
                  /\ \/ /\ nodes[id] = NONE
                     \/ /\ nodes[id] # NONE
                        /\ m.config.epoch > nodes[id].config.epoch
      m == Recv(valid)
   IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ nodeOp[id] = "GetConfig"
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "ComputeShare"]
    /\ nodes' = [nodes EXCEPT ![id] = [config |-> m.config,
                                       last_committed_config
                                         |-> LastCommittedConfig(id),
                                       is_committed |-> FALSE,
                                       is_aborted |-> FALSE,
                                       has_share |-> FALSE,
                                       acks |-> {}]]
    /\ msgs' = UNION {msgs, BcastGetShare(m.config.members \ {id},
                                          id,
                                          m.config.epoch)}
    /\ UNCHANGED <<nexus, nexusOp, crashed>>

RecvGetConfig(id) ==
  LET valid(m) ==  /\ m.type = "get_config"
                   /\ m.to = id
                   /\ nodes[id].last_committed_config # NONE
                   /\ m.epoch = nodes[id].last_committed_config.epoch
                   /\ m.from \in nodes[id].last_committed_config.members
                   \* Disable if the config was already received
                   /\ \/ nodes[m.from] = NONE
                      \/ /\ nodes[m.from] # NONE
                         /\ nodes[m.from].config.epoch # m.epoch
   
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ nodeOp[id] # "Expunged"
    /\ id \notin crashed.nodes
    /\ msgs' = UNION {msgs, {[type |-> "config",
                              config |-> nodes[id].config,
                              to |-> m.from,
                              from |-> id]}}
    /\ UNCHANGED <<nexusOp, nexus, nodes, nodeOp, crashed>>

RecvGetShare(id) ==
  LET valid(m) == /\ m.type = "get_share"
                  /\ m.to = id
                  /\ CollectingShares(m.from)
                  /\ nodes[id] # NONE
                  /\ \/ /\ nodes[id].last_committed_config # NONE
                        /\ m.epoch = nodes[id].last_committed_config.epoch
                        /\ m.from \in nodes[id].last_committed_config.members
                     \* Commit occurred, but this node hasn't learned of it yet.
                     \* It must return the share so a crashed node that did learn
                     \* can unlock.
                     \/ /\ m.epoch = nodes[id].config.epoch 
                        /\ m.from \in nodes[id].config.members
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ id \notin crashed.nodes
    /\ nodeOp[id] # "Expunged"
    /\ msgs' = UNION {msgs, {[type |-> "share",
                             epoch |-> m.epoch,
                             to |-> m.from,
                             from |-> id]}}
    /\ UNCHANGED <<nexusOp, nexus, nodes, nodeOp, crashed>>

(* The requesting node is part of the current committed configuration, but
   doesn't know it yet. We must inform it. *)
RecvGetShareFromStaleNode(id) ==
  LET valid(m) == /\ m.type = "get_share"
                  /\ CollectingShares(m.from)
                  /\ m.to = id
                  /\ nodes[id] # NONE
                  /\ nodes[id].last_committed_config # NONE
                  /\ m.epoch < nodes[id].last_committed_config.epoch
                  /\ m.from \in nodes[id].last_committed_config.members
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ nodeOp[id] # "Expunged"
    /\ id \notin crashed.nodes
    /\ msgs' = UNION {msgs, {[type |-> "commit_advance",
                              config |-> nodes[id].last_committed_config,
                              to |-> m.from,
                              from |-> id]}}
    /\ UNCHANGED <<nexusOp, nexus, nodes, nodeOp, crashed>>

(* A node has the latest committed configuration and is now learning to commit
   it. *)
RecvCommitAdvanceWhenPrepared(id) ==
  LET valid(m) == /\ m.type = "commit_advance"
                  /\ m.to = id
                  /\ m.from \notin crashed.nodes
                  /\ nodes[id] # NONE
                  /\ nodes[id].config.epoch = m.config.epoch
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ nodeOp[id] = "Unlock"
    /\ id \notin crashed.nodes
    /\ nodes[id].has_share
    /\ ~nodes[id].is_committed
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
    /\ nodes' = [nodes EXCEPT ![id].is_committed = TRUE,
                              ![id].acks = {},
                              ![id].last_committed_config = nodes[id].config]
    /\ UNCHANGED <<nexusOp, nexus, msgs, crashed>>

(* A node is operating with a stale config and must advance to the latest
   committed config. To do this it must comput its share and then commit. *)
RecvCommitAdvanceStaleConfigOrMissingShare(id) ==
  LET valid(m) == /\ m.type = "commit_advance"
                  /\ m.to = id
                  /\ m.from \notin crashed.nodes
                  /\ nodes[id] # NONE
                  /\ \/ nodes[id].config.epoch <  m.config.epoch
                     \* This can happens when a node restarts and tries to unlock
                     \* while it was trying previously to compute the share.
                     \/ /\ nodes[id].config.epoch = m.config.epoch
                        /\ ~nodes[id].has_share
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ nodeOp[id] = "Unlock"
    /\ id \notin crashed.nodes
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "UnlockComputeShare"]
    /\ nodes' = [nodes EXCEPT ![id].config = m.config,
                              ![id].is_committed = FALSE,
                              ![id].is_aborted = FALSE,
                              ![id].has_share = FALSE,
                              ![id].acks = {}]
    /\ msgs' = UNION {msgs, BcastGetShare(nodes[id].config.members \ {id},
                                          id,
                                          m.config.epoch)}
    /\ UNCHANGED <<nexusOp, nexus, crashed>>


RecvCommit(id) ==
  LET valid(m) == /\ m.type = "commit"
                  /\ m.to = id
                  /\ nodes[id] # NONE
                  /\ m.epoch = nodes[id].config.epoch
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ \/ nodeOp[id] = "Idle"
       \/ nodeOp[id] = "Uncommitted"
    /\ Assert(nodes[id].has_share,
              <<"Commits should only be sent to nodes that have prepared", id>>)
    /\ nodeOp' = [nodeOp EXCEPT ![id] = "Idle"]
    /\ nodes' = [nodes EXCEPT ![id].is_committed = TRUE,
                              ![id].last_committed_config = nodes[id].config]
    /\ msgs' = UNION {msgs, {[type |-> "commit_ack",
                              epoch |-> m.epoch,
                              from |-> id]}}
    /\ UNCHANGED <<nexusOp, nexus, crashed>>


NexusRecvCommitAck ==
  LET valid(m) ==
        /\ m.type = "commit_ack"
        /\ m.epoch = nexus.config.epoch
        /\ m.from \notin nexus.committed_nodes
      m == Recv(valid)
  IN
    /\ m # NONE
    /\ nexusOp = "Committing"
    /\ LET committed == nexus.committed_nodes \union {m.from} IN
       IF committed = nexus.config.members THEN
        (* We prepare it in case the ack came in response to a
           `PrepareAndCommit` In any event, no node will ack a commit without
           first seeing a prepare, so this is never incorrect. *)
         /\ nexus' = [nexus EXCEPT !.committed_nodes = {},
                                   !.prepared_nodes = {}]
         /\ nexusOp' = "Idle"
         /\ UNCHANGED <<nodes, nodeOp, crashed, msgs>>
       ELSE
         /\ nexus' = [nexus EXCEPT !.committed_nodes = committed,
                                   !.prepared_nodes = @ \union {m.from}]
         /\ UNCHANGED <<nexusOp, nodes, nodeOp, crashed, msgs>>


CrashNode ==
  \* Order of crashes doesn't matter. Let's not explode the state space
  /\ CRASHSET \ crashed.nodes # {}
  /\ LET id == CHOOSE id \in CRASHSET \ crashed.nodes: TRUE IN
    /\ crashed.counts[id] < MAX_CRASHES_PER_NODE
    /\ crashed' = [crashed EXCEPT !.nodes = @ \union {id},
                                  !.counts[id] = @ + 1]
    \* The node crashed while preparing or about to prepare
    \* We assume in the latter case that the message is lost
    /\ IF /\ nexusOp = "Preparing"
          /\ nexus.config.coordinator = id
       THEN
         /\ nexusOp' = "Idle"
         /\ nexus' = [nexus EXCEPT !.is_aborted = TRUE,
                                   !.committed_nodes = {},
                                   !.prepared_nodes= {}]
         /\ UNCHANGED <<nodes, nodeOp, msgs>>
       ELSE
         /\ UNCHANGED <<nexus, nexusOp, nodes, nodeOp, msgs>>


RestartNode ==
  \* Order of restarts doesn't matter. Let's not explode the state space.
  /\ crashed.nodes # {}
  /\ LET id == CHOOSE id \in crashed.nodes: TRUE IN
    /\ crashed' = [crashed EXCEPT !.nodes = @ \ {id}]
    \* Nexus started a reconfiguration after we crashed
    \* Assume the reconfigure message was lost
    /\ IF /\ nexusOp = "Preparing"
          /\ nexus.config.coordinator = id
       THEN
          /\ nexusOp' = "Idle"
          /\ nexus' = [nexus EXCEPT !.is_aborted = TRUE,
                                    !.committed_nodes = {},
                                    !.prepared_nodes= {}]
       ELSE
         /\ UNCHANGED <<nexus, nexusOp>>
    /\ IF nodeOp[id] = "Uncommitted" \/ nodeOp[id] = "Expunged" THEN
         UNCHANGED <<nodeOp, nodes, msgs>>
       ELSE IF LastCommittedConfig(id) = NONE THEN
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "Uncommitted"]
         /\ UNCHANGED <<nodes, msgs>>
       ELSE
         /\ nodeOp' = [nodeOp EXCEPT ![id] = "Unlock"]
         /\ nodes' = [nodes EXCEPT ![id].acks = {id}]
         /\ msgs' =
            UNION {msgs, BcastGetShare(
               nodes[id].last_committed_config.members \ {id},
               id,
               nodes[id].last_committed_config.epoch)}

RecvShare(id) ==
    \/ RecvShareWhileCoordinating(id)
    \/ RecvShareWhileComputingShare(id)
    \/ RecvShareWhileUnlocking(id)
    \/ RecvShareWhileUnlockingComputeShare(id)

RecvRpy(id) ==
    \/ RecvPrepareAck(id)
    \/ RecvShare(id)
    \/ RecvConfig(id)
    \/ RecvCommitAdvanceWhenPrepared(id)
    \/ RecvCommitAdvanceStaleConfigOrMissingShare(id)

RecvPeerReq(id) ==
    \/ RecvPrepare(id)
    \/ RecvGetConfig(id)
    \/ RecvGetShare(id)
    \/ RecvGetShareFromStaleNode(id)

RecvNexusReq(id) ==
    \/ RecvInitialConfig(id)
    \/ RecvReconfigure(id)
    \/ RecvPrepareAndCommitMissingConfig(id)
    \/ RecvPrepareAndCommitMissingShare(id)
    \/ RecvPrepareAndCommitAlreadyPrepared(id)
    \/ RecvCommit(id)

Next ==
  \/ NexusStartReconfiguration
  \/ NexusCommit
  \/ NexusRecvCommitAck
  \/ CrashNode
  \/ RestartNode
  (* It would be nice to put \E m in msgs here rather than inside Recv actions.
     Unfortunately that results in making those methods not visible to coverage.
     I think only model values really work outside of `Next`. *)
  \/ \E id \in NODES:
    \/ RecvNexusReq(id)
    \/ RecvPeerReq(id)
    \/ RecvRpy(id)

Fairness == WF_vars(Next)

Spec == Init /\ [][Next]_vars

FairSpec == Spec /\ Fairness



===============================================================================
