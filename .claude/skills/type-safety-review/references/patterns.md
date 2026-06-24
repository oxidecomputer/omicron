# Type Safety Patterns: Detailed Examples

Concrete examples illustrating each anti-pattern. Use these to calibrate what each pattern looks like in context.

---

## Category 1: Stringly-typed values

### Example: Switch slot

**Before:**
```rust
// API type
pub struct LoopbackAddressCreate {
    pub switch_location: String,  // only "switch0" or "switch1" are valid
}

// Database column
-- switch_location TEXT NOT NULL
```

**After — enum everywhere:**
```rust
// Rust type
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SwitchSlot {
    Switch0,
    Switch1,
}

// Database schema
CREATE TYPE IF NOT EXISTS db_switch_slot AS ENUM (
    'switch0',
    'switch1'
);

// Diesel conversion
impl ToSql<DbSwitchSlot, Pg> for SwitchSlot { ... }
impl FromSql<DbSwitchSlot, Pg> for SwitchSlot { ... }
```

**Why this matters:** With `String`, invalid values reach the database layer before the error is caught (or not at all, if no validation was added). With an enum, the OpenAPI spec enforces valid values, the Rust compiler enforces valid values, and the database schema enforces valid values—three independent layers, all for free.

---

### Example: BGP peer state as string

**Before:**
```rust
fn is_peer_established(state: &str) -> bool {
    state == "Established"
}
```

**After:**
```rust
#[derive(Debug, PartialEq, Eq)]
enum PeerState {
    Idle,
    Connect,
    Active,
    OpenSent,
    OpenConfirm,
    Established,
}

fn is_peer_established(state: PeerState) -> bool {
    state == PeerState::Established
}
```

**Red flags to spot:** `match s.as_str()`, `if s == "..."`, `s.contains("...")` on domain-concept strings.

---

## Category 2: `Display`/`FromStr` as footguns

### Example: `LldpAdminStatus`

**Before:**
```rust
impl fmt::Display for LldpAdminStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LldpAdminStatus::Enabled => write!(f, "enabled"),
            LldpAdminStatus::Disabled => write!(f, "disabled"),
            LldpAdminStatus::RxOnly => write!(f, "rx_only"),   // note: underscore
            LldpAdminStatus::TxOnly => write!(f, "tx_only"),
        }
    }
}
```

In this specific case, the wicket UI wants to display `"rx only"` (with a space), while the SMF property layer wants `"rx_only"`. With a single `Display`, one of them has to be wrong, or one has to work around the trait.

**After:**
```rust
impl LldpAdminStatus {
    /// Returns the string used as the SMF property value for this status.
    pub fn to_smf_property(&self) -> &'static str {
        match self {
            LldpAdminStatus::Enabled => "enabled",
            LldpAdminStatus::Disabled => "disabled",
            LldpAdminStatus::RxOnly => "rx_only",
            LldpAdminStatus::TxOnly => "tx_only",
        }
    }
}

// In the UI layer:
fn display_label(status: LldpAdminStatus) -> &'static str {
    match status {
        LldpAdminStatus::Enabled => "enabled",
        LldpAdminStatus::Disabled => "disabled",
        LldpAdminStatus::RxOnly => "rx only",    // natural English
        LldpAdminStatus::TxOnly => "tx only",
    }
}
```

**When `Display` is fine:** For types that have one canonical human-readable form for all contexts (e.g., a `Name` type, an IP address). The problem is types where different contexts want different string forms.

**Red flags to spot:**
- `impl Display` plus a comment like "used for serialization" or "used for SMF"
- `impl FromStr` that parses values coming from multiple unrelated sources
- `.to_string()` on a type passed directly into a config file or external API

---

## Category 3: Multiple representations / sentinel values

### Example: BGP unnumbered peer address

**Before:**
```rust
// In one part of the code:
peer_addr: Option<IpAddr>,  // None means unnumbered

// In another part:
peer_addr: IpAddr,          // Ipv6Addr::UNSPECIFIED means unnumbered

// In the database:
peer_addr: Option<String>,  // NULL means unnumbered
```

Converting between these representations requires careful, error-prone code at every boundary. The invariant is enforced only by convention. Instead, this version has an unambiguous, statically-checked representation for unnumbered peers:

**After — one explicit type:**
```rust
pub enum RouterPeerAddress {
    Numbered(SpecifiedAddr),   // cannot be unspecified, loopback, multicast
    Unnumbered,
}

// SpecifiedAddr rejects invalid addresses at construction time:
pub struct SpecifiedAddr(IpAddr);
impl SpecifiedAddr {
    pub fn new(addr: IpAddr) -> Option<Self> {
        if addr.is_unspecified() || addr.is_loopback() || addr.is_multicast() {
            None
        } else {
            Some(Self(addr))
        }
    }
}
```

**Red flags to spot:**
- Comments like "use X to represent absence" or "0 means disabled"
- `if addr == Ipv6Addr::UNSPECIFIED` guards on values that could come in multiple shapes
- Parallel conversions: `Option<T>` in one layer, a sentinel in another, `Option<String>` in the database
- Structs with `enabled: bool` plus fields that only make sense when `enabled` is true

---

## Category 4: Magic literals / missing constants

### Example: BGP timer defaults

**Before:**
```rust
// In one file:
let config = BgpPeerConfig {
    hold_time: 6,
    idle_hold_time: 3,
    delay_open: 0,
    connect_retry: 3,
    keepalive: 2,
};

// In a test file:
assert_eq!(peer.hold_time, 6);
assert_eq!(peer.keepalive, 2);

// In a third file:
if hold_time < 6 { return Err(...) }
```

When a timer value changes, all three locations must be updated. There's no guarantee they stay in sync, and no name explaining what `6` means.

**After:**
```rust
pub const BGP_DEFAULT_HOLD_TIME_SECS: u64 = 6;
pub const BGP_DEFAULT_IDLE_HOLD_TIME_SECS: u64 = 3;
pub const BGP_DEFAULT_DELAY_OPEN_SECS: u64 = 0;
pub const BGP_DEFAULT_CONNECT_RETRY_SECS: u64 = 3;
pub const BGP_DEFAULT_KEEPALIVE_SECS: u64 = 2;

// All sites reference the constant:
let config = BgpPeerConfig {
    hold_time: BGP_DEFAULT_HOLD_TIME_SECS,
    ...
};
```

**Red flags to spot:** The same numeric literal appearing in more than one file; literals with comments like `// 6 seconds`; literals that are structurally related (e.g., hold time = 3× keepalive) but expressed independently.

---

## Category 5: Missing newtypes for domain values

### Example: UUID kinds

**Before:**
```rust
pub struct InstanceRecord {
    pub id: Uuid,
    pub sled_id: Uuid,
    pub vmm_id: Uuid,
    pub boot_disk_id: Uuid,
}

fn find_instance(instance_id: Uuid, sled_id: Uuid) -> Result<...> { ... }

// Call site — are these in the right order?
find_instance(record.sled_id, record.id)  // WRONG, but compiles
```

**After (using `newtype-uuid`):**
```rust
// In uuid-kinds/src/lib.rs:
impl_typed_uuid_kind!(
    Instance => "instance",
    Sled => "sled",
    Vmm => "vmm",
    Disk => "disk",
);

pub struct InstanceRecord {
    pub id: TypedUuid<Instance>,
    pub sled_id: TypedUuid<Sled>,
    pub vmm_id: TypedUuid<Vmm>,
    pub boot_disk_id: TypedUuid<Disk>,
}

fn find_instance(instance_id: TypedUuid<Instance>, sled_id: TypedUuid<Sled>) -> Result<...> { ... }

// This now fails to compile:
find_instance(record.sled_id, record.id)  // type error!
```

### Example: Byte counts

**Before:**
```rust
fn allocate_disk(size_bytes: u64) { ... }

// Caller passes GiB, not bytes — wrong but compiles:
allocate_disk(disk.size_gib)
```

**After:**
```rust
fn allocate_disk(size: ByteCount) { ... }
// ByteCount is defined in common/src/api/external/
```

**Red flags to spot:** Multiple `Uuid` fields in the same struct; `u64` or `i64` parameters named `_bytes`, `_gib`, `_mib` suggesting unit mismatch risk; functions with two or more parameters of the same primitive type.

---

## Category 6: Implicit runtime panics

### Example: Documented unwrap

**Before:**
```rust
let name = path.file_name().unwrap();
```

**After:**
```rust
// unwrap: `file_name()` returns None only for paths ending in `..`,
// but we got this path from `read_dir_utf8()` which skips `..`.
let name = path.file_name().unwrap();
```

### Example: Map subscript

**Before:**
```rust
let handler = handlers[&event_type];  // panics if missing
```

**After:**
```rust
let handler = handlers.get(&event_type)
    .ok_or_else(|| anyhow!("no handler for {event_type:?}"))?;
```

### Example: `as` cast

**Before:**
```rust
let count = items.len() as i64;  // silently truncates on 32-bit if len > i64::MAX
```

**After:**
```rust
let count = i64::try_from(items.len())
    .context("item count overflows i64")?;
```

**When `as` is fine:** Intentional truncation/wrapping that is understood and documented. For example, bitwise masking. Clippy's `cast_lossless` and `cast_possible_truncation` lints flag the problematic cases.

---

## Category 7: Weak enum / bool usage

### Example: Boolean function arguments

**Before:**
```rust
fn sync_config(state: &State, verbose: bool, dry_run: bool) { ... }

// Call site — which bool is which?
sync_config(&state, false, true)
```

**After:**
```rust
#[derive(Debug, Clone, Copy)]
enum Verbosity { Verbose, Quiet }

#[derive(Debug, Clone, Copy)]
enum DryRun { Yes, No }

fn sync_config(state: &State, verbosity: Verbosity, dry_run: DryRun) { ... }

// Call site is now self-documenting:
sync_config(&state, Verbosity::Quiet, DryRun::Yes)
```

### Example: Wildcard match arm

**Before:**
```rust
match event {
    Event::Foo => handle_foo(),
    Event::Bar => handle_bar(),
    _ => {}  // silently ignores any new variants
}
```

**After (when all cases matter):**
```rust
match event {
    Event::Foo => handle_foo(),
    Event::Bar => handle_bar(),
    Event::Baz => {}  // explicitly considered and intentionally ignored
}
```

### Example: Inline enum variant data vs. named struct

**Before:**
```rust
pub enum SiloUser {
    Jit {
        external_id: String,
        silo_id: Uuid,
        roles: Vec<Role>,
    },
    Scim { ... },
}

// Cannot write a function that only accepts JIT users:
fn jit_only(user: SiloUser) {
    let SiloUser::Jit { external_id, .. } = user else {
        return Err("wrong type");  // runtime error for what could be compile-time
    };
}
```

**After:**
```rust
pub struct SiloUserJit {
    pub external_id: String,
    pub silo_id: Uuid,
    pub roles: Vec<Role>,
}

pub enum SiloUser {
    Jit(SiloUserJit),
    Scim(SiloUserScim),
}

// Now this is a compile-time guarantee:
fn jit_only(user: SiloUserJit) { ... }
```

---

## Category 8: Missing full-struct destructuring in serialization

### Example: SQL INSERT without destructuring

**Before:**
```rust
// If someone adds `planner_config.new_field`, this code compiles but
// silently omits the new field from the database row.
sql_query("INSERT INTO reconfigurator_config (version, planner_enabled, time_modified) \
           SELECT $1, $2, $3 ...")
    .bind::<BigInt, _>(switches.version.into())
    .bind::<Bool, _>(switches.config.planner_enabled)
    .bind::<Timestamptz, _>(switches.time_modified)
    .execute_async(conn)
```

**After:**
```rust
// This statement exists to cause a compile error if the struct changes.
// If you get an error here, update the query below.
let ReconfiguratorConfigView {
    version,
    config: ReconfiguratorConfig {
        planner_enabled,
        planner_config: PlannerConfig { add_zones_with_mupdate_override },
        tuf_repo_pruner_enabled,
    },
    time_modified,
} = *switches;

sql_query("INSERT INTO reconfigurator_config \
           (version, planner_enabled, time_modified, \
            add_zones_with_mupdate_override, tuf_repo_pruner_enabled) \
           SELECT $1, $2, $3, $4, $5 ...")
    .bind::<BigInt, SqlU32>(version.into())
    .bind::<Bool, _>(planner_enabled)
    .bind::<Timestamptz, _>(time_modified)
    .bind::<Bool, _>(add_zones_with_mupdate_override)
    .bind::<Bool, _>(tuf_repo_pruner_enabled)
    .execute_async(conn)
```

**Red flags to spot:**
- `.bind()` chains that access `value.field` directly
- `impl From<MyStruct> for DbRow` that lists fields individually without a destructuring `let`
- `serde::Serialize` derived but with `#[serde(skip)]` on fields that probably shouldn't be skipped

---

## Category 9: `Vec` when uniqueness matters

### Example: Collection of sled IDs

**Before:**
```rust
pub struct Policy {
    // Must not contain duplicates.
    pub sleds: Vec<SledUuid>,
}

fn add_sled(policy: &mut Policy, sled_id: SledUuid) {
    if !policy.sleds.contains(&sled_id) {  // O(n) and easy to forget
        policy.sleds.push(sled_id);
    }
}
```

**After:**
```rust
pub struct Policy {
    pub sleds: BTreeSet<SledUuid>,
}

fn add_sled(policy: &mut Policy, sled_id: SledUuid) {
    policy.sleds.insert(sled_id);  // uniqueness is automatic
}
```

**Why this matters:** Any code that builds or extends the `Vec` must remember to deduplicate. Any code that iterates or searches the `Vec` must be written to tolerate duplicates. Changing the type to `BTreeSet` makes the compiler enforce the invariant everywhere.

---

### Example: Map keyed by identifier field

**Before:**
```rust
// The key must match zone.id — this is checked by convention, not the compiler.
pub zones: BTreeMap<OmicronZoneUuid, OmicronZoneConfig>,

// Insertion is error-prone:
zones.insert(zone.id, zone);   // fine
zones.insert(other_id, zone);  // compiles, wrong at runtime
```

**After (using `iddqd::IdOrdMap`):**
```rust
use iddqd::IdOrdMap;

pub zones: IdOrdMap<OmicronZoneConfig>,
// OmicronZoneConfig must impl IdOrd, which provides the key (zone.id).
// The map owns both key and value as one unit.

// Insertion no longer takes a separate key:
zones.insert_unique(zone)?;  // key is derived from zone.id automatically
```

**Why this matters:** `BTreeMap<Id, Value>` has an implicit invariant that `key == value.id`. `IdOrdMap` eliminates that invariant entirely: the key *is* the value's identifier, enforced by the type. There is no way to insert a value under the wrong key.

**Red flags to spot:**
- `BTreeMap<XxxUuid, SomeStruct>` where `SomeStruct` has a field of type `XxxUuid`
- Comments like "key must equal value.id" or "keyed by the zone's own id"
- Manual `map.insert(thing.id, thing)` patterns
- `Vec<T>` with a comment saying "no duplicates" or followed by `.sort(); .dedup()`

---

## Calibration notes

**Not every instance of these patterns is a problem.** Context matters:

- A `String` field is fine if the valid values are truly open-ended (e.g., a user-provided description).
- `Display` is fine if the type has one canonical string form used everywhere.
- A single `unwrap()` with a clear comment is fine; it's the undocumented ones that are risky.
- `bool` is fine for truly binary properties with obvious meaning at the call site.

**Prioritize findings where:**
1. The type appears in multiple files or across a client-server boundary
2. The type is likely to grow (new variants, new fields)
3. A mistake would be silent at runtime rather than producing an obvious error
4. The fix is localized and the blast radius of the current problem is large
