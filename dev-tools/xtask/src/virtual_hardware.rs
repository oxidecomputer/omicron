// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Subcommand: cargo xtask virtual-hardware

use anyhow::{Context, Result, anyhow, bail};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{Parser, Subcommand};
use macaddr::MacAddr;
use serde::Deserialize;
use std::process::{Command, Output};
use std::str::FromStr;

#[derive(Subcommand)]
enum Commands {
    /// Create virtual hardware to simulate a Gimlet
    Create {
        /// The physical link over which Chelsio links are simulated
        ///
        /// Will be inferred by `dladm show-phys` if unsupplied.
        #[clap(long, env)]
        physical_link: Option<String>,

        /// Sets `promisc-filtered` off for the sc0_1 vnic.
        ///
        /// Won't do anything if unsupplied.
        #[clap(long)]
        promiscuous_filter_off: bool,

        /// The gateway IP address of your local network
        ///
        /// Will be inferred via `netstat` if unsupplied.
        #[clap(long)]
        gateway_ip: Option<String>,

        /// The configured mode for softnpu
        #[clap(long, env, default_value = "zone")]
        softnpu_mode: String,

        /// The MAC address of your gateway IP
        ///
        /// Will be inferred via `arp` if unsupplied.
        #[clap(long)]
        gateway_mac: Option<String>,

        #[command(flatten)]
        pxa: Pxa,

        #[clap(long, default_value = PXA_MAC_DEFAULT)]
        pxa_mac: String,

        /// Size in bytes for created vdevs
        #[clap(long, default_value_t = 20 * GB)]
        vdev_size: u64,
    },
    /// Destroy virtual hardware which was initialized with "Create"
    Destroy,
}

/// Describes which objects should be manipulated by these commands.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum Scope {
    /// Everything (this is the default).
    All,
    /// Only storage (e.g. vdevs).
    Disks,
    /// Only networking (e.g. SoftNPU).
    Network,
}

#[derive(clap::Args)]
#[group(multiple = true)]
pub struct Pxa {
    /// The first IP address your Oxide cluster can use.
    ///
    /// Requires `pxa-end`.
    #[clap(long = "pxa-start", requires = "end", env = "PXA_START")]
    start: Option<String>,

    /// The last IP address your Oxide cluster can use
    ///
    /// Requires `pxa-start`.
    #[clap(long = "pxa-end", requires = "start", env = "PXA_END")]
    end: Option<String>,
}

#[derive(Parser)]
pub struct Args {
    #[clap(long, value_enum, default_value_t = Scope::All)]
    scope: Scope,

    /// The directory in which virtual devices are stored
    #[clap(long, default_value = "/var/tmp")]
    vdev_dir: Utf8PathBuf,

    #[command(subcommand)]
    command: Commands,
}

static NO_INSTALL_MARKER: &'static str = "/etc/opt/oxide/NO_INSTALL";
const GB: u64 = 1 << 30;

const ARP: &'static str = "/usr/sbin/arp";
const DLADM: &'static str = "/usr/sbin/dladm";
const IPADM: &'static str = "/usr/sbin/ipadm";
const MODINFO: &'static str = "/usr/sbin/modinfo";
const MODUNLOAD: &'static str = "/usr/sbin/modunload";
const NETSTAT: &'static str = "/usr/bin/netstat";
const OPTEADM: &'static str = "/opt/oxide/opte/bin/opteadm";
const PFEXEC: &'static str = "/usr/bin/pfexec";
const PING: &'static str = "/usr/sbin/ping";
const SWAP: &'static str = "/usr/sbin/swap";
const ZFS: &'static str = "/usr/sbin/zfs";
const ZLOGIN: &'static str = "/usr/sbin/zlogin";
const ZPOOL: &'static str = "/usr/sbin/zpool";
const ZONEADM: &'static str = "/usr/sbin/zoneadm";

const SIDECAR_LITE_COMMIT: &'static str =
    "a95b7a9f78c08125f4e34106f5c885c7e9f2e8d5";
const SOFTNPU_COMMIT: &'static str = "3203c51cf4473d30991b522062ac0df2e045c2f2";
const PXA_MAC_DEFAULT: &'static str = "a8:e1:de:01:70:1d";

const PXA_WARNING: &'static str = r#"  You have not set up the proxy-ARP environment variables
  PXA_START and PXA_END. These variables are necessary to allow
  SoftNPU to respond to ARP requests for the portion of the
  network you've dedicated to Omicron.
  You must either destroy / recreate the Omicron environment
  with these variables or run `scadm standalon add-proxy-arp`
  in the SoftNPU zone later"#;

pub fn run_cmd(args: Args) -> Result<()> {
    if Utf8Path::new(NO_INSTALL_MARKER).exists() {
        bail!("This system has the marker file {NO_INSTALL_MARKER}, aborting");
    }

    let workspace_root = match crate::load_workspace() {
        Ok(metadata) => metadata.workspace_root,
        Err(_err) => {
            let pwd = Utf8PathBuf::try_from(std::env::current_dir()?)?;
            eprintln!(
                "Couldn't find Cargo.toml, using {pwd} as workspace root"
            );
            pwd
        }
    };

    let smf_path = "smf/sled-agent/non-gimlet/config.toml";
    let sled_agent_config = workspace_root.join(smf_path);
    if !sled_agent_config.exists() {
        bail!("Could not find {smf_path}. We need it to configure vdevs");
    }

    let npuzone_path = "out/npuzone/npuzone";
    let npu_zone = workspace_root.join(npuzone_path);
    if !npu_zone.exists() {
        bail!("Could not find {npuzone_path}. We need it to configure SoftNPU");
    }

    match args.command {
        Commands::Create {
            physical_link,
            promiscuous_filter_off,
            softnpu_mode,
            gateway_ip,
            gateway_mac,
            pxa,
            pxa_mac,
            vdev_size,
        } => {
            let physical_link = if let Some(l) = physical_link {
                l
            } else {
                default_physical_link()?
            };

            println!("creating virtual hardware");
            if matches!(args.scope, Scope::All | Scope::Disks) {
                ensure_vdevs(&sled_agent_config, &args.vdev_dir, vdev_size)?;
            }
            if matches!(args.scope, Scope::All | Scope::Network)
                && softnpu_mode == "zone"
            {
                ensure_simulated_links(&physical_link, promiscuous_filter_off)?;
                ensure_softnpu_zone(&npu_zone)?;
                initialize_softnpu_zone(gateway_ip, gateway_mac, pxa, pxa_mac)?;
            }
            println!("created virtual hardware");
        }
        Commands::Destroy => {
            println!("destroying virtual hardware");
            verify_omicron_uninstalled()?;
            demount_backingfs()?;
            if matches!(args.scope, Scope::All | Scope::Network) {
                unload_xde_driver()?;
                remove_softnpu_zone(&npu_zone)?;
                remove_vnics()?;
            }
            if matches!(args.scope, Scope::All | Scope::Disks) {
                destroy_vdevs(&sled_agent_config, &args.vdev_dir)?;
            }
            println!("destroyed virtual hardware");
        }
    }

    Ok(())
}

fn verify_omicron_uninstalled() -> Result<()> {
    let mut cmd = Command::new("svcs");
    cmd.arg("svc:/oxide/sled-agent:default");
    if let Ok(_) = execute(cmd) {
        bail!(
            "Omicron is still installed, please run `omicron-package uninstall` first"
        );
    }
    Ok(())
}

// Some services have their working data overlaid by backing mounts from the
// internal boot disk. Before we can destroy the ZFS pools, we need to unmount
// these.
fn demount_backingfs() -> Result<()> {
    const BACKED_SERVICES: &str = "svc:/system/fmd:default";
    println!("Disabling {BACKED_SERVICES}");
    svcadm_temporary_toggle(BACKED_SERVICES, false)?;
    for dataset in zfs_list_internal("noauto", "yes")? {
        println!("unmounting: {dataset}");
        zfs_umount(&dataset)?;
    }
    println!("Re-enabling {BACKED_SERVICES}");
    svcadm_temporary_toggle(BACKED_SERVICES, true)?;
    Ok(())
}

fn unload_xde_driver() -> Result<()> {
    let cmd = Command::new(MODINFO);
    let output = execute(cmd)?;

    let id = String::from_utf8(output.stdout)
        .context("Invalid modinfo output")?
        .lines()
        .find_map(|line| {
            let (id, desc) = line.trim().split_once(' ')?;
            if !desc.contains("xde") {
                return None;
            }
            return Some(id.to_string());
        });

    let Some(id) = id else {
        println!("xde driver already unloaded");
        return Ok(());
    };
    println!("unloading xde driver:\na) clearing underlay...");
    let mut cmd = Command::new(PFEXEC);
    cmd.args([OPTEADM, "clear-xde-underlay"]);
    if let Err(e) = execute(cmd) {
        // This is explicitly non-fatal: the underlay is only set when
        // sled-agent is running. We still need to be able to tear
        // down the driver if we immediately call create->destroy.
        println!("\tFailed or already unset: {e}");
    }

    println!("b) unloading module...");
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(MODUNLOAD);
    cmd.arg("-i");
    cmd.arg(id);
    execute(cmd)?;
    Ok(())
}

fn remove_softnpu_zone(npu_zone: &Utf8Path) -> Result<()> {
    println!("ensuring softnpu zone destroyed");
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(npu_zone);
    cmd.args([
        "destroy",
        "sidecar",
        "--omicron-zone",
        "--ports",
        "sc0_0,tfportrear0_0",
        "--ports",
        "sc0_1,tfportqsfp0_0",
    ]);
    if let Err(output) = execute(cmd) {
        // Don't throw an error if the zone was already removed
        if output.to_string().contains("No such zone configured") {
            println!("zone {npu_zone} already destroyed");
            return Ok(());
        } else {
            return Err(output);
        }
    }
    Ok(())
}

fn remove_vnics() -> Result<()> {
    delete_address("lo0/underlay")?;
    delete_interface("sc0_1")?;
    delete_vnic("sc0_1")?;

    for i in 0..=1 {
        let net = format!("net{i}");
        let sc = format!("sc{i}_0");

        delete_interface(&net)?;
        delete_simnet(&net)?;
        delete_simnet(&sc)?;
    }

    Ok(())
}

fn ensure_simulated_links(
    physical_link: &str,
    promiscuous_filter_off: bool,
) -> Result<()> {
    for i in 0..=1 {
        let net = format!("net{i}");
        let sc = format!("sc{i}_0");
        if !simnet_exists(&net) {
            create_simnet(&net)?;
            create_simnet(&sc)?;
            modify_simnet(&sc, &net)?;
            set_linkprop(&sc, "mtu", "9000")?;
        }
        println!("Simnet {net}/{sc} exists");
    }

    let sc = "sc0_1".to_string();
    if !vnic_exists(&sc) {
        create_vnic(&sc, physical_link, PXA_MAC_DEFAULT)?;
        if promiscuous_filter_off {
            set_linkprop(&sc, "promisc-filtered", "off")?;
        }
    }
    println!("Vnic {sc} exists");
    Ok(())
}

fn ensure_softnpu_zone(npu_zone: &Utf8Path) -> Result<()> {
    let zones = zoneadm_list()?;
    if !zones.iter().any(|z| z == "sidecar_softnpu") {
        if !npu_zone.exists() {
            bail!(
                "npu binary is not installed. Please re-run ./tools/install_prerequisites.sh"
            );
        }

        let mut cmd = Command::new(PFEXEC);
        cmd.arg(npu_zone);
        cmd.args([
            "create",
            "sidecar",
            "--omicron-zone",
            "--ports",
            "sc0_0,tfportrear0_0",
            "--ports",
            "sc0_1,tfportqsfp0_0",
            "--sidecar-lite-commit",
            SIDECAR_LITE_COMMIT,
            "--softnpu-commit",
            SOFTNPU_COMMIT,
        ]);
        execute(cmd)?;
    }

    Ok(())
}

fn initialize_softnpu_zone(
    gateway_ip: Option<String>,
    gateway_mac: Option<String>,
    pxa: Pxa,
    pxa_mac: String,
) -> Result<()> {
    let gateway_ip = match gateway_ip {
        Some(ip) => ip,
        None => default_gateway_ip()?,
    };
    println!("Using {gateway_ip} as gateway ip");

    let gateway_mac = get_gateway_mac(gateway_mac, &gateway_ip)?.to_string();
    println!("using {gateway_mac} as gateway mac");

    // Configure upstream network gateway ARP entry
    println!("configuring SoftNPU ARP entry");
    run_scadm_command(vec!["add-arp-entry", &gateway_ip, &gateway_mac])?;

    match (pxa.start, pxa.end) {
        (Some(start), Some(end)) => {
            println!("configuring SoftNPU proxy ARP");
            run_scadm_command(vec!["add-proxy-arp", &start, &end, &pxa_mac])?;
        }
        _ => {
            eprintln!("{PXA_WARNING}");
        }
    }

    let output = run_scadm_command(vec!["dump-state"])?;
    let stdout = String::from_utf8(output.stdout)
        .context("Invalid dump-state output")?;
    println!("SoftNPU state:");
    for line in stdout.lines() {
        println!("  {line}");
    }

    Ok(())
}

fn run_scadm_command(args: Vec<&str>) -> Result<Output> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([
        ZLOGIN,
        "sidecar_softnpu",
        "/softnpu/scadm",
        "--server",
        "/softnpu/server",
        "--client",
        "/softnpu/client",
        "standalone",
    ]);
    for arg in &args {
        cmd.arg(arg);
    }
    execute(cmd)
}

fn default_gateway_ip() -> Result<String> {
    let mut cmd = Command::new(NETSTAT);
    cmd.args(["-rn", "-f", "inet"]);
    let output = execute(cmd)?;

    String::from_utf8(output.stdout)
        .context("Invalid netstat output")?
        .lines()
        .find_map(|line| {
            let mut columns = line.trim().split_whitespace();
            let dst = columns.next()?;
            let gateway = columns.next()?;

            if dst == "default" {
                return Some(gateway.to_owned());
            }
            None
        })
        .ok_or_else(|| anyhow!("No default gateway found"))
}

fn get_gateway_mac(
    gateway_mac: Option<String>,
    gateway_ip: &str,
) -> Result<MacAddr> {
    match gateway_mac {
        Some(mac) => Ok(MacAddr::from_str(&mac)?),
        None => {
            let attempts = 3;
            for i in 0..=attempts {
                println!(
                    "Pinging {gateway_ip} and sleeping ({i} / {attempts})"
                );
                let mut cmd = Command::new(PING);
                cmd.arg(&gateway_ip);
                execute(cmd)?;
                std::thread::sleep(std::time::Duration::from_secs(1));
            }

            let mut cmd = Command::new(ARP);
            cmd.arg("-an");
            let output = execute(cmd)?;

            let mac = String::from_utf8(output.stdout)
                .context("Invalid arp output")?
                .lines()
                .find_map(|line| {
                    let mut columns = line.trim().split_whitespace().skip(1);
                    let ip = columns.next()?;
                    let mac = columns.last()?;
                    if ip == gateway_ip {
                        return Some(mac.to_string());
                    }
                    None
                })
                .ok_or_else(|| anyhow!("No gateway MAC found"))?;
            Ok(MacAddr::from_str(&mac)?)
        }
    }
}

/// This is a subset of omicron-sled-agent's "config/Config" structure.
///
/// We don't depend on it directly to avoid rebuilding whenever the
/// Sled Agent changes, though it's important for us to stay in sync
/// to parse these fields correctly.
#[derive(Clone, Debug, Deserialize)]
struct SledAgentConfig {
    /// Optional list of virtual devices to be used as "discovered disks".
    pub vdevs: Option<Vec<Utf8PathBuf>>,
}

impl SledAgentConfig {
    fn read(path: &Utf8Path) -> Result<Self> {
        let config = std::fs::read_to_string(path)?;
        toml::from_str(&config)
            .context("Could not parse sled agent config as toml")
    }
}

fn ensure_vdevs(
    sled_agent_config: &Utf8Path,
    vdev_dir: &Utf8Path,
    vdev_size: u64,
) -> Result<()> {
    let config = SledAgentConfig::read(sled_agent_config)?;

    let Some(vdevs) = &config.vdevs else {
        bail!("No vdevs found in this configuration");
    };

    for vdev in vdevs {
        let vdev_path = if vdev.is_absolute() {
            vdev.to_owned()
        } else {
            vdev_dir.join(vdev)
        };

        if vdev_path.exists() {
            println!("{vdev_path} already exists");
        } else {
            println!("creating {vdev_path}");
            let file = std::fs::File::create(&vdev_path)?;
            file.set_len(vdev_size)?;
        }
    }
    Ok(())
}

const ZVOL_ROOT: &str = "/dev/zvol/dsk";

fn destroy_vdevs(
    sled_agent_config: &Utf8Path,
    vdev_dir: &Utf8Path,
) -> Result<()> {
    let swap_devices = swap_list()?;
    let zpools = omicron_zpool_list()?;

    for zpool in &zpools {
        println!("destroying: {zpool}");
        // Remove any swap devices that appear used by this zpool
        for swap_device in &swap_devices {
            if swap_device
                .starts_with(Utf8PathBuf::from(ZVOL_ROOT).join(&zpool))
            {
                println!("Removing {swap_device} from {zpool}");
                swap_delete(&swap_device)?;
            }
        }

        // Then remove the zpool itself
        zpool_destroy(zpool)?;
        println!("destroyed: {zpool}");
    }

    // Remove the vdev files themselves, if they are regular files
    let config = SledAgentConfig::read(sled_agent_config)?;
    if let Some(vdevs) = &config.vdevs {
        for vdev in vdevs {
            let vdev_path = if vdev.is_absolute() {
                vdev.to_owned()
            } else {
                vdev_dir.join(vdev)
            };

            if !vdev_path.exists() {
                continue;
            }

            let metadata = std::fs::metadata(&vdev_path)?;

            if metadata.file_type().is_file() {
                std::fs::remove_file(&vdev_path)?;
                println!("deleted {vdev_path}");
            }
        }
    }

    Ok(())
}

fn execute(mut cmd: Command) -> Result<Output> {
    let output = cmd
        .output()
        .context(format!("Could not start command: {:?}", cmd.get_program()))?;
    if !output.status.success() {
        let stderr =
            String::from_utf8(output.stderr).unwrap_or_else(|_| String::new());

        bail!(
            "{:?} failed: {} (stderr: {stderr})",
            cmd.get_program(),
            output.status
        )
    }

    Ok(output)
}

// Lists all files used for swap
fn swap_list() -> Result<Vec<Utf8PathBuf>> {
    let mut cmd = Command::new(SWAP);
    cmd.arg("-l");

    let output = cmd.output().context("Could not start swap")?;
    if !output.status.success() {
        if let Ok(stderr) = String::from_utf8(output.stderr) {
            // This is an exceptional case - if there are no swap devices,
            // we treat this error case as an "empty result".
            if stderr.trim() == "No swap devices configured" {
                return Ok(vec![]);
            }
            eprint!("{}", stderr);
        }
        bail!("swap failed: {}", output.status);
    }

    Ok(String::from_utf8(output.stdout)
        .context("Invalid swap output")?
        .lines()
        .skip(1)
        .filter_map(|line| {
            line.split_whitespace().next().map(|s| Utf8PathBuf::from(s))
        })
        .collect())
}

// Deletes a specific swap file
fn swap_delete(file: &Utf8Path) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(SWAP);
    cmd.arg("-d");
    cmd.arg(file);
    execute(cmd)?;
    Ok(())
}

static ZPOOL_PREFIXES: [&'static str; 2] = ["oxp_", "oxi_"];

// Lists all zpools managed by omicron.
fn omicron_zpool_list() -> Result<Vec<String>> {
    let mut cmd = Command::new(ZPOOL);
    cmd.args(["list", "-Hpo", "name"]);
    let output = execute(cmd)?;

    Ok(String::from_utf8(output.stdout)
        .context("Invalid zpool list output")?
        .lines()
        .filter_map(|line| {
            let pool = line.trim().to_string();
            if ZPOOL_PREFIXES.iter().any(|pfx| pool.starts_with(pfx)) {
                Some(pool)
            } else {
                None
            }
        })
        .collect())
}

fn svcadm_temporary_toggle(svc: &str, enable: bool) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg("svcadm");
    if enable {
        cmd.arg("enable");
    } else {
        cmd.arg("disable");
    }
    cmd.arg("-st");
    cmd.arg(svc);
    execute(cmd)?;
    Ok(())
}

fn zfs_list_internal(canmount: &str, mounted: &str) -> Result<Vec<String>> {
    let mut cmd = Command::new(ZFS);
    cmd.args(["list", "-rHpo", "name,canmount,mounted"]);
    let output = execute(cmd)?;

    Ok(String::from_utf8(output.stdout)
        .context("Invalid zfs list output")?
        .lines()
        .filter_map(|line| {
            let mut cols = line.trim().split_whitespace();
            let dataset = cols.next()?;
            if !dataset.starts_with("oxi_") {
                return None;
            }
            if canmount != cols.next()? {
                return None;
            }
            if mounted != cols.next()? {
                return None;
            }
            return Some(dataset.to_string());
        })
        .collect())
}

fn zfs_umount(dataset: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([ZFS, "umount"]);
    cmd.arg(dataset);
    execute(cmd)?;
    Ok(())
}

fn zpool_destroy(pool: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([ZFS, "destroy", "-r"]);
    cmd.arg(pool);
    execute(cmd)?;

    // This can fail with an "already unmounted" error, which we opt to ignore.
    //
    // If it was important, then the zpool destroy command should fail below
    // anyway.
    let mut cmd = Command::new(PFEXEC);
    cmd.args([ZFS, "unmount"]);
    cmd.arg(pool);
    if let Err(err) = execute(cmd) {
        eprintln!(
            "Failed to unmount {pool}: {err}, attempting to destroy anyway"
        );
    }

    let mut cmd = Command::new(PFEXEC);
    cmd.args([ZPOOL, "destroy"]);
    cmd.arg(pool);
    execute(cmd)?;

    Ok(())
}

fn delete_address(addr: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(IPADM);
    cmd.arg("delete-addr");
    cmd.arg(addr);

    let output = cmd.output().context("Failed to start ipadm")?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        if stderr.contains("Object not found") {
            return Ok(());
        }
        bail!("ipadm delete-addr failed: {} (stderr: {stderr})", output.status);
    }

    Ok(())
}

fn delete_interface(iface: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(IPADM);
    cmd.arg("delete-if");
    cmd.arg(iface);

    let output = cmd.output().context("Failed to start ipadm")?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        if stderr.contains("Interface does not exist") {
            return Ok(());
        }
        bail!("ipadm delete-if failed: {} (stderr: {stderr})", output.status);
    }

    Ok(())
}

fn delete_vnic(vnic: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(DLADM);
    cmd.arg("delete-vnic");
    cmd.arg(vnic);

    let output = cmd.output().context("Failed to start dladm")?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        if stderr.contains("invalid link name") {
            return Ok(());
        }
        bail!("dladm delete-vnic failed: {} (stderr: {stderr})", output.status);
    }

    Ok(())
}

fn delete_simnet(simnet: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.arg(DLADM);
    cmd.arg("delete-simnet");
    cmd.arg("-t");
    cmd.arg(simnet);

    let output = cmd.output().context("Failed to start dladm")?;
    if !output.status.success() {
        let stderr = String::from_utf8(output.stderr)?;
        if stderr.contains("not found") {
            return Ok(());
        }
        bail!(
            "dleadm delete-simnet failed: {} (stderr: {stderr})",
            output.status
        );
    }

    Ok(())
}

fn default_physical_link() -> Result<String> {
    let mut cmd = Command::new(DLADM);
    cmd.args(["show-phys", "-p", "-o", "LINK"]);
    let output = execute(cmd)?;

    Ok(String::from_utf8(output.stdout)
        .context("Invalid dladm output")?
        .lines()
        .next()
        .ok_or_else(|| anyhow!("Empty dladm output"))?
        .to_string())
}

// Returns "true" if the VNIC exists.
//
// Returns false if it does not exist, or if we cannot tell.
fn vnic_exists(vnic: &str) -> bool {
    let mut cmd = Command::new(DLADM);
    cmd.args(["show-vnic", "-p", "-o", "LINK"]);
    cmd.arg(vnic);
    match execute(cmd) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn create_vnic(vnic: &str, physical_link: &str, mac: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([DLADM, "create-vnic", "-t"]);
    cmd.arg(vnic);
    cmd.arg("-l");
    cmd.arg(physical_link);
    cmd.arg("-m");
    cmd.arg(mac);
    execute(cmd)?;
    Ok(())
}

fn create_simnet(simnet: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([DLADM, "create-simnet", "-t"]);
    cmd.arg(simnet);
    execute(cmd)?;
    Ok(())
}

fn modify_simnet(simnet: &str, peer: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([DLADM, "modify-simnet", "-t", "-p"]);
    cmd.arg(peer);
    cmd.arg(simnet);
    execute(cmd)?;
    Ok(())
}

fn set_linkprop(link: &str, key: &str, value: &str) -> Result<()> {
    let mut cmd = Command::new(PFEXEC);
    cmd.args([DLADM, "set-linkprop", "-p"]);
    cmd.arg(format!("{key}={value}"));
    cmd.arg(link);
    execute(cmd)?;
    Ok(())
}

// Returns "true" if the simnet exists.
//
// Returns false if it does not exist, or if we cannot tell.
fn simnet_exists(simnet: &str) -> bool {
    let mut cmd = Command::new(DLADM);
    cmd.args(["show-simnet", "-p", "-o", "LINK"]);
    cmd.arg(simnet);
    match execute(cmd) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn zoneadm_list() -> Result<Vec<String>> {
    let mut cmd = Command::new(ZONEADM);
    cmd.arg("list");
    let output = execute(cmd)?;

    Ok(String::from_utf8(output.stdout)
        .context("Invalid zoneadm output")?
        .lines()
        .map(|line| line.trim().to_owned())
        .collect())
}
