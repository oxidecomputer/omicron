use signal_hook::consts::SIGHUP;
use signal_hook::consts::SIGINT;
use signal_hook::consts::SIGQUIT;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;
use slog::Drain;
use slog::Logger;
use slog::debug;
use slog::error;
use slog::info;
use slog::o;
use std::sync::Arc;

#[cfg(target_os = "illumos")]
mod illumos {
    use illumos_utils::contract;
    use illumos_utils::contract::ContractType;
    use illumos_utils::contract::Control;
    use illumos_utils::contract::Template;
    use illumos_utils::contract::Watcher;
    use slog::Logger;
    use slog::error;
    use slog::info;
    use std::sync::Arc;

    const TOFINO_DEVICE: &str = "/dev/tofino/1";

    // The list() operation in the zone crate returns all the installed zones, when
    // we really only care about whether the switch zone is running.
    fn get_running_zones() -> Result<Vec<String>, zone::ZoneError> {
        let zoneadm_output = std::process::Command::new(illumos_utils::PFEXEC)
            .env_clear()
            .arg(illumos_utils::ZONEADM)
            .arg("list")
            .arg("-p")
            .output()
            .map_err(zone::ZoneError::Command)?;
        Ok(zone::Adm::parse_list_output(&zoneadm_output)?
            .iter()
            .map(|z| z.name.to_string())
            .collect::<Vec<String>>())
    }

    fn halt_switch_zone(log: &Logger) {
        match get_running_zones() {
            Ok(zones) => {
                if !zones.iter().any(|name| name == "oxz_switch") {
                    info!(log, "switch zone not running");
                    return;
                }
            }
            Err(e) => {
                error!(log, "failed to get list of zones: {e:?}");
                return;
            }
        }
        info!(log, "halting the switch zone");
        if let Err(e) = std::process::Command::new(illumos_utils::PFEXEC)
            .env_clear()
            .arg(illumos_utils::ZONEADM)
            .arg("-z")
            .arg("oxz_switch")
            .arg("halt")
            .output()
        {
            error!(log, "failed to halt switch zone: {e:?}");
        }
    }

    // Spin until we find a tofino device
    fn wait_for_tofino(log: &Logger) -> tofino::TofinoNode {
        loop {
            // We start with a quick and cheap check to see if the tofino might
            // be present before doing a deeper check by reading the full device
            // tree.
            if let Ok(true) = std::fs::exists(TOFINO_DEVICE) {
                match tofino::get_tofino() {
                    Err(e) => error!(log, "failed devinfo lookup: {e:?}"),
                    Ok(Some(x)) => return x,
                    _ => {}
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    // Monitor the presence of a tofino ASIC.  We use the device filesystem to
    // detect the initial presence of an ASIC and then use a device contract to
    // detect if and when the ASIC goes away.
    pub(crate) fn monitor(log: Arc<Logger>) {
        info!(&log, "tofino monitor online");
        loop {
            wait_for_tofino(&log);
            info!(&log, "tofino device found");

            // Set up a device contract, asking the kernel to notify us when the
            // tofino goes away, and to avoid detaching the device driver until
            // we've done some necessary cleanup.
            let ctid = match Template::new(ContractType::Device) {
                Ok(template) => match template.create() {
                    Ok(c) => c,
                    Err(e) => {
                        error!(&log, "unable to create tofino contract: {e:?}");
                        continue;
                    }
                },
                Err(e) => {
                    error!(
                        &log,
                        "unable to open tofino contract template: {e:?}"
                    );
                    continue;
                }
            };
            let ctl = match Control::new(ContractType::Device, ctid) {
                Ok(c) => c,
                Err(e) => {
                    error!(&log, "unable to create tofino contract: {e:?}");
                    continue;
                }
            };

            let watcher = Watcher::new(ContractType::Device);
            loop {
                let ev = watcher.watch(&log);
                match ev.typ {
                    contract::CT_DEV_EV_OFFLINE => {
                        info!(&log, "Got tofino removed notification");
                        if ev.ctid != ctid {
                            info!(&log, "event for wrong contract");
                            continue;
                        }

                        // The device detach mechanism in the kernel will block for
                        // up to a minute, waiting for us to acknowledge this event.
                        // Illumos will not detach the device driver while the
                        // device is still in use.  If a device is assigned to a
                        // zone, that counts as "in use".  By halting the zone and
                        // deferring that "ack" until the zone is gone, we enable
                        // the device to be detached cleanly, which will
                        // subsequently allow the device to be re-attached cleanly
                        // if/when the sidecar is powered back on.
                        halt_switch_zone(&log);
                        info!(&log, "acknowledging the remove event");
                        if let Err(e) = ctl.ack(ev.event_id) {
                            error!(&log, "{e:?}");
                        }
                        std::thread::sleep(std::time::Duration::from_secs(2));
                    }
                    contract::CT_EV_NEGEND => {
                        info!(&log, "closing out the device contract");
                        if let Err(e) = ctl.abandon() {
                            error!(&log, "{e:?}");
                        }

                        info!(&log, "tofino monitor exiting");
                        // Exiting with a status code of 0 communicates to our
                        // sled-agent parent that the tofino is gone and that we
                        // have shut down the switch zone.
                        std::process::exit(0);
                    }
                    x => error!(&log, "unexpected device event: {x}"),
                }
            }
        }
    }
}

fn wait_for_signal(log: Arc<Logger>) {
    let mut signals = match Signals::new(&[SIGHUP, SIGINT, SIGQUIT, SIGTERM]) {
        Ok(s) => s,
        Err(e) => {
            panic!("failed to set up signal handler: {e:?}");
        }
    };

    let handle = signals.handle();
    for signal in signals.forever() {
        debug!(&log, "Caught {signal:?}");
        match signal {
            SIGHUP | SIGINT | SIGQUIT | SIGTERM => break,
            x => error!(&log, "caught unrequested signal: {x:?}"),
        }
    }
    handle.close();
}

fn main() {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let log = Logger::root(
        slog_term::FullFormat::new(plain).build().fuse(),
        o!("unit" => "tofino-monitor"),
    );
    let log = Arc::new(log);
    #[cfg(target_os = "illumos")]
    {
        let monitor_log = log.clone();
        std::thread::spawn(move || illumos::monitor(monitor_log));
    }

    let signal_log = log.clone();
    wait_for_signal(signal_log);
    info!(&log, "process exiting");
}
