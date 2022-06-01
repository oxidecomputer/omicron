// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use omicron_sled_agent::bootstrap;
use omicron_sled_agent::illumos::addrobj::AddrObject;
use omicron_sled_agent::illumos::{dladm, zone};
use std::io;
use std::net::IpAddr;

struct AddressCleanup {
    addrobj: AddrObject,
}

impl Drop for AddressCleanup {
    fn drop(&mut self) {
        let _ = zone::Zones::delete_address(None, &self.addrobj);
    }
}

// TODO(https://github.com/oxidecomputer/omicron/issues/1032):
//
// This test has value when hacking on multicast bootstrap address
// swapping, but is known to be flaky. It is being set to ignored
// for the following reasons:
//
// - It still can provide value when modifying the bootstrap address
// swap locally.
// - It is known to be flaky.
// - According to
// <https://rfd.shared.oxide.computer/rfd/0259#_bootstrap_address_advertisement_and_discovery>,
// we are planning on performing address swapping via Maghemite, so
// the implementation being tested here will eventually change enough
// to render the test obsolete.
#[tokio::test]
#[ignore]
async fn test_multicast_bootstrap_address() {
    // Setup the bootstrap address.
    //
    // This modifies global state of the target machine, creating
    // an address named "testbootstrap6", akin to what the bootstrap
    // agent should do.
    let etherstub = dladm::Dladm::create_etherstub().unwrap();
    let link = dladm::Dladm::create_etherstub_vnic(&etherstub).unwrap();

    let phys_link = dladm::Dladm::find_physical().unwrap();
    let address =
        bootstrap::agent::bootstrap_address(phys_link.clone()).unwrap();
    let address_name = "testbootstrap6";
    let addrobj = AddrObject::new(&link.0, address_name).unwrap();
    zone::Zones::ensure_has_global_zone_v6_address(
        link,
        *address.ip(),
        address_name,
    )
    .unwrap();

    // Cleanup-on-drop removal of the bootstrap address.
    let _cleanup = AddressCleanup { addrobj };

    // Create the multicast pair.
    let loopback = true;
    let interface = 0;
    let (sender, listener) = bootstrap::multicast::new_ipv6_udp_pair(
        address.ip(),
        loopback,
        interface,
    )
    .unwrap();

    // Create a receiver task which reads for messages that have
    // been broadcast, verifies the message, and returns the
    // calling address.
    let message = b"Hello World!";
    let receiver_task_handle = tokio::task::spawn(async move {
        let mut buf = vec![0u8; 32];
        let (len, addr) = listener.recv_from(&mut buf).await?;
        assert_eq!(message.len(), len);
        assert_eq!(message, &buf[..message.len()]);
        assert_eq!(addr.ip(), IpAddr::V6(*address.ip()));
        Ok::<_, io::Error>(addr)
    });

    // Send a message repeatedly, and exit successfully if we
    // manage to receive the response.
    tokio::pin!(receiver_task_handle);
    let mut send_count = 0;
    loop {
        tokio::select! {
            result = sender.send_to(message, bootstrap::multicast::multicast_address()) => {
                assert_eq!(message.len(), result.unwrap());
                send_count += 1;
                if send_count > 10 {
                    panic!("10 multicast UDP messages sent with no response");
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
            result = &mut receiver_task_handle => {
                let addr = result.unwrap().unwrap();
                eprintln!("Receiver received message: {:#?}", addr);
                assert_eq!(addr.ip(), IpAddr::V6(*address.ip()));
                break;
            }
        }
    }
}
