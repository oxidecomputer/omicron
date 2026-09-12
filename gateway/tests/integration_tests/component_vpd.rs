// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use gateway_messages::SpPort;
use gateway_test_utils::setup;
use gateway_types::component::SpType;
use gateway_types::component_vpd::{
    Barcode, ComponentVpd, Mpn1Barcode, OxideBarcode, PmbusDevice, SledFanTray,
    Tmp11x,
};

#[tokio::test]
async fn pmbus_vpd() {
    fn mk_ibc(date: &str, serial_number: &str) -> ComponentVpd {
        ComponentVpd::Pmbus(PmbusDevice {
            mfr_id: Some(b"Flex".to_vec()),
            mfr_model: Some(b"BMR4913203851".to_vec()),
            mfr_revision: Some(b"R1C A".to_vec()),
            mfr_location: Some(b"CB6".to_vec()),
            mfr_date: Some(date.as_bytes().to_vec()),
            mfr_serial: Some(serial_number.as_bytes().to_vec()),
            ic_device_id: None,
            ic_device_rev: None,
        })
    }

    let testctx = setup::test_setup("pmbus_vpd", SpPort::One).await;
    let client = &testctx.client;

    for (sp_type, slot, date, serial_number) in [
        (SpType::Switch, 0, "2023-02-25", "FP1C481050"),
        (SpType::Switch, 1, "2023-02-26", "FP1C481051"),
        (SpType::Sled, 0, "2023-02-27", "FP1C481052"),
        (SpType::Sled, 1, "2023-02-28", "FP1C481053"),
    ] {
        let vpd = client
            .sp_component_vpd_get(&sp_type, slot, "ibc")
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            vpd,
            mk_ibc(date, serial_number),
            "IBC PMBus VPD for {sp_type} {slot}"
        );
    }

    testctx.teardown().await;
}

#[tokio::test]
async fn tmp11x_vpd() {
    let testctx = setup::test_setup("tmp11x_vpd", SpPort::One).await;
    let client = &testctx.client;
    for (slot, component, eeprom1, eeprom2, eeprom3) in [
        (0, "dev-1", 0x0101, 0x0102, 0x0103),
        (0, "dev-2", 0x0201, 0x0202, 0x0203),
        (1, "dev-0", 0x1001, 0x1002, 0x1003),
        (1, "dev-1", 0x1101, 0x1102, 0x1103),
        (1, "dev-2", 0x1201, 0x1202, 0x1203),
    ] {
        let vpd = client
            .sp_component_vpd_get(&SpType::Sled, slot, component)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            vpd,
            ComponentVpd::Tmp11x(Tmp11x {
                device_id: 0x0117,
                eeprom1,
                eeprom2,
                eeprom3,
            }),
            "TMP117 VPD for sled {slot} {component}"
        );
    }

    testctx.teardown().await;
}

#[tokio::test]
async fn single_barcode_vpd() {
    let testctx = setup::test_setup("single_barcode_vpd", SpPort::One).await;
    let client = &testctx.client;
    for (slot, serial_number) in [(0, "2SHRKFN1"), (1, "2SHRKFN2")] {
        let vpd = client
            .sp_component_vpd_get(&SpType::Sled, slot, "dev-6")
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            vpd,
            ComponentVpd::OxideBarcode(OxideBarcode {
                part_number: "913-0000028".to_owned(),
                revision: 1,
                serial_number: serial_number.to_owned(),
            }),
            "Sharkfin barcode for sled {slot}"
        );
    }

    testctx.teardown().await;
}

#[tokio::test]
async fn fan_tray_barcode_vpd() {
    let testctx = setup::test_setup("fan_tray_barcode_vpd", SpPort::One).await;
    let client = &testctx.client;
    let vpd = client
        .sp_component_vpd_get(&SpType::Sled, 0, "fan-tray-vpd")
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        vpd,
        ComponentVpd::SledFanTray(SledFanTray {
            identity: OxideBarcode {
                part_number: "991-0000151".to_owned(),
                revision: 1,
                serial_number: "TST01234567".to_owned(),
            },
            vpd_board_identity: OxideBarcode {
                part_number: "913-0000027".to_owned(),
                revision: 1,
                serial_number: "TST01234568".to_owned(),
            },
            fan0: Barcode::Mpn1(Mpn1Barcode {
                manufacturer: "SYD".to_owned(),
                part_number: "9CRA0848P8G012".to_owned(),
                revision: "C".to_owned(),
                serial_number: "WWYY1SSS".to_owned(),
            }),
            fan1: Barcode::Oxide(OxideBarcode {
                part_number: "125-0000456".to_owned(),
                revision: 2,
                serial_number: "FAN01234569".to_owned(),
            }),
            fan2: Barcode::Mpn1(Mpn1Barcode {
                manufacturer: "ABC".to_owned(),
                part_number: "ASDF-1000".to_owned(),
                revision: "032".to_owned(),
                serial_number: "123456789".to_owned(),
            }),
        }),
        "fan tray for sled 0"
    );

    testctx.teardown().await;
}
