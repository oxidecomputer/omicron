// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Simualting a Root of Trust

use crate::config::SpCommonConfig;
use sprockets_rot::common::certificates::SerialNumber;
use sprockets_rot::common::Ed25519PublicKey;
use sprockets_rot::salty;
use sprockets_rot::RotConfig;
use sprockets_rot::RotSprocket;

pub(crate) trait RotSprocketExt {
    // Returns the (derived-from-config) manufacturing public key and the
    // `RotSprocket`.
    fn bootstrap_from_config(
        config: &SpCommonConfig,
    ) -> (Ed25519PublicKey, Self);
}

impl RotSprocketExt for RotSprocket {
    fn bootstrap_from_config(
        config: &SpCommonConfig,
    ) -> (Ed25519PublicKey, Self) {
        let manufacturing_keypair =
            salty::Keypair::from(&config.manufacturing_root_cert_seed);
        let device_id_keypair =
            salty::Keypair::from(&config.device_id_cert_seed);
        let serial_number = SerialNumber(config.serial_number);
        let config = RotConfig::bootstrap_for_testing(
            &manufacturing_keypair,
            device_id_keypair,
            serial_number,
        );
        let manufacturing_public_key =
            Ed25519PublicKey(manufacturing_keypair.public.to_bytes());
        (manufacturing_public_key, Self::new(config))
    }
}
