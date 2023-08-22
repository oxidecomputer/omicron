// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::host::parse::InputParser;
use crate::host::{ServiceName, ZoneName};

use helios_fusion::Input;
use helios_fusion::SVCADM;

pub enum Command {
    Enable { zone: Option<ZoneName>, service: ServiceName },
    Disable { zone: Option<ZoneName>, service: ServiceName },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(mut input: Input) -> Result<Self, Self::Error> {
        if input.program != SVCADM {
            return Err(format!("Not svcadm command: {}", input.program));
        }

        let mut input = InputParser::new(input);

        let zone = if input.shift_arg_if("-z")? {
            Some(ZoneName(input.shift_arg()?))
        } else {
            None
        };

        match input.shift_arg()?.as_str() {
            "enable" => {
                // Intentionally ignored
                input.shift_arg_if("-t")?;
                let service = ServiceName(input.shift_arg()?);
                input.no_args_remaining()?;
                Ok(Command::Enable { zone, service })
            }
            "disable" => {
                // Intentionally ignored
                input.shift_arg_if("-t")?;
                let service = ServiceName(input.shift_arg()?);
                input.no_args_remaining()?;
                Ok(Command::Disable { zone, service })
            }
            command => return Err(format!("Unexpected command: {command}")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn enable() {
        let Command::Enable { zone, service } = Command::try_from(
            Input::shell(format!(
                "{SVCADM} -z myzone enable -t foobar"
            )),
        ).unwrap() else {
            panic!("wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(service.0, "foobar");

        assert!(Command::try_from(Input::shell(format!("{SVCADM} enable")))
            .err()
            .unwrap()
            .contains("Missing argument"));
    }

    #[test]
    fn disable() {
        let Command::Disable { zone, service } = Command::try_from(
            Input::shell(format!(
                "{SVCADM} -z myzone disable -t foobar"
            )),
        ).unwrap() else {
            panic!("wrong command");
        };

        assert_eq!(zone.unwrap().0, "myzone");
        assert_eq!(service.0, "foobar");

        assert!(Command::try_from(Input::shell(format!("{SVCADM} disable")))
            .err()
            .unwrap()
            .contains("Missing argument"));
    }
}
