// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::cli::parse::InputParser;
use crate::host::LinkName;

use helios_fusion::Input;
use helios_fusion::ROUTE;
use ipnetwork::IpNetwork;
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq)]
pub enum RouteTarget {
    Default,
    DefaultV4,
    DefaultV6,
    ByAddress(IpNetwork),
}

impl RouteTarget {
    fn shift_target(input: &mut InputParser) -> Result<Self, String> {
        let force_v4 = input.shift_arg_if("-inet")?;
        let force_v6 = input.shift_arg_if("-inet6")?;

        let target = match (force_v4, force_v6, input.shift_arg()?.as_str()) {
            (true, true, _) => {
                return Err("Cannot force both v4 and v6".to_string())
            }
            (true, false, "default") => RouteTarget::DefaultV4,
            (false, true, "default") => RouteTarget::DefaultV6,
            (false, false, "default") => RouteTarget::Default,
            (_, _, other) => {
                let net =
                    IpNetwork::from_str(other).map_err(|e| e.to_string())?;
                if force_v4 && !net.is_ipv4() {
                    return Err(format!("{net} is not ipv4"));
                }
                if force_v6 && !net.is_ipv6() {
                    return Err(format!("{net} is not ipv6"));
                }
                RouteTarget::ByAddress(net)
            }
        };
        Ok(target)
    }
}

pub(crate) enum Command {
    Add {
        destination: RouteTarget,
        gateway: RouteTarget,
        interface: Option<LinkName>,
    },
}

impl TryFrom<Input> for Command {
    type Error = String;

    fn try_from(input: Input) -> Result<Self, Self::Error> {
        if input.program != ROUTE {
            return Err(format!("Not route command: {}", input.program));
        }

        let mut input = InputParser::new(input);

        match input.shift_arg()?.as_str() {
            "add" => {
                let destination = RouteTarget::shift_target(&mut input)?;
                let gateway = RouteTarget::shift_target(&mut input)?;

                let interface = if let Ok(true) = input.shift_arg_if("-ifp") {
                    Some(LinkName(input.shift_arg()?))
                } else {
                    None
                };
                input.no_args_remaining()?;
                Ok(Command::Add { destination, gateway, interface })
            }
            command => return Err(format!("Unsupported command: {}", command)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use ipnetwork::IpNetwork;
    use std::str::FromStr;

    #[test]
    fn add() {
        // Valid command
        let Command::Add { destination, gateway, interface } =
            Command::try_from(Input::shell(format!(
                "{ROUTE} add -inet6 fd00::/16 default -ifp mylink"
            )))
            .unwrap();
        assert_eq!(
            destination,
            RouteTarget::ByAddress(IpNetwork::from_str("fd00::/16").unwrap())
        );
        assert_eq!(gateway, RouteTarget::Default);
        assert_eq!(interface.unwrap().0, "mylink");

        // Valid command
        let Command::Add { destination, gateway, interface } =
            Command::try_from(Input::shell(format!(
                "{ROUTE} add -inet default 127.0.0.1/8"
            )))
            .unwrap();
        assert_eq!(destination, RouteTarget::DefaultV4);
        assert_eq!(
            gateway,
            RouteTarget::ByAddress(IpNetwork::from_str("127.0.0.1/8").unwrap())
        );
        assert!(interface.is_none());

        // Invalid address family
        assert!(Command::try_from(Input::shell(format!(
            "{ROUTE} add -inet -inet6 default 127.0.0.1/8"
        )))
        .err()
        .unwrap()
        .contains("Cannot force both v4 and v6"));

        // Invalid address family
        assert!(Command::try_from(Input::shell(format!(
            "{ROUTE} add -inet6 default -inet6 127.0.0.1/8"
        )))
        .err()
        .unwrap()
        .contains("127.0.0.1/8 is not ipv6"));
    }
}
