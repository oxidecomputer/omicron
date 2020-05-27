/*!
 * Simulated server controller
 */

/*
 * TODO see the TODO for oxide-controller.
 */

use std::io::{stderr, Write};
use std::process::exit;

use clap::{App, Arg};

#[tokio::main]
async fn main() {
    let args = App::new("server-controller")
        .after_help("See README.adoc for more information")
        .arg(Arg::with_name("OXIDE_CONTROLLER_IP:PORT").required(true).index(1))
        .get_matches_safe();

    let matches = match args {
        Ok(m) => m,
        Err(e) => {
            let _ = write!(stderr(), "{}", e);
            exit(1);
        }
    };

    let controller_addr = matches.value_of("OXIDE_CONTROLLER_IP:PORT").unwrap();
    /* XXX */
    todo!()
    // if let Err(error) =
    //     oxide_api_prototype::run_server_controller(&controller_addr).await
    // {
    //     eprintln!("{}: {}", std::env::args().nth(0).unwrap(), error);
    //     std::process::exit(1);
    // }
}
