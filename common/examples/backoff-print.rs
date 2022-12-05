// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Show example retry intervals and times for the internal service policy

use omicron_common::backoff;
use omicron_common::backoff::Backoff;

fn main() {
    let mut policy = backoff::retry_policy_internal_service();
    let mut total_duration = std::time::Duration::from_secs(0);
    loop {
        let nmin = total_duration.as_secs() / 60;
        let nsecs = total_duration.as_secs() % 60;
        let nmillis = total_duration.as_millis() % 1000;
        print!("at T={:3}m{:02}.{:03}s: ", nmin, nsecs, nmillis);

        if let Some(next) = policy.next_backoff() {
            let nmin = next.as_secs() / 60;
            let nsecs = next.as_secs() % 60;
            let nmillis = next.as_millis() % 1000;
            println!("wait {:3}m{:02}.{:03}s", nmin, nsecs, nmillis);
            total_duration = total_duration + next;

            if nmin >= 60 {
                break;
            }
        }
    }
}
