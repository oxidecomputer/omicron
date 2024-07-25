// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::anyhow;
use omicron_common::cmd::CmdError;
use slog::{info, Logger};
use std::fs::{copy, create_dir_all};
use uzers::{get_group_by_name, get_user_by_name};

// User information for the switch zone
pub struct SwitchZoneUser {
    user: String,
    group: String,
    gecos: String,
    nopasswd: bool,
    homedir: Option<String>,
    shell: String,
    profiles: Option<Vec<String>>,
}

impl SwitchZoneUser {
    pub fn new(
        user: String,
        group: String,
        gecos: String,
        nopasswd: bool,
        shell: String,
    ) -> Self {
        Self {
            user,
            group,
            gecos,
            nopasswd,
            homedir: None,
            shell,
            profiles: None,
        }
    }

    pub fn with_homedir(mut self, homedir: String) -> Self {
        self.homedir = Some(homedir);
        self
    }

    pub fn with_profiles(mut self, profiles: Vec<String>) -> Self {
        self.profiles = Some(profiles);
        self
    }

    fn add_new_group_for_user(&self) -> Result<(), CmdError> {
        match get_group_by_name(&self.group) {
            Some(_) => {}
            None => {
                let cmd = std::process::Command::new("groupadd")
                    .arg(&self.group)
                    .output()
                    .map_err(|err| {
                        CmdError::Failure(anyhow!(
                            "Could not execute `groupadd {}`: {}",
                            self.group,
                            err
                        ))
                    })?;

                if !cmd.status.success() {
                    return Err(CmdError::Failure(anyhow!(
                        "Could not add group: {} status: {}",
                        self.group,
                        cmd.status
                    )));
                }
            }
        };
        Ok(())
    }

    fn add_new_user(&self) -> Result<(), CmdError> {
        match get_user_by_name(&self.user) {
            Some(_) => {}
            None => {
                let cmd = std::process::Command::new("useradd")
                    .args([
                        "-m",
                        "-s",
                        &self.shell,
                        "-g",
                        &self.group,
                        "-c",
                        &self.gecos,
                        &self.user,
                    ])
                    .output()
                    .map_err(|err| {
                        CmdError::Failure(anyhow!(
                            "Could not execute `useradd -m -s {} -g {} -c {} {}`: {}",
                            self.shell, self.group, self.gecos, self.user, err
                        ))
                    })?;

                if !cmd.status.success() {
                    return Err(CmdError::Failure(anyhow!(
                        "Could not add user: {} status: {}",
                        self.user,
                        cmd.status
                    )));
                }
            }
        };
        Ok(())
    }

    fn enable_passwordless_login(&self) -> Result<(), CmdError> {
        let cmd = std::process::Command::new("passwd")
            .args(["-d", &self.user])
            .output()
            .map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not execute `passwd -d {}`: {}",
                    self.user,
                    err
                ))
            })?;

        if !cmd.status.success() {
            return Err(CmdError::Failure(anyhow!(
                "Could not enable password-less login: {} status: {}",
                self.user,
                cmd.status
            )));
        }
        Ok(())
    }

    fn disable_password_based_login(&self) -> Result<(), CmdError> {
        let cmd = std::process::Command::new("passwd")
            .args(["-N", &self.user])
            .output()
            .map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not execute `passwd -N {}`: {}",
                    self.user,
                    err
                ))
            })?;

        if !cmd.status.success() {
            return Err(CmdError::Failure(anyhow!(
                "Could not disable password-based logins: {} status: {}",
                self.user,
                cmd.status
            )));
        }
        Ok(())
    }

    fn assign_user_profiles(&self) -> Result<(), CmdError> {
        let Some(ref profiles) = self.profiles else {
            return Err(CmdError::Failure(anyhow!(
                "Profile list must not be empty to assign user profiles",
            )));
        };

        let mut profile_list: String = Default::default();
        for profile in profiles {
            profile_list.push_str(&profile)
        }

        let cmd = std::process::Command::new("usermod")
            .args(["-P", &profile_list, &self.user])
            .output()
            .map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not execute `usermod -P {} {}`: {}",
                    profile_list,
                    self.user,
                    err
                ))
            })?;

        if !cmd.status.success() {
            return Err(CmdError::Failure(anyhow!(
                "Could not assign user profiles: {} status: {}",
                self.user,
                cmd.status
            )));
        }
        Ok(())
    }

    fn remove_user_profiles(&self) -> Result<(), CmdError> {
        let cmd = std::process::Command::new("usermod")
            .args(["-P", "", &self.user])
            .output()
            .map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not execute `usermod -P '' {}`: {}",
                    self.user,
                    err
                ))
            })?;

        if !cmd.status.success() {
            return Err(CmdError::Failure(anyhow!(
                "Could not remove user profiles: {} status: {}",
                self.user,
                cmd.status
            )));
        }
        Ok(())
    }

    fn set_up_home_directory_and_startup_files(&self) -> Result<(), CmdError> {
        let Some(ref homedir) = self.homedir else {
            return Err(CmdError::Failure(anyhow!(
                "A home directory must be provided to set up home directory and startup files",
            )));
        };

        create_dir_all(&homedir).map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not execute create directory {} and its parents: {}",
                &homedir,
                err
            ))
        })?;

        let mut home_bashrc = homedir.clone();
        home_bashrc.push_str("/.bashrc");
        copy("/root/.bashrc", &home_bashrc).map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not copy file from /root/.bashrc to {}: {}",
                &homedir,
                err
            ))
        })?;

        let mut home_profile = homedir.clone();
        home_profile.push_str("/.profile");
        copy("/root/.profile", &home_profile).map_err(|err| {
            CmdError::Failure(anyhow!(
                "Could not copy file from /root/.profile to {}: {}",
                &homedir,
                err
            ))
        })?;

        // Not using std::os::unix::fs::chown here because it doesn't support
        // recursive option.
        let cmd = std::process::Command::new("chown")
            .args(["-R", &self.user, &homedir])
            .output()
            .map_err(|err| {
                CmdError::Failure(anyhow!(
                    "Could not execute `chown -R {} {}`: {}",
                    self.user,
                    homedir,
                    err
                ))
            })?;

        if !cmd.status.success() {
            return Err(CmdError::Failure(anyhow!(
                "Could not change ownership: {} status: {}",
                homedir,
                cmd.status
            )));
        }
        Ok(())
    }

    pub fn setup_switch_zone_user(self, log: &Logger) -> Result<(), CmdError> {
        info!(&log, "Add a new group for the user"; "group" => ?self.group, "user" => ?self.user);
        self.add_new_group_for_user()?;

        info!(&log, "Add the user"; "user" => ?self.user, "shell" => ?self.shell,
        "group" => &self.group, "gecos" => &self.gecos);
        self.add_new_user()?;

        // Either enable password-less login (wicket) or disable password-based logins
        // completely (support, which logs in via ssh key).
        if self.nopasswd {
            info!(&log, "Enable password-less login for user"; "user" => ?self.user);
            self.enable_passwordless_login()?;
        } else {
            info!(&log, "Disable password-based logins"; "user" => ?self.user);
            self.disable_password_based_login()?;
        };

        if let Some(_) = &self.profiles {
            info!(&log, "Assign user profiles"; "user" => ?self.user, "profiles" => ?self.profiles);
            self.assign_user_profiles()?;
        } else {
            info!(&log, "Remove user profiles"; "user" => ?self.user);
            self.remove_user_profiles()?;
        };

        if let Some(_) = self.homedir {
            info!(&log, "Set up home directory and startup files"; "user" => ?self.user, "home directory" => ?self.homedir);
            self.set_up_home_directory_and_startup_files()?;
        }
        Ok(())
    }
}
