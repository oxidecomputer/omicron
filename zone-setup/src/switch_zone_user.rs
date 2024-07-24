// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context};
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

    fn add_new_group_for_user(&self) -> anyhow::Result<()> {
        match get_group_by_name(&self.group) {
            Some(_) => {}
            None => {
                let cmd = std::process::Command::new("groupadd")
                    .arg(&self.group)
                    .output()
                    .with_context(|| {
                        format!("Could not execute `groupadd {}`", self.group)
                    })?;

                // TODO-john add stdout/stderr?
                if !cmd.status.success() {
                    bail!(
                        "Could not add group: {} status: {}",
                        self.group,
                        cmd.status
                    );
                }
            }
        };
        Ok(())
    }

    fn add_new_user(&self) -> anyhow::Result<()> {
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
                    .with_context(|| {
                        format!(
                            "Could not execute `useradd -m -s {} -g {} -c {} {}`",
                            self.shell, self.group, self.gecos, self.user,
                        )
                    })?;

                // TODO-john add stdout/stderr?
                if !cmd.status.success() {
                    bail!(
                        "Could not add user: {} status: {}",
                        self.user,
                        cmd.status
                    );
                }
            }
        };
        Ok(())
    }

    fn enable_passwordless_login(&self) -> anyhow::Result<()> {
        let cmd = std::process::Command::new("passwd")
            .args(["-d", &self.user])
            .output()
            .with_context(|| {
                format!("Could not execute `passwd -d {}`", self.user)
            })?;

        // TODO-john add stdout/stderr?
        if !cmd.status.success() {
            bail!(
                "Could not enable password-less login: {} status: {}",
                self.user,
                cmd.status
            );
        }
        Ok(())
    }

    fn disable_password_based_login(&self) -> anyhow::Result<()> {
        let cmd = std::process::Command::new("passwd")
            .args(["-N", &self.user])
            .output()
            .with_context(|| {
                format!("Could not execute `passwd -N {}`", self.user)
            })?;

        // TODO-john add stdout/stderr?
        if !cmd.status.success() {
            bail!(
                "Could not disable password-based logins: {} status: {}",
                self.user,
                cmd.status
            );
        }
        Ok(())
    }

    fn assign_user_profiles(&self) -> anyhow::Result<()> {
        let Some(ref profiles) = self.profiles else {
            bail!("Profile list must not be empty to assign user profiles",);
        };

        let mut profile_list: String = Default::default();
        for profile in profiles {
            // TODO-john need separator?
            profile_list.push_str(&profile)
        }

        let cmd = std::process::Command::new("usermod")
            .args(["-P", &profile_list, &self.user])
            .output()
            .with_context(|| {
                format!(
                    "Could not execute `usermod -P {} {}`",
                    profile_list, self.user,
                )
            })?;

        // TODO-john add stdout/stderr?
        if !cmd.status.success() {
            bail!(
                "Could not assign user profiles: {} status: {}",
                self.user,
                cmd.status
            );
        }
        Ok(())
    }

    fn remove_user_profiles(&self) -> anyhow::Result<()> {
        let cmd = std::process::Command::new("usermod")
            .args(["-P", "", &self.user])
            .output()
            .with_context(|| {
                format!("Could not execute `usermod -P '' {}`", self.user)
            })?;

        if !cmd.status.success() {
            bail!(
                "Could not remove user profiles: {} status: {}",
                self.user,
                cmd.status
            );
        }
        Ok(())
    }

    fn set_up_home_directory_and_startup_files(&self) -> anyhow::Result<()> {
        let Some(homedir) = &self.homedir else {
            bail!(
                "A home directory must be provided to set up home directory and startup files",
            );
        };

        create_dir_all(&homedir).with_context(|| {
            format!(
                "Could not execute create directory {} and its parents",
                homedir,
            )
        })?;

        let mut home_bashrc = homedir.clone();
        home_bashrc.push_str("/.bashrc");
        copy("/root/.bashrc", &home_bashrc).with_context(|| {
            format!("Could not copy file from /root/.bashrc to {homedir}")
        })?;

        let mut home_profile = homedir.clone();
        home_profile.push_str("/.profile");
        copy("/root/.profile", &home_profile).with_context(|| {
            format!("Could not copy file from /root/.profile to {homedir}")
        })?;

        // TODO-john check for recursive version
        // Not using std::os::unix::fs::chown here because it doesn't support
        // recursive option.
        let cmd = std::process::Command::new("chown")
            .args(["-R", &self.user, &homedir])
            .output()
            .with_context(|| {
                format!("Could not execute `chown -R {} {homedir}`", self.user)
            })?;

        if !cmd.status.success() {
            bail!(
                "Could not change ownership: {} status: {}",
                homedir,
                cmd.status
            );
        }
        Ok(())
    }

    pub fn setup_switch_zone_user(self, log: &Logger) -> anyhow::Result<()> {
        info!(
            log, "Add a new group for the user";
            "group" => &self.group,
            "user" => &self.user,
        );
        self.add_new_group_for_user()?;

        info!(
            log, "Add the user";
            "user" => &self.user,
            "shell" => &self.shell,
            "group" => &self.group,
            "gecos" => &self.gecos,
        );
        self.add_new_user()?;

        // Either enable password-less login (wicket) or disable password-based
        // logins completely (support, which logs in via ssh key).
        if self.nopasswd {
            info!(
                log, "Enable password-less login for user";
                "user" => &self.user,
            );
            self.enable_passwordless_login()?;
        } else {
            info!(
                log, "Disable password-based logins";
                "user" => &self.user,
            );
            self.disable_password_based_login()?;
        };

        if let Some(_) = &self.profiles {
            info!(
                log, "Assign user profiles";
                "user" => &self.user,
                "profiles" => ?self.profiles,
            );
            self.assign_user_profiles()?;
        } else {
            info!(
                log, "Remove user profiles";
                "user" => &self.user,
            );
            self.remove_user_profiles()?;
        };

        if let Some(_) = self.homedir {
            info!(
                log, "Set up home directory and startup files";
                "user" => &self.user,
                "home directory" => ?self.homedir,
            );
            self.set_up_home_directory_and_startup_files()?;
        }

        Ok(())
    }
}
