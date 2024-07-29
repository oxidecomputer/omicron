// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use anyhow::{bail, Context};
use camino::{Utf8Path, Utf8PathBuf};
use illumos_utils::{execute, ExecutionError};
use slog::{info, Logger};
use std::fs::{copy, create_dir_all};
use uzers::{get_group_by_name, get_user_by_name};

// User information for the switch zone
pub struct SwitchZoneUser {
    user: String,
    group: String,
    gecos: String,
    nopasswd: bool,
    homedir: Option<Utf8PathBuf>,
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

    pub fn with_homedir(mut self, homedir: Utf8PathBuf) -> Self {
        self.homedir = Some(homedir);
        self
    }

    pub fn with_profiles(mut self, profiles: Vec<String>) -> Self {
        self.profiles = Some(profiles);
        self
    }

    fn add_new_group_for_user(&self) -> Result<(), ExecutionError> {
        if get_group_by_name(&self.group).is_none() {
            execute(
                &mut std::process::Command::new("groupadd").arg(&self.group),
            )?;
        }
        Ok(())
    }

    fn add_new_user(&self) -> Result<(), ExecutionError> {
        if get_user_by_name(&self.user).is_none() {
            execute(&mut std::process::Command::new("useradd").args([
                "-m",
                "-s",
                &self.shell,
                "-g",
                &self.group,
                "-c",
                &self.gecos,
                &self.user,
            ]))?;
        }
        Ok(())
    }

    fn enable_passwordless_login(&self) -> Result<(), ExecutionError> {
        execute(
            &mut std::process::Command::new("passwd").args(["-d", &self.user]),
        )?;
        Ok(())
    }

    fn disable_password_based_login(&self) -> Result<(), ExecutionError> {
        execute(
            &mut std::process::Command::new("passwd").args(["-N", &self.user]),
        )?;
        Ok(())
    }

    fn assign_user_profiles(
        &self,
        profiles: &[String],
    ) -> Result<(), ExecutionError> {
        let profiles = profiles.join(",");

        execute(
            &mut std::process::Command::new("usermod")
                .args(["-P", &profiles, &self.user]),
        )?;
        Ok(())
    }

    fn set_up_home_directory_and_startup_files(
        &self,
        homedir: &Utf8Path,
    ) -> anyhow::Result<()> {
        create_dir_all(&homedir).with_context(|| {
            format!(
                "Could not execute create directory {} and its parents",
                homedir,
            )
        })?;

        let home_bashrc = homedir.join(".bashrc");
        copy("/root/.bashrc", &home_bashrc).with_context(|| {
            format!("Could not copy file from /root/.bashrc to {homedir}")
        })?;

        let home_profile = homedir.join(".profile");
        copy("/root/.profile", &home_profile).with_context(|| {
            format!("Could not copy file from /root/.profile to {homedir}")
        })?;

        // Not using std::os::unix::fs::chown here because it doesn't support
        // recursive option.
        let cmd = std::process::Command::new("chown")
            .args(["-R", &self.user, homedir.as_str()])
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
        self.add_new_group_for_user().context("failed to create group")?;

        info!(
            log, "Add the user";
            "user" => &self.user,
            "shell" => &self.shell,
            "group" => &self.group,
            "gecos" => &self.gecos,
        );
        self.add_new_user().context("failed to create user")?;

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

        if let Some(profiles) = &self.profiles {
            info!(
                log, "Assign user profiles";
                "user" => &self.user,
                "profiles" => ?profiles,
            );
            self.assign_user_profiles(profiles)?;
        } else {
            info!(
                log, "Remove user profiles";
                "user" => &self.user,
            );
            self.assign_user_profiles(&[])?;
        };

        if let Some(homedir) = &self.homedir {
            info!(
                log, "Set up home directory and startup files";
                "user" => &self.user,
                "home directory" => ?homedir,
            );
            self.set_up_home_directory_and_startup_files(homedir)?;
        }

        Ok(())
    }
}
