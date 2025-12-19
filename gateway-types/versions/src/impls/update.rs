// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

use dropshot::HttpError;
use gateway_messages::UpdateStatus;

use crate::latest::update::{
    SpComponentResetError, SpUpdateStatus, UpdatePreparationProgress,
};

impl From<HttpError> for SpComponentResetError {
    fn from(err: HttpError) -> Self {
        Self::Other {
            message: err.external_message,
            error_code: err.error_code,
            internal_message: err.internal_message,
            status: err.status_code,
        }
    }
}

impl From<UpdateStatus> for SpUpdateStatus {
    fn from(status: UpdateStatus) -> Self {
        match status {
            UpdateStatus::None => Self::None,
            UpdateStatus::Preparing(status) => Self::Preparing {
                id: status.id.into(),
                progress: status.progress.map(Into::into),
            },
            UpdateStatus::SpUpdateAuxFlashChckScan {
                id, total_size, ..
            } => Self::InProgress {
                id: id.into(),
                bytes_received: 0,
                total_bytes: total_size,
            },
            UpdateStatus::InProgress(status) => Self::InProgress {
                id: status.id.into(),
                bytes_received: status.bytes_received,
                total_bytes: status.total_size,
            },
            UpdateStatus::Complete(id) => Self::Complete { id: id.into() },
            UpdateStatus::Aborted(id) => Self::Aborted { id: id.into() },
            UpdateStatus::Failed { id, code } => {
                Self::Failed { id: id.into(), code }
            }
            UpdateStatus::RotError { id, error } => {
                Self::RotError { id: id.into(), message: format!("{error:?}") }
            }
        }
    }
}

impl From<gateway_messages::UpdatePreparationProgress>
    for UpdatePreparationProgress
{
    fn from(progress: gateway_messages::UpdatePreparationProgress) -> Self {
        Self { current: progress.current, total: progress.total }
    }
}
