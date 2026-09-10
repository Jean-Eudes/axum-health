use futures::FutureExt;
use futures::future::{BoxFuture, join_all};
use serde::Serialize;
use std::{io, path::Path};

use super::{ComponentHealth, HealthCheck};

#[derive(Debug, Clone, Serialize)]
struct DiskDetails {
    disks: Vec<DiskCheck>,
}

pub(super) struct DiskHealthCheck {
    disks: Vec<crate::DiskConfig>,
}

impl DiskHealthCheck {
    pub(super) fn new(config: &[crate::DiskConfig]) -> Self {
        Self {
            disks: config.to_vec(),
        }
    }
}

impl HealthCheck for DiskHealthCheck {
    fn check(&self) -> BoxFuture<'static, ComponentHealth> {
        let disks = self.disks.clone();

        async move {
            let results = join_all(disks.into_iter().map(|disk| async move {
                match disk_usage_percent_remaining(&disk.path) {
                    Ok((total_bytes, available_bytes, percent_remaining)) => {
                        let status = if percent_remaining <= disk.threshold {
                            "DOWN"
                        } else {
                            "UP"
                        };

                        DiskCheck {
                            path: disk.path.display().to_string(),
                            status,
                            total_bytes: Some(total_bytes),
                            available_bytes: Some(available_bytes),
                            percent_remaining: Some(percent_remaining),
                            threshold: disk.threshold,
                            error: None,
                        }
                    }
                    Err(err) => DiskCheck {
                        path: disk.path.display().to_string(),
                        status: "DOWN",
                        total_bytes: None,
                        available_bytes: None,
                        percent_remaining: None,
                        threshold: disk.threshold,
                        error: Some(err.to_string()),
                    },
                }
            }))
            .await;

            ComponentHealth {
                status: if results.iter().all(|check| check.status == "UP") {
                    "UP"
                } else {
                    "DOWN"
                },
                details: serde_json::to_value(DiskDetails { disks: results })
                    .expect("DiskDetails is always serializable"),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Clone, Serialize)]
struct DiskCheck {
    path: String,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    available_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    percent_remaining: Option<u8>,
    threshold: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn disk_usage_percent_remaining(path: &Path) -> io::Result<(u64, u64, u8)> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains nul byte"))?;

    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    let stat = unsafe { stat.assume_init() };
    let block_size = if stat.f_frsize != 0 {
        stat.f_frsize
    } else {
        stat.f_bsize
    };
    let total_bytes = stat.f_blocks.saturating_mul(block_size);
    let available_bytes = stat.f_bavail.saturating_mul(block_size);

    if total_bytes == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "filesystem reports zero total bytes",
        ));
    }

    let percent_remaining = ((available_bytes.saturating_mul(100)) / total_bytes) as u8;
    Ok((total_bytes, available_bytes, percent_remaining))
}
