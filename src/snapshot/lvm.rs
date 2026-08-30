use std::path::PathBuf;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use super::{
    BoxFuture, CommandRunner, CommandSpec, ProviderCapabilities, SnapshotHandle, SnapshotProvider,
    SnapshotRequest, PROVIDER_TIMEOUT,
};
use crate::error::{Error, Result};

const LVS: &str = "/usr/sbin/lvs";
const LVCREATE: &str = "/usr/sbin/lvcreate";
const LVREMOVE: &str = "/usr/sbin/lvremove";
const MANAGED_TAG: &str = "zerostun.snapshot";
const SNAPSHOT_PREFIX: &str = "zerostun-";

#[derive(Debug, Clone)]
pub struct LvmProvider<R> {
    runner: R,
}

impl<R> LvmProvider<R> {
    pub fn new(runner: R) -> Self {
        Self { runner }
    }

    fn supported_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            crash_consistent: true,
            read_only: true,
            quiesce: false,
            changed_block: false,
        }
    }
}

impl<R: CommandRunner + Clone> LvmProvider<R> {
    async fn list(&self, target: Option<&str>, cancel: &CancellationToken) -> Result<Vec<LvRow>> {
        let mut spec = CommandSpec::new(LVS)
            .arg("--reportformat")
            .arg("json")
            .arg("--options")
            .arg("vg_name,lv_name,lv_tags,lv_attr");
        if let Some(target) = target {
            spec = spec.arg(target);
        }
        let output = self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await?;
        parse_lvs(&output.stdout)
    }

    async fn remove(&self, id: &str, cancel: &CancellationToken) -> Result<()> {
        self.runner
            .run(
                &CommandSpec::new(LVREMOVE).arg("--force").arg(id),
                PROVIDER_TIMEOUT,
                cancel,
            )
            .await?;
        Ok(())
    }

    async fn require_managed_snapshot(
        &self,
        id: &str,
        cancel: &CancellationToken,
    ) -> Result<LvRow> {
        validate_managed_id(id)?;
        let rows = self.list(Some(id), cancel).await?;
        let matching: Vec<LvRow> = rows.into_iter().filter(|row| row.id() == id).collect();
        if matching.len() != 1 {
            return Err(Error::Snapshot(format!(
                "managed LVM snapshot was not found: {id}"
            )));
        }
        let row = matching
            .into_iter()
            .next()
            .ok_or_else(|| Error::Snapshot(format!("managed LVM snapshot was not found: {id}")))?;
        if !row.is_managed() {
            return Err(Error::Snapshot(
                "refusing to use an LVM volume not tagged as ZeroStun-managed".to_string(),
            ));
        }
        if !row.is_read_only_snapshot() {
            return Err(Error::Snapshot(
                "LVM snapshot is not a verified read-only snapshot".to_string(),
            ));
        }
        Ok(row)
    }
}

impl<R: CommandRunner + Clone + 'static> SnapshotProvider for LvmProvider<R> {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>> {
        Box::pin(async move {
            self.list(None, cancel).await?;
            Ok(Self::supported_capabilities())
        })
    }

    fn create<'a>(
        &'a self,
        request: &'a SnapshotRequest,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<SnapshotHandle>> {
        Box::pin(async move {
            request.validate_requirements(Self::supported_capabilities())?;
            let target = validate_lvm_target(&request.target)?;
            let (vg, _) = split_lvm_id(target)?;
            let rows = self.list(Some(target), cancel).await?;
            if !rows.iter().any(|row| row.id() == target) {
                return Err(Error::Snapshot(format!(
                    "LVM logical volume was not found: {target}"
                )));
            }

            let name = new_managed_name()?;
            let id = format!("{vg}/{name}");
            let created = self
                .runner
                .run(
                    &CommandSpec::new(LVCREATE)
                        .arg("--snapshot")
                        .arg("--permission")
                        .arg("r")
                        .arg("--extents")
                        .arg("20%ORIGIN")
                        .arg("--addtag")
                        .arg(MANAGED_TAG)
                        .arg("--name")
                        .arg(&name)
                        .arg(target),
                    PROVIDER_TIMEOUT,
                    cancel,
                )
                .await;
            if created.is_err() {
                let cleanup_cancel = CancellationToken::new();
                if self
                    .require_managed_snapshot(&id, &cleanup_cancel)
                    .await
                    .is_ok()
                {
                    let _ = self.remove(&id, &cleanup_cancel).await;
                }
                created?;
            }

            Ok(SnapshotHandle {
                source: mapper_path(&id)?,
                id,
            })
        })
    }

    fn open_source<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            let expected = mapper_path(&handle.id)?;
            if handle.source != expected {
                return Err(Error::Snapshot(
                    "LVM snapshot handle source does not match its stable mapper path".to_string(),
                ));
            }
            self.require_managed_snapshot(&handle.id, cancel).await?;
            Ok(expected)
        })
    }

    fn cleanup<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if handle.source != mapper_path(&handle.id)? {
                return Err(Error::Snapshot(
                    "LVM snapshot handle source does not match its stable mapper path".to_string(),
                ));
            }
            self.require_managed_snapshot(&handle.id, cancel).await?;
            self.remove(&handle.id, cancel).await
        })
    }

    fn recover<'a>(&'a self, cancel: &'a CancellationToken) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let mut ids: Vec<String> = self
                .list(None, cancel)
                .await?
                .into_iter()
                .filter(|row| row.is_managed() && row.is_read_only_snapshot())
                .map(|row| row.id())
                .collect();
            ids.sort();
            ids.reverse();
            for id in &ids {
                self.remove(id, cancel).await?;
            }
            Ok(ids)
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::supported_capabilities()
    }
}

#[derive(Debug, Deserialize)]
struct LvsOutput {
    report: Vec<LvsReport>,
    #[serde(default)]
    log: Vec<LvsLogEntry>,
}

#[derive(Debug, Deserialize)]
struct LvsReport {
    #[serde(default)]
    lv: Vec<LvRow>,
}

#[derive(Debug, Deserialize)]
struct LvsLogEntry {
    #[serde(default)]
    log_type: String,
    #[serde(default)]
    log_message: String,
}

#[derive(Debug, Deserialize)]
struct LvRow {
    vg_name: String,
    lv_name: String,
    #[serde(default)]
    lv_tags: String,
    #[serde(default)]
    lv_attr: String,
}

impl LvRow {
    fn id(&self) -> String {
        format!("{}/{}", self.vg_name, self.lv_name)
    }

    fn is_managed(&self) -> bool {
        self.lv_name.starts_with(SNAPSHOT_PREFIX)
            && self.lv_tags.split(',').any(|tag| tag == MANAGED_TAG)
            && validate_component(&self.vg_name).is_ok()
            && validate_component(&self.lv_name).is_ok()
    }

    fn is_read_only_snapshot(&self) -> bool {
        let attr = self.lv_attr.as_bytes();
        attr.first().copied() == Some(b's') && attr.get(1).copied() == Some(b'r')
    }
}

fn parse_lvs(bytes: &[u8]) -> Result<Vec<LvRow>> {
    let parsed: LvsOutput = serde_json::from_slice(bytes)
        .map_err(|error| Error::Snapshot(format!("invalid lvs JSON response: {error}")))?;
    if let Some(entry) = parsed.log.iter().find(|entry| entry.log_type == "error") {
        return Err(Error::Snapshot(format!(
            "lvs reported a processing error: {}",
            entry.log_message
        )));
    }
    Ok(parsed
        .report
        .into_iter()
        .flat_map(|report| report.lv)
        .collect())
}

fn validate_component(value: &str) -> Result<&str> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'_' | b'.' | b'-'))
    {
        return Err(Error::Snapshot(
            "LVM target contains an unsafe volume-group or logical-volume name".to_string(),
        ));
    }
    Ok(value)
}

fn split_lvm_id(id: &str) -> Result<(&str, &str)> {
    let (vg, lv) = id
        .split_once('/')
        .ok_or_else(|| Error::Snapshot("LVM target must use the exact vg/lv form".to_string()))?;
    if lv.contains('/') {
        return Err(Error::Snapshot(
            "LVM target must use the exact vg/lv form".to_string(),
        ));
    }
    Ok((validate_component(vg)?, validate_component(lv)?))
}

fn validate_lvm_target(target: &str) -> Result<&str> {
    split_lvm_id(target)?;
    Ok(target)
}

fn validate_managed_id(id: &str) -> Result<()> {
    let (_, lv) = split_lvm_id(id)?;
    if !lv.starts_with(SNAPSHOT_PREFIX) {
        return Err(Error::Snapshot(
            "LVM snapshot identifier is not ZeroStun-managed".to_string(),
        ));
    }
    Ok(())
}

fn mapper_path(id: &str) -> Result<PathBuf> {
    let (vg, lv) = split_lvm_id(id)?;
    let escape = |value: &str| value.replace('-', "--");
    Ok(PathBuf::from(format!(
        "/dev/mapper/{}-{}",
        escape(vg),
        escape(lv)
    )))
}

fn new_managed_name() -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Snapshot(format!(
            "failed to generate LVM snapshot identifier: {error}"
        ))
    })?;
    Ok(format!("{SNAPSHOT_PREFIX}{}", hex::encode(random)))
}
