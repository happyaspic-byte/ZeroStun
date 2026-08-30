use std::collections::BTreeSet;
use std::path::PathBuf;

use tokio_util::sync::CancellationToken;

use super::{
    BoxFuture, CommandRunner, CommandSpec, ProviderCapabilities, SnapshotHandle, SnapshotProvider,
    SnapshotRequest, PROVIDER_TIMEOUT,
};
use crate::error::{Error, Result};

const ZFS: &str = "/usr/sbin/zfs";
const SNAPSHOT_PREFIX: &str = "zerostun-";
const FS_MOUNT_ROOT: &str = "/run/zerostun/zfs";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZfsTargetKind {
    Filesystem,
    Volume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZfsTargetCapabilities {
    pub kind: ZfsTargetKind,
    pub capabilities: ProviderCapabilities,
    pub mounted_filesystem_source: bool,
    pub block_device_source: bool,
}

#[derive(Debug, Clone)]
pub struct ZfsProvider<R> {
    runner: R,
}

impl<R> ZfsProvider<R> {
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

impl<R: CommandRunner + Clone> ZfsProvider<R> {
    pub async fn probe_target(
        &self,
        target: &str,
        cancel: &CancellationToken,
    ) -> Result<ZfsTargetCapabilities> {
        let target = validate_zfs_dataset(target)?;
        let rows = self.list_datasets(Some(target), cancel).await?;
        let row = rows
            .iter()
            .find(|row| row.name == target)
            .ok_or_else(|| Error::Snapshot(format!("ZFS dataset was not found: {target}")))?;
        Ok(ZfsTargetCapabilities {
            kind: row.kind,
            capabilities: Self::supported_capabilities(),
            mounted_filesystem_source: row.kind == ZfsTargetKind::Filesystem,
            block_device_source: row.kind == ZfsTargetKind::Volume,
        })
    }

    async fn list_datasets(
        &self,
        target: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Vec<DatasetRow>> {
        let mut spec = CommandSpec::new(ZFS)
            .arg("list")
            .arg("-H")
            .arg("-p")
            .arg("-o")
            .arg("name,type,mountpoint")
            .arg("-t")
            .arg("filesystem,volume");
        if let Some(target) = target {
            spec = spec.arg(target);
        }
        let output = self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await?;
        parse_datasets(&output.stdout)
    }

    async fn list_clones(
        &self,
        target: Option<&str>,
        cancel: &CancellationToken,
    ) -> Result<Vec<CloneRow>> {
        let mut spec = CommandSpec::new(ZFS)
            .arg("list")
            .arg("-H")
            .arg("-p")
            .arg("-o")
            .arg("name,type,mounted,origin")
            .arg("-t")
            .arg("filesystem,volume");
        if let Some(target) = target {
            spec = spec.arg(target);
        }
        let output = self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await?;
        parse_clones(&output.stdout)
    }

    async fn list_snapshots(&self, cancel: &CancellationToken) -> Result<Vec<String>> {
        let output = self
            .runner
            .run(
                &CommandSpec::new(ZFS)
                    .arg("list")
                    .arg("-H")
                    .arg("-p")
                    .arg("-o")
                    .arg("name")
                    .arg("-t")
                    .arg("snapshot"),
                PROVIDER_TIMEOUT,
                cancel,
            )
            .await?;
        parse_snapshot_names(&output.stdout)
    }

    async fn run(&self, spec: CommandSpec, cancel: &CancellationToken) -> Result<()> {
        self.runner.run(&spec, PROVIDER_TIMEOUT, cancel).await?;
        Ok(())
    }

    async fn remove_clone(
        &self,
        clone: &str,
        kind: ZfsTargetKind,
        mounted: bool,
        cancel: &CancellationToken,
    ) -> Result<()> {
        if kind == ZfsTargetKind::Filesystem && mounted {
            self.run(CommandSpec::new(ZFS).arg("unmount").arg(clone), cancel)
                .await?;
        }
        self.run(CommandSpec::new(ZFS).arg("destroy").arg(clone), cancel)
            .await
    }

    async fn destroy_snapshot(&self, snapshot: &str, cancel: &CancellationToken) -> Result<()> {
        self.run(CommandSpec::new(ZFS).arg("destroy").arg(snapshot), cancel)
            .await
    }
}

impl<R: CommandRunner + Clone + 'static> SnapshotProvider for ZfsProvider<R> {
    fn probe<'a>(
        &'a self,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ProviderCapabilities>> {
        Box::pin(async move {
            self.list_datasets(None, cancel).await?;
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
            let target = validate_zfs_dataset(&request.target)?;
            let rows = self.list_datasets(Some(target), cancel).await?;
            let row = rows
                .iter()
                .find(|row| row.name == target)
                .ok_or_else(|| Error::Snapshot(format!("ZFS dataset was not found: {target}")))?;
            let suffix = new_suffix()?;
            let snapshot = format!("{target}@{suffix}");
            let clone = clone_name(&snapshot)?;
            let source = source_path(&snapshot, row.kind)?;

            self.run(CommandSpec::new(ZFS).arg("snapshot").arg(&snapshot), cancel)
                .await?;
            let clone_spec = match row.kind {
                ZfsTargetKind::Filesystem => CommandSpec::new(ZFS)
                    .arg("clone")
                    .arg("-o")
                    .arg("readonly=on")
                    .arg("-o")
                    .arg(format!("mountpoint={}", source.display()))
                    .arg(&snapshot)
                    .arg(&clone),
                ZfsTargetKind::Volume => CommandSpec::new(ZFS)
                    .arg("clone")
                    .arg("-o")
                    .arg("readonly=on")
                    .arg(&snapshot)
                    .arg(&clone),
            };
            self.run(clone_spec, cancel).await?;
            if row.kind == ZfsTargetKind::Filesystem {
                self.run(CommandSpec::new(ZFS).arg("mount").arg(&clone), cancel)
                    .await?;
            }

            Ok(SnapshotHandle {
                id: snapshot,
                source,
            })
        })
    }

    fn open_source<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<PathBuf>> {
        Box::pin(async move {
            let expected_kind = validate_handle(handle)?;
            let clone_name = clone_name(&handle.id)?;
            let clones = self.list_clones(Some(&clone_name), cancel).await?;
            let clone = clones
                .iter()
                .find(|clone| clone.name == clone_name && clone.origin == handle.id)
                .ok_or_else(|| {
                    Error::Snapshot(format!("managed ZFS clone was not found: {clone_name}"))
                })?;
            if clone.kind != expected_kind {
                return Err(Error::Snapshot(
                    "ZFS clone type does not match the snapshot handle source semantics"
                        .to_string(),
                ));
            }
            if clone.kind == ZfsTargetKind::Filesystem && !clone.mounted {
                return Err(Error::Snapshot(
                    "managed ZFS filesystem clone is not mounted".to_string(),
                ));
            }
            Ok(handle.source.clone())
        })
    }

    fn cleanup<'a>(
        &'a self,
        handle: &'a SnapshotHandle,
        cancel: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let expected_kind = validate_handle(handle)?;
            let clone_name = clone_name(&handle.id)?;
            let clones = self.list_clones(Some(&clone_name), cancel).await?;
            let clone = clones
                .iter()
                .find(|clone| clone.name == clone_name && clone.origin == handle.id)
                .ok_or_else(|| {
                    Error::Snapshot(
                        "refusing cleanup because the managed ZFS clone was not verified"
                            .to_string(),
                    )
                })?;
            if clone.kind != expected_kind {
                return Err(Error::Snapshot(
                    "refusing cleanup because ZFS clone type does not match the handle".to_string(),
                ));
            }
            self.remove_clone(&clone.name, clone.kind, clone.mounted, cancel)
                .await?;
            self.destroy_snapshot(&handle.id, cancel).await
        })
    }

    fn recover<'a>(&'a self, cancel: &'a CancellationToken) -> BoxFuture<'a, Result<Vec<String>>> {
        Box::pin(async move {
            let mut clones = self.list_clones(None, cancel).await?;
            let mut snapshots = self.list_snapshots(cancel).await?;
            let mut clone_names = BTreeSet::new();
            let mut clone_origins = BTreeSet::new();
            for clone in &clones {
                if !clone_names.insert(clone.name.clone())
                    || !clone_origins.insert(clone.origin.clone())
                {
                    return Err(Error::Snapshot(
                        "duplicate managed ZFS recovery resource detected".to_string(),
                    ));
                }
            }

            clones.sort_by(|left, right| left.origin.cmp(&right.origin));
            clones.reverse();
            let mut recovered = Vec::new();
            for clone in clones {
                self.remove_clone(&clone.name, clone.kind, clone.mounted, cancel)
                    .await?;
                self.destroy_snapshot(&clone.origin, cancel).await?;
                recovered.push(clone.origin);
            }

            snapshots.retain(|snapshot| !clone_origins.contains(snapshot));
            snapshots.sort();
            snapshots.reverse();
            for snapshot in snapshots {
                self.destroy_snapshot(&snapshot, cancel).await?;
                recovered.push(snapshot);
            }
            Ok(recovered)
        })
    }

    fn capabilities(&self) -> ProviderCapabilities {
        Self::supported_capabilities()
    }
}

#[derive(Debug)]
struct DatasetRow {
    name: String,
    kind: ZfsTargetKind,
}

#[derive(Debug)]
struct CloneRow {
    name: String,
    kind: ZfsTargetKind,
    mounted: bool,
    origin: String,
}

fn parse_kind(value: &str) -> Result<ZfsTargetKind> {
    match value {
        "filesystem" => Ok(ZfsTargetKind::Filesystem),
        "volume" => Ok(ZfsTargetKind::Volume),
        _ => Err(Error::Snapshot(format!(
            "unsupported ZFS dataset type in machine output: {value}"
        ))),
    }
}

fn parse_utf8_lines(bytes: &[u8]) -> Result<impl Iterator<Item = &str>> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::Snapshot("ZFS machine output was not UTF-8".to_string()))?;
    Ok(text.lines().filter(|line| !line.is_empty()))
}

fn parse_datasets(bytes: &[u8]) -> Result<Vec<DatasetRow>> {
    parse_utf8_lines(bytes)?
        .map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != 3 {
                return Err(Error::Snapshot(
                    "invalid ZFS dataset machine output".to_string(),
                ));
            }
            validate_zfs_dataset(fields[0])?;
            Ok(DatasetRow {
                name: fields[0].to_string(),
                kind: parse_kind(fields[1])?,
            })
        })
        .collect()
}

fn parse_clones(bytes: &[u8]) -> Result<Vec<CloneRow>> {
    parse_utf8_lines(bytes)?
        .filter_map(|line| {
            let fields: Vec<&str> = line.split('\t').collect();
            if fields.len() != 4 {
                return Some(Err(Error::Snapshot(
                    "invalid ZFS clone machine output".to_string(),
                )));
            }
            if fields[3] == "-" || !fields[3].contains(&format!("@{SNAPSHOT_PREFIX}")) {
                return None;
            }
            Some((|| {
                validate_zfs_dataset(fields[0])?;
                validate_snapshot_id(fields[3])?;
                if clone_name(fields[3])? != fields[0] {
                    return Err(Error::Snapshot(
                        "managed ZFS clone name does not match its origin".to_string(),
                    ));
                }
                let kind = parse_kind(fields[1])?;
                let mounted = match fields[2] {
                    "yes" => true,
                    "no" | "-" => false,
                    value => {
                        return Err(Error::Snapshot(format!(
                            "invalid ZFS mounted state in machine output: {value}"
                        )))
                    }
                };
                if kind == ZfsTargetKind::Volume && mounted {
                    return Err(Error::Snapshot(
                        "ZFS volume clone unexpectedly reports a mounted filesystem".to_string(),
                    ));
                }
                Ok(CloneRow {
                    name: fields[0].to_string(),
                    kind,
                    mounted,
                    origin: fields[3].to_string(),
                })
            })())
        })
        .collect()
}

fn parse_snapshot_names(bytes: &[u8]) -> Result<Vec<String>> {
    parse_utf8_lines(bytes)?
        .filter(|line| line.contains(&format!("@{SNAPSHOT_PREFIX}")))
        .map(|line| {
            validate_snapshot_id(line)?;
            Ok(line.to_string())
        })
        .collect()
}

fn validate_zfs_dataset(value: &str) -> Result<&str> {
    if value.is_empty()
        || value.contains('@')
        || value.contains(',')
        || value.starts_with('/')
        || value.ends_with('/')
        || value.split('/').any(|component| {
            component.is_empty()
                || component == "."
                || component == ".."
                || !component.bytes().all(|byte| {
                    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-' | b':')
                })
        })
    {
        return Err(Error::Snapshot(
            "ZFS target contains unsafe or mixed dataset semantics".to_string(),
        ));
    }
    Ok(value)
}

fn validate_snapshot_id(value: &str) -> Result<(&str, &str)> {
    let (dataset, suffix) = value.split_once('@').ok_or_else(|| {
        Error::Snapshot("ZFS snapshot identifier must contain exactly one @".to_string())
    })?;
    validate_zfs_dataset(dataset)?;
    if suffix.contains('@')
        || !suffix.starts_with(SNAPSHOT_PREFIX)
        || suffix.len() <= SNAPSHOT_PREFIX.len()
        || !suffix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(Error::Snapshot(
            "ZFS snapshot identifier is not ZeroStun-managed".to_string(),
        ));
    }
    Ok((dataset, suffix))
}

fn clone_name(snapshot: &str) -> Result<String> {
    let (dataset, suffix) = validate_snapshot_id(snapshot)?;
    let parent = dataset
        .rsplit_once('/')
        .map_or(dataset, |(parent, _)| parent);
    Ok(format!("{parent}/{suffix}"))
}

fn source_path(snapshot: &str, kind: ZfsTargetKind) -> Result<PathBuf> {
    let clone = clone_name(snapshot)?;
    match kind {
        ZfsTargetKind::Filesystem => Ok(PathBuf::from(format!(
            "{FS_MOUNT_ROOT}/{}",
            snapshot.replace(['/', '@'], "_")
        ))),
        ZfsTargetKind::Volume => Ok(PathBuf::from(format!("/dev/zvol/{clone}"))),
    }
}

fn validate_handle(handle: &SnapshotHandle) -> Result<ZfsTargetKind> {
    validate_snapshot_id(&handle.id)?;
    let fs = source_path(&handle.id, ZfsTargetKind::Filesystem)?;
    let volume = source_path(&handle.id, ZfsTargetKind::Volume)?;
    if handle.source == fs {
        Ok(ZfsTargetKind::Filesystem)
    } else if handle.source == volume {
        Ok(ZfsTargetKind::Volume)
    } else {
        Err(Error::Snapshot(
            "ZFS snapshot handle has a mismatched or unsafe source path".to_string(),
        ))
    }
}

fn new_suffix() -> Result<String> {
    let mut random = [0_u8; 8];
    getrandom::fill(&mut random).map_err(|error| {
        Error::Snapshot(format!(
            "failed to generate ZFS snapshot identifier: {error}"
        ))
    })?;
    Ok(format!("{SNAPSHOT_PREFIX}{}", hex::encode(random)))
}
