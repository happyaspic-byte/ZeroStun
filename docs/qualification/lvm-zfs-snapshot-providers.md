# LVM and ZFS Snapshot Provider Qualification

Date: 2026-08-30
Branch: `worktree-zerostun-core`
Verification level: `contract-tested`

## Scope

`LvmProvider` and `ZfsProvider` implement the object-safe `SnapshotProvider`
contract exclusively through `CommandRunner` and `CommandSpec`. No shell is
used. Commands inherit the runner's five-second timeout, cancellation,
32-KiB-per-stream output bound, cleared environment, and secret redaction.

## Contract evidence

- `tests/lvm_contract.rs`: LVM JSON probe, target validation, read-only tagged
  snapshot creation, stable device-mapper path escaping, open verification,
  cleanup, reverse-order stale recovery, every injected lifecycle failure,
  timeout, cancellation, redaction, tampered handle rejection, and ownership
  checks (`zerostun.snapshot` tag plus `lv_attr` snapshot/read-only bits).
- `tests/zfs_contract.rs`: machine-readable probe, filesystem/ZVOL capability
  split, filesystem snapshot/clone/mount lifecycle, ZVOL block-device
  lifecycle, reverse cleanup, stale clone and orphan snapshot recovery, every
  injected lifecycle failure, mixed semantic rejection before mutation,
  timeout, cancellation, redaction, handle validation, and ownership checks
  (`org.zerostun:managed=on`, readonly, and filesystem mountpoint).
- `tests/snapshot_contract.rs`: shared exact-argv runner contract, real process
  timeout/cancellation/output bounds, environment clearing, redaction, and
  object safety.

## Host tooling detection

Only non-destructive availability and listing probes were permitted.

| Provider | Detection | Result | Storage mutation |
| --- | --- | --- | --- |
| LVM | `/usr/sbin/lvs --reportformat json --options vg_name,lv_name,lv_tags,lv_attr` | Binary present; host access is non-root and LVM lock access is denied, so no disposable integration environment is available | None |
| ZFS | `/usr/sbin/zfs` executable check | Binary absent | None |

Because no isolated disposable root storage environment was available, neither
provider is labeled `integration-tested` or `hardware-verified`.

## Safety properties

- Unsupported quiesce and changed-block requirements fail before any command.
- LVM accepts only exact validated `vg/lv` targets. Cleanup requires a
  `zerostun-` identifier, the `zerostun.snapshot` tag, and `lv_attr`
  snapshot/read-only bits on the named volume.
- LVM source paths are derived, not accepted from command output, using LVM's
  stable `/dev/mapper` hyphen escaping.
- ZFS accepts exactly one validated dataset target and rejects snapshot syntax,
  comma-separated mixed targets, traversal, and command punctuation before
  probe or mutation.
- ZFS filesystem and ZVOL sources have distinct typed capabilities and lifecycle
  paths; mounted filesystem clones are unmounted before clone destruction,
  followed by snapshot destruction.
- Recovery lists only resources whose identifiers and ownership relationships
  match ZeroStun's managed prefix and removes them in deterministic reverse
  order. Unmanaged resources are ignored. ZFS additionally requires
  `org.zerostun:managed=on` on clones and snapshots, plus a matching
  filesystem mountpoint for mounted clones.

## Verification results

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --all-targets --all-features --offline -- -D warnings` | PASS |
| `cargo test --lib --bins --tests --all-features --offline -- --test-threads=1` | PASS: 164 passed, 0 failed, 0 ignored across 14 suites (including `lvm_contract` and `zfs_contract`) |
| `cargo build --release --offline` | PASS |
| `cargo audit` | PASS: 182 dependency packages scanned, 0 vulnerabilities reported |

The support matrix in `README.md` intentionally retains `contract-tested`
until a disposable LVM and ZFS integration environment is exercised without
touching host storage.
