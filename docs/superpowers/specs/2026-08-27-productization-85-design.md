# ZeroStun Productization 85 Design

Date: 2026-08-27
Status: Approved
Last scorecard: 2026-08-30

Current evidence-based score is **40/100**:

| Area | Weight | Earned | Evidence |
| --- | ---: | ---: | --- |
| Core backup engine | 25 | 25 | Round-trip, integrity, bounded pipeline, CI |
| Repository lifecycle | 15 | 15 | `docs/qualification/repository-lifecycle.md` |
| Snapshot adapters | 25 | 0 | Not implemented |
| Operations | 15 | 0 | No daemon |
| Distribution | 10 | 0 | No release archives/SBOM |
| Reliability/security | 10 | 0 | Fault tests exist for lifecycle; product gate still requires soak/release/audit completeness |

Lifecycle 15 is earned only for the local repository product. Platforms remain unverified. 85/100 still requires daemon, distribution, and reliability end-to-end.

## 1. Objective

Raise ZeroStun from a portable local backup-engine MVP to a productized backup
system scoring at least 85/100 against its stated edge and fault-tolerant
mission. Product completeness must be based on executable evidence rather than
code volume or mocked integrations.

The target covers six areas:

| Area | Weight | Target evidence |
| --- | ---: | --- |
| Core backup engine | 25 | Existing round-trip, integrity, bounded pipeline, CI |
| Repository lifecycle | 15 | Delete, retention, GC, repair, crash recovery |
| Snapshot adapters | 25 | Common contracts plus LVM, ZFS, Proxmox, everRun/ztC |
| Operations | 15 | Daemon, scheduler, retry, cancellation, systemd |
| Distribution | 10 | GNU/musl archives, checksums, SBOM, release workflow |
| Reliability/security | 10 | Fault injection, migration, fuzz/property, audits |

Platform verification levels are explicit:

- `contract-tested`: adapter passes the fake-runner contract suite.
- `integration-tested`: adapter passes against a disposable service or VM.
- `hardware-verified`: adapter passes on representative hardware.

No platform is called fully supported before hardware verification. The current
machine has no LVM, ZFS, Proxmox, or Stratus tooling available. LVM/ZFS and
Stratus labs exist but usable credentials are not yet available. Proxmox lab
availability is not confirmed.

## 2. Delivery decomposition

This is a sequence of seven subprojects, each with its own implementation and
verification cycle:

1. Repository lifecycle: delete, prune, GC, repair.
2. Snapshot abstraction and provider contract suite.
3. LVM and ZFS providers.
4. Daemon, scheduler, status, cancellation, systemd.
5. Proxmox and everRun/ztC providers.
6. Release archives, SBOM, checksums, supply-chain checks.
7. Real-lab, soak, performance, and host-impact validation.

A failed subproject does not justify weakening another subproject's safety
contract.

## 3. Repository lifecycle

### 3.1 Delete

`delete <backup-id>` writes a tombstone in redb. It does not delete chunks.
Deleted backups are hidden from normal list, inspect, verify, and restore but
remain recoverable until GC finalizes the tombstone.

All destructive commands default to dry-run. Mutation requires `--apply`.
Human and JSON output describe the exact planned mutations.

### 3.2 Retention

`prune` evaluates a retention policy and produces a deterministic deletion
plan. Supported selectors:

- keep the newest N backups,
- keep daily backups for N days,
- keep weekly backups for N weeks,
- keep monthly backups for N months,
- protect explicit backup IDs.

Policy evaluation uses UTC timestamps. A backup selected by multiple rules is
kept if any rule retains it. `prune --apply` records tombstones only.

### 3.3 Garbage collection

GC runs under the exclusive repository writer lock:

1. Read all completed, non-deleted manifests and mark their content IDs.
2. Scan the chunk tree.
3. Rename unmarked chunks into `trash/<gc-id>` on the same filesystem.
4. Commit a GC journal containing the moved paths and target state.
5. Remove trash files and mark the journal complete.

A crash before the journal commit is recoverable by scanning trash. A crash
after journal commit resumes deletion. Re-running the same GC operation is
idempotent.

Reader leases prevent GC while verify or restore is active. A stale lease is
recoverable by process identity and timeout checks, never by elapsed time alone.

### 3.4 Repair

`repair` is read-only by default. It checks:

- redb completed index versus encoded manifests,
- manifests versus chunk presence,
- chunk header validity and content integrity,
- tombstone and GC journal consistency,
- orphan temporary and trash entries.

`repair --apply` may rebuild the redb index from valid manifests and resume or
rollback a GC journal. It never fabricates missing data or rewrites a corrupt
chunk.

## 4. Snapshot provider architecture

### 4.1 Interface

An internal `SnapshotProvider` contract defines:

- `probe`: validate tools, API, permissions, target, and capability set.
- `create`: create a read-only snapshot with a ZeroStun-generated identifier.
- `open_source`: return the stable block or file source.
- `cleanup`: remove the snapshot after success, failure, or cancellation.
- `recover`: find and clean snapshots left by interrupted runs.
- `capabilities`: report consistency, read-only enforcement, quiesce, and
  changed-block support.

Providers are built into the single binary. External plugin processes and
arbitrary shell hooks are excluded.

### 4.2 Command runner boundary

All process execution goes through a typed command runner:

- executable and each argument are separate values,
- no shell command interpolation,
- stdout/stderr have size limits,
- every command has a timeout and cancellation token,
- secrets are redacted from logs and structured errors,
- exit status, stdout schema, and cleanup behavior are classified.

A fake runner records argv and returns fixture responses. The same provider
contract suite runs with fake and real runners.

### 4.3 LVM

- Probe with `lvs --reportformat json`.
- Create a read-only snapshot using `lvcreate --snapshot --permission r`.
- Return the snapshot LV path as the backup source.
- Remove using `lvremove`.
- Recover snapshots whose ZeroStun metadata references no active run.

### 4.4 ZFS

ZFS filesystem and ZVOL targets expose separate capabilities:

- probe with machine-readable `zfs list`,
- create with `zfs snapshot`,
- provide a stable clone, mounted snapshot path, or ZVOL device as appropriate,
- remove clone then snapshot in reverse order,
- reject unsupported mixed dataset semantics before creating anything.

### 4.5 Proxmox

The provider uses an API token. It probes VM configuration and storage, creates
a VM snapshot when the storage supports it, resolves disk volumes, and exports
a stable source. Contract tests use recorded schemas with redacted fixtures.
It remains contract-tested until a real Proxmox environment passes the real
suite.

### 4.6 everRun and ztC

The Stratus provider separates API transport from lifecycle logic. Probe checks
node-pair state and FT synchronization health. Snapshot creation is refused
during an unsafe synchronization state. Fixtures validate supported API
schemas. Hardware verification requires the actual appliance and credentials.

### 4.7 Credential handling

Credentials come from environment variables or an explicitly configured 0600
file. They are never accepted as positional arguments, persisted in redb, or
included in logs or JSON reports.

## 5. Daemon and scheduler

The existing binary gains a `daemon` subcommand; no second runtime binary is
introduced.

### 5.1 Configuration

TOML jobs define:

- source and provider,
- repository,
- cron schedule,
- explicit UTC or IANA timezone,
- retention policy,
- read/write limits,
- worker and queue limits,
- retry policy.

CLI values override configuration only for one-shot commands. Daemon job
configuration is validated before the service starts accepting work.

### 5.2 State machine

redb stores jobs and runs with states:

`queued`, `running`, `succeeded`, `failed`, `cancelled`, `recovering`.

A job cannot run concurrently with itself. Restart recovery moves interrupted
runs to `recovering`, cleans snapshots, and then classifies the run as failed
or retryable.

### 5.3 Scheduling and retry

Cron scheduling uses an injected clock for tests. Catch-up behavior runs at
most one missed occurrence per job after restart. Retry uses exponential
backoff and bounded jitter for transient API or I/O errors only. Configuration,
integrity, and unsupported-capability failures do not retry.

### 5.4 Shutdown and cancellation

SIGTERM stops admission, cancels running stages, cleans the snapshot, commits
run state, and exits within a configured deadline. A failure to clean the
snapshot creates a recoverable cleanup record and a failed run.

### 5.5 Operator surface

Commands:

- `daemon status`,
- `jobs list`,
- `runs list`,
- `cancel <run-id>`,
- `metrics --json`.

Structured tracing includes job ID, run ID, provider, bytes, duration, dedupe,
limiter wait, and cleanup outcome.

A hardened systemd unit uses a dedicated or dynamic user, `NoNewPrivileges`,
and `ProtectSystem`. Device privileges are explicit opt-ins for LVM/ZFS.

## 6. Distribution and supply chain

Semver tag builds produce:

- Linux x86_64 GNU archive,
- Linux x86_64 musl archive when dependencies support static linking,
- SHA-256 checksum file,
- CycloneDX SBOM,
- shell completions and man page,
- systemd unit and sample configuration.

The release workflow does not create a tag automatically. Tagging and public
release publication remain explicit release actions.

The Rust toolchain and dependency lockfile are pinned. CI runs rustfmt, clippy,
tests, release build, RustSec audit, and license/source policy checks.

## 7. Verification strategy

### 7.1 Every push

- unit and property tests,
- repository fault tests,
- fake-runner provider contract tests,
- CLI integration tests,
- format, clippy, audit, license checks.

### 7.2 Privileged integration

LVM and ZFS run in disposable VMs or protected self-hosted runners. Tests must
create, use, verify, restore, and remove real snapshots. They also inject create,
open, backup, and cleanup failures.

### 7.3 Protected platform environments

Proxmox and Stratus suites use protected environments with scoped credentials.
They verify probe, snapshot lifecycle, backup, restore, cleanup, cancellation,
and secret redaction.

### 7.4 Soak and fault matrix

A 24-hour soak repeatedly executes backup, verify, delete, GC, and restore.
Fault injection covers process termination, disk full, permission loss,
corrupt metadata, network timeout, snapshot cleanup failure, and daemon restart.

### 7.5 Performance and host impact

Benchmarks record CPU, RSS, throughput, compression ratio, rate-limit error,
and queue saturation. Host-impact validation compares application p99 latency
at baseline and during backup. Zero-Stun does not claim zero host stun until a
specific platform and workload satisfy a published acceptance threshold.

## 8. Product acceptance

The product reaches 85/100 only when:

- lifecycle, daemon, release, and reliability suites pass real end-to-end tests,
- no confirmed data-loss finding remains,
- restored output is byte-identical,
- restart, repair, migration, and GC fault tests pass,
- security, advisory, and license gates are clean,
- each platform states its actual verification level,
- unsupported or unverified integrations are not marketed as complete.

“All platforms complete” additionally requires hardware verification for LVM,
ZFS, Proxmox, everRun, and ztC. Missing credentials are a test-lab blocker, not
a reason to lower the gate.
