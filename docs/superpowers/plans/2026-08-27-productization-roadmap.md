# ZeroStun Productization Roadmap

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the seven independently testable subprojects required by the approved 85/100 productization design.

**Architecture:** Preserve the single-binary Rust architecture while adding bounded subsystems behind narrow interfaces. Each phase ships independently, retains all prior safety gates, and records platform verification as contract-tested, integration-tested, or hardware-verified.

**Tech Stack:** Rust 2021, Tokio, redb, FastCDC, BLAKE3, clap, serde, tracing, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-27-productization-85-design.md`

## Global Constraints

- Never call a platform fully supported before hardware verification.
- Destructive repository operations default to dry-run and require `--apply`.
- Preserve the existing single executable deployment model.
- Never interpolate external command arguments through a shell.
- Credentials never appear in positional arguments, redb, logs, or JSON.
- Every phase uses TDD, fault injection, clippy with `-D warnings`, release build, and RustSec audit.
- A failed phase cannot weaken an earlier safety or integrity contract.

---

## Phase sequence

### Phase 1: Repository lifecycle

Implement the complete plan in `docs/superpowers/plans/2026-08-27-repository-lifecycle.md`.

Acceptance: tombstone delete, deterministic retention, reader leases, journaled mark-and-sweep GC, read-only/apply repair, crash recovery, human/JSON CLI, and fault tests pass.

### Phase 2: Snapshot abstraction

Create `src/snapshot/mod.rs`, `src/snapshot/runner.rs`, and `tests/snapshot_contract.rs`. Define `SnapshotProvider`, `SnapshotHandle`, `ProviderCapabilities`, typed `CommandSpec`, bounded command output, timeout/cancellation, fake runner fixtures, secret redaction, and the reusable contract suite.

Acceptance: fake providers pass probe/create/open/cleanup/recover success and every injected failure; argv is exact and secrets never appear in diagnostics.

### Phase 3: LVM and ZFS

Create `src/snapshot/lvm.rs`, `src/snapshot/zfs.rs`, `tests/lvm_contract.rs`, `tests/zfs_contract.rs`, and protected integration workflows. Implement machine-readable probes, read-only lifecycle, reverse-order cleanup, stale-resource recovery, and explicit capability rejection.

Acceptance: contract suites pass in normal CI; disposable privileged environment performs real create/backup/verify/restore/cleanup before either provider becomes integration-tested.

### Phase 4: Daemon and operations

Create focused modules under `src/daemon/`: `config.rs`, `state.rs`, `scheduler.rs`, `runner.rs`, and `shutdown.rs`. Add daemon/jobs/runs/cancel/metrics CLI surfaces, redb state tables, injected clock, one missed-run catch-up, retry classification, graceful SIGTERM cleanup, systemd unit, and sample config.

Acceptance: restart, duplicate suppression, cancellation, retry, snapshot cleanup, corrupted state, and service smoke tests pass.

### Phase 5: Proxmox and Stratus

Create `src/snapshot/proxmox.rs`, `src/snapshot/stratus.rs`, redacted fixtures, contract tests, and protected real-lab workflows. Separate API transport from lifecycle logic and enforce unsafe FT synchronization refusal.

Acceptance: both providers are contract-tested; each is promoted only after protected hardware suite success.

### Phase 6: Distribution

Add pinned Rust toolchain, GNU/musl build matrix, release archives, SHA-256 checksums, CycloneDX SBOM, shell completions, man page, systemd/sample config packaging, and license/source policy checks. Release only on an existing semver tag.

Acceptance: an unpublished test tag build produces installable archives whose checksums, SBOM, completions, man page, and service files validate.

### Phase 7: Real-lab and qualification

Add protected workflows and scripts for the 24-hour soak, process-kill, disk-full, permission loss, metadata corruption, timeout, cleanup failure, restart, RSS, throughput, limiter error, queue saturation, and host-latency delta.

Acceptance: all available hardware suites and the complete fault matrix pass; each platform badge states its actual verification level; no confirmed data-loss finding remains.

## Completion gate

Run the scorecard from the design after every phase. Product completeness reaches 85/100 only after lifecycle, daemon, distribution, and reliability pass real end-to-end tests. “All platforms complete” additionally requires hardware verification of LVM, ZFS, Proxmox, everRun, and ztC.
