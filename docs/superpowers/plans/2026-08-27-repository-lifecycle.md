# Repository Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add safe backup deletion, deterministic retention, reader leases, journaled garbage collection, and repository repair to ZeroStun.

**Architecture:** Keep redb authoritative for completed and tombstoned backup metadata. Implement destructive operations as immutable plans that default to dry-run, then apply plans under the writer lock. GC moves unreferenced chunks to same-filesystem trash before journaled deletion; repair reports facts separately from mutations.

**Tech Stack:** Rust 2021, redb 4.2, serde/serde_json, clap 4.6, Tokio, existing repository and manifest formats.

**Spec:** `docs/superpowers/specs/2026-08-27-productization-85-design.md`

## Global Constraints

- Delete, prune, GC, and repair default to dry-run; mutation requires `--apply`.
- Completed backup bytes in `TABLE_BACKUPS` remain immutable.
- Tombstones hide backups from list, inspect, verify, and restore but do not remove chunks.
- GC may delete only a chunk absent from every completed non-tombstoned manifest.
- GC must refuse while an active reader lease exists.
- Missing or corrupt data is reported, never fabricated.
- JSON plans are stable serde structures, not formatted human strings.
- Every mutation runs under the exclusive repository writer lock.

## File structure

- Create `src/lifecycle/mod.rs`: shared plan and result types.
- Create `src/lifecycle/delete.rs`: tombstone planning and apply logic.
- Create `src/lifecycle/retention.rs`: pure UTC retention evaluation.
- Create `src/lifecycle/lease.rs`: reader lease acquisition, release, and validation.
- Create `src/lifecycle/gc.rs`: mark, trash journal, apply, resume.
- Create `src/lifecycle/repair.rs`: repository inspection and index repair.
- Modify `src/repository.rs`: lifecycle tables and low-level transactional methods.
- Modify `src/engine.rs`: reader leases around verify and restore.
- Modify `src/main.rs`: delete, prune, gc, repair command surfaces.
- Modify `src/error.rs`: lifecycle-specific classified errors and exit codes.
- Modify `src/lib.rs`: lifecycle exports.
- Create `tests/lifecycle_delete.rs`.
- Create `tests/lifecycle_retention.rs`.
- Create `tests/lifecycle_gc.rs`.
- Create `tests/lifecycle_repair.rs`.
- Create `tests/lifecycle_cli.rs`.
- Modify `README.md` and `docs/repository-format.md`.

---

### Task 1: Tombstone data model and visibility

**Files:**
- Create: `src/lifecycle/mod.rs`
- Create: `src/lifecycle/delete.rs`
- Modify: `src/repository.rs`
- Modify: `src/error.rs`
- Modify: `src/lib.rs`
- Test: `tests/lifecycle_delete.rs`

**Interfaces:**
- Produces: `DeletePlan { backup_id: String, already_deleted: bool }`.
- Produces: `DeleteResult { backup_id: String, tombstoned: bool }`.
- Produces: `Repository::plan_delete(&self, backup_id: &str) -> Result<DeletePlan>`.
- Produces: `Repository::apply_delete(&self, plan: &DeletePlan) -> Result<DeleteResult>`.
- Produces: `Repository::is_tombstoned(&self, backup_id: &str) -> Result<bool>`.
- Changes: `load_manifest`, `list_backups`, and `list_backup_summaries` hide tombstoned backups.

- [ ] **Step 1: Write failing tombstone visibility tests**

```rust
#[tokio::test]
async fn tombstone_hides_backup_without_deleting_chunks() {
    let fixture = backup_fixture().await;
    let chunk_paths = fixture.chunk_paths();
    let plan = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    assert!(!plan.already_deleted);
    let result = fixture.repo.apply_delete(&plan).unwrap();
    assert!(result.tombstoned);
    assert!(fixture.repo.load_manifest(&fixture.backup_id).is_err());
    assert!(!fixture.repo.list_backups().unwrap().contains(&fixture.backup_id));
    assert!(chunk_paths.iter().all(|path| path.exists()));
}

#[tokio::test]
async fn delete_is_idempotent() {
    let fixture = backup_fixture().await;
    let first = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    fixture.repo.apply_delete(&first).unwrap();
    let second = fixture.repo.plan_delete(&fixture.backup_id).unwrap();
    assert!(second.already_deleted);
    let result = fixture.repo.apply_delete(&second).unwrap();
    assert!(!result.tombstoned);
}
```

- [ ] **Step 2: Run tests and verify RED**

Run: `cargo test --test lifecycle_delete --offline`

Expected: compile failure because `plan_delete`, `apply_delete`, and lifecycle types do not exist.

- [ ] **Step 3: Define lifecycle types and redb tombstone table**

```rust
pub const TOMBSTONES: TableDefinition<&str, u64> = TableDefinition::new("tombstones");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeletePlan {
    pub backup_id: String,
    pub already_deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteResult {
    pub backup_id: String,
    pub tombstoned: bool,
}
```

Initialize the table in `Repository::init`. `apply_delete` must acquire no nested lock; its caller owns the writer lock in CLI orchestration. Use a redb write transaction and store the tombstone timestamp in UTC milliseconds.

- [ ] **Step 4: Enforce tombstone visibility**

Add an internal `load_manifest_including_deleted` method. Public `load_manifest` checks `is_tombstoned` first and returns `Error::BackupDeleted(backup_id)` when deleted. Listing skips tombstoned keys.

- [ ] **Step 5: Run focused and regression tests**

Run:

```bash
cargo test --test lifecycle_delete --offline
cargo test --all-targets --all-features --offline
```

Expected: all tests pass; chunks still exist after tombstone.

- [ ] **Step 6: Commit**

```bash
git add src/lifecycle src/repository.rs src/error.rs src/lib.rs tests/lifecycle_delete.rs
git commit -m "feat: add tombstone-based backup deletion"
```

---

### Task 2: Deterministic retention planning

**Files:**
- Create: `src/lifecycle/retention.rs`
- Modify: `src/lifecycle/mod.rs`
- Test: `tests/lifecycle_retention.rs`

**Interfaces:**
- Consumes: `BackupSummaryItem` from `src/repository.rs`.
- Produces: `RetentionPolicy { keep_last: usize, daily_days: u32, weekly_weeks: u32, monthly_months: u32, protected_ids: BTreeSet<String> }`.
- Produces: `PrunePlan { keep: Vec<String>, delete: Vec<String>, evaluated_at_unix_ms: u64 }`.
- Produces: `evaluate_retention(backups: &[BackupSummaryItem], policy: &RetentionPolicy, now_unix_ms: u64) -> Result<PrunePlan>`.

- [ ] **Step 1: Write failing pure retention tests**

```rust
#[test]
fn newest_and_protected_backups_are_kept() {
    let backups = fixtures_at_utc_days(&[0, 1, 2, 3, 4]);
    let policy = RetentionPolicy {
        keep_last: 2,
        protected_ids: BTreeSet::from(["bkp-day-4".to_string()]),
        ..RetentionPolicy::default()
    };
    let plan = evaluate_retention(&backups, &policy, day_ms(10)).unwrap();
    assert_eq!(plan.keep, vec!["bkp-day-0", "bkp-day-1", "bkp-day-4"]);
    assert_eq!(plan.delete, vec!["bkp-day-2", "bkp-day-3"]);
}

#[test]
fn daily_weekly_monthly_buckets_are_deterministic() {
    let backups = calendar_fixture();
    let policy = RetentionPolicy {
        keep_last: 1,
        daily_days: 7,
        weekly_weeks: 4,
        monthly_months: 3,
        protected_ids: BTreeSet::new(),
    };
    let a = evaluate_retention(&backups, &policy, FIXED_NOW).unwrap();
    let b = evaluate_retention(&backups, &policy, FIXED_NOW).unwrap();
    assert_eq!(a, b);
    assert!(a.keep.iter().all(|id| !a.delete.contains(id)));
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test lifecycle_retention --offline`

Expected: compile failure for undefined retention types and evaluator.

- [ ] **Step 3: Implement UTC bucket evaluation**

Use integer UTC calculations only. Daily bucket is `unix_ms / 86_400_000`. Weekly bucket is `(unix_ms / 86_400_000) / 7`. Monthly selection uses calendar year/month derived by a small dependency only if its license and MSRV pass existing policy; otherwise store an internal tested civil-date converter. Sort input by `(created_unix_ms descending, backup_id ascending)`. The newest backup in each selected bucket wins.

- [ ] **Step 4: Validate policy boundaries**

Reject a policy with all selectors zero and no protected IDs. Reject protected IDs absent from the provided set only when strict mode is requested; default planning reports them in a warning list.

- [ ] **Step 5: Run focused and regression tests**

Run:

```bash
cargo test --test lifecycle_retention --offline
cargo test --all-targets --all-features --offline
cargo clippy --all-targets --all-features -- -D warnings
```

- [ ] **Step 6: Commit**

```bash
git add src/lifecycle/retention.rs src/lifecycle/mod.rs tests/lifecycle_retention.rs Cargo.toml Cargo.lock
git commit -m "feat: add deterministic backup retention planning"
```

---

### Task 3: Reader leases

**Files:**
- Create: `src/lifecycle/lease.rs`
- Modify: `src/repository.rs`
- Modify: `src/engine.rs`
- Test: `tests/lifecycle_gc.rs`

**Interfaces:**
- Produces: `ReaderLease { lease_id: String, backup_id: String, pid: u32, process_start_token: String, acquired_unix_ms: u64 }`.
- Produces: `Repository::acquire_reader_lease(&self, backup_id: &str) -> Result<ReaderLeaseGuard>`.
- Produces: `Repository::active_reader_leases(&self) -> Result<Vec<ReaderLease>>`.
- Produces: `Repository::remove_stale_reader_leases(&self) -> Result<Vec<String>>`.

- [ ] **Step 1: Write failing lease tests**

```rust
#[tokio::test]
async fn reader_lease_is_visible_until_guard_drops() {
    let fixture = backup_fixture().await;
    let guard = fixture.repo.acquire_reader_lease(&fixture.backup_id).unwrap();
    let leases = fixture.repo.active_reader_leases().unwrap();
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].backup_id, fixture.backup_id);
    drop(guard);
    assert!(fixture.repo.active_reader_leases().unwrap().is_empty());
}

#[test]
fn stale_lease_requires_process_identity_mismatch() {
    let fixture = repo_fixture();
    fixture.insert_lease(fake_dead_pid_lease());
    let removed = fixture.repo.remove_stale_reader_leases().unwrap();
    assert_eq!(removed.len(), 1);
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test lifecycle_gc verify_holds_reader_lease_until_completion --offline`

Expected: missing lease methods and types.

- [ ] **Step 3: Implement lease table and guard**

Use `READER_LEASES: TableDefinition<&str, &[u8]>`. Guard `Drop` removes its exact lease ID. On Linux, process identity combines PID and `/proc/<pid>/stat` start time to prevent PID reuse. On unsupported platforms, do not automatically remove a lease; require explicit repair apply.

- [ ] **Step 4: Wrap verify and restore**

Acquire the reader lease immediately after resolving a non-deleted manifest. Hold it until verify or restore returns. Restore may reuse a single lease while invoking internal verification; avoid nested duplicate leases by extracting `verify_manifest(repo, manifest)`.

- [ ] **Step 5: Run focused and regression tests**

Run:

```bash
cargo test --test lifecycle_gc --offline
cargo test --all-targets --all-features --offline
```

- [ ] **Step 6: Commit**

```bash
git add src/lifecycle/lease.rs src/repository.rs src/engine.rs tests/lifecycle_gc.rs
git commit -m "feat: protect active readers with repository leases"
```

---

### Task 4: Journaled mark-and-sweep GC

**Files:**
- Create: `src/lifecycle/gc.rs`
- Modify: `src/repository.rs`
- Modify: `src/error.rs`
- Test: `tests/lifecycle_gc.rs`

**Interfaces:**
- Produces: `GcPlan { gc_id: String, live_chunks: u64, reclaim_chunks: Vec<ChunkMove>, reclaim_bytes: u64 }`.
- Produces: `ChunkMove { content_id: String, source: PathBuf, trash: PathBuf, bytes: u64 }`.
- Produces: `GcJournal { plan: GcPlan, phase: GcPhase, moved: Vec<String> }`.
- Produces: `GcPhase::{Planned, Moving, Committed, Deleting, Complete}`.
- Produces: `Repository::plan_gc(&self) -> Result<GcPlan>`.
- Produces: `Repository::apply_gc(&self, plan: &GcPlan) -> Result<GcResult>`.
- Produces: `Repository::recover_gc(&self) -> Result<Vec<GcRecoveryResult>>`.

- [ ] **Step 1: Write failing shared-chunk and orphan tests**

```rust
#[tokio::test]
async fn gc_preserves_chunks_referenced_by_live_backup() {
    let fixture = two_overlapping_backups().await;
    fixture.delete_first().unwrap();
    let plan = fixture.repo.plan_gc().unwrap();
    assert!(!plan.reclaim_chunks.iter().any(|m| m.content_id == fixture.shared_cid));
    fixture.repo.apply_gc(&plan).unwrap();
    assert!(fixture.repo.read_chunk(&fixture.shared_id()).is_ok());
}

#[tokio::test]
async fn gc_reclaims_last_reference_and_orphan() {
    let fixture = one_backup_plus_orphan().await;
    fixture.delete_only_backup().unwrap();
    let plan = fixture.repo.plan_gc().unwrap();
    assert_eq!(plan.reclaim_chunks.len(), fixture.all_chunk_count + 1);
    let result = fixture.repo.apply_gc(&plan).unwrap();
    assert_eq!(result.reclaimed_chunks, plan.reclaim_chunks.len() as u64);
}

#[tokio::test]
async fn gc_refuses_active_reader_lease() {
    let fixture = backup_fixture().await;
    let _guard = fixture.repo.acquire_reader_lease(&fixture.backup_id).unwrap();
    let error = fixture.repo.plan_gc().unwrap_err();
    assert!(error.to_string().contains("active reader"));
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test lifecycle_gc gc_preserves_chunks_referenced_by_live_backup --offline`

Expected: missing GC types and methods.

- [ ] **Step 3: Implement deterministic mark and plan**

Load all completed manifests including tombstone state. Mark only non-tombstoned content IDs. Scan exactly two-hex-prefix chunk directories; reject unexpected names into repair findings rather than treating them as content IDs. Sort `reclaim_chunks` by content ID.

- [ ] **Step 4: Implement same-filesystem trash and journal**

Create `trash/<gc-id>`. Persist encoded journal in `GC_JOURNALS: TableDefinition<&str, &[u8]>` before each phase transition. For each move, create parent directory and `rename(source, trash)`. Update moved IDs transactionally after each bounded batch of 128. After phase `Committed`, delete trash files, then tombstoned manifest files and redb entries, then set `Complete`.

- [ ] **Step 5: Implement recovery**

For `Moving`, restore moved files to source if the journal was never committed. For `Committed` or `Deleting`, continue deleting trash and finalizing tombstones. Recovery is idempotent when files already occupy the correct final location.

- [ ] **Step 6: Add fault-injection tests**

Use a test-only `GcFaultPoint` injected after the Nth rename and after journal commit. Assert a fresh `Repository::open` plus `recover_gc` restores or completes the exact expected state.

- [ ] **Step 7: Run focused and regression tests**

Run:

```bash
cargo test --test lifecycle_gc --offline
cargo test --all-targets --all-features --offline
cargo clippy --all-targets --all-features -- -D warnings
```

- [ ] **Step 8: Commit**

```bash
git add src/lifecycle/gc.rs src/repository.rs src/error.rs tests/lifecycle_gc.rs docs/repository-format.md
git commit -m "feat: add crash-recoverable repository garbage collection"
```

---

### Task 5: Repair inspection and safe apply

**Files:**
- Create: `src/lifecycle/repair.rs`
- Modify: `src/repository.rs`
- Test: `tests/lifecycle_repair.rs`

**Interfaces:**
- Produces: `RepairFinding { severity: FindingSeverity, kind: FindingKind, path: Option<PathBuf>, backup_id: Option<String>, content_id: Option<String>, detail: String }`.
- Produces: `RepairReport { findings: Vec<RepairFinding>, valid_manifests: u64, valid_chunks: u64 }`.
- Produces: `RepairPlan { rebuild_index: bool, gc_recoveries: Vec<String>, stale_leases: Vec<String> }`.
- Produces: `Repository::inspect_repair(&self) -> Result<RepairReport>`.
- Produces: `Repository::plan_repair(&self, report: &RepairReport) -> Result<RepairPlan>`.
- Produces: `Repository::apply_repair(&self, plan: &RepairPlan) -> Result<RepairResult>`.

- [ ] **Step 1: Write failing repair tests**

```rust
#[tokio::test]
async fn repair_reports_missing_chunk_without_fabrication() {
    let fixture = backup_fixture().await;
    std::fs::remove_file(&fixture.first_chunk_path).unwrap();
    let report = fixture.repo.inspect_repair().unwrap();
    assert!(report.findings.iter().any(|f| f.kind == FindingKind::MissingChunk));
    let plan = fixture.repo.plan_repair(&report).unwrap();
    fixture.repo.apply_repair(&plan).unwrap();
    assert!(!fixture.first_chunk_path.exists());
}

#[test]
fn repair_rebuilds_index_only_from_valid_manifest_copies() {
    let fixture = repo_with_lost_index_and_valid_manifest_file();
    let report = fixture.repo.inspect_repair().unwrap();
    let plan = fixture.repo.plan_repair(&report).unwrap();
    assert!(plan.rebuild_index);
    fixture.repo.apply_repair(&plan).unwrap();
    assert_eq!(fixture.repo.list_backups().unwrap(), vec![fixture.backup_id]);
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test lifecycle_repair --offline`

Expected: missing repair types and methods.

- [ ] **Step 3: Implement read-only inspection**

Inspect VERSION, redb tables, manifest copies, chunk names/headers/content, tombstones, leases, temp, trash, and GC journals. Cap findings at a configurable safe maximum and report truncation explicitly.

- [ ] **Step 4: Implement conservative repair planning**

Only plan index rebuild from manifest files whose magic, version, root hash, sequence, and every referenced chunk verify. Plan stale-lease removal only when process identity mismatch is proven. Delegate GC recovery to `recover_gc`.

- [ ] **Step 5: Implement apply and idempotency**

Build a new redb index in `tmp/index-repair-<id>.redb`, fsync, acquire writer lock, then atomic rename into place while retaining `index.redb.previous` until the new database reopens successfully. Re-running repair must produce no new mutations.

- [ ] **Step 6: Run focused and regression tests**

Run:

```bash
cargo test --test lifecycle_repair --offline
cargo test --all-targets --all-features --offline
cargo audit
```

- [ ] **Step 7: Commit**

```bash
git add src/lifecycle/repair.rs src/repository.rs tests/lifecycle_repair.rs
git commit -m "feat: add read-only repository inspection and safe repair"
```

---

### Task 6: Lifecycle CLI and JSON plans

**Files:**
- Modify: `src/main.rs`
- Modify: `src/error.rs`
- Test: `tests/lifecycle_cli.rs`
- Modify: `README.md`

**Interfaces:**
- Consumes all lifecycle plan/result types.
- Produces CLI: `delete`, `prune`, `gc`, and `repair`.
- Produces global behavior: dry-run by default; `--apply` mutates.

- [ ] **Step 1: Write failing CLI tests**

```rust
#[test]
fn delete_defaults_to_dry_run_and_json_is_stable() {
    let fixture = cli_backup_fixture();
    let output = zerostun_cmd()
        .args(["--json", "delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.backup_id])
        .output()
        .unwrap();
    assert!(output.status.success());
    let plan: DeletePlan = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(plan.backup_id, fixture.backup_id);
    assert!(fixture.repository().load_manifest(&fixture.backup_id).is_ok());
}

#[test]
fn delete_apply_hides_backup() {
    let fixture = cli_backup_fixture();
    zerostun_cmd()
        .args(["delete", "--repo"])
        .arg(&fixture.repo)
        .args(["--backup-id", &fixture.backup_id, "--apply"])
        .assert()
        .success();
    assert!(fixture.repository().load_manifest(&fixture.backup_id).is_err());
}
```

Add equivalent dry-run/apply assertions for prune, GC, and repair.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --test lifecycle_cli --offline`

Expected: clap rejects unknown lifecycle commands.

- [ ] **Step 3: Add typed clap subcommands**

```rust
Delete { repo: PathBuf, backup_id: String, apply: bool },
Prune { repo: PathBuf, keep_last: usize, daily_days: u32, weekly_weeks: u32, monthly_months: u32, protect: Vec<String>, apply: bool },
Gc { repo: PathBuf, apply: bool },
Repair { repo: PathBuf, apply: bool },
```

Plan before acquiring the writer lock. For apply, reacquire the lock and validate the plan is still current before mutation. Exit nonzero for a stale plan, active reader, or critical repair finding.

- [ ] **Step 4: Add human and JSON output**

JSON serializes the plan or result directly. Human output prints action, backup/chunk count, bytes, warnings, and whether it was dry-run or applied. Never print an affirmative deletion message during dry-run.

- [ ] **Step 5: Document safe operator workflow**

Add README examples that always show dry-run before `--apply`. Document tombstone reversibility before GC and irreversibility after GC.

- [ ] **Step 6: Run complete verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features --offline
cargo build --release
cargo audit
```

- [ ] **Step 7: Run real CLI smoke**

Create two overlapping backups, dry-run delete/prune/GC, apply one tombstone, prove the remaining backup restores byte-identically, apply GC, run repair read-only, and prove the remaining backup still verifies and restores.

- [ ] **Step 8: Commit**

```bash
git add src/main.rs src/error.rs tests/lifecycle_cli.rs README.md
git commit -m "feat: expose safe repository lifecycle commands"
```

---

### Task 7: Review, GitHub CI, and scorecard

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/superpowers/specs/2026-08-27-productization-85-design.md`
- Create: `docs/qualification/repository-lifecycle.md`

**Interfaces:**
- Consumes lifecycle CLI and tests.
- Produces qualification evidence and updated product score.

- [ ] **Step 1: Add lifecycle smoke to CI**

Extend the existing CLI smoke with delete dry-run/apply, GC dry-run/apply, repair, and byte-identical restore of a shared-chunk survivor.

- [ ] **Step 2: Run independent code review**

Review specifically for shared-chunk loss, stale plans, lease races, journal ordering, path traversal, symlink traversal, redb/file divergence, disk full, and interrupted rename/delete.

- [ ] **Step 3: Fix every confirmed Critical or Important finding with a failing regression test**

Run the focused regression test before and after each fix.

- [ ] **Step 4: Write qualification report**

Record commands, test counts, fault matrix, real CLI transcript summary, known limits, and score contribution. Repository lifecycle earns its 15 points only when every destructive path passes.

- [ ] **Step 5: Final local verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features --offline
cargo build --release
cargo audit
```

- [ ] **Step 6: Push feature and main, then wait for CI**

```bash
git push origin worktree-zerostun-core
git push origin worktree-zerostun-core:main
run_id=$(gh run list --repo happyaspic-byte/ZeroStun --branch main --limit 1 --json databaseId --jq '.[0].databaseId')
gh run watch "$run_id" --repo happyaspic-byte/ZeroStun --exit-status
```

- [ ] **Step 7: Commit qualification evidence**

```bash
git add .github/workflows/ci.yml docs/qualification/repository-lifecycle.md docs/superpowers/specs/2026-08-27-productization-85-design.md
git commit -m "test: qualify repository lifecycle safety"
```
