# Repository Lifecycle Qualification

Date: 2026-08-30
Branch: `worktree-zerostun-core`
HEAD at qualification: `717560c`
PR: https://github.com/happyaspic-byte/ZeroStun/pull/1

## Commands

```bash
export TMPDIR=/home/ubuntu/.claude/jobs/1e9d9d0e/tmp
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --offline -- -D warnings
cargo test --lib --bins --tests --all-features --offline -- --test-threads=1
cargo build --release --offline
```

GitHub Actions `ci` run 20 on `717560c` concluded success:
https://github.com/happyaspic-byte/ZeroStun/actions/runs/33303110263

## Test counts

| Suite | Passed |
| --- | ---: |
| lib unit | 16 |
| cli_smoke | 1 |
| core_pipeline | 6 |
| error_and_edge_cases | 7 |
| lifecycle_cli | 12 |
| lifecycle_delete | 14 |
| lifecycle_gc | 36 |
| lifecycle_repair | 9 |
| lifecycle_retention | 14 |
| property_roundtrip | 2 |
| **total** | **117** |

`cargo test --all-targets` is not used: `engine_bench` rejects `--test-threads`.

## Fault matrix

| Fault | Result |
| --- | --- |
| Injected rename crash | Recovery rolls back to `chunks/` |
| Injected committed crash | Recovery rolls forward, tombstones finalize |
| CID became live after crash | `recover_gc` errors; chunk remains |
| CID reused then tombstoned after crash | `recover_gc` errors; chunk remains; undelete works |
| Dual source+trash copies | Fail closed, no overwrite |
| Symlinked chunk/trash/manifests | Rejected; external sentinel untouched |
| Active reader lease | `plan_gc`/`apply_gc` refuse |
| Stale PID / start-token mismatch | Lease removed only when identity mismatch is proven |
| Missing live chunk | Plan fails closed |
| Missing chunk during repair | Reported; never fabricated |
| Index loss with valid manifest copies | Rebuild from verified manifests |
| Dry-run delete/prune/gc/repair | No mutation |
| CLI critical repair finding | Nonzero exit |

## CLI transcript summary

`tests/lifecycle_cli.rs::overlapping_backup_lifecycle_smoke` and CI `CLI smoke`:

1. Two overlapping backups.
2. `delete` / `gc` without `--apply` print plans only.
3. `delete --apply` tombstones the first backup.
4. Remaining backup restores byte-identically.
5. `gc --apply` reclaims unreferenced chunks.
6. `repair` read-only; remaining backup still verifies and restores.

Destructive commands default to dry-run. Human dry-run output does not claim deletion.

## Known limits

- `apply_gc` / `recover_gc` / `apply_delete` are writer-lock primitives; they do not nest `flock`. CLI apply paths take the lock. Library callers must hold it.
- `verify` maps GC-barrier lease refusal into `VerifyReport { is_valid: false }` rather than a dedicated locked exit. Restore propagates the GC error.
- Directory `fsync` is a Unix contract; non-Unix is a no-op.
- Repair never fabricates missing or corrupt chunk bytes.
- Snapshot adapters, daemon, distribution, and hardware labs are out of scope for this qualification.

## Score contribution

Repository lifecycle weight is 15/100. Evidence for delete, retention, GC, repair, crash recovery, and CLI is executable. Remaining productization areas are unscored here.
