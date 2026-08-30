# Daemon and Operations Qualification

Date: 2026-08-30
Branch: `worktree-zerostun-core`
Verification level: local-and-ci

## Scope

`src/daemon/` implements the Phase 4 operations surface on the existing redb
state foundation. No second binary is produced; all daemon behavior lives in
the `zerostun` library and CLI.

## Modules

| Module | Responsibility |
| --- | --- |
| `config.rs` | TOML admission with deny-unknown-fields, UTC/IANA timezone allowlist, retry bound validation, duplicate job rejection |
| `state.rs` | redb tables (`daemon_jobs`, `daemon_runs`, `daemon_schedule`, `daemon_recovery`, `daemon_state_version`), six run statuses, same-job duplicate suppression inside one write transaction, bounded JSON payloads, fail-closed decode |
| `scheduler.rs` | injected `ManualClock`, interval due computation, at-most-one restart catch-up |
| `runner.rs` | snapshot create/open, bounded transient-only retry, always-cleanup before commit, cleanup-failure recovery record, structured `tracing` fields |
| `shutdown.rs` | SIGTERM admission stop, running-stage cancellation, deadline-bounded cleanup wait |

## CLI surfaces

`daemon status`, `daemon run`, `jobs list`, `runs list`, `cancel <run-id>`,
`metrics --json` — all with `--config` and JSON output.

## Verification results

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --all-targets --all-features --offline -- -D warnings` | PASS |
| `cargo test --lib --bins --tests --all-features --offline -- --test-threads=1` | PASS: 179 passed, 0 failed, 0 ignored across 16 suites |
| `cargo build --release --offline` | PASS |
| `cargo audit` | PASS: 0 vulnerabilities |

## Contract coverage

`tests/daemon_contract.rs` (11 tests): TOML validation and timezone checks,
all six run statuses, duplicate same-job rejection, corrupted-state fail-closed
read without data replacement, injected-clock scheduling with one catch-up,
retry classification (transient-only, bounded exponential), runner success with
snapshot cleanup and metrics commit, cancellation cleanup with `Cancelled`
commit, cleanup failure producing a recoverable record and failed run,
restart recovery moving interrupted runs to `Recovering` once, SIGTERM
admission/cancellation/deadline enforcement, and stable metrics JSON.

`tests/daemon_cli.rs` (4 tests): `daemon status`, `jobs list`, `runs list`,
`metrics --json` stability, `cancel` of a running job, invalid-config
rejection before status output, and SIGTERM shutdown within the configured
deadline.

## Safety properties

- Job admission fails closed on unsafe identifiers, empty providers/targets,
  zero intervals, unknown timezones, and invalid retry bounds before any run.
- Same-job concurrency is rejected inside the admission transaction.
- Interrupted runs are moved to `Recovering` exactly once per restart.
- Cleanup always runs after a snapshot exists, and a failed cleanup records a
  recoverable snapshot reference instead of losing it.
- Daemon state payloads are bounded to 64 KiB and corrupt rows fail reads
  without overwriting stored data.
- The systemd unit uses a dynamic dedicated user, `NoNewPrivileges`,
  `ProtectSystem=strict`, `ProtectHome`, a closed device policy with explicit
  `/dev/mapper` and `/dev/zvol` read opt-ins, and a bounded `TimeoutStopSec`
  matching the configured shutdown deadline.

## Known limitations

- Timezone handling is an allowlist covering UTC and common IANA zones; full
  tz database support is deferred until a timezone dependency is added.
- The daemon run loop admits and executes jobs sequentially; parallel job
  execution is intentionally out of scope for this phase.
- `bytes_processed`, `dedupe_bytes`, and `limiter_wait_ms` are recorded on the
  run record and emitted in structured logs, but the daemon does not yet
  execute real backup pipeline work, so these are zero until engine
  integration lands.
