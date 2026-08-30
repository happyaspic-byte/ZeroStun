# ZeroStun

Lightweight, bounded-I/O backup engine for edge and fault-tolerant systems.

ZeroStun stores files as content-addressed, immutable chunks. It hashes original
bytes with BLAKE3, compresses with zstd or lz4, and records a canonical root hash
so backups can be verified and restored byte-for-byte.

## What this MVP guarantees

- Sequential backup of regular files into a local repository
- FastCDC v2020 chunking with configurable min/avg/max sizes
- Content IDs computed from original chunk bytes, independent of codec
- Optional read/write token-bucket rate limits
- Bounded in-flight payload (`max_chunk * (queue_depth + workers)`)
- Atomic chunk and manifest publish (temp file + fsync + rename)
- Full `verify` that re-decompresses and re-hashes every chunk
- Restore to a temporary file, then atomic rename
- Failed backups are not listed as completed

## What this MVP does not guarantee

- Host application latency of 0 seconds
- Crash-consistent snapshots of live files
- Encryption, signatures, or remote authentication
- Network replication, S3, or regulatory WORM enforcement
- Hardware-verified Proxmox, everRun, or ztC integration

Rate limits bound backup I/O. They are not a proof that another process on the
same host will never stall. See `docs/zero-stun-contract.md`.

## Snapshot provider support

| Provider | Source semantics | Verification level | Notes |
| --- | --- | --- | --- |
| LVM | Read-only logical-volume snapshot at a stable `/dev/mapper` path | `contract-tested` | Exact-argv `lvs`/`lvcreate`/`lvremove`, tagged stale-snapshot recovery; no disposable root storage lab was available |
| ZFS filesystem | Read-only clone mounted under `/run/zerostun/zfs` | `contract-tested` | Snapshot, clone, mount, reverse cleanup, and stale-resource recovery are fault-injected |
| ZFS ZVOL | Read-only clone exposed under `/dev/zvol` | `contract-tested` | Block-device lifecycle is separated from filesystem mount semantics |
| Proxmox | Derived read-only VM snapshot path under `/dev/pve` | `contract-tested` | Typed HTTP transport, token from env or mode-0600 file, VM/storage probe, ownership-bound cleanup; no disposable lab was configured |
| everRun | Derived read-only workload snapshot path under `/dev/stratus` | `contract-tested` | Explicit everRun schema, FT synchronization probe, mutation refused while unsynchronized |
| ztC | Derived read-only workload snapshot path under `/dev/stratus` | `contract-tested` | Explicit ztC schema, node-pair/quorum probe, same lifecycle as everRun with a distinct API prefix |

`contract-tested` means provider commands or HTTP requests, parsing, failures,
timeout, cancellation, redaction, validation, and recovery are verified against
a fake exact-argv runner or typed HTTP transport. It does not claim integration
or hardware verification. This host had non-root LVM tooling and no ZFS binary,
so only non-destructive availability probes were run; no host storage was
modified. No Proxmox or Stratus lab credentials or isolated disposable targets
were configured, so no live API mutation was attempted.

## Build

The declared MSRV policy is Rust 1.85; release builds are pinned to Rust 1.97.1.
The current lockfile requires a later compiler through `redb`, `fastcdc`, and
`criterion`, so MSRV 1.85 is documented rather than a passing compile gate.

```bash
cargo build --release --locked
./target/release/zerostun --help

# Local unpublished archive; never creates a tag or release.
scripts/package-release.sh \
  --target x86_64-unknown-linux-gnu \
  --output-dir dist
sha256sum -c dist/zerostun-*.tar.gz.sha256
```

Archives include generated completions/man page, systemd/sample configuration,
license files, and a CycloneDX SBOM. Installation, upgrade, rollback, and
uninstall steps are documented in `docs/qualification/distribution.md`.

## Usage

```bash
zerostun init --repo /var/lib/zerostun/repo

zerostun backup \
  --repo /var/lib/zerostun/repo \
  --source /path/to/file.bin \
  --read-rate 50MiB \
  --write-rate 50MiB \
  --codec zstd:3

zerostun inspect --repo /var/lib/zerostun/repo --backup-id bkp-...
zerostun verify  --repo /var/lib/zerostun/repo --backup-id bkp-...
zerostun restore --repo /var/lib/zerostun/repo --backup-id bkp-... --target /tmp/restored.bin
zerostun list    --repo /var/lib/zerostun/repo

# Lifecycle commands default to dry-run. Mutation requires --apply.
zerostun delete --repo /var/lib/zerostun/repo --backup-id bkp-...
zerostun delete --repo /var/lib/zerostun/repo --backup-id bkp-... --apply

zerostun prune --repo /var/lib/zerostun/repo --keep-last 7 --daily-days 14
zerostun prune --repo /var/lib/zerostun/repo --keep-last 7 --daily-days 14 --apply

zerostun gc --repo /var/lib/zerostun/repo
zerostun gc --repo /var/lib/zerostun/repo --apply

zerostun repair --repo /var/lib/zerostun/repo
zerostun repair --repo /var/lib/zerostun/repo --apply

zerostun daemon status --config /etc/zerostun/daemon.toml --json
zerostun jobs list --config /etc/zerostun/daemon.toml --json
zerostun runs list --config /etc/zerostun/daemon.toml --json
zerostun cancel <run-id> --config /etc/zerostun/daemon.toml --json
zerostun metrics --json --config /etc/zerostun/daemon.toml
zerostun daemon run --config /etc/zerostun/daemon.toml

A hardened systemd unit and sample job configuration live in
`packaging/zerostun-daemon.service` and `packaging/daemon.toml`.
```

JSON output is available with `--json`. JSON prints the plan or result struct directly.

Delete writes a tombstone. The backup is hidden from list, inspect, verify, and restore, but chunks remain until GC. A tombstone is reversible until GC finalizes it; after GC the backup and unreferenced chunks are gone permanently.

## Safety notes

- Do not point `--source` at a path inside the repository.
- Restore refuses to overwrite an existing target unless `--force` is set.
- A concurrent write lock is exclusive; a second writer fails immediately.
- Live files may change during backup. ZeroStun fails the job if size or mtime
  changes between start and finish. That is not a filesystem snapshot.

## Tests

```bash
cargo test --lib --bins --tests
cargo clippy --lib --bin zerostun -- -D warnings
```

## License

Apache-2.0 OR MIT
