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
- Proxmox, everRun, or ztC integration

Rate limits bound backup I/O. They are not a proof that another process on the
same host will never stall. See `docs/zero-stun-contract.md`.

## Snapshot provider support

| Provider | Source semantics | Verification level | Notes |
| --- | --- | --- | --- |
| LVM | Read-only logical-volume snapshot at a stable `/dev/mapper` path | `contract-tested` | Exact-argv `lvs`/`lvcreate`/`lvremove`, tagged stale-snapshot recovery; no disposable root storage lab was available |
| ZFS filesystem | Read-only clone mounted under `/run/zerostun/zfs` | `contract-tested` | Snapshot, clone, mount, reverse cleanup, and stale-resource recovery are fault-injected |
| ZFS ZVOL | Read-only clone exposed under `/dev/zvol` | `contract-tested` | Block-device lifecycle is separated from filesystem mount semantics |
| Proxmox | Not implemented | unsupported | Planned production provider |
| everRun / ztC | Not implemented | unsupported | Planned hardware providers |

`contract-tested` means provider commands, parsing, failures, timeout,
cancellation, redaction, validation, and recovery are verified against a fake
exact-argv runner. It does not claim integration or hardware verification.
This host had non-root LVM tooling and no ZFS binary, so only non-destructive
availability probes were run; no host storage was modified.

## Build

Requires Rust 1.85+ (tested on 1.97.1).

```bash
cargo build --release
./target/release/zerostun --help
```

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
