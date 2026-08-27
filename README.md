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
- Proxmox, everRun, ztC, LVM, or ZFS integration
- Backup deletion or garbage collection

Rate limits bound backup I/O. They are not a proof that another process on the
same host will never stall. See `docs/zero-stun-contract.md`.

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
```

JSON output is available with `--json`.

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
