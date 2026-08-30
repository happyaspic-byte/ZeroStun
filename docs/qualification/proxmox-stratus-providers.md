# Proxmox and Stratus Snapshot Provider Qualification

Date: 2026-08-30
Branch: `worktree-zerostun-core`
Verification level: `contract-tested`

## Scope

`ProxmoxProvider` and `StratusProvider` implement the object-safe
`SnapshotProvider` contract exclusively through `HttpTransport`. API transport
is separated from snapshot lifecycle logic. No shell interpolation is used.
Requests inherit the shared five-second timeout, cancellation, 32 KiB body
bound, HTTPS origin binding, relative-path validation, and secret redaction.
Asynchronous Proxmox UPIDs fail closed instead of being treated as completed
snapshots. No live HTTP client is included at this verification level.

API tokens are loaded only from an environment variable or an explicit regular
file whose Unix mode is exactly `0600`. Tokens are never accepted as literals or
positional arguments, persisted in snapshot handles, written into request
bodies, or printed in diagnostics.

## Contract evidence

- `tests/proxmox_contract.rs`: VM/storage capability probe, exact
  method/path/body lifecycle, ownership GET before DELETE, reverse-order
  recovery of `zerostun-*` snapshots, missing token, missing env token, insecure
  `0644` token file, `0600` token file without Debug leakage, unsupported
  storage types before POST, HTTP status and schema failures, timeout,
  cancellation, redaction, unsafe VMID/handle rejection, HTTPS-only origin,
  token-id header injection rejection, and origin-free path validation.
- `tests/stratus_contract.rs`: everRun and ztC schema variants, unsafe FT
  synchronization rejected before mutation with a single GET, exact create/open
  /cleanup/recover requests, missing auth, timeout, cancellation, redaction,
  unsafe target/handle rejection, unowned snapshot rejected before DELETE, and
  HTTPS-only origin.
- Recorded JSON fixtures under `tests/fixtures/proxmox/` and
  `tests/fixtures/stratus/` contain no credentials, hostnames of real labs, or
  serial numbers.

## Lab credential detection

Only the presence of configured safe lab credentials was checked. Values were
not printed.

| Provider | Presence check | Result | Network mutation |
| --- | --- | --- | --- |
| Proxmox | `ZEROSTUN_PROXMOX_TOKEN` or `ZEROSTUN_PROXMOX_TOKEN_FILE` | Absent | None |
| everRun / ztC | `ZEROSTUN_STRATUS_TOKEN` or `ZEROSTUN_STRATUS_TOKEN_FILE` | Absent | None |

No isolated disposable Proxmox or Stratus target was configured, so no live
network mutation was attempted.

## Safety properties

- Unsupported quiesce and changed-block requirements fail before any request.
- Request paths must be origin-free, start with `/`, and cannot contain `://`,
  parent-directory components, or control characters.
- HTTPS origins without userinfo or fragment are required. HTTP is rejected
  before the first request.
- Tokens appear only in the `Authorization` header and are redacted from
  recorded requests, Debug output, and error text.
- Proxmox create re-probes VM status and snapshot-capable storage
  (`lvmthin`/`zfspool`/`rbd`/`qcow2` with `images` or `rootdir` content) before
  POST. Source paths are derived, not accepted from API output.
- Cleanup and open require a `zerostun-` identifier and a matching derived
  source path. Cleanup additionally verifies ownership over GET before DELETE.
- Stratus create probes node-pair/FT health first. `synchronized=false` or a
  non-protected state refuses snapshot mutation with no POST.
- Recovery deletes only ZeroStun-managed snapshot names, in reverse
  lexicographic order. Unmanaged snapshots such as `current` or `user-snap` are
  ignored.

## Verification results

Focused contract tests:

- `proxmox_contract`: 10 passed
- `stratus_contract`: 8 passed

Full crate gates:

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --all-targets --all-features --offline -- -D warnings` | PASS |
| `cargo test --lib --bins --tests --all-features --offline -- --test-threads=1` | PASS: 197 passed, 0 failed, 0 ignored across 18 suites |
| `cargo build --release --offline` | PASS |
| `cargo audit --file Cargo.lock` | PASS: 182 dependency packages scanned, 0 vulnerabilities reported |

Until an isolated disposable lab is exercised, the README support matrix remains
`contract-tested` and must not be labeled `hardware-verified`.
