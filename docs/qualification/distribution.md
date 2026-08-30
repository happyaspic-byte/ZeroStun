# Distribution and Supply-Chain Qualification

Date: 2026-08-30
Branch: `worktree-zerostun-core`
Verification level: `local-and-ci`

## Policy

`rust-toolchain.toml` pins release and CI builds to Rust 1.97.1 with rustfmt and
clippy. `Cargo.toml` declares MSRV 1.90, the true minimum for the locked graph
because `redb 4.2.0` requires rustc 1.90. CI tests the 1.97.1 pin and a
packaging regression that the declared rust-version is not older than 1.90.

`deny.toml` allows only SPDX licenses present in the current dependency graph:
Apache-2.0 (including LLVM exception), MIT, Unicode-3.0, CC0-1.0, MIT-0, and
Unlicense. Unknown registries and Git sources are denied; only crates.io is
allowed. Wildcard dependencies are denied and duplicate versions are reported.

## Artifact contents

`scripts/package-release.sh` creates a target-specific `.tar.gz` and adjacent
SHA-256 checksum without publishing or creating a tag. The archive contains:

- `bin/zerostun`
- bash, zsh, and fish completions generated from the clap command model
- a generated `zerostun(1)` man page
- `zerostun-daemon.service` and `daemon.toml.example`
- Apache-2.0 and MIT license texts
- a CycloneDX 1.5 JSON SBOM generated directly from `Cargo.toml` and `Cargo.lock`
  without fetching unused-platform crates. This SBOM lists lockfile package
  names, versions, and PURLs; it does not include crate hashes, registry
  evidence, or signed provenance.

Required local tools are Cargo/Rust, Bash, Python 3, GNU tar, gzip, `install`,
and `sha256sum`. A musl build additionally needs the Rust musl target and
`musl-gcc`. With dependencies cached, the script builds with `--locked
--offline`; tests can inject an already-built binary. Temporary files are
created under `TMPDIR` when set, so hosts with `/tmp` quota can still package.

## Release workflow

`.github/workflows/release.yml` runs only for an existing `v*` tag push or an
explicit `workflow_dispatch` packaging-test input (`mode=test`). It validates
tag/version equality, tests the crate, builds GNU and musl x86_64 archives, and
uploads short-lived workflow artifacts. It never creates a tag or GitHub
release. Third-party actions are pinned to immutable commit SHAs with the
reviewed tag recorded only as a comment; action updates require a
source-policy review. `.github/workflows/ci.yml` uses the same SHA-pinned
Rust 1.97.1 action, `contents: read`, `persist-credentials: false`, `--locked`
Cargo invocations, and `cargo deny`. CI installs `cargo-deny 0.20.2` with
`--locked`, runs `cargo fetch --locked` so unused-platform lockfile crates
are present, fetches advisory data, then runs
`cargo deny --offline --locked check licenses sources bans advisories`. The
job fails if `cargo-deny` is missing.

No release or tag was created in this phase. Final release publication remains
blocked on the full release gate and the required 24-hour soak.

## Local verification

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | PASS |
| `cargo clippy --all-targets --all-features --locked --offline -- -D warnings` | PASS |
| `cargo test --lib --bins --tests --all-features --locked --offline -- --test-threads=1` | PASS: 199 passed, 0 failed, 0 ignored across 19 suites, including `release_packaging` |
| `cargo build --release --locked --offline` | PASS |
| `cargo audit --file Cargo.lock --no-fetch` | PASS: 185 dependency packages scanned, 0 vulnerabilities reported |
| `cargo deny --offline --locked check licenses sources bans advisories` | PASS: licenses, sources, and advisories denied-closed; duplicate crate versions reported as warn-only |

A local unpublished GNU archive was produced with
`scripts/package-release.sh` using `TMPDIR` in the workspace. Checksum
verification and archive contents (binary, generated completions, man page,
systemd unit, sample config, licenses, CycloneDX 1.5 SBOM) were inspected.
musl packaging is exercised in CI when the musl compiler and target are
available; this host did not have `musl-gcc` installed.

## Operator installation

1. Verify the archive from the same directory as its checksum:
   `sha256sum -c zerostun-*.tar.gz.sha256`.
2. Extract the archive and inspect the SBOM/license files.
3. Install `bin/zerostun` to `/usr/bin/zerostun`.
4. Install completions and `share/man/man1/zerostun.1` into the matching system
   directories.
5. Create the dedicated `zerostun` service account and writable
   `/var/lib/zerostun` and `/run/zerostun` paths as required by local policy.
6. Copy `daemon.toml.example` to `/etc/zerostun/daemon.toml`, replace the sample
   target, then install and review the systemd unit before enabling it.
7. Run `systemd-analyze verify` and `zerostun daemon status --config
   /etc/zerostun/daemon.toml` before `systemctl enable --now`.

## Upgrade and rollback

Stop the daemon, back up `/etc/zerostun` and `/var/lib/zerostun`, verify the new
archive, replace only the binary/assets, run `systemctl daemon-reload`, and
restart. Keep the previous verified archive for rollback. Repository format
upgrades are not performed implicitly by this package; use the qualification
notes for the target version before replacing a production binary.

## Uninstall

Disable and stop the service, remove installed binary/assets/unit, and run
`systemctl daemon-reload`. Preserve `/etc/zerostun` and `/var/lib/zerostun` by
default. Delete configuration, state, repositories, and the service account only
after a separate backup and explicit operator decision.
