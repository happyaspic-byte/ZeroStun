use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn run(command: &mut Command, what: &str) -> std::process::Output {
    let output = command.output().unwrap_or_else(|error| {
        panic!("failed to launch {what}: {error}");
    });
    assert!(
        output.status.success(),
        "{what} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn find_one(directory: &Path, suffix: &str) -> PathBuf {
    let matches: Vec<PathBuf> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.to_string_lossy().ends_with(suffix))
        .collect();
    assert_eq!(matches.len(), 1, "expected one *{suffix} in {directory:?}");
    matches[0].clone()
}

#[test]
fn release_packaging_builds_verifiable_offline_archive() {
    let root = repo_root();
    let temp = tempfile::tempdir().unwrap();
    let dist = temp.path().join("dist");
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_zerostun"));

    run(
        Command::new("bash")
            .arg(root.join("scripts/package-release.sh"))
            .arg("--binary")
            .arg(&binary)
            .arg("--target")
            .arg("x86_64-unknown-linux-gnu")
            .arg("--output-dir")
            .arg(&dist)
            .env("TMPDIR", temp.path())
            .current_dir(&root),
        "release packaging",
    );

    let archive = find_one(&dist, ".tar.gz");
    let checksum = PathBuf::from(format!("{}.sha256", archive.display()));
    assert!(checksum.is_file());
    run(
        Command::new("sha256sum")
            .arg("-c")
            .arg(checksum.file_name().unwrap())
            .current_dir(&dist),
        "checksum verification",
    );

    let unpack = temp.path().join("unpack");
    fs::create_dir(&unpack).unwrap();
    run(
        Command::new("tar")
            .arg("-xzf")
            .arg(&archive)
            .arg("-C")
            .arg(&unpack),
        "archive extraction",
    );

    let package_root = fs::read_dir(&unpack)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let installed_binary = package_root.join("bin/zerostun");
    let version = run(
        Command::new(&installed_binary).arg("--version"),
        "packaged --version",
    );
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).trim(),
        "zerostun 0.1.0"
    );
    let help = run(
        Command::new(&installed_binary).arg("--help"),
        "packaged --help",
    );
    assert!(String::from_utf8_lossy(&help.stdout).contains("bounded-I/O backup engine"));

    for required in [
        "share/bash-completion/completions/zerostun",
        "share/zsh/site-functions/_zerostun",
        "share/fish/vendor_completions.d/zerostun.fish",
        "share/man/man1/zerostun.1",
        "lib/systemd/system/zerostun-daemon.service",
        "etc/zerostun/daemon.toml.example",
        "share/doc/zerostun/LICENSE-APACHE",
        "share/doc/zerostun/LICENSE-MIT",
        "share/doc/zerostun/sbom.cdx.json",
    ] {
        assert!(package_root.join(required).is_file(), "missing {required}");
    }

    let bash = fs::read_to_string(package_root.join("share/bash-completion/completions/zerostun"))
        .unwrap();
    assert!(bash.contains("zerostun"));
    let man = fs::read_to_string(package_root.join("share/man/man1/zerostun.1")).unwrap();
    assert!(man.contains("zerostun") || man.contains("ZEROSTUN"));

    let sbom: serde_json::Value = serde_json::from_slice(
        &fs::read(package_root.join("share/doc/zerostun/sbom.cdx.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(sbom["bomFormat"], "CycloneDX");
    assert_eq!(sbom["specVersion"], "1.5");
    assert_eq!(sbom["metadata"]["component"]["name"], "zerostun");
    assert_eq!(sbom["metadata"]["component"]["version"], "0.1.0");
    assert!(sbom["components"]
        .as_array()
        .is_some_and(|items| !items.is_empty()));
}

#[test]
fn supply_chain_policy_is_explicit_and_release_never_creates_tags() {
    let root = repo_root();
    let toolchain = fs::read_to_string(root.join("rust-toolchain.toml")).unwrap();
    assert!(toolchain.contains("channel = \"1.97.1\""));
    assert!(toolchain.contains("rustfmt"));
    assert!(toolchain.contains("clippy"));
    let cargo_toml = fs::read_to_string(root.join("Cargo.toml")).unwrap();
    assert!(cargo_toml.contains("rust-version = \"1.90\""));
    let declared_msrv = cargo_toml
        .lines()
        .find_map(|line| line.strip_prefix("rust-version = \"")?.strip_suffix('"'))
        .unwrap();
    let minor: u32 = declared_msrv.split('.').nth(1).unwrap().parse().unwrap();
    assert!(minor >= 90, "current redb requires Rust 1.90 or newer");

    let deny = fs::read_to_string(root.join("deny.toml")).unwrap();
    for license in [
        "\"Apache-2.0\"",
        "\"MIT\"",
        "\"Unicode-3.0\"",
        "\"CC0-1.0\"",
        "\"MIT-0\"",
        "\"Unlicense\"",
    ] {
        assert!(
            deny.contains(license),
            "deny.toml does not mention {license}"
        );
    }
    assert!(!deny.contains("\"BSD-2-Clause\""));
    assert!(!deny.to_ascii_lowercase().contains("unknown = \"allow\""));
    assert!(deny.contains("https://github.com/rust-lang/crates.io-index"));

    let ci = fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(ci.contains("permissions:\n  contents: read"));
    assert!(ci.contains("persist-credentials: false"));
    assert!(ci.contains("command -v cargo-deny"));
    assert!(ci.contains("cargo fetch --locked"));
    assert!(ci.contains("cargo deny --offline --locked check licenses sources bans advisories"));
    assert!(ci.contains("--locked"));

    let release = fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    assert!(release.contains("tags: [\"v*\"]"));
    assert!(release.contains("workflow_dispatch:"));
    assert!(release.contains("default: test"));
    assert!(release.contains("x86_64-unknown-linux-gnu"));
    assert!(release.contains("x86_64-unknown-linux-musl"));
    assert!(release.contains("package-release.sh"));
    assert!(release.contains("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"));
    assert!(release.contains("actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683"));
    assert!(release.contains("dtolnay/rust-toolchain@032958afbdc797a9164d3bc0b56325c1308924a5"));
    assert!(!release.contains("git tag"));
    assert!(!release.contains("tag_name:"));
}
