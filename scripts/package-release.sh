#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: scripts/package-release.sh [--binary PATH] --target TRIPLE --output-dir DIR

Builds a local release archive without publishing or creating a tag. If
--binary is omitted, Cargo builds the requested target with --locked --offline.
Required tools: cargo, python3, tar, gzip, sha256sum, and install.
USAGE
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
binary=""
target=""
output_dir=""

while (($#)); do
  case "$1" in
    --binary)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      binary=$2
      shift 2
      ;;
    --target)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      target=$2
      shift 2
      ;;
    --output-dir)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      output_dir=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

[[ $target =~ ^[A-Za-z0-9_.-]+$ ]] || { echo "invalid target triple" >&2; exit 2; }
[[ -n $output_dir ]] || { usage; exit 2; }

if [[ -z $binary ]]; then
  cargo build --manifest-path "$repo_root/Cargo.toml" --release --locked --offline --target "$target"
  binary="$repo_root/target/$target/release/zerostun"
fi
[[ -f $binary && -x $binary ]] || { echo "binary is not an executable regular file: $binary" >&2; exit 2; }

scratch=${TMPDIR:-/tmp}
stage=$(mktemp -d -p "$scratch")
cleanup() {
  rm -rf "$stage"
}
trap cleanup EXIT

version=$(python3 - "$repo_root/Cargo.toml" <<'PY'
import sys, tomllib
with open(sys.argv[1], "rb") as handle:
    manifest = tomllib.load(handle)
print(manifest["package"]["version"])
PY
)
package="zerostun-${version}-${target}"
root="$stage/$package"
assets="$stage/assets"

install -d \
  "$root/bin" \
  "$root/share/bash-completion/completions" \
  "$root/share/zsh/site-functions" \
  "$root/share/fish/vendor_completions.d" \
  "$root/share/man/man1" \
  "$root/lib/systemd/system" \
  "$root/etc/zerostun" \
  "$root/share/doc/zerostun" \
  "$assets"
install -m 0755 "$binary" "$root/bin/zerostun"

"$binary" generate-assets --output-dir "$assets"
install -m 0644 "$assets/zerostun.bash" "$root/share/bash-completion/completions/zerostun"
install -m 0644 "$assets/_zerostun" "$root/share/zsh/site-functions/_zerostun"
install -m 0644 "$assets/zerostun.fish" "$root/share/fish/vendor_completions.d/zerostun.fish"
install -m 0644 "$assets/zerostun.1" "$root/share/man/man1/zerostun.1"
install -m 0644 "$repo_root/packaging/zerostun-daemon.service" "$root/lib/systemd/system/zerostun-daemon.service"
install -m 0644 "$repo_root/packaging/daemon.toml" "$root/etc/zerostun/daemon.toml.example"
install -m 0644 "$repo_root/LICENSE-APACHE" "$root/share/doc/zerostun/LICENSE-APACHE"
install -m 0644 "$repo_root/LICENSE-MIT" "$root/share/doc/zerostun/LICENSE-MIT"

python3 - "$repo_root/Cargo.toml" "$repo_root/Cargo.lock" "$root/share/doc/zerostun/sbom.cdx.json" <<'PY'
import json, sys, tomllib

with open(sys.argv[1], "rb") as handle:
    manifest = tomllib.load(handle)
root_name = manifest["package"]["name"]
root_version = manifest["package"]["version"]
root_license = manifest["package"].get("license")

packages = []
current = None
with open(sys.argv[2], encoding="utf-8") as handle:
    for raw in handle:
        line = raw.rstrip("\n")
        if line == "[[package]]":
            if current:
                packages.append(current)
            current = {}
            continue
        if current is None:
            continue
        if line.startswith("name = "):
            current["name"] = line.split("=", 1)[1].strip().strip('"')
        elif line.startswith("version = "):
            current["version"] = line.split("=", 1)[1].strip().strip('"')
        elif line == "":
            packages.append(current)
            current = None
    if current:
        packages.append(current)

def component(name, version, kind="library", license=None):
    item = {
        "type": kind,
        "bom-ref": f"pkg:cargo/{name}@{version}",
        "name": name,
        "version": version,
        "purl": f"pkg:cargo/{name}@{version}",
    }
    if license:
        item["licenses"] = [{"expression": license}]
    return item

bom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {
        "component": component(root_name, root_version, "application", root_license)
    },
    "components": [
        component(pkg["name"], pkg["version"])
        for pkg in sorted(packages, key=lambda item: (item["name"], item["version"]))
        if not (pkg["name"] == root_name and pkg["version"] == root_version)
    ],
}
with open(sys.argv[3], "w", encoding="utf-8") as handle:
    json.dump(bom, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

mkdir -p "$output_dir"
archive="$output_dir/$package.tar.gz"
epoch=${SOURCE_DATE_EPOCH:-0}
tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner -C "$stage" -czf "$archive" "$package"
(
  cd "$output_dir"
  sha256sum "$package.tar.gz" > "$package.tar.gz.sha256"
)
printf '%s\n' "$archive"
