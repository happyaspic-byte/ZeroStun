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
metadata=$(mktemp -p "$scratch")
stage=$(mktemp -d -p "$scratch")
cleanup() {
  rm -f "$metadata"
  rm -rf "$stage"
}
trap cleanup EXIT

cargo metadata --manifest-path "$repo_root/Cargo.toml" --format-version 1 --locked --offline > "$metadata"
version=$(python3 - "$metadata" <<'PY'
import json, sys
metadata = json.load(open(sys.argv[1], encoding="utf-8"))
root = metadata["resolve"]["root"]
package = next(pkg for pkg in metadata["packages"] if pkg["id"] == root)
print(package["version"])
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

python3 - "$metadata" "$root/share/doc/zerostun/sbom.cdx.json" <<'PY'
import json, sys
metadata = json.load(open(sys.argv[1], encoding="utf-8"))
root_id = metadata["resolve"]["root"]
root = next(pkg for pkg in metadata["packages"] if pkg["id"] == root_id)

def component(pkg, kind="library"):
    item = {
        "type": kind,
        "bom-ref": pkg["id"],
        "name": pkg["name"],
        "version": pkg["version"],
        "purl": f"pkg:cargo/{pkg['name']}@{pkg['version']}",
    }
    if pkg.get("license"):
        item["licenses"] = [{"expression": pkg["license"]}]
    return item

bom = {
    "bomFormat": "CycloneDX",
    "specVersion": "1.5",
    "version": 1,
    "metadata": {"component": component(root, "application")},
    "components": [
        component(pkg)
        for pkg in sorted(metadata["packages"], key=lambda p: (p["name"], p["version"]))
        if pkg["id"] != root_id
    ],
}
with open(sys.argv[2], "w", encoding="utf-8") as handle:
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
