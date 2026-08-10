#!/usr/bin/env bash
#
# DLQ Inspector — one-command installer for macOS and Linux.
#
# Downloads the latest release binary for your platform, verifies it against
# the release's checksums.txt, and installs it. No Go toolchain required.
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.sh | bash
#
# Environment overrides (mainly for testing / pinning):
#   DLQ_VERSION       - specific release tag to install (default: latest)
#   DLQ_OS            - force OS: linux | darwin
#   DLQ_ARCH          - force arch: amd64 | arm64
#   DLQ_INSTALL_DIR   - install location (default: ~/.local/bin)
#   DLQ_BASE_URL      - test hook: serve the release assets from a local
#                       mirror instead of github.com (used by
#                       scripts/test-installers.sh)
#
set -euo pipefail

REPO="HalxDocs/dlq_inspector"
VERSION="${DLQ_VERSION:-}"
INSTALL_DIR="${DLQ_INSTALL_DIR:-$HOME/.local/bin}"

detect_os() {
  case "$(uname -s)" in
    Linux*)  echo "linux" ;;
    Darwin*) echo "darwin" ;;
    *)
      echo "Unsupported OS: $(uname -s). macOS and Linux have one-command installers; on Windows use scripts/install.ps1 or the .zip from the releases page." >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo "amd64" ;;
    aarch64 | arm64) echo "arm64" ;;
    *)
      echo "Unsupported architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

fetch() { # fetch <url> [-o <file>]
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$@"
  elif command -v wget >/dev/null 2>&1; then
    local args=()
    local out=""
    while [[ $# -gt 0 ]]; do
      case "$1" in
        -o) out="$2"; shift 2 ;;
        *)  args+=("$1"); shift ;;
      esac
    done
    if [[ -n "$out" ]]; then wget -q "${args[@]}" -O "$out"; else wget -qO- "${args[@]}"; fi
  else
    echo "Neither curl nor wget is available; install one of them first." >&2
    exit 1
  fi
}

OS="${DLQ_OS:-$(detect_os)}"
ARCH="${DLQ_ARCH:-$(detect_arch)}"

# Resolve the release tag (latest, or the pinned DLQ_VERSION).
resolve_version() {
  if [[ -n "$VERSION" ]]; then
    case "$VERSION" in
      v*) echo "$VERSION" ;;
      *)  echo "v$VERSION" ;;
    esac
    return
  fi
  local body tag
  body="$(fetch "https://api.github.com/repos/$REPO/releases/latest")"
  tag="$(printf '%s\n' "$body" | grep -m1 '"tag_name"' | sed 's/.*"tag_name": *"\([^"]*\)".*/\1/')"
  if [[ -z "$tag" ]]; then
    echo "Could not resolve the latest release for $REPO." >&2
    exit 1
  fi
  echo "$tag"
}

TAG="$(resolve_version)"
VER="${TAG#v}"
ASSET="dlq-inspector_${VER}_${OS}_${ARCH}.tar.gz"
BASE_URL="${DLQ_BASE_URL:-https://github.com/$REPO/releases/download/$TAG}"

echo "DLQ Inspector $TAG ($OS/$ARCH)"
echo "Downloading $ASSET ..."

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

fetch "$BASE_URL/$ASSET" -o "$TMPDIR/$ASSET"
fetch "$BASE_URL/checksums.txt" -o "$TMPDIR/checksums.txt"

# Verify the sha256 against checksums.txt (accepts both 'hash  file' and
# bsdtar's 'hash *file' forms).
expected="$(grep -E "(^| )$ASSET\$" "$TMPDIR/checksums.txt" | head -1 | awk '{print $1}')"
actual="$(sha256sum "$TMPDIR/$ASSET" | awk '{print $1}')"
if [[ -z "$expected" ]]; then
  echo "checksums.txt has no entry for $ASSET — refusing to install." >&2
  exit 1
fi
if [[ "$expected" != "$actual" ]]; then
  echo "Checksum verification FAILED for $ASSET — refusing to install." >&2
  exit 1
fi
echo "Checksum verified (sha256)."

tar -xzf "$TMPDIR/$ASSET" -C "$TMPDIR"

mkdir -p "$INSTALL_DIR"
install -m 0755 "$TMPDIR/dlq" "$INSTALL_DIR/dlq"

echo "Installed to $INSTALL_DIR/dlq"
if "$INSTALL_DIR/dlq" version >/dev/null 2>&1; then
  "$INSTALL_DIR/dlq" version
else
  echo "(could not execute the binary to print its version here)" >&2
fi
echo

case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *)
    echo "NOTE: $INSTALL_DIR is not on your PATH. Add it (e.g. in ~/.bashrc or ~/.zshrc):"
    echo "  export PATH=\"$INSTALL_DIR:\$PATH\""
    ;;
esac
echo "Upgrade later with: dlq self-update --confirm"
