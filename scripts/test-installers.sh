#!/usr/bin/env bash
#
# Negative-path test for the one-command installers.
#
# Serves a fake release tree (dlq-inspector_1.0.0_<os>_<arch> archives plus a
# checksums.txt) from a local Node HTTP server, then asserts:
#
#   control   - both installers install a good archive and report
#               "Checksum verified"
#   negative  - when the archive on the server is corrupted (same filename,
#               different bytes), both installers refuse to install, fail
#               non-zero, and leave no binary behind
#
# The installers are pointed at the local mirror via their test hooks
# (DLQ_BASE_URL / -BaseUrl) so no real GitHub traffic is involved.
#
# Requirements: bash, node (HTTP server), tar, sha256sum, curl, the Go
# toolchain (builds a tiny dummy dlq.exe for the PowerShell control path),
# and powershell or pwsh (zip creation + the PowerShell installer test).
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
SERVER_PID=""
PORT="${DLQ_TEST_PORT:-18765}"
BASE="http://127.0.0.1:$PORT"

PWSH=""
if command -v pwsh >/dev/null 2>&1; then PWSH=pwsh; elif command -v powershell >/dev/null 2>&1; then PWSH=powershell; fi

# The PowerShell installer is cross-platform: it installs dlq.exe on Windows
# and dlq elsewhere. Assert whichever name applies to this host.
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) PS_BIN_NAME="dlq.exe" ;;
  *) PS_BIN_NAME="dlq" ;;
esac

# Git Bash passes /tmp/... virtual paths to native Windows programs via MSYS
# conversion, which PowerShell mishandles; convert explicitly where present.
if command -v cygpath >/dev/null 2>&1; then
  WIN_ROOT="$(cygpath -w "$ROOT")"
  WIN_WORK="$(cygpath -w "$WORK")"
else
  WIN_ROOT="$ROOT"
  WIN_WORK="$WORK"
fi

cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

for tool in node tar sha256sum curl go; do
  command -v "$tool" >/dev/null 2>&1 || fail "missing required tool: $tool"
done
[[ -n "$PWSH" ]] || fail "missing powershell/pwsh (needed for the Windows installer test)"

# --- build a fake release tree -------------------------------------------
# The dummy binaries live in separate source dirs: Git Bash's MSYS layer
# resolves a new file "dlq" to an existing "dlq.exe" in the same directory,
# which would silently clobber the compiled binary.
FAKE="$WORK/fake"
mkdir -p "$FAKE/linux" "$FAKE/win"

# A tiny real Go binary so the PowerShell control path can execute dlq.exe.
DUMMY_GO="$WORK/dummy.go"
cat > "$DUMMY_GO" <<'EOF'
package main

import "fmt"

func main() { fmt.Println("dummy-dlq 1.0.0") }
EOF
echo "building dummy dlq.exe ..."
go build -o "$FAKE/win/dlq.exe" "$DUMMY_GO"

# Dummy POSIX binary for the bash installer control path.
printf '#!/bin/sh\necho dummy-dlq 1.0.0\n' > "$FAKE/linux/dlq"
chmod +x "$FAKE/linux/dlq"

LINUX_ASSET="dlq-inspector_1.0.0_linux_amd64.tar.gz"
WIN_ASSET="dlq-inspector_1.0.0_windows_amd64.zip"

tar -C "$FAKE/linux" -czf "$FAKE/$LINUX_ASSET" dlq
"$PWSH" -NoProfile -Command "
  Compress-Archive -Path '$WIN_WORK/fake/win/dlq.exe' -DestinationPath '$WIN_WORK/fake/$WIN_ASSET' -Force
" || fail "could not create the fake windows zip"

write_checksums() {
  local linux_hash win_hash
  linux_hash="$(sha256sum "$FAKE/$LINUX_ASSET" | awk '{print $1}')"
  win_hash="$(sha256sum "$FAKE/$WIN_ASSET" | awk '{print $1}')"
  printf '%s  %s\n%s  %s\n' "$linux_hash" "$LINUX_ASSET" "$win_hash" "$WIN_ASSET" > "$FAKE/checksums.txt"
}
write_checksums

# --- tiny static file server ---------------------------------------------
cat > "$WORK/server.js" <<'EOF'
const http = require('http');
const fs = require('fs');
const path = require('path');
const root = process.argv[2];
http.createServer((req, res) => {
  const p = path.join(root, path.basename(req.url));
  if (!fs.existsSync(p)) { res.writeHead(404); res.end('not found'); return; }
  res.writeHead(200, { 'Content-Type': 'application/octet-stream' });
  fs.createReadStream(p).pipe(res);
}).listen(parseInt(process.argv[3], 10), '127.0.0.1');
EOF

node "$WORK/server.js" "$FAKE" "$PORT" &
SERVER_PID=$!

# Wait for the server to answer.
for _ in $(seq 1 50); do
  curl -fs "$BASE/checksums.txt" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -fs "$BASE/checksums.txt" >/dev/null 2>&1 || fail "local test server never came up"

# --- bash installer --------------------------------------------------------
echo "== bash installer: control (good archive)"
DLQ_BASE_URL="$BASE" DLQ_OS=linux DLQ_ARCH=amd64 DLQ_VERSION=v1.0.0 \
  DLQ_INSTALL_DIR="$WORK/out-good" bash "$ROOT/scripts/install.sh" > "$WORK/good.log" 2>&1 \
  || { cat "$WORK/good.log" >&2; fail "bash installer failed on the control path"; }
grep -qi "checksum verified" "$WORK/good.log" || fail "bash installer did not report checksum verification"
[[ -x "$WORK/out-good/dlq" ]] || fail "bash installer did not install the binary"
echo "PASS bash installer control"

echo "== bash installer: negative (corrupted archive)"
printf 'CORRUPTED-ARCHIVE-BYTES-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' > "$FAKE/$LINUX_ASSET"
if DLQ_BASE_URL="$BASE" DLQ_OS=linux DLQ_ARCH=amd64 DLQ_VERSION=v1.0.0 \
  DLQ_INSTALL_DIR="$WORK/out-bad" bash "$ROOT/scripts/install.sh" > "$WORK/bad.log" 2>&1; then
  cat "$WORK/bad.log" >&2
  fail "bash installer accepted a corrupted archive (exit 0)"
fi
grep -qi "checksum" "$WORK/bad.log" || { cat "$WORK/bad.log" >&2; fail "bash installer did not report a checksum failure"; }
[[ ! -e "$WORK/out-bad/dlq" ]] || fail "bash installer installed a binary despite the checksum mismatch"
echo "PASS bash installer refuses corrupted archive"

# --- PowerShell installer ----------------------------------------------------
# The ps1 downloads the asset for the HOST platform: on Linux/macOS that is the
# linux/darwin tar.gz (which the bash negative test just corrupted), on Windows
# the windows zip. Restore the linux archive and refresh checksums.txt first,
# then corrupt whichever asset the ps1 will actually fetch.
case "$(uname -s)" in
  MINGW* | MSYS* | CYGWIN*) PS_OS="windows"; PS_ASSET="$WIN_ASSET" ;;
  *) PS_OS="linux"; PS_ASSET="$LINUX_ASSET" ;;
esac

tar -C "$FAKE/linux" -czf "$FAKE/$LINUX_ASSET" dlq
write_checksums

echo "== powershell installer: control (good $PS_OS archive)"
set +e
"$PWSH" -NoProfile -ExecutionPolicy Bypass -File "$WIN_ROOT/scripts/install.ps1" \
  -Version v1.0.0 -BaseUrl "$BASE" -InstallDir "$WIN_WORK/ps-out-good" -NoPath > "$WORK/ps-good.log" 2>&1
PS_EXIT=$?
set -e
if [[ $PS_EXIT -ne 0 ]]; then cat "$WORK/ps-good.log" >&2; fail "powershell installer failed on the control path (exit $PS_EXIT)"; fi
grep -qi "checksum verified" "$WORK/ps-good.log" || fail "powershell installer did not report checksum verification"
[[ -f "$WORK/ps-out-good/$PS_BIN_NAME" ]] || fail "powershell installer did not install the binary"
echo "PASS powershell installer control"

echo "== powershell installer: negative (corrupted $PS_OS archive)"
printf 'CORRUPTED-ARCHIVE-BYTES-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' > "$FAKE/$PS_ASSET"
if "$PWSH" -NoProfile -ExecutionPolicy Bypass -File "$WIN_ROOT/scripts/install.ps1" \
  -Version v1.0.0 -BaseUrl "$BASE" -InstallDir "$WIN_WORK/ps-out-bad" -NoPath > "$WORK/ps-bad.log" 2>&1; then
  cat "$WORK/ps-bad.log" >&2
  fail "powershell installer accepted a corrupted archive (exit 0)"
fi
grep -qi "checksum" "$WORK/ps-bad.log" || { cat "$WORK/ps-bad.log" >&2; fail "powershell installer did not report a checksum failure"; }
[[ ! -e "$WORK/ps-out-bad/$PS_BIN_NAME" ]] || fail "powershell installer installed a binary despite the checksum mismatch"
echo "PASS powershell installer refuses corrupted archive"

echo
echo "All installer checks passed: control installs, corrupted archives are refused."
