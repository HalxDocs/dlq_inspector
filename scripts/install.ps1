<#
.DESCRIPTION
DLQ Inspector — one-command installer for Windows, macOS, and Linux.

Downloads the latest release binary for your platform, verifies it against the
release's checksums.txt, and installs it. No Go toolchain required. Works in
Windows PowerShell 5.1 and PowerShell 7+ on any OS.

Usage:
  irm https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.ps1 | iex

Parameters:
  -Version      Specific release tag to install (default: latest)
  -InstallDir   Install location (default: $env:LOCALAPPDATA\dlq on Windows,
                ~/.local/bin elsewhere)
  -NoPath       Skip adding InstallDir to the PATH
  -BaseUrl      Test hook: serve the release assets from a local mirror
                instead of github.com (used by scripts/test-installers.sh)
#>
[CmdletBinding()]
param(
  [string]$Version = "",
  [string]$InstallDir = "",
  [switch]$NoPath,
  [string]$BaseUrl = ""
)

$ErrorActionPreference = "Stop"
# PS 5.1's per-chunk progress rendering makes Invoke-WebRequest crawl on
# large binaries; silence it and use the basic parser for raw downloads.
$ProgressPreference = "SilentlyContinue"
$Repo = "HalxDocs/dlq_inspector"

# Detect the platform. $IsWindows/$IsLinux/$IsMacOS exist in PowerShell 7+;
# Windows PowerShell 5.1 leaves them $null, so fall back to PlatformID.
if ($IsWindows) { $os = "windows" }
elseif ($IsMacOS) { $os = "darwin" }
elseif ($IsLinux) { $os = "linux" }
elseif ([Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT) { $os = "windows" }
else { throw "Unsupported OS: $([Environment]::OSVersion.Platform)" }

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
switch ($arch) {
  "X64" { $arch = "amd64" }
  "Arm64" { $arch = "arm64" }
  default { throw "Unsupported architecture: $arch" }
}

if ([string]::IsNullOrEmpty($InstallDir)) {
  if ($os -eq "windows") {
    $InstallDir = Join-Path $env:LOCALAPPDATA "dlq"
  } else {
    $InstallDir = "$HOME/.local/bin"
  }
}

# Resolve the release tag (latest, or the pinned -Version).
if ([string]::IsNullOrEmpty($Version)) {
  $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -Headers @{ "User-Agent" = "dlq-installer" }
  $Version = $release.tag_name
}
$tag = $Version
$ver = $tag.TrimStart("v")

$ext = if ($os -eq "windows") { "zip" } else { "tar.gz" }
$asset = "dlq-inspector_${ver}_${os}_${arch}.${ext}"
if ([string]::IsNullOrEmpty($BaseUrl)) {
  $baseUrl = "https://github.com/$Repo/releases/download/$tag"
} else {
  $baseUrl = $BaseUrl
}

Write-Host "DLQ Inspector $tag ($os/$arch)"
Write-Host "Downloading $asset ..."

# GetTempPath() works on Windows (TEMP) and Unix ($TMPDIR or /tmp); the
# Windows-only $env:TEMP is null under pwsh on macOS/Linux.
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ("dlq-install-" + [guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tmp | Out-Null
try {
  $zip = Join-Path $tmp $asset
  $csFile = Join-Path $tmp "checksums.txt"
  Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/$asset" -OutFile $zip
  Invoke-WebRequest -UseBasicParsing -Uri "$baseUrl/checksums.txt" -OutFile $csFile

  # Verify the sha256 against checksums.txt (accepts both 'hash  file' and
  # bsdtar's 'hash *file' forms). Read from disk: PS 5.1's .Content property
  # mangles raw text downloads, but Get-Content handles them correctly.
  $line = (Get-Content -Path $csFile | Where-Object { $_ -match ([regex]::Escape($asset) + "\s*$") } | Select-Object -First 1)
  if (-not $line) { throw "checksums.txt has no entry for $asset" }
  $expected = ($line -split "\s+")[0].ToLowerInvariant()
  $actual = (Get-FileHash -Algorithm SHA256 -Path $zip).Hash.ToLowerInvariant()
  if ($expected -ne $actual) { throw "Checksum verification FAILED for $asset" }
  Write-Host "Checksum verified (sha256)."

  if ($os -eq "windows") {
    Expand-Archive -Path $zip -DestinationPath $tmp -Force
    $extracted = Join-Path $tmp "dlq.exe"
    $binName = "dlq.exe"
  } else {
    & tar -xzf $zip -C $tmp
    $extracted = Join-Path $tmp "dlq"
    $binName = "dlq"
  }
  New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
  Copy-Item -Path $extracted -Destination $InstallDir -Force

  $bin = Join-Path $InstallDir $binName
  Write-Host "Installed to $bin"
  & $bin version

  if (-not $NoPath) {
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($userPath -notlike "*$InstallDir*") {
      [Environment]::SetEnvironmentVariable("Path", "$userPath;$InstallDir", "User")
      Write-Host "Added $InstallDir to your user PATH (new terminals only)."
    }
  }
}
finally {
  Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}

Write-Host "Upgrade later with: dlq self-update --confirm"
