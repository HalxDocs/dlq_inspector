<#
.DESCRIPTION
DLQ Inspector — one-command installer for Windows.

Downloads the latest release binary for your platform, verifies it against the
release's checksums.txt, and installs it. No Go toolchain required.

Usage:
  irm https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.ps1 | iex

Parameters:
  -Version      Specific release tag to install (default: latest)
  -InstallDir   Install location (default: $env:LOCALAPPDATA\dlq)
  -NoPath       Skip adding InstallDir to the user PATH
#>
[CmdletBinding()]
param(
  [string]$Version = "",
  [string]$InstallDir = "",
  [switch]$NoPath
)

$ErrorActionPreference = "Stop"
# PS 5.1's per-chunk progress rendering makes Invoke-WebRequest crawl on
# large binaries; silence it and use the basic parser for raw downloads.
$ProgressPreference = "SilentlyContinue"
$Repo = "HalxDocs/dlq_inspector"

if ([string]::IsNullOrEmpty($InstallDir)) {
  $InstallDir = Join-Path $env:LOCALAPPDATA "dlq"
}

# Resolve the release tag (latest, or the pinned -Version).
if ([string]::IsNullOrEmpty($Version)) {
  $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -Headers @{ "User-Agent" = "dlq-installer" }
  $Version = $release.tag_name
}
$tag = $Version
$ver = $tag.TrimStart("v")

$arch = $env:PROCESSOR_ARCHITECTURE
switch ($arch) {
  "AMD64" { $arch = "amd64" }
  "ARM64" { $arch = "arm64" }
  default { throw "Unsupported architecture: $arch" }
}

$asset = "dlq-inspector_${ver}_windows_${arch}.zip"
$baseUrl = "https://github.com/$Repo/releases/download/$tag"

Write-Host "DLQ Inspector $tag (windows/$arch)"
Write-Host "Downloading $asset ..."

$tmp = Join-Path $env:TEMP ("dlq-install-" + [guid]::NewGuid().ToString("N"))
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

  Expand-Archive -Path $zip -DestinationPath $tmp -Force
  New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
  Copy-Item -Path (Join-Path $tmp "dlq.exe") -Destination $InstallDir -Force

  $bin = Join-Path $InstallDir "dlq.exe"
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
