# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
#
# Install Copycord on Windows from a GitHub release:
#
#   irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
#
# Environment:
#   COPYCORD_CHANNEL=stable|beta  stable (default): newest release;
#                                 beta: newest release, pre-releases included.
#                                 Saved as the `copycord update` channel.
#   COPYCORD_VERSION=x.y.z        install exactly release vx.y.z instead.
#   COPYCORD_ALLOW_ROOT=1         allow running as Administrator.
#
# Installs uv (https://docs.astral.sh/uv/) if it is missing, then installs the
# release wheel with `uv tool install`. It does not touch Docker.

function Install-Copycord {
    # Inside the function so `irm | iex` leaves the caller's session settings alone.
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'

    $Repo = 'Copycord/Copycord'
    $Api = "https://api.github.com/repos/$Repo/releases"
    $UvInstaller = 'https://astral.sh/uv/install.ps1'
    $Headers = @{ Accept = 'application/vnd.github+json'; 'User-Agent' = 'copycord-install' }

    function Fail([string]$Message) {
        throw "copycord install: ERROR: $Message"
    }

    function Invoke-Native([string]$What, [scriptblock]$Command) {
        & $Command
        if ($LASTEXITCODE -ne 0) { Fail "$What failed (exit $LASTEXITCODE)" }
    }

    # --- checks ---------------------------------------------------------------

    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if ($isAdmin -and $env:COPYCORD_ALLOW_ROOT -ne '1') {
        Fail 'do not run this as Administrator; Copycord installs for your user. Set COPYCORD_ALLOW_ROOT=1 to do it anyway.'
    }

    $channel = if ($env:COPYCORD_CHANNEL) { $env:COPYCORD_CHANNEL } else { 'stable' }
    if ($channel -notin @('stable', 'beta')) {
        Fail "COPYCORD_CHANNEL must be stable or beta, not '$channel'"
    }
    $version = if ($env:COPYCORD_VERSION) { $env:COPYCORD_VERSION.TrimStart('v') } else { '' }

    # --- plan -----------------------------------------------------------------

    $uv = Get-Command uv -ErrorAction SilentlyContinue
    Write-Host 'Copycord installer'
    if ($version) {
        Write-Host "  release:  v$version (COPYCORD_VERSION)"
    } else {
        Write-Host "  channel:  $channel"
    }
    if ($uv) {
        Write-Host "  uv:       found at $($uv.Source)"
    } else {
        Write-Host "  uv:       not found, will install it with $UvInstaller"
    }
    Write-Host '  then:     uv tool install --force <release wheel URL>'
    Write-Host ''

    # --- uv -------------------------------------------------------------------

    if (-not $uv) {
        Write-Host 'Installing uv'
        Invoke-Native 'the uv installer' {
            powershell -NoProfile -ExecutionPolicy ByPass -Command "irm $UvInstaller | iex"
        }
        $env:Path = "$env:USERPROFILE\.local\bin;$env:Path"
        $uv = Get-Command uv -ErrorAction SilentlyContinue
        if (-not $uv) { Fail 'uv was installed but is not on PATH; open a new terminal and run this again' }
    }

    # --- release --------------------------------------------------------------

    try {
        if ($version) {
            $release = Invoke-RestMethod -Uri "$Api/tags/v$version" -Headers $Headers -TimeoutSec 30
        } elseif ($channel -eq 'beta') {
            $list = Invoke-RestMethod -Uri "${Api}?per_page=30" -Headers $Headers -TimeoutSec 30
            $release = $list | Where-Object { -not $_.draft } | Select-Object -First 1
        } else {
            # GitHub's latest release: newest that is not a draft or a pre-release.
            $release = Invoke-RestMethod -Uri "$Api/latest" -Headers $Headers -TimeoutSec 30
        }
    } catch {
        Fail "could not get the release from ${Api}: $($_.Exception.Message)"
    }
    if (-not $release) { Fail "no $channel release found on https://github.com/$Repo/releases" }
    $tag = $release.tag_name
    $wheel = $release.assets | Where-Object { $_.name -like 'copycord-*.whl' } | Select-Object -First 1
    if (-not $wheel) {
        Fail "release $tag has no wheel asset (copycord-*.whl); see https://github.com/$Repo/releases/tag/$tag"
    }
    $wheelUrl = $wheel.browser_download_url

    Write-Host "Installing Copycord $tag"
    Write-Host "  $wheelUrl"
    Invoke-Native 'uv tool install' { uv tool install --force $wheelUrl }

    # --- PATH -----------------------------------------------------------------

    $binDir = (uv tool dir --bin)
    if ($LASTEXITCODE -ne 0 -or -not $binDir) { Fail 'could not get the uv tool bin directory' }
    $binDir = $binDir.Trim()
    if (($env:Path -split ';') -notcontains $binDir) {
        Write-Host "Adding $binDir to PATH (uv tool update-shell)"
        Invoke-Native 'uv tool update-shell' { uv tool update-shell }
        $env:Path = "$binDir;$env:Path"
    }

    # --- update channel -------------------------------------------------------

    if (-not $version -and $env:COPYCORD_CHANNEL) {
        Invoke-Native "saving the update channel $channel" {
            & (Join-Path $binDir 'copycord.exe') update --channel $channel --no-install
        }
    }

    Write-Host ''
    Write-Host "Copycord $tag is installed."
    Write-Host 'Next steps (open a new terminal first if copycord is not found):'
    Write-Host '  copycord doctor                  check the setup'
    Write-Host '  copycord run                     start Copycord'
    Write-Host '  copycord service install --user  run it as a service at login'
}

Install-Copycord
