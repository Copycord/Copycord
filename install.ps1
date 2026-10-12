# =============================================================================
#  Copycord
#  Copyright (C) 2026 github.com/Copycord
#
#  This source code is released under the GNU Affero General Public License
#  version 3.0. A copy of the license is available at:
#  https://www.gnu.org/licenses/agpl-3.0.en.html
# =============================================================================
#
# Install Copycord on Windows:
#
#   irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1 | iex
#
# With options:
#
#   & ([scriptblock]::Create((irm https://raw.githubusercontent.com/Copycord/Copycord/main/install.ps1))) -Version v4-dev
#
# Two install methods:
#   release (default)  the wheel asset of a GitHub release
#   git                a clone of any repo and ref (branch, tag or commit),
#                      frontend built with npm, no release needed
#
# Installs uv (https://docs.astral.sh/uv/) if it is missing, then installs
# Copycord with `uv tool install`. The git method also needs git and Node 20+;
# Node is installed per user when it is missing. It does not touch Docker.
# Run with -Help for the parameters and their environment variables.

param(
    [ValidateSet('release', 'git')]
    [string]$InstallMethod = $(if ($env:COPYCORD_INSTALL_METHOD) { $env:COPYCORD_INSTALL_METHOD } else { 'release' }),
    [switch]$Git,
    [string]$Repo = $(if ($env:COPYCORD_REPO) { $env:COPYCORD_REPO } else { 'Copycord/Copycord' }),
    [Alias('Tag')]
    [string]$Version = $env:COPYCORD_VERSION,
    [switch]$Beta,
    [string]$GitDir = $env:COPYCORD_GIT_DIR,
    [string]$CopycordHome = $env:COPYCORD_HOME,
    [switch]$NoOnboard,
    [switch]$Verify,
    [switch]$DryRun,
    [switch]$ShowOutput,
    [switch]$Help
)

function Install-Copycord {
    param($Opts)
    # Inside the function so `irm | iex` leaves the caller's session settings alone.
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'

    $DefaultRepo = 'Copycord/Copycord'
    $UvInstaller = 'https://astral.sh/uv/install.ps1'
    $NodeDist = 'https://nodejs.org/dist'
    $NodeMinMajor = 20
    $NodeVersion = if ($env:COPYCORD_NODE_VERSION) { $env:COPYCORD_NODE_VERSION } else { '22.12.0' }
    $Headers = @{ Accept = 'application/vnd.github+json'; 'User-Agent' = 'copycord-install' }

    if ($Opts.Help) {
        @"
Copycord installer

Usage:
  irm https://raw.githubusercontent.com/$DefaultRepo/main/install.ps1 | iex
  & ([scriptblock]::Create((irm https://raw.githubusercontent.com/$DefaultRepo/main/install.ps1))) [options]

Options:
  -Repo owner/name            GitHub repository (default: $DefaultRepo)
  -Version <value>            a release version (4.0.0, v4.0.0, 4.0.0b1) installs
  (alias -Tag)                that release; anything else (a branch, tag or
                              commit sha) installs from git at that ref
  -Beta                       release: newest release, pre-releases included;
                              saved as the ``copycord update`` channel
  -CopycordHome <path>        Copycord home: data, logs, backups, src\ and node\
                              (default: %USERPROFILE%\.copycord)
  -GitDir <path>              git: checkout directory
                              (default: <home>\src)
  -NoOnboard                  skip the post-install ``copycord doctor`` run
  -Verify                     after installing, run ``copycord version`` and
                              ``copycord doctor`` and fail if either fails
  -DryRun                     print the plan and the actions, change nothing
  -ShowOutput                 stream the output of every step
  -Help                       show this help

Advanced:
  -InstallMethod release|git and -Git force the method instead of inferring it
  from -Version; COPYCORD_INSTALL_METHOD does too.

Environment (parameters win):
  COPYCORD_REPO, COPYCORD_VERSION, COPYCORD_CHANNEL=stable|beta,
  COPYCORD_BETA=1, COPYCORD_GIT_DIR, COPYCORD_HOME, COPYCORD_NO_ONBOARD=1, COPYCORD_VERIFY=1,
  COPYCORD_DRY_RUN=1, COPYCORD_NODE_VERSION, COPYCORD_ALLOW_ROOT=1, NO_COLOR=1

Test a branch, no release needed:
  & ([scriptblock]::Create((irm https://raw.githubusercontent.com/$DefaultRepo/v4-dev/install.ps1))) -Version v4-dev
"@ | Write-Host
        return
    }

    # --- options ------------------------------------------------------------

    $method = if ($Opts.Git) { 'git' } else { $Opts.InstallMethod }
    $inferred = ''
    if ($method -notin @('release', 'git')) {
        throw "copycord install: -InstallMethod must be release or git, not '$method'"
    }
    $repo = $Opts.Repo
    if ($repo -notmatch '^[^/\s]+/[^/\s]+$') { throw "copycord install: -Repo must look like owner/name, not '$repo'" }
    $channel = if ($env:COPYCORD_CHANNEL) { $env:COPYCORD_CHANNEL } else { 'stable' }
    $channelSet = [bool]$env:COPYCORD_CHANNEL
    if ($Opts.Beta -or $env:COPYCORD_BETA -eq '1') { $channel = 'beta'; $channelSet = $true }
    if ($channel -notin @('stable', 'beta')) {
        throw "copycord install: COPYCORD_CHANNEL must be stable or beta, not '$channel'"
    }
    $onboard = -not ($Opts.NoOnboard -or $env:COPYCORD_NO_ONBOARD -eq '1')
    $verify = $Opts.Verify -or $env:COPYCORD_VERIFY -eq '1'
    $dryRun = $Opts.DryRun -or $env:COPYCORD_DRY_RUN -eq '1'
    $showOutput = [bool]$Opts.ShowOutput
    $version = if ($Opts.Version) { $Opts.Version } else { '' }
    # -Version alone picks the method: a release version (optional v, then
    # digits.digits.digits and an optional pre-release suffix) is a release,
    # anything else is a git ref. An explicit method wins.
    if (-not $Opts.MethodSet -and -not $Opts.Git -and $version) {
        $method = if ($version -cmatch '^v?\d+\.\d+\.\d+([-.]?(a|b|c|rc|alpha|beta|pre|preview|dev)[.-]?[0-9A-Za-z.]*)?$') { 'release' } else { 'git' }
        $inferred = ', inferred from -Version'
    }
    $homeDir = if ($Opts.CopycordHome) { $Opts.CopycordHome } else { Join-Path $env:USERPROFILE '.copycord' }
    if (-not [IO.Path]::IsPathRooted($homeDir)) { Write-Host "copycord install: -CopycordHome must be an absolute path (got $homeDir)"; return }
    $homeDir = $homeDir.TrimEnd('\', '/')
    $homePointer = Join-Path $env:USERPROFILE '.copycord-home'
    $env:COPYCORD_HOME = $homeDir
    $gitDir = if ($Opts.GitDir) { $Opts.GitDir } else { Join-Path $homeDir 'src' }
    $nodeHome = Join-Path $homeDir 'node'
    $api = "https://api.github.com/repos/$repo/releases"
    $repoUrl = "https://github.com/$repo"
    if ($method -eq 'release') { $version = $version.TrimStart('v') } else { $ref = if ($version) { $version } else { 'main' } }

    # --- output -------------------------------------------------------------

    $useColor = -not [Console]::IsOutputRedirected -and -not $env:NO_COLOR
    $utf = [Console]::OutputEncoding.CodePage -eq 65001
    $okMark = if ($utf) { [string][char]0x2713 } else { 'ok' }
    $failMark = if ($utf) { [string][char]0x2717 } else { 'x' }
    $frames = if ($utf) { @(0x280B, 0x2819, 0x2839, 0x2838, 0x283C, 0x2834, 0x2826, 0x2827, 0x2807, 0x280F) | ForEach-Object { [string][char]$_ } } else { @('|', '/', '-', '\') }
    $box = if ($utf) { @{ TL = [char]0x256D; TR = [char]0x256E; BL = [char]0x2570; BR = [char]0x256F; H = [char]0x2500; V = [char]0x2502 } } else { @{ TL = '+'; TR = '+'; BL = '+'; BR = '+'; H = '-'; V = '|' } }

    function Out([string]$Text, [string]$Color, [switch]$NoNewline) {
        if ($useColor -and $Color) { Write-Host $Text -ForegroundColor $Color -NoNewline:$NoNewline }
        else { Write-Host $Text -NoNewline:$NoNewline }
    }
    function Fail([string]$Message) {
        Out ''
        Out "  $failMark error: " Red -NoNewline; Out $Message
        Out ''
        throw "copycord install: $Message"
    }
    function Done([string]$Text) { Out "  $okMark " Green -NoNewline; Out $Text }
    function Info([string]$Text) { Out '  - ' DarkGray -NoNewline; Out $Text }
    function Warn([string]$Text) { Out '  ! ' Yellow -NoNewline; Out $Text }
    function Dry([string]$Text) { Out '  dry run: ' Yellow -NoNewline; Out $Text }
    function PlanLine([string]$Key, [string]$Value) { Out ('  {0,-10} ' -f $Key) DarkGray -NoNewline; Out $Value }

    $script:step = 0
    $script:stepTotal = 0
    function Step([string]$Title) {
        $script:step++
        Out ''
        Out "[$($script:step)/$($script:stepTotal)] " Magenta -NoNewline; Out $Title
    }

    $log = [IO.Path]::GetTempFileName()
    function Run([string]$Label, [string]$File, [string[]]$Arguments, [string]$Cwd = (Get-Location).Path) {
        # Run a program with a spinner and its output in $log; on failure show
        # the tail of the output and stop. -ShowOutput streams it instead.
        if ($showOutput) {
            Info $Label
            Push-Location $Cwd
            try { & $File @Arguments } finally { Pop-Location }
            if ($LASTEXITCODE -ne 0) { Fail "$Label failed (exit $LASTEXITCODE, output above)" }
            Done $Label
            return
        }
        $quoted = ($Arguments | ForEach-Object { if ($_ -match '[\s"]') { '"' + ($_ -replace '"', '\"') + '"' } else { $_ } }) -join ' '
        $err = "$log.err"
        $p = Start-Process -FilePath $File -ArgumentList $quoted -WorkingDirectory $Cwd -NoNewWindow -PassThru `
            -RedirectStandardOutput $log -RedirectStandardError $err
        # Touch the handle now: without it ExitCode stays empty once the process exits.
        $null = $p.Handle
        $i = 0
        while (-not $p.HasExited) {
            if ($useColor) { Out ("`r  " + $frames[$i % $frames.Count] + " $Label") Magenta -NoNewline }
            $i++
            Start-Sleep -Milliseconds 100
        }
        $p.WaitForExit()
        if ($useColor) { Write-Host ("`r" + (' ' * ($Label.Length + 6)) + "`r") -NoNewline }
        Get-Content $err -ErrorAction SilentlyContinue | Add-Content $log
        Remove-Item $err -ErrorAction SilentlyContinue
        if ($p.ExitCode -ne 0) {
            Out "  $failMark $Label" Red
            Out '    last lines of output:' DarkGray
            Get-Content $log -Tail 25 | ForEach-Object { Out "    $_" }
            Fail "$Label failed (exit $($p.ExitCode))"
        }
        Done $Label
    }

    # --- checks -------------------------------------------------------------

    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($identity)
    $isAdmin = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
    if ($isAdmin -and $env:COPYCORD_ALLOW_ROOT -ne '1') {
        if ($dryRun) { Warn 'running as Administrator; a real install refuses unless COPYCORD_ALLOW_ROOT=1' }
        else { Fail 'do not run this as Administrator; Copycord installs for your user. Set COPYCORD_ALLOW_ROOT=1 to do it anyway.' }
    }

    $needNode = $false
    if ($method -eq 'git') {
        $node = Get-Command node -ErrorAction SilentlyContinue
        $major = 0
        if ($node) { [void][int]::TryParse(((& node -v) -replace '^v', '').Split('.')[0], [ref]$major) }
        if ($major -lt $NodeMinMajor -or -not (Get-Command npm -ErrorAction SilentlyContinue)) { $needNode = $true }
    }
    $uv = Get-Command uv -ErrorAction SilentlyContinue

    # --- plan ---------------------------------------------------------------

    Out ''
    @(
        '    ___                             _ ',
        '   / __|___ _ __ _  _ __ ___ _ _ __| |',
        "  | (__/ _ \ '_ \ || / _/ _ \ '_/ _`` |",
        '   \___\___/ .__/\_, \__\___/_| \__,_|'
    ) | ForEach-Object { Out $_ Magenta }
    Out '           |_|   |__/' Magenta -NoNewline; Out '   installer' DarkGray
    Out ''
    Out '  Plan'
    if ($method -eq 'release') {
        PlanLine 'home' $homeDir
        PlanLine 'method' "release (GitHub release wheel$inferred)"
        PlanLine 'repo' $repo
        if ($version) { PlanLine 'version' "v$version" } else { PlanLine 'channel' "$channel (newest release)" }
    } else {
        PlanLine 'home' $homeDir
        PlanLine 'method' "git (build from source, no release needed$inferred)"
        PlanLine 'repo' $repoUrl
        PlanLine 'ref' $ref
        PlanLine 'checkout' $gitDir
        if ($needNode) { PlanLine 'node' "Node $NodeVersion into $nodeHome (per user)" }
        else { PlanLine 'node' "$(& node -v) at $((Get-Command node).Source)" }
    }
    if ($uv) { PlanLine 'uv' $uv.Source } else { PlanLine 'uv' "not found, will install it from $UvInstaller" }
    if ($verify) { PlanLine 'verify' 'copycord version and copycord doctor' }
    if (-not $onboard) { PlanLine 'onboard' 'skipped (-NoOnboard)' }
    if ($dryRun) { PlanLine 'dry run' 'nothing will be changed' }

    # uv, install, finishing up; release: find the release; git: fetch, build.
    $script:stepTotal = if ($method -eq 'git') { 5 } else { 4 }
    if ($needNode) { $script:stepTotal++ }
    if ($verify -or $onboard) { $script:stepTotal++ }

    try {
        # --- uv -------------------------------------------------------------

        Step 'Preparing uv'
        if ($uv) {
            Done "uv found: $(& uv --version)"
        } elseif ($dryRun) {
            Dry "irm $UvInstaller | iex"
        } else {
            Run 'Installing uv' 'powershell' @('-NoProfile', '-ExecutionPolicy', 'ByPass', '-Command', "irm $UvInstaller | iex")
            $env:Path = "$env:USERPROFILE\.local\bin;$env:Path"
            if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
                Fail 'uv was installed but is not on PATH; open a new terminal and run this again'
            }
        }

        if ($method -eq 'release') {
            # --- release method ---------------------------------------------

            Step 'Finding the release'
            if ($dryRun) {
                if ($version) { Dry "look up release v$version on $repoUrl/releases" }
                else { Dry "look up the newest $channel release on $repoUrl/releases" }
                $label = if ($version) { "v$version" } else { "$channel release" }
                Step 'Installing Copycord'
                Dry "uv tool install --force <copycord wheel of $label>"
            } else {
                try {
                    if ($version) {
                        $release = Invoke-RestMethod -Uri "$api/tags/v$version" -Headers $Headers -TimeoutSec 30
                    } elseif ($channel -eq 'beta') {
                        $list = Invoke-RestMethod -Uri "${api}?per_page=30" -Headers $Headers -TimeoutSec 30
                        $release = $list | Where-Object { -not $_.draft } | Select-Object -First 1
                    } else {
                        # GitHub's latest release: newest that is not a draft or a pre-release.
                        $release = Invoke-RestMethod -Uri "$api/latest" -Headers $Headers -TimeoutSec 30
                    }
                } catch {
                    $hint = if ($version) { " If $version is a branch, tag or commit, use -Version with that name, e.g. -Version v4-dev." } else { '' }
                    Fail "could not get the release from ${api}: $($_.Exception.Message)$hint"
                }
                if (-not $release) { Fail "no $channel release found on $repoUrl/releases" }
                $tag = $release.tag_name
                $wheel = $release.assets | Where-Object { $_.name -like 'copycord-*.whl' } | Select-Object -First 1
                if (-not $wheel) { Fail "release $tag has no wheel asset (copycord-*.whl); see $repoUrl/releases/tag/$tag" }
                Done "Release $tag"
                Info $wheel.browser_download_url

                Step 'Installing Copycord'
                Run "uv tool install copycord $tag" 'uv' @('tool', 'install', '--force', $wheel.browser_download_url)
                $label = $tag
            }
        } else {
            # --- git method -------------------------------------------------

            if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
                Fail 'git is not installed; install it (winget install --id Git.Git) and run this again'
            }
            if ((Test-Path $gitDir) -and -not (Test-Path (Join-Path $gitDir '.git')) -and (Get-ChildItem $gitDir -Force | Select-Object -First 1)) {
                Fail "$gitDir exists and is not a git checkout; move it or pass -GitDir"
            }

            if ($needNode) {
                Step "Installing Node $NodeVersion"
                $arch = switch ($env:PROCESSOR_ARCHITECTURE) { 'AMD64' { 'x64' } 'ARM64' { 'arm64' } default { Fail "no Node build for $env:PROCESSOR_ARCHITECTURE; install Node $NodeMinMajor+ yourself" } }
                $nodeName = "node-v$NodeVersion-win-$arch"
                if ($dryRun) {
                    Dry "download $NodeDist/v$NodeVersion/$nodeName.zip, check its sha256, unpack into $nodeHome"
                } else {
                    # The official zip, checked against SHASUMS256.txt; no admin rights needed.
                    Info "Node $NodeVersion into $nodeHome"
                    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("copycord-node-" + [guid]::NewGuid())
                    New-Item -ItemType Directory -Path $tmp | Out-Null
                    try {
                        $sums = (Invoke-WebRequest -UseBasicParsing -Uri "$NodeDist/v$NodeVersion/SHASUMS256.txt").Content
                        $line = ($sums -split "`n") | Where-Object { $_ -match "\s$([regex]::Escape("$nodeName.zip"))$" } | Select-Object -First 1
                        if (-not $line) { Fail "no checksum for $nodeName.zip" }
                        $expected = ($line -split '\s+')[0]
                        $zip = Join-Path $tmp 'node.zip'
                        Invoke-WebRequest -UseBasicParsing -Uri "$NodeDist/v$NodeVersion/$nodeName.zip" -OutFile $zip
                        $actual = (Get-FileHash -Algorithm SHA256 $zip).Hash.ToLower()
                        if ($actual -ne $expected) { Fail "Node sha256 mismatch: expected $expected, got $actual" }
                        New-Item -ItemType Directory -Force -Path $nodeHome | Out-Null
                        Remove-Item -Recurse -Force (Join-Path $nodeHome $nodeName) -ErrorAction SilentlyContinue
                        Expand-Archive -Path $zip -DestinationPath $nodeHome
                    } finally {
                        Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
                    }
                    $env:Path = "$(Join-Path $nodeHome $nodeName);$env:Path"
                    Done "node $(& node -v), npm $(& npm -v)"
                }
            }

            Step "Fetching $repo@$ref"
            if ($dryRun) {
                Dry "git clone $repoUrl.git $gitDir (or fetch when it exists)"
                Dry "git checkout --detach $ref"
            } else {
                if (Test-Path (Join-Path $gitDir '.git')) {
                    Run 'Updating the remote' 'git' @('-C', $gitDir, 'remote', 'set-url', 'origin', "$repoUrl.git")
                } else {
                    Run "Cloning $repoUrl" 'git' @('clone', '--filter=blob:none', '--no-checkout', "$repoUrl.git", $gitDir)
                }
                Run 'Fetching' 'git' @('-C', $gitDir, 'fetch', '--force', '--prune', '--tags', 'origin')
                & git -C $gitDir rev-parse -q --verify "refs/remotes/origin/$ref^{commit}" *> $null
                if ($LASTEXITCODE -eq 0) { $target = "refs/remotes/origin/$ref" }
                else {
                    & git -C $gitDir rev-parse -q --verify "refs/tags/$ref^{commit}" *> $null
                    if ($LASTEXITCODE -eq 0) { $target = "refs/tags/$ref" }
                    else {
                        Run "Fetching $ref" 'git' @('-C', $gitDir, 'fetch', '--force', 'origin', $ref)
                        $target = 'FETCH_HEAD'
                    }
                }
                Run "Checking out $ref" 'git' @('-C', $gitDir, 'checkout', '--force', '--detach', $target)
                Run 'Cleaning the checkout' 'git' @('-C', $gitDir, 'clean', '-fdx', '-e', 'code/admin/frontend/node_modules')
                $sha = (& git -C $gitDir rev-parse --short HEAD).Trim()
                Done "$ref at $sha"
            }

            Step 'Building the admin frontend'
            $frontend = Join-Path $gitDir 'code\admin\frontend'
            if ($dryRun) {
                Dry "cd $frontend; npm ci; npm run build"
            } else {
                # A fresh dist for this ref; hatch_build.py then ships it in the wheel.
                Remove-Item -Recurse -Force (Join-Path $frontend 'dist') -ErrorAction SilentlyContinue
                $npm = (Get-Command npm).Source
                Run 'npm ci' $npm @('ci', '--no-audit', '--no-fund') $frontend
                Run 'npm run build' $npm @('run', 'build') $frontend
            }

            Step 'Installing Copycord'
            if ($dryRun) {
                Dry "uv tool install --force --reinstall-package copycord $gitDir"
                $label = $ref
            } else {
                # --reinstall-package: uv would otherwise reuse a cached build of
                # the same directory from an older ref.
                Run "uv tool install copycord from $gitDir" 'uv' @('tool', 'install', '--force', '--reinstall-package', 'copycord', $gitDir)
                $label = "$ref ($sha)"
            }
        }

        # --- PATH and update channel ----------------------------------------

        Step 'Finishing up'
        $exe = 'copycord'
        if ($dryRun) {
            Dry "uv tool update-shell, when uv's tool bin directory is not on PATH"
        } else {
            $binDir = (& uv tool dir --bin)
            if ($LASTEXITCODE -ne 0 -or -not $binDir) { Fail 'could not get the uv tool bin directory' }
            $binDir = $binDir.Trim()
            if (($env:Path -split ';') -notcontains $binDir) {
                Run "Adding $binDir to PATH (uv tool update-shell)" 'uv' @('tool', 'update-shell')
                $env:Path = "$binDir;$env:Path"
            } else {
                Done "$binDir is on PATH"
            }
            $exe = Join-Path $binDir 'copycord.exe'
        }

        # Save a custom home where the CLI and the service find it without the
        # environment variable (common/paths.py reads ~/.copycord-home).
        $defaultHome = Join-Path $env:USERPROFILE '.copycord'
        if ($homeDir -ne $defaultHome) {
            if ($dryRun) { Dry "save $homeDir in $homePointer" }
            else {
                New-Item -ItemType Directory -Force -Path $homeDir | Out-Null
                Set-Content -Path $homePointer -Value $homeDir -Encoding UTF8
                Done "Saved the Copycord home in $homePointer"
            }
        } elseif ((Test-Path $homePointer) -and -not $dryRun) { Remove-Item -Force $homePointer }

        # The channel file (update.json) is written by `copycord update --no-install`,
        # so its format stays with the CLI.
        $channelArgs = @()
        if ($method -eq 'release') {
            if (-not $version -and $channelSet) { $channelArgs = @('--channel', $channel) }
        } elseif ($repo -eq $DefaultRepo) {
            $isBranch = $dryRun
            if (-not $dryRun) {
                & git -C $gitDir rev-parse -q --verify "refs/remotes/origin/$ref" *> $null
                $isBranch = $LASTEXITCODE -eq 0
            }
            if ($isBranch) { $channelArgs = @('--channel', 'dev', '--branch', $ref) }
            else { Info "$ref is not a branch: the ``copycord update`` channel is left as it is" }
        } else {
            Warn "``copycord update`` follows $DefaultRepo, not $repo; run this installer again to update from the fork"
        }
        if ($channelArgs) {
            if ($dryRun) { Dry "copycord update $($channelArgs -join ' ') --no-install" }
            else { Run "Saving the update channel ($($channelArgs -join ' '))" $exe ($channelArgs + '--no-install') }
        }

        # --- verify / onboard -----------------------------------------------

        if ($verify) {
            Step 'Verifying'
            if ($dryRun) { Dry 'copycord version; copycord doctor' }
            else {
                Run 'copycord version' $exe @('version')
                Get-Content $log | ForEach-Object { Out "    $_" }
                Run 'copycord doctor' $exe @('doctor')
                Get-Content $log | ForEach-Object { Out "    $_" }
            }
        } elseif ($onboard) {
            Step 'Checking the setup'
            if ($dryRun) { Dry 'copycord doctor' }
            else {
                & $exe doctor *> $log
                $doctorOk = $LASTEXITCODE -eq 0
                Get-Content $log | ForEach-Object { Out "    $_" }
                if ($doctorOk) { Done 'copycord doctor passed' }
                else { Warn 'copycord doctor reported problems; fix the FAIL lines above, then run it again' }
            }
        }
    } finally {
        Remove-Item $log -ErrorAction SilentlyContinue
    }

    # --- done ---------------------------------------------------------------

    Out ''
    if ($dryRun) {
        Out '  Dry run complete: ' Yellow -NoNewline; Out 'nothing was changed.'
        Out ''
        return
    }
    $port = if ($env:ADMIN_PORT) { $env:ADMIN_PORT } else { '8080' }
    $rule = [string]$box.H * 54
    Out ("  " + $box.TL + $rule + $box.TR) Magenta
    foreach ($text in @(
            "Copycord $label is installed",
            "Home: $homeDir",
            '',
            'Next:',
            '  copycord           start Copycord',
            '  copycord update    update it later',
            "  http://localhost:$port",
            '                     open the web UI once it runs'
        )) {
        Out ("  " + $box.V + ' ') Magenta -NoNewline
        Out ('{0,-52}' -f $text) -NoNewline
        Out (' ' + $box.V) Magenta
    }
    Out ("  " + $box.BL + $rule + $box.BR) Magenta
    Out ''
    Out '  Open a new terminal first if copycord is not found.' DarkGray
    Out ''
}

Install-Copycord -Opts @{
    InstallMethod = $InstallMethod; MethodSet = ($PSBoundParameters.ContainsKey('InstallMethod') -or [bool]$env:COPYCORD_INSTALL_METHOD); Git = $Git; Repo = $Repo; Version = $Version; Beta = $Beta
    GitDir = $GitDir; CopycordHome = $CopycordHome; NoOnboard = $NoOnboard; Verify = $Verify; DryRun = $DryRun
    ShowOutput = $ShowOutput; Help = $Help
}
