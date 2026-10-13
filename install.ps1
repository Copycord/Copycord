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
# Installs uv if it is missing, then Copycord with `uv tool install`, either
# from a release wheel (default) or from a git ref built locally, which needs
# git and Node 20+. Run with -Help for the parameters.

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
    [switch]$Verify,
    [switch]$DryRun,
    [switch]$ShowOutput,
    [switch]$Force,
    [switch]$Help
)

function Install-Copycord {
    param($Opts)
    # Inside the function so `irm | iex` leaves the caller's session settings alone.
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'

    $DefaultRepo = 'Copycord/Copycord'
    $PythonVersion = '3.12'  # Python the installer asks uv for: the recommended, tested version
    $UvInstaller = 'https://astral.sh/uv/install.ps1'
    $NodeDist = 'https://nodejs.org/dist'
    $NodeMinMajor = 20
    # Keep in step with NODE_VERSION in src/copycord/node_runtime.py.
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
                              saved as the copycord update channel
  -CopycordHome <path>        Copycord home: data, logs, backups, src\ and node\
                              (default: %USERPROFILE%\.copycord)
  -GitDir <path>              git: checkout directory
                              (default: <home>\src)
  -Verify                     after installing, run copycord --version and
                              copycord doctor and fail if either fails
  -DryRun                     print the plan and the actions, change nothing
  -ShowOutput                 stream the output of every step
  -Force                      reinstall even when this version is already
                              installed (without it the installer stops there)
  -Help                       show this help

Advanced:
  -InstallMethod release|git and -Git force the method instead of inferring it
  from -Version; COPYCORD_INSTALL_METHOD does too.

Environment (parameters win):
  COPYCORD_REPO, COPYCORD_VERSION, COPYCORD_CHANNEL=stable|beta,
  COPYCORD_BETA=1, COPYCORD_GIT_DIR, COPYCORD_HOME, COPYCORD_VERIFY=1,
  COPYCORD_DRY_RUN=1, COPYCORD_NODE_VERSION, COPYCORD_ALLOW_ROOT=1,
  COPYCORD_FORCE=1, NO_COLOR=1

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
    $verify = $Opts.Verify -or $env:COPYCORD_VERIFY -eq '1'
    $dryRun = $Opts.DryRun -or $env:COPYCORD_DRY_RUN -eq '1'
    $force = $Opts.Force -or $env:COPYCORD_FORCE -eq '1'
    $showOutput = [bool]$Opts.ShowOutput
    $version = if ($Opts.Version) { $Opts.Version } else { '' }
    # Without an explicit method, a -Version that looks like a release version
    # means release, anything else a git ref.
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
    # Clickable links (OSC 8) only in Windows Terminal and VS Code; the classic console shows the codes.
    $hyperlinks = $useColor -and ([bool]$env:WT_SESSION -or $env:TERM_PROGRAM -eq 'vscode')
    # Windows Terminal shows OSC 9;4 progress on the tab while a step runs.
    $progressBar = $useColor -and [bool]$env:WT_SESSION
    $esc = [char]27
    $okMark = if ($utf) { [string][char]0x2713 } else { '+' }
    $failMark = if ($utf) { [string][char]0x2717 } else { 'x' }
    $dotMark = if ($utf) { [string][char]0x2022 } else { '-' }
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
    function Info([string]$Text) { Out "  $dotMark " DarkGray -NoNewline; Out $Text }
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

    function Spent([int]$Seconds) {
        if ($Seconds -lt 60) { return "${Seconds}s" }
        return '{0}m {1:00}s' -f [Math]::Floor($Seconds / 60), ($Seconds % 60)
    }

    $log = [IO.Path]::GetTempFileName()
    function Run([string]$Label, [string]$File, [string[]]$Arguments, [string]$Cwd = (Get-Location).Path) {
        # Output goes to $log; on failure print its tail and stop.
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
        $clock = [Diagnostics.Stopwatch]::StartNew()
        $tail = ''
        # The finally also runs on Ctrl-C, so the tab never stays busy.
        try {
            if ($progressBar) { Write-Host "$esc]9;4;3;0$esc\" -NoNewline }
            while (-not $p.HasExited) {
                if ($useColor) {
                    # The time spent shows from 3 seconds on.
                    $secs = [int][Math]::Floor($clock.Elapsed.TotalSeconds)
                    if ($secs -ge 3) { $tail = '  ' + (Spent $secs) }
                    Out ("`r  " + $frames[$i % $frames.Count] + " $Label") Magenta -NoNewline
                    if ($tail) { Out $tail DarkGray -NoNewline }
                }
                $i++
                Start-Sleep -Milliseconds 100
            }
            $p.WaitForExit()
        } finally {
            if ($progressBar) { Write-Host "$esc]9;4;0;0$esc\" -NoNewline }
        }
        if ($useColor) { Write-Host ("`r" + (' ' * ($Label.Length + $tail.Length + 6)) + "`r") -NoNewline }
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

    # An upgrade or reinstall ends with `copycord doctor --fix`.
    $upgrade = [bool](Get-Command copycord -ErrorAction SilentlyContinue) -or
        (Test-Path (Join-Path $env:USERPROFILE '.local\bin\copycord.exe')) -or
        (Test-Path (Join-Path $homeDir '.env')) -or (Test-Path (Join-Path $homeDir 'data.db'))

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

    # --- already installed? -------------------------------------------------

    function RemoteHead([string]$Name) {
        # Commit of a branch or tag of $repoUrl (a tag peeled), or $null.
        $ErrorActionPreference = 'Continue'
        if (-not (Get-Command git -ErrorAction SilentlyContinue)) { return $null }
        $env:GIT_TERMINAL_PROMPT = '0'
        $lines = @(& git ls-remote "$repoUrl.git" "refs/heads/$Name" "refs/tags/$Name" "refs/tags/$Name^{}" 2>$null)
        if ($LASTEXITCODE -ne 0) { return $null }
        $found = @{}
        foreach ($line in $lines) {
            $parts = "$line" -split '\s+'
            if ($parts.Count -ge 2) { $found[$parts[1]] = $parts[0] }
        }
        foreach ($key in @("refs/heads/$Name", "refs/tags/$Name^{}", "refs/tags/$Name")) {
            if ($found.ContainsKey($key)) { return $found[$key] }
        }
        return $null
    }

    function InstalledExe {
        # The installed copycord.exe, or $null.
        $exe = Get-Command copycord -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($exe) { return $exe.Source }
        foreach ($dir in @($env:UV_TOOL_BIN_DIR, $env:XDG_BIN_HOME, (Join-Path $env:USERPROFILE '.local\bin'))) {
            if ($dir -and (Test-Path (Join-Path $dir 'copycord.exe'))) { return Join-Path $dir 'copycord.exe' }
        }
        return $null
    }

    $script:wasRunning = $false
    function StopRunning {
        # A running Copycord holds its files open, so the reinstall stops it first
        # (copycord service stop waits until its processes are gone) and starts it after.
        if ($dryRun) { Dry 'copycord service stop, when Copycord runs as a service (started again after)'; return }
        $old = InstalledExe
        if (-not $old) { return }
        $prevPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        $running = $false
        try {
            $doc = (& $old service status --json 2>$null) -join "`n"
            if ($doc) { $running = [bool](($doc | ConvertFrom-Json).running) }
        } catch { $running = $false } finally { $ErrorActionPreference = $prevPreference }
        if (-not $running) { return }
        Run 'Stopping Copycord (copycord service stop)' $old @('service', 'stop')
        $script:wasRunning = $true
    }

    function AlreadyInstalled {
        # The installed label when it is what this run would install, else $null.
        # Any doubt (no network, unknown output) means install.
        # Continue: stderr from a native command must not stop the check.
        $ErrorActionPreference = 'Continue'
        $exePath = InstalledExe
        if (-not $exePath) { return $null }
        $oldNoColor = $env:NO_COLOR
        $env:NO_COLOR = '1'
        try { $out = @(& $exePath --version 2>$null) } finally { $env:NO_COLOR = $oldNoColor }
        if ($LASTEXITCODE -ne 0 -or $out.Count -lt 2) { return $null }
        if ("$($out[0])" -notmatch '^(\S+)(?: \(([0-9a-f]{7,40})\))?$') { return $null }
        $haveVersion = $Matches[1]
        $haveCommit = if ($Matches.Count -gt 2) { $Matches[2] } else { '' }
        # "dev channel · v4-dev" on line 2; older versions print "channel dev (branch v4-dev)".
        $chanLine = $out | Where-Object { "$_" -like 'channel *' } | Select-Object -First 1
        if ($chanLine) {
            $chan = "$chanLine".Substring(8)
        } elseif ("$($out[1])" -match '^(stable|beta|dev) channel(?:\s+\S+\s+(\S+))?$') {
            $chan = if ($Matches[1] -eq 'dev') { "dev (branch $($Matches[2]))" } else { $Matches[1] }
        } else { return $null }
        if ($method -eq 'git') {
            if (-not $haveCommit) { return $null }
            if ($ref -cmatch '^[0-9a-f]{7,40}$') {
                $want = $ref
            } else {
                # The installer saves this channel for a branch of the default repo.
                if ($repo -eq $DefaultRepo -and $chan -ne "dev (branch $ref)") { return $null }
                $want = RemoteHead $ref
            }
            if (-not $want -or -not $want.StartsWith($haveCommit)) { return $null }
            return "$ref ($($haveCommit.Substring(0, 7)))"
        }
        if ($chan -notin @('stable', 'beta')) { return $null }
        if ($version) {
            $want = $version
        } else {
            if ($channelSet -and $chan -ne $channel) { return $null }
            if ($channel -eq 'beta') {
                $list = Invoke-RestMethod -Uri "${api}?per_page=30" -Headers $Headers -TimeoutSec 30
                $want = ($list | Where-Object { -not $_.draft } | Select-Object -First 1).tag_name
            } else {
                $want = (Invoke-RestMethod -Uri "$api/latest" -Headers $Headers -TimeoutSec 30).tag_name
            }
            if (-not $want) { return $null }
            $want = "$want".TrimStart('v')
        }
        if ($haveVersion.TrimStart('v') -ne $want) { return $null }
        return "v$want"
    }

    if (-not $force -and -not $dryRun) {
        $have = $null
        try { $have = AlreadyInstalled } catch { $have = $null }
        if ($have) {
            Done "Copycord $have is already installed"
            Info 'Run copycord update to update it, or rerun with -Force to reinstall.'
            Out ''
            return
        }
    }

    # Only a dry run prints the plan.
    if ($dryRun) {
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
        if ($upgrade) { PlanLine 'repair' 'copycord doctor --fix (an upgrade or reinstall)' }
        if ($verify) { PlanLine 'verify' 'copycord --version and copycord doctor' }
        PlanLine 'dry run' 'nothing will be changed'
    }

    $script:stepTotal = if ($method -eq 'git') { 5 } else { 4 }
    if ($needNode) { $script:stepTotal++ }
    if ($upgrade) { $script:stepTotal++ }
    if ($verify) { $script:stepTotal++ }

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
                if ($upgrade) { StopRunning }
                Dry "uv tool install --force --python $PythonVersion <copycord wheel of $label>"
            } else {
                try {
                    if ($version) {
                        $release = Invoke-RestMethod -Uri "$api/tags/v$version" -Headers $Headers -TimeoutSec 30
                    } elseif ($channel -eq 'beta') {
                        $list = Invoke-RestMethod -Uri "${api}?per_page=30" -Headers $Headers -TimeoutSec 30
                        $release = $list | Where-Object { -not $_.draft } | Select-Object -First 1
                    } else {
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
                StopRunning
                Run "uv tool install copycord $tag" 'uv' @('tool', 'install', '--force', '--python', $PythonVersion, $wheel.browser_download_url)
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
            # The installer resets and cleans the checkout, so it only touches one it
            # made (marked in .git) or a clean Copycord checkout of $repoUrl.
            $gitMarker = Join-Path $gitDir '.git\copycord-installer'
            if ((Test-Path (Join-Path $gitDir '.git')) -and -not (Test-Path $gitMarker)) {
                $prevPreference = $ErrorActionPreference
                $ErrorActionPreference = 'Continue'
                try {
                    $origin = "$(& git -C $gitDir remote get-url origin 2>$null)".Trim()
                    $changes = @(& git -C $gitDir status --porcelain --untracked-files=no 2>$null)
                    $statusOk = $LASTEXITCODE -eq 0
                } finally { $ErrorActionPreference = $prevPreference }
                $want = "$repoUrl" -replace '\.git$', ''
                if (($origin -replace '\.git$', '' -replace '/$', '') -ne $want) {
                    Fail "$gitDir is a git checkout of $(if ($origin) { $origin } else { 'something else' }), not $repoUrl. The installer would reset it and delete files in it, so it stops here. Pass -GitDir with a new folder."
                }
                if (-not $statusOk -or $changes.Count -gt 0) {
                    Fail "$gitDir has changes that are not committed. The installer would reset it and lose them, so it stops here. Commit or move them, or pass -GitDir with a new folder."
                }
            }

            if ($needNode) {
                Step "Installing Node $NodeVersion"
                $arch = switch ($env:PROCESSOR_ARCHITECTURE) { 'AMD64' { 'x64' } 'ARM64' { 'arm64' } default { Fail "no Node build for $env:PROCESSOR_ARCHITECTURE; install Node $NodeMinMajor+ yourself" } }
                $nodeName = "node-v$NodeVersion-win-$arch"
                if (Test-Path (Join-Path (Join-Path $nodeHome $nodeName) 'npm.cmd')) {
                    $env:Path = "$(Join-Path $nodeHome $nodeName);$env:Path"
                    Done "reusing $(Join-Path $nodeHome $nodeName)"
                } elseif ($dryRun) {
                    Dry "download $NodeDist/v$NodeVersion/$nodeName.zip, check its sha256, unpack into $nodeHome"
                } else {
                    # The official zip, checked against SHASUMS256.txt.
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
                # Marks the checkout as the installer's, so a later run may reset it.
                if (-not (Test-Path $gitMarker)) { [IO.File]::WriteAllText($gitMarker, "made by the Copycord installer`n") }
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
                Run 'Cleaning the checkout' 'git' @('-C', $gitDir, 'clean', '-fdx', '-e', 'src/copycord/admin/frontend/node_modules')
                $sha = (& git -C $gitDir rev-parse --short HEAD).Trim()
                Done "$ref at $sha"
            }

            Step 'Building the admin frontend'
            $frontend = Join-Path $gitDir 'src\copycord\admin\frontend'
            if ($dryRun) {
                Dry "cd $frontend; npm ci; npm run build"
            } else {
                Remove-Item -Recurse -Force (Join-Path $frontend 'dist') -ErrorAction SilentlyContinue
                # npm.cmd, not npm.ps1: Start-Process cannot launch a .ps1 directly.
                $npm = (Get-Command npm.cmd).Source
                Run 'npm ci' $npm @('ci', '--no-audit', '--no-fund') $frontend
                Run 'npm run build' $npm @('run', 'build') $frontend
            }

            Step 'Installing Copycord'
            if ($upgrade) { StopRunning }
            if ($dryRun) {
                Dry "uv tool install --force --python $PythonVersion --reinstall-package copycord $gitDir"
                $label = $ref
            } else {
                # Otherwise uv reuses a cached build of this directory from an older ref.
                Run "uv tool install copycord from $gitDir" 'uv' @('tool', 'install', '--force', '--python', $PythonVersion, '--reinstall-package', 'copycord', $gitDir)
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

        # Save a custom home in ~/.copycord-home so the CLI and service find it.
        $defaultHome = Join-Path $env:USERPROFILE '.copycord'
        if ($homeDir -ne $defaultHome) {
            if ($dryRun) { Dry "save $homeDir in $homePointer" }
            else {
                New-Item -ItemType Directory -Force -Path $homeDir | Out-Null
                # No BOM: Windows PowerShell's -Encoding UTF8 writes one.
                [IO.File]::WriteAllText($homePointer, $homeDir, (New-Object Text.UTF8Encoding $false))
                Done "Saved the Copycord home in $homePointer"
            }
        } elseif ((Test-Path $homePointer) -and -not $dryRun) { Remove-Item -Force $homePointer }

        # Write <home>\.env once, never overwriting it. The installed CLI writes it
        # (copycord config init), so the default settings live in one place.
        $envFile = Join-Path $homeDir '.env'
        if (-not (Test-Path $envFile)) {
            if ($dryRun) { Dry "write the settings file $envFile (copycord config init)" }
            else {
                # Windows PowerShell turns stderr lines into errors under 'Stop'.
                $prevPreference = $ErrorActionPreference
                $ErrorActionPreference = 'Continue'
                try { $wrote = & $exe config init 2>> "$log.err" } finally { $ErrorActionPreference = $prevPreference }
                if ($LASTEXITCODE -eq 0 -and $wrote) {
                    $envFile = ([string]@($wrote)[-1]).Trim()
                    Done "Wrote the settings file $envFile"
                } else {
                    Warn "could not write $envFile; copycord config set creates it later"
                }
            }
        }

        # The CLI writes update.json, so its format lives in one place.
        $channelArgs = @()
        if ($method -eq 'release') {
            if (-not $version -and $channelSet) { $channelArgs = @('--channel', $channel) }
        } elseif ($repo -eq $DefaultRepo) {
            $isBranch = $dryRun
            if (-not $dryRun) {
                & git -C $gitDir rev-parse -q --verify "refs/remotes/origin/$ref" *> $null
                $isBranch = $LASTEXITCODE -eq 0
            }
            if ($isBranch) { $channelArgs = @('--channel', 'dev', '--ref', $ref) }
            else { Info "$ref is not a branch: the copycord update channel is left as it is" }
        } else {
            Warn "copycord update follows $DefaultRepo, not $repo; run this installer again to update from the fork"
        }
        if ($channelArgs) {
            if ($dryRun) { Dry "copycord update $($channelArgs -join ' ') --no-install" }
            else { Run "Saving the update channel ($($channelArgs -join ' '))" $exe (@('update') + $channelArgs + '--no-install') }
        }

        # --- repair ---------------------------------------------------------

        # Piped, the doctor never prompts. Its exit status does not fail the install.
        if ($upgrade) {
            Step 'Repairing (copycord doctor --fix)'
            if ($dryRun) { Dry 'copycord doctor --fix --brief' }
            else {
                # Windows PowerShell turns stderr lines into errors under 'Stop'.
                $prevPreference = $ErrorActionPreference
                $prevNoColor = $env:NO_COLOR
                $ErrorActionPreference = 'Continue'
                # Plain output, restyled with the installer's marks.
                $env:NO_COLOR = '1'
                try {
                    & $exe doctor --fix --brief 2>&1 | ForEach-Object {
                        $line = "$_"
                        if ($line -match '^([+x!]|\u2713|\u2717) (.*)$') {
                            $text = $Matches[2]
                            if ($Matches[1] -eq '!') { Warn $text }
                            elseif ($Matches[1] -in @('x', [string][char]0x2717)) { Out "  $failMark " Red -NoNewline; Out $text }
                            else { Done $text }
                        }
                        elseif ($line.StartsWith('PASS  ')) { Done $line.Substring(6) }
                        elseif ($line.StartsWith('WARN  ')) { Warn $line.Substring(6) }
                        elseif ($line.StartsWith('FAIL  ')) { Out "  $failMark " Red -NoNewline; Out $line.Substring(6) }
                        else { Out "    $line" }
                    }
                } finally {
                    $ErrorActionPreference = $prevPreference
                    $env:NO_COLOR = $prevNoColor
                }
                $global:LASTEXITCODE = 0
            }
        }

        # Start again what the reinstall stopped.
        if ($script:wasRunning) {
            $script:wasRunning = $false
            Run 'Starting Copycord again (copycord service start)' $exe @('service', 'start')
        }

        # --- verify ---------------------------------------------------------

        if ($verify) {
            Step 'Verifying'
            if ($dryRun) { Dry 'copycord --version; copycord doctor' }
            else {
                Run 'copycord --version' $exe @('--version')
                Get-Content $log | ForEach-Object { Out "    $_" }
                Run 'copycord doctor' $exe @('doctor')
                Get-Content $log | ForEach-Object { Out "    $_" }
            }
        }
    } finally {
        Remove-Item $log -ErrorAction SilentlyContinue
        # A failed install still starts the Copycord it stopped.
        if ($script:wasRunning) {
            $again = InstalledExe
            $global:LASTEXITCODE = 1
            if ($again) { try { & $again service start *> $null } catch { $global:LASTEXITCODE = 1 } }
            if (-not $again -or $LASTEXITCODE -ne 0) { Warn 'Copycord was stopped for the install; start it with: copycord service start' }
        }
    }

    # --- done ---------------------------------------------------------------

    Out ''
    if ($dryRun) {
        Out '  Dry run complete: ' Yellow -NoNewline; Out 'nothing was changed.'
        Out ''
        return
    }
    function BoxLine([string]$Text) {
        Out ("  " + $box.V + ' ') Magenta -NoNewline
        Out ('{0,-52}' -f $Text) -NoNewline
        Out (' ' + $box.V) Magenta
    }
    function BoxLink([string]$Url) {
        # A box row with the URL, clickable where the terminal supports it.
        $row = '{0,-52}' -f "    $Url"
        if ($hyperlinks) { $row = $row.Replace($Url, "$esc]8;;$Url$esc\$Url$esc]8;;$esc\") }
        Out ("  " + $box.V + ' ') Magenta -NoNewline
        Out $row -NoNewline
        Out (' ' + $box.V) Magenta
    }
    function BoxItem([string]$Text) {
        Out ("  " + $box.V + ' ') Magenta -NoNewline
        Out '! ' Yellow -NoNewline
        Out ('{0,-50}' -f $Text) -NoNewline
        Out (' ' + $box.V) Magenta
    }
    function EnvValue([string]$Key) {
        # A setting from <home>\.env through the CLI, else the file's own line, else ''.
        $prevPreference = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        $value = ''
        try {
            $doc = (& $exe config get $Key --show-secrets --json 2>$null) -join "`n"
            if ($LASTEXITCODE -eq 0 -and $doc) { $value = [string](($doc | ConvertFrom-Json).value) }
        } catch { $value = '' } finally { $ErrorActionPreference = $prevPreference }
        if (-not $value -and (Test-Path $envFile)) {
            $line = Get-Content $envFile | Where-Object { $_ -match "^\s*$Key\s*=" } | Select-Object -Last 1
            if ($line) { $value = ($line -replace "^\s*$Key\s*=\s*", '').Trim().Trim('"', "'") }
        }
        return $value
    }
    # The dashboard address and password come from the settings file, not this shell.
    $port = EnvValue 'ADMIN_PORT'
    if ($port -notmatch '^\d+$') { $port = '8080' }
    $dashHost = EnvValue 'DASHBOARD_HOST'
    if (-not $dashHost -or $dashHost -in @('127.0.0.1', '0.0.0.0', '::', '::1')) { $dashHost = 'localhost' }
    elseif ($dashHost -match ':') { $dashHost = "[$dashHost]" }
    $password = EnvValue 'PASSWORD'
    $rule = [string]$box.H * 54
    Out ("  " + $box.TL + $rule + $box.TR) Magenta
    foreach ($text in @(
            "Copycord $label is installed",
            "Home: $homeDir",
            "Settings: $envFile",
            '',
            'Next:',
            '  copycord run       start Copycord',
            '  copycord restart   apply a changed setting',
            '  copycord update    update it later',
            '',
            'Needs your attention:'
        )) { BoxLine $text }
    BoxItem 'Finish the setup in the dashboard once it runs:'
    BoxLink "http://${dashHost}:$port"
    if ($password) {
        BoxLine "    password: $password"
        BoxLine '    (PASSWORD in the settings file above)'
    }

    Out ("  " + $box.BL + $rule + $box.BR) Magenta
    Out ''
    Out '  Open a new terminal first if copycord is not found.' DarkGray
    Out ''
}

Install-Copycord -Opts @{
    InstallMethod = $InstallMethod; MethodSet = ($PSBoundParameters.ContainsKey('InstallMethod') -or [bool]$env:COPYCORD_INSTALL_METHOD); Git = $Git; Repo = $Repo; Version = $Version; Beta = $Beta
    GitDir = $GitDir; CopycordHome = $CopycordHome; Verify = $Verify; DryRun = $DryRun
    ShowOutput = $ShowOutput; Force = $Force; Help = $Help
}
