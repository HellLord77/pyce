$ScriptVersion = "0.3.15"
################################################################################
$PythonOptimize = 2  # FIXME https://github.com/pyinstaller/pyinstaller/issues/3379
$PythonHashSeed = 0

$EntryPoint = "src/init.py"
$Icon = ""
$Version = ""
$Manifest = ""
$Contents = "."
$OneFile = $False
$NoConsole = $False
$ElevatedProc = $False
$RemoteProc = $False
$UPX = $False
$ModuleGraph = $True
$Debug = $False
$Datas = @()
$Datas32 = @()
$Datas64 = @()
$Imports = @()
$Excludes = @()
$HooksDirs = @()
$RuntimeHooks = @()
$ExtraArgs = @()

$CythonSourceGlobs = @(
    "src/main.py")
# $CythonSourceGlobs = @()
$CythonExcludeGlobs = @()  # FIXME https://github.com/cython/cython/issues/5542
$CythonNoDocstrings = $True
$CythonRemove = $True
$CythonExtraArgs = @()

$NuitkaSources = @()
$NuitkaRemove = $True
$NuitkaExtraArgs = @()

$mypycSources = @()
$mypycRemove = $True
$mypycExtraArgs = @()

$CodeRunBefore = @()
$CodeRunBeforeRemote = @()
$CodeRunAfter = @()
$CodeRunAfterRemote = @()

$UPXDir = ""
$MinifyJsonRegExs = @()
################################################################################
$CodePythonIs64Bit = @(
    "from sys import maxsize"
    "print(maxsize > 2 ** 32)")
$CodeExtSuffix = @(
    "from sysconfig import get_config_var"
    "print(get_config_var('EXT_SUFFIX'))")
$CodeSysTag = @(
    "from setuptools._vendor.packaging import tags"
    "tag = next(tags.sys_tags())"
    "print(f'{tag._interpreter}-{tag.platform}')"
)
$CodeGlobTemplate = @(
    "from Cython.Build.Dependencies import extended_iglob"
    "print(';'.join(extended_iglob(r'{0}')).replace('\\', '/'))")
$CodeModuleGraphTemplate = @(
    "from itertools import islice"
    "from sys import argv"
    "from PyInstaller.lib.modulegraph.__main__ import create_graph, parse_arguments"
    "from PyInstaller.lib.modulegraph.modulegraph import ExcludedModule"
    "argv.append(r'{0}')"
    "options = parse_arguments()"
    "graph = create_graph(options.scripts, options.domods, options.debug, options.excludes, options.addpath)"
    "print(';'.join(m.identifier for m in islice(graph.iter_graph(), 1, None) if not isinstance(m, ExcludedModule)))")
$CodeModuleGraphSmartTemplate = @(
    "from sys import path"
    "from PyInstaller.building.build_main import Analysis, initialize_modgraph"
    "from PyInstaller.config import CONF"
    "path.append(r'{0}')"
    "CONF['workpath'] = r'{0}'"
    "analysis = Analysis.__new__(Analysis)"
    "graph = initialize_modgraph(r'{0}'.split(), r'{1}'.split(';') if r'{1}' else [])"
    "graph.add_script(r'{0}')"
    "graph.process_post_graph_hooks(analysis)"
    "print(';'.join(module.identifier for module in graph.iter_graph()))")
$MinifyJsonLocal = $False
$UPXLocal = $False
$UpdateManifest = $True
$ModuleGraphSmart = $True
$ModuleGraphReduce = $True
$StripSymbols = $False
$CythonCPP = $False
$CythonRemoveC = $False
$NuitkaRemoveBuild = $True
$RemoveOnThrow = $True
$ForceLocal = $False
$mypycCacheDir = ".mypy_cache"
$PatSemVer = "(?<major>0|[1-9]\d*)\.(?<minor>0|[1-9]\d*)\.(?<patch>0|[1-9]\d*)(?:-(?<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?"

function Get-IntFromBytes([byte[]]$Bytes, [System.Numerics.BigInteger[]]$Range = $null) {
    $Integer = [System.Numerics.BigInteger]::new($Bytes)
    if ($Range) { $Integer = $Integer % $Range[1] + $Range[0] }
    return $Integer
}

function ToArray($Object) {
    if ($Object -isnot [array]) { $Object = @($Object) }
    return @($Object)
}

function Get-Memoized([System.Management.Automation.FunctionInfo]$Function) {
    $Memo = @{}
    {
        if (-not $Memo.ContainsKey("$args")) {
            Write-Host "[Memoize] $Function $args"
            $Memo.Add("$args", (& $Function.ScriptBlock $args))
        }
        return $Memo["$args"]
    }.GetNewClosure()
}

function Get-InsertedArray([array]$Array, $Index, $Value) {
    if ($Index -lt 0 -or $Index -gt $Array.Length) { throw }
    elseif ($Index -eq 0) { $Array = $Value + $Array }
    elseif ($Index -eq $Array.Length) { $Array = $Array + $Value }
    else { $Array = $Array[0..($Index - 1)] + $Value + $Array[$Index..($Array.Length - 1)] }
    return ToArray $Array
}

function Get-RemovedArray([array]$Array, $Index) {
    if ($Index -lt 0 -or $Index -ge $Array.Length) { throw }
    elseif ($Index -eq 0) { $Array = $Array[1..($Array.Length - 1)] }
    elseif ($Index -eq ($Array.Length - 1)) { $Array = $Array[0..($Array.Length - 2)] }
    else { $Array = $Array[0..($Index - 1)] + $Array[($Index + 1)..($Array.Length - 1)] }
    return ToArray $Array
}

function Get-RemovedItemArray([array]$Array, $Item, [int]$Count = 1) {
    for ($i = 0; $i -lt $Count; $i++) {
        if (-not $Array) { break }
        $Index = $Array.IndexOf($Item)
        if ($Index -eq -1) { break }
        else { $Array = Get-RemovedArray $Array $Index }
    }
    return ToArray $Array
}

function Start-Base64Process([string]$Base64, [string]$ArgList = "") {
    $TempFile = New-TemporaryFile
    [IO.File]::WriteAllBytes($TempFile, [Convert]::FromBase64String($Base64))
    Start-Process $TempFile -ArgumentList $ArgList -Wait
    $TempFile.Delete()
}

function Get-ExeSize([System.IO.FileStream]$Stream) {
    $Buffer = [byte[]]::New(4096)
    $Stream.Read($Buffer, 0, 4096) | Out-Null
    $HeaderOffset = [System.BitConverter]::ToInt32($Buffer, 60)
    $HeadersSize = 248
    if ([System.BitConverter]::ToUInt16($Buffer, $HeaderOffset + 4) -eq 0x8664) {
        $HeadersSize += 16
    }
    $SectionCount = [System.BitConverter]::ToUInt16($Buffer, $HeaderOffset + 6)
    $MaxPointer = 0
    $Size = 0
    for ($i = 0; $i -lt $SectionCount; $i++) {
        $SectionOffset = $HeaderOffset + $HeadersSize + $i * 40
        $RawDataPointer = [System.BitConverter]::ToInt32($Buffer, $SectionOffset + 20)
        if ($RawDataPointer -gt $MaxPointer) {
            $MaxPointer = $RawDataPointer
            $Size = $MaxPointer + [System.BitConverter]::ToInt32($Buffer, $SectionOffset + 16)
        }
    }
    return $Size
}

function Copy-File([string]$Source, [string]$Destination, [int]$Timeout = 0) {
    Write-Host "[Copy] $Source -> $Destination"
    New-Item (Split-Path $Destination -Parent) -ItemType Directory -ErrorAction SilentlyContinue
    $EndTime = [int](Get-Date -UFormat %s) + $Timeout
    do {
        Copy-Item $Source -Destination $Destination -ErrorAction SilentlyContinue
        Start-Sleep 0.01
    } while (($EndTime -ge ([int](Get-Date -UFormat %s))) -and -not (Test-Path $Destination -PathType Leaf))
}

function MinifyJsonFile([string]$Path, [string]$OutPath) {
    if (-not $OutPath) { $OutPath = $Path }
    Write-Host "[Minify] $Path -> $OutPath"
    ConvertFrom-Json (Get-Content -Raw $Path) | ConvertTo-Json -Depth 100 -Compress | Set-Content $OutPath
}

function Get-DeveloperPath {
    $InstallPath = & (Install-PackageChoco "vswhere") -latest -property installationPath
    & "${Env:COMSPEC}" /s /c "`"$InstallPath\Common7\Tools\vsdevcmd.bat`" -no_logo && set" | ForEach-Object {
        $Name, $Value = $_ -Split '=', 2
        if ($Name -eq "PATH") { return $Value }
    }
}

function Update-Manifest([string]$ExePath, [string]$ManifestPath) {
    $TempFile = New-TemporaryFile
    Copy-Item $ExePath -Destination $TempFile
    if (-not (Get-Command mt -ErrorAction SilentlyContinue)) { $Env:PATH += ";$(Get-DeveloperPath)" }
    mt -updateresource:"$ExePath;#1" -manifest "$ManifestPath" -nologo
    $TempStream = [System.IO.File]::OpenRead($TempFile)
    $ExeStream = [System.IO.File]::OpenWrite($ExePath)
    $TempStream.Seek($( Get-ExeSize $TempStream ), [System.IO.SeekOrigin]::Begin) | Out-Null
    $ExeStream.Seek(0, [System.IO.SeekOrigin]::End) | Out-Null
    $TempStream.CopyTo($ExeStream)
    $ExeStream.Close()
    $TempStream.Close()
    $TempFile.Delete()
}

function Install-PackagePip($Package, $Import = "", $Force = $False) {
    if (-not $Import) { $Import = $Package }
    if (-not $Force) {
        Start-PythonCode "import $Import" $False | Out-Null
        if (-not $?) { $Force = $True }
    }
    if ($Force) {
        Write-Host "[pip] $Package"
        pip install $Package --upgrade | Write-Host
    }
    return (Start-PythonCode "import $Import; print($Import.__file__)" $False)
}

function Install-PackageChoco($Package, $Command = "", $Force = $False) {
    if (-not $Command) { $Command = $Package }
    if (-not $Force) {
        Get-Command $Command -ErrorAction SilentlyContinue | Out-Null
        if (-not $?) { $Force = $True }
    }
    if ($Force) {
        Write-Host "[choco] $Package"
        choco install $Package --yes | Write-Host
    }
    return (Get-Command $Command).Path
}

function Start-PythonCode([string[]]$Lines, [bool]$Verbose = $True, [bool]$Throw = $True) {
    $Code = $Lines -Join "; "
    if ($Verbose) { Write-Host "[python] $Code" }
    try { return python -c $Code }
    finally { if (-not $? -and $Throw) { throw } }
}

function Get-ProjectName {
    return Split-Path $( if ($IsRemote) { $Env:GITHUB_REPOSITORY }
        else { Split-Path -Path (Get-Location) -Leaf } ) -Leaf
}

function Get-IsPython64Bit {
    return [System.Convert]::ToBoolean((Start-PythonCode $CodePythonIs64Bit $False))
}

$GetIsPython64Bit = Get-Memoized (Get-Command Get-IsPython64Bit)

function Get-ExtSuffix {
    return Start-PythonCode $CodeExtSuffix $False
}

$GetExtSuffix = Get-Memoized (Get-Command Get-ExtSuffix)

function Get-ModuleGraph([bool]$Smart = $True) {
    if ($Smart) {
        $TempDir = Join-Path $Env:TEMP (New-Guid)
        New-Item $TempDir -ItemType Directory | Out-Null
        $CodeModuleGraphSmartProcess = @() + $CodeModuleGraphSmartTemplate
        $CodeModuleGraphSmartProcess[3] = $CodeModuleGraphSmartTemplate[3] -f (Split-Path $EntryPoint -Parent)
        $CodeModuleGraphSmartProcess[4] = $CodeModuleGraphSmartTemplate[4] -f $TempDir
        $CodeModuleGraphSmartProcess[6] = $CodeModuleGraphSmartTemplate[6] -f "$Excludes", ($HooksDirs -Join ";")
        $CodeModuleGraphSmartProcess[7] = $CodeModuleGraphSmartTemplate[7] -f $EntryPoint
        $Modules = (Start-PythonCode $CodeModuleGraphSmartProcess $False) -Split ";"
        $Modules = Get-RemovedItemArray $Modules (Resolve-Path $EntryPoint).ToString()
    }
    else {
        # TODO ? fix (ignores hooks)
        $CodeModuleGraph = @() + $CodeModuleGraphTemplate
        $CodeModuleGraph[4] = $CodeModuleGraphTemplate[4] -f $EntryPoint
        foreach ($Exclude in $Excludes) {
            $CodeModuleGraph = Get-InsertedArray $CodeModuleGraph 5 ($CodeModuleGraphTemplate[4] -f "-x=$Exclude")
        }
        $Modules = (Start-PythonCode $CodeModuleGraph $False) -Split ";"
    }
    return ToArray $Modules
}

function Get-PyInstallerArgs {
    $ArgList = @("--noconfirm")
    if ($OneFile) { $ArgList += "--onefile" }
    if ($Contents) { $ArgList += "--contents-directory=$Contents" }
    $BaseSrcDir = Split-Path (Split-Path $EntryPoint -Parent) -Leaf
    foreach ($Data in $($Datas + $(If (& $GetIsPython64Bit) { $Datas64 } else { $Datas32 }))) {
        $DataSrc = Join-Path $BaseSrcDir $Data
        if ($Data.Contains(";")) {
            $DataParts = $DataSrc -Split ";"
            $DataSrc = $DataParts[0]
            $DataDst = $DataParts[1]
        }
        else {
            if (Test-Path $DataSrc -PathType Leaf) { $DataDst = Split-Path $Data -Parent }
            else { $DataDst = $Data }
        }
        if (-not $DataDst) { $DataDst = "." }
        $ArgList += "--add-data=""$DataSrc;$DataDst"""
    }

    foreach ($Import in $Imports) { $ArgList += "--hidden-import=$Import" }
    if ($ModuleGraph) {
        $Modules = Get-ModuleGraph $ModuleGraphSmart
        foreach ($Module in $Modules) {
            $ArgList += "--hidden-import=$Module"
        }
    }

    foreach ($HooksDir in $HooksDirs) { $ArgList += "--additional-hooks-dir=$HooksDir" }
    foreach ($RuntimeHook in $RuntimeHooks) { $ArgList += "--runtime-hook=$RuntimeHook" }
    foreach ($Exclude in $Excludes) { $ArgList += "--exclude-module=$Exclude" }
    if ($Debug) { $ArgList += "--debug=all" }
    if ($StripSymbols) { $ArgList += "--strip" }

    if ($NoConsole) { $ArgList += "--windowed" }
    if ($Icon) { $ArgList += "--icon=$Icon" }
    if ($Version) { $ArgList += "--version-file=$Version" }
    if ($Manifest -and -not $UpdateManifest) { $ArgList += "--manifest=$Manifest" }
    if ($ElevatedProc) { $ArgList += "--uac-admin" }
    if ($RemoteProc) { $ArgList += "--uac-uiaccess" }
    
    if ($UPX -and ($UPXLocal -or $IsRemote)) {
        if (-not (Get-Command upx -ErrorAction SilentlyContinue)) {
            if (-not $UPXDir) {
                $UPXDir = Split-Path (Install-PackageChoco "upx") -Parent
            }
            $ArgList += "--upx-dir=$UPXDir"
        }
    }
    else { $ArgList += "--noupx" }

    return ToArray ($ArgList + $ExtraArgs)
}

function Get-ReducedModuleGraphArgs([string[]]$ArgList, [bool]$Verbose = $False) {
    $Modules = Get-ModuleGraph $ModuleGraphSmart
    foreach ($Module in $Modules) {
        $ArgList = Get-RemovedItemArray $ArgList "--hidden-import=$Module"
        if ($Verbose) { Write-Host "[Reduced] $Module" }
    }
    return ToArray $ArgList
}

function Remove-PyInstaller([bool]$Verbose = $False, [bool]$Throw = $True) {
    $Paths = @("build", "dist", "*.spec")
    foreach ($Path in $Paths) {
        try {
            Remove-Item $Path -Force -Recurse
            if ($Verbose) { Write-Host "[Removed] $Path" }
        }
        catch { if ($Throw) { throw } }
    }
}

function Remove-pyd([string[]]$Sources, [bool]$Verbose, [bool]$Throw) {
    foreach ($Source in $Sources) {
        $Path = $Source.Substring(0, $Source.LastIndexOf(".")) + (& $GetExtSuffix)
        try {
            Remove-Item $Path -Force
            if ($Verbose) { Write-Host "[Removed] $Path" }
        }
        catch { if ($Throw) { throw } }
    }
}

function Get-CythonSources {
    $CythonSources = @()
    $CodeGlob = @() + $CodeGlobTemplate
    foreach ($CythonSourceGlob in $CythonSourceGlobs) {
        $CodeGlob[1] = $CodeGlobTemplate[1] -f $CythonSourceGlob
        $CythonSources += (Start-PythonCode $CodeGlob $False) -Split ";"
    }
    foreach ($CythonExcludeGlob in $CythonExcludeGlobs) {
        $CodeGlob[1] = $CodeGlobTemplate[1] -f $CythonExcludeGlob
        foreach ($Exclude in (Start-PythonCode $CodeGlob $False) -Split ";") {
            $CythonSources = Get-RemovedItemArray $CythonSources $Exclude ([int]::MaxValue)
        }
    }
    return ToArray $CythonSources
}

$GetCythonSources = Get-Memoized (Get-Command Get-CythonSources)

function Remove-Cython([bool]$Verbose = $False, [bool]$Throw = $True) {
    Remove-pyd (& $GetCythonSources) $Verbose $Throw
    if ($CythonRemoveC) {
        foreach ($Source in & $GetCythonSources) {
            $Base = Source.Substring(0, $Source.LastIndexOf("."))
            foreach ($Ext in @("c", "cpp")) {
                $Path = "$Base.$Ext"
                if (Remove-Item $Path -Force -ErrorAction SilentlyContinue) {
                    if ($Verbose) { Write-Host "[Removed] $Path" }
                }
            }
        }
    }
}

function Get-CythonArgs {
    $ArgsList = @("-3", "--inplace")
    if ($CythonNoDocstrings) {
        $ArgsList += "--no-docstrings"
    }
    if ($CythonCPP) {
        $ArgsList += "--cplus"
    }
    foreach ($CythonExcludeGlob in $CythonExcludeGlobs) {
        $ArgsList += "--exclude=$CythonExcludeGlob"
    }
    return ToArray ($ArgsList + $CythonSourceGlobs + $CythonExtraArgs)
}

function Remove-Nuitka([bool]$Verbose = $False, [bool]$Throw = $True) {
    Remove-pyd $NuitkaSources $Verbose $Throw
}

function Get-NuitkaArgs {
    $ArgList = @("--assume-yes-for-downloads", "--no-pyi-file", "--module")
    if ($NuitkaRemoveBuild) { $ArgList += "--remove-output" }
    return ToArray ($ArgList + $NuitkaExtraArgs)
}

function Remove-mypyc([bool]$Verbose = $False, [bool]$Throw = $True) {
    Remove-pyd $mypycSources $Verbose $Throw
}

function Get-mypycArgs {
    $ArgList = @("--install-types", "--non-interactive")
    if ($mypycCacheDir) { $ArgList += "--cache-dir=$(Join-Path $PSScriptRoot $mypycCacheDir)" }
    return ToArray ($ArgList + $mypycExtraArgs)
}

function Install-Requirements {
    python -m ensurepip --upgrade
    python -m pip install pip setuptools wheel --upgrade

    # $TempDir = Join-Path $Env:TEMP (New-Guid)  # FIXME https://github.com/pyinstaller/pyinstaller/issues/4824
    $TempDir = Join-Path (Split-Path (Get-Location) -Qualifier) (Get-Random)
    New-Item $TempDir -ItemType Directory
    try {
        Push-Location $TempDir
        python -m pip download pyinstaller --no-deps --no-binary pyinstaller
        $Source = (Get-ChildItem -Attributes Archive).FullName
        tar -xf $Source
        Set-Location (Join-Path $Source.Substring(0, $Source.Length - ".tar.gz".Length) "bootloader")
        python ./waf all
        Set-Location ..
        python -m pip install .
        Pop-Location
    }
    finally { Remove-Item $TempDir -Force -Recurse }

    if ($CythonSourceGlobs) { python -m pip install cython }
    if ($NuitkaSources) { python -m pip install nuitka }
    if ($mypycSources) { python -m pip install mypy }

    if (Test-Path requirements.txt -PathType Leaf) { python -m pip install -r requirements.txt }
}

function Remove-All([bool]$PyInstaller = $False, [bool]$Verbose = $True, [bool]$Throw = $False) {
    if ($PyInstaller) { Remove-PyInstaller $Verbose $Throw }
    Remove-Cython $Verbose $Throw
    Remove-Nuitka $Verbose $Throw
    Remove-mypyc $Verbose $Throw
}

function Write-Build {
    try {
        if ($CodeRunBefore) {
            Start-PythonCode $CodeRunBefore
        }
        if ($IsRemote -and $CodeRunBeforeRemote) {
            Start-PythonCode $CodeRunBeforeRemote
        }

        if ($MinifyJsonLocal -or $IsRemote) {
            foreach ($MinifyJsonRegEx in $MinifyJsonRegExs) {
                foreach ($Json in  Get-ChildItem -Path . -Filter $MinifyJsonRegEx) {
                    MinifyJsonFile $Json.FullName
                }
            }
        }

        Remove-All -Verbose $False
        $PyInstallerArgs = Get-PyInstallerArgs
        if ($CythonSourceGlobs) {
            $CythonArgs = Get-CythonArgs
            Write-Host "cythonize $CythonArgs"
            cythonize $CythonArgs
        }
        if ($NuitkaSources) {
            $NuitkaArgsBase = Get-NuitkaArgs
            foreach ($Source in $NuitkaSources) {
                $NuitkaArgs = $NuitkaArgsBase + @("--output-dir=$(Split-Path $Source -Parent)", $Source)
                Write-Host "nuitka $NuitkaArgs"
                nuitka $NuitkaArgs
            }
        }
        if ($mypycSources) {
            $mypycArgsBase = Get-mypycArgs
            foreach ($Source in $mypycSources) {
                Push-Location (Split-Path $Source -Parent)
                $mypycArgs = $mypycArgsBase + @(Split-Path $Source -Leaf)
                Write-Host "mypyc $mypycArgs"
                mypyc $mypycArgs
                Pop-Location
            }
        }
        if ($ModuleGraph -and $ModuleGraphReduce) {
            $PyInstallerArgs = Get-ReducedModuleGraphArgs $PyInstallerArgs
        }

        $BuildName = Get-ProjectName
        $MatchInfo = Select-String $EntryPoint -Pattern "__version__\s*=\s*['`"](?<semver>$PatSemVer)['`"]"
        $BuildVersion = if ($MatchInfo) { $MatchInfo.Matches[0].Groups["semver"].Value } else { "0.0.0" }
        $BuildTag = Start-PythonCode $CodeSysTag $False
        $FullName = "$BuildName-$BuildVersion-$BuildTag"
        if ($IsRemote) { "NAME=$FullName" >> $Env:GITHUB_ENV }

        $PythonOptimizeOld = $Env:PYTHONOPTIMIZE
        $PythonHashSeedOld = $Env:PYTHONHASHSEED
        $Env:PYTHONOPTIMIZE = $PythonOptimize
        if (-not $PythonHashSeed) { $PythonHashSeed = Get-IntFromBytes ([System.Text.Encoding]::ASCII.GetBytes($FullName)) @(1, 4294967295) }
        $Env:PYTHONHASHSEED = $PythonHashSeed
        Write-Host "[PYTHONOPTIMIZE] $Env:PYTHONOPTIMIZE"
        Write-Host "[PYTHONHASHSEED] $Env:PYTHONHASHSEED"

        $PyInstallerArgs += @("--name=$FullName", $EntryPoint)
        Write-Host "pyinstaller $PyInstallerArgs"
        pyinstaller $PyInstallerArgs

        $Env:PYTHONOPTIMIZE = $PythonOptimizeOld
        $Env:PYTHONHASHSEED = $PythonHashSeedOld

        $DistPath = Join-Path "dist" $FullName
        if ($OneFile) { $ExePath = "$DistPath.exe" }
        else {
            $ExePath = Join-Path $DistPath "$BuildName.exe"
            Move-Item (Join-Path $DistPath "$FullName.exe") $ExePath -Force
        }

        if ($Manifest -and $UpdateManifest) { Update-Manifest $ExePath $Manifest }

        if ($CythonRemove) { Remove-Cython }
        if ($NuitkaRemove) { Remove-Nuitka }
        if ($mypycRemove) { Remove-mypyc }

        if ($CodeRunAfter) { Start-PythonCode $CodeRunAfter }
        if ($IsRemote -and $CodeRunAfterRemote) { Start-PythonCode $CodeRunAfterRemote }
    }
    finally { if ($RemoveOnThrow) { Remove-All -Verbose $False } }
}

function Write-MEGA {
    if (-not $Env:MEGA_USERNAME -or -not $Env:MEGA_PASSWORD) {
        throw
    }
    if (-not (Get-Command mega-help -ErrorAction SilentlyContinue)) {
        $Env:PATH += ";$( Join-Path $Env:LOCALAPPDATA "MEGAcmd" )"
        Install-PackageChoco "megacmd" "mega-help"
    }
    mega-login $Env:MEGA_USERNAME $Env:MEGA_PASSWORD
    mega-put dist (Join-Path "$(Get-ProjectName)-cp$( $Env:PYTHON_VERSION -Replace "\.", """" )-$Env:ARCHITECTURE" ((Get-Date -Format o -AsUTC) -Replace ":", "."))
    mega-logout
}

$IsRemote = -not $ForceLocal -and (Test-Path Env:CI)

$ErrorActionPreference = "Stop"
if ($Args) {
    switch ($Args[0]) {
        "install" { Install-Requirements; Break }
        "clean" { Remove-All $True; Break }
        "build" { Write-Build; Break }
        "push" { Write-MEGA; Break }
        Default { throw }
    }
}
else { Write-Build }
