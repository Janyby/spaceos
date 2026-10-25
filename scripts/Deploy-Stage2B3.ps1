param([switch]$Approved)
$ErrorActionPreference = 'Stop'
if (!$Approved) { throw 'Explicit deployment approval required' }
$root='C:\spaceos'
Set-Location $root
$scope='550e8400-e29b-41d4-a716-446655440000'
$db="$root\runtime\core.db"
$stage="$root\target\stage2b3-preflight"
$expected=@{'space-core.exe'='53EA8B02F1386CEDE16D0A9934014E6A33745DEADE2D9B57FAB97163C8D555B1';'space-bridge.exe'='67C3175ECB1CDF9B8B35EB40EFAB7055F6D2A425C0B0AD793E5A1EEE4ABC7617'}
function Assert-Hash($path,$hash) { if ((Get-FileHash -LiteralPath $path).Hash -ne $hash) { throw "Hash mismatch: $path" } }
foreach($name in $expected.Keys) { Assert-Hash "$stage\release\$name" $expected[$name] }
Assert-Hash "$stage\rollback-world.html" '6B0BA29E9AA59E5600625865973A412BF8D136C40165BF7D83BACD9BC785C62B'
Assert-Hash "$root\space-bridge\static\world.html" 'E75E99389AE1560CB4364D341C8A7808A51B3B9A5BE3508EC2C5D3B82DF8C6FB'
& node "$root\scripts\Verify-WorldRollback.cjs"
if($LASTEXITCODE) {throw 'Rollback verification failed'}
$auditText = & "$root\scripts\Read-SpaceOSEnvironment.ps1"
$audit = $auditText | ConvertFrom-Json
foreach($a in $audit) {
 foreach($setting in $a.GatewayEnvironment.PSObject.Properties) { if(!$setting.Value.MatchesCaller) {throw 'Gateway environment mismatch'} }
 if($a.Environment.SPACEOS_BRIDGE_CORE_PIPE -or $a.Environment.SPACEOS_CORE_PIPE) {throw 'Unexpected IPC override'}
 if($a.Environment.SPACEOS_DB -and $a.Environment.SPACEOS_DB -ine $db) {throw 'Unexpected DB override'}
}
$all=@(Get-CimInstance Win32_Process)
$targets=@()
foreach($name in @('space-bridge','filesystem-provider','browser-provider','space-core')) {
 $found=@($all | Where-Object Name -eq "$name.exe")
 if($found.Count -ne 1 -or $found[0].ExecutablePath -ine "$root\target\release\$name.exe") {throw "Unexpected process set: $name"}
 if(@($all | Where-Object ProcessId -eq $found[0].ParentProcessId).Count) {throw 'Living launcher parent requires review'}
 $targets += $found[0]
}
$listener=@(Get-NetTCPConnection -LocalPort 8765 -State Listen)
if($listener.Count -ne 1 -or $listener[0].LocalAddress -ne '127.0.0.1' -or $listener[0].OwningProcess -ne $targets[0].ProcessId) {throw 'Unexpected Bridge listener'}
$cdp=Invoke-RestMethod 'http://127.0.0.1:9222/json/version' -TimeoutSec 5
if(!$cdp.webSocketDebuggerUrl) {throw 'CDP unavailable'}
$tabs=Invoke-RestMethod 'http://127.0.0.1:8765/api/browser/tabs' -TimeoutSec 15
if($tabs.result.result.status -ne 'Completed') {throw 'Browser provider operation failed'}
$gateway=Invoke-RestMethod 'http://127.0.0.1:8765/api/agent/sessions' -TimeoutSec 10
$context=Invoke-RestMethod 'http://127.0.0.1:8765/api/context' -TimeoutSec 5
if($context.context.semantic_world_id -cne $scope) {throw 'Unexpected scope'}
if(!(Test-Path -LiteralPath $db)) {throw 'Database missing'}
$backup=Join-Path "$root\runtime" ('backup-stage2b3-'+(Get-Date -Format 'yyyyMMdd-HHmmss-fff'))
New-Item -ItemType Directory -Path $backup | Out-Null
New-Item -ItemType Directory -Path "$backup\bin","$backup\db","$backup\logs" | Out-Null
$auditText | Set-Content -LiteralPath "$backup\effective-environment-redacted.json" -Encoding UTF8
$targets | Select-Object ProcessId,ParentProcessId,ExecutablePath,CreationDate | ConvertTo-Json | Set-Content -LiteralPath "$backup\previous-processes.json" -Encoding UTF8
$context.context | Select-Object object_count,semantic_world_id,space_id | ConvertTo-Json | Set-Content -LiteralPath "$backup\previous-context.json" -Encoding UTF8
Copy-Item -LiteralPath "$root\launch.ps1","$root\scripts\Deploy-Stage2B3.ps1" -Destination $backup
Copy-Item -LiteralPath "$root\space-bridge\static\world.html","$stage\rollback-world.html" -Destination $backup
foreach($name in $expected.Keys) {
 Copy-Item -LiteralPath "$root\target\release\$name" -Destination "$backup\bin\$name"
 Assert-Hash "$backup\bin\$name" (Get-FileHash -LiteralPath "$root\target\release\$name").Hash
}
Write-Output "BACKUP_DIRECTORY=$backup"
# Validate ALL identities before ANY shutdown; keep handles to prevent PID-reuse mistakes.
$handles=@()
foreach($p in $targets) {
 $fresh=Get-CimInstance Win32_Process -Filter "ProcessId=$($p.ProcessId)"
 if(!$fresh -or $fresh.ExecutablePath -ine $p.ExecutablePath -or $fresh.CreationDate -ne $p.CreationDate) {throw 'Process identity changed before shutdown'}
 $handle=Get-Process -Id $p.ProcessId
 $null=$handle.Handle
 if($handle.StartTime.ToUniversalTime() -ne $p.CreationDate.ToUniversalTime()) {throw 'Process handle identity changed'}
 $handles += $handle
}
$now=@(Get-CimInstance Win32_Process | Where-Object Name -in @('space-core.exe','space-bridge.exe','filesystem-provider.exe','browser-provider.exe'))
if($now.Count -ne $targets.Count -or @(Compare-Object ($targets.ProcessId | Sort-Object) ($now.ProcessId | Sort-Object)).Count) {throw 'Process set changed before shutdown'}
foreach($handle in $handles) {
 if($handle.HasExited) {throw 'Process exited before shutdown'}
 $handle.Kill()
 if(!$handle.WaitForExit(10000)) {throw 'Stop timeout'}
 Write-Output "STOPPED=$($handle.Id)"
}
$deadline=(Get-Date).AddSeconds(15)
do {
 $pipes=@(Get-ChildItem -LiteralPath '\\.\pipe\' | Where-Object Name -in @('spaceos-core','spaceos-browser-provider','spaceos-fs-provider'))
 $remaining=@(Get-CimInstance Win32_Process | Where-Object Name -in @('space-core.exe','space-bridge.exe','filesystem-provider.exe','browser-provider.exe'))
 $listeners=@(Get-NetTCPConnection -LocalPort 8765 -State Listen -ErrorAction SilentlyContinue)
 if(!$pipes.Count -and !$remaining.Count -and !$listeners.Count) {break}
 Start-Sleep -Milliseconds 200
} while((Get-Date) -lt $deadline)
if($pipes.Count -or $remaining.Count -or $listeners.Count) {throw 'Endpoints/processes not released'}
# Hold exclusive read handles to every existing SQLite member during cold copying.
$streams=@{}
try {
 foreach($suffix in @('','-wal','-shm','-journal')) {
  $path="$db$suffix"
  if(Test-Path -LiteralPath $path) { $streams[$path]=[IO.File]::Open($path,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None) }
 }
 foreach($path in $streams.Keys) {
  $dest=Join-Path "$backup\db" ([IO.Path]::GetFileName($path))
  $out=[IO.File]::Open($dest,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
  try {$streams[$path].CopyTo($out); $out.Flush($true)} finally {$out.Dispose()}
 }
} finally {foreach($stream in $streams.Values) {$stream.Dispose()}}
$manifest=@()
foreach($path in $streams.Keys) {
 $dest=Join-Path "$backup\db" ([IO.Path]::GetFileName($path))
 $hash=(Get-FileHash -LiteralPath $path).Hash
 Assert-Hash $dest $hash
 $manifest += @{file=[IO.Path]::GetFileName($path);sha256=$hash;bytes=(Get-Item -LiteralPath $dest).Length}
}
$manifest | ConvertTo-Json | Set-Content -LiteralPath "$backup\db-manifest.json" -Encoding UTF8
$integrity = & node "$root\scripts\Inspect-BackupSqlite.cjs" "$backup\db\core.db"
if($LASTEXITCODE) {throw 'Backup integrity check failed; no binaries replaced'}
$integrity | Set-Content -LiteralPath "$backup\sqlite-before.json" -Encoding UTF8
Write-Output "COLD_BACKUP_VERIFIED=$integrity"
foreach($name in $expected.Keys) {Copy-Item -LiteralPath "$stage\release\$name" -Destination "$root\target\release\$name" -Force; Assert-Hash "$root\target\release\$name" $expected[$name]}
function Start-ServiceBinary($name,[hashtable]$vars) {
 $saved=@{}
 try {
  foreach($key in $vars.Keys) {$saved[$key]=[Environment]::GetEnvironmentVariable($key,'Process');[Environment]::SetEnvironmentVariable($key,$vars[$key],'Process')}
  $p=Start-Process -FilePath "$root\target\release\$name.exe" -WorkingDirectory $root -WindowStyle Hidden -PassThru -RedirectStandardOutput "$backup\logs\$name-out.log" -RedirectStandardError "$backup\logs\$name-err.log"
  Write-Host "STARTED=$name PID=$($p.Id)"
  return $p
 } finally {foreach($key in $saved.Keys) {[Environment]::SetEnvironmentVariable($key,$saved[$key],'Process')}}
}
$core=Start-ServiceBinary 'space-core' @{SPACEOS_DB=$db;SPACEOS_SEMANTIC_WORLD_ID=$scope}
$ready=$false
for($i=0;$i -lt 30;$i++) {
 $pipe=[IO.Pipes.NamedPipeClientStream]::new('.','spaceos-core',[IO.Pipes.PipeDirection]::InOut)
 try {
  $pipe.Connect(500)
  $writer=[IO.StreamWriter]::new($pipe,[Text.UTF8Encoding]::new($false),1024,$true);$writer.AutoFlush=$true
  $reader=[IO.StreamReader]::new($pipe)
  $writer.WriteLine('{"method":"ping","params":{}}')
  $read=$reader.ReadLineAsync()
  if($read.Wait(2000)) {$reply=$read.Result|ConvertFrom-Json; $ready=($reply.ok -and $reply.result.pong)}
 } catch {} finally {$pipe.Dispose()}
 if($ready) {break}
 if($core.HasExited) {throw 'Core exited'}
 Start-Sleep -Milliseconds 200
}
if(!$ready) {throw 'Core ping failed'}
$bridge=Start-ServiceBinary 'space-bridge' @{SPACEOS_SEMANTIC_WORLD_ID=$scope;SPACEOS_BRIDGE_CORE_PIPE=$null}
$browser=Start-ServiceBinary 'browser-provider' @{SPACEOS_PROVIDER_ID='browser';SPACEOS_BROWSER_CDP_BASE='http://127.0.0.1:9222';SPACEOS_CORE_PIPE=$null}
$filesystem=Start-ServiceBinary 'filesystem-provider' @{SPACEOS_PROVIDER_ID='filesystem';SPACEOS_ROOT='C:\spaceos\data';SPACEOS_ALLOWED_ROOTS='C:\spaceos\data'}
@($core,$bridge,$browser,$filesystem) | Select-Object Id,ProcessName,StartTime | ConvertTo-Json | Set-Content -LiteralPath "$backup\started-processes.json" -Encoding UTF8
Write-Output "STARTUP_COMPLETE; verify readiness separately. Backup=$backup"
# Deliberately no automatic database rollback or service termination on failure.
