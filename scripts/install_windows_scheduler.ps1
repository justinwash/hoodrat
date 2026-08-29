# hoodrat-install-windows-scheduler.ps1
# Installs a Windows Task Scheduler entry that runs the Hoodrat combined
# dashboard+scheduler (app mode) at logon so the bot can operate unattended.
#
# Usage (PowerShell, from the repo root):
#   powershell -ExecutionPolicy Bypass -File scripts/install_windows_scheduler.ps1
#
# Safety: this only schedules `cargo run --release -- app`. The live config
# (hoodrat.json) controls whether the scheduler may act and whether the
# firewall may submit orders. gateways.submit defaults to false.

param(
    [string]$TaskName = "HoodratTerminal",
    [string]$ProjectDir = (Get-Location).Path,
    [string]$Cargo = "cargo",
    [switch]$RunAsUser
)

$ErrorActionPreference = "Stop"

$working = Resolve-Path $ProjectDir
$actionArgs = "run --release -- app"

$action = New-ScheduledTaskAction `
    -Execute $Cargo `
    -Argument $actionArgs `
    -WorkingDirectory $working

$trigger = New-ScheduledTaskTrigger -AtLogOn
if ($RunAsUser) {
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
}

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force

Write-Host "Registered scheduled task '$TaskName'."
Write-Host "  workdir : $working"
Write-Host "  command : $Cargo $actionArgs"
Write-Host "Safest op: keep hoodrat.json gateway.submit=false until you're ready."
Write-Host "Manual control:"
Write-Host "  SchTasks /Query /TN $TaskName"
Write-Host "  SchTasks /Run  /TN $TaskName"
Write-Host "  SchTasks /End  /TN $TaskName"
Write-Host "  SchTasks /Delete /TN $TaskName /F"
