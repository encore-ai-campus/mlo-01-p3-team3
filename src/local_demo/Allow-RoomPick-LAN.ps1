#Requires -RunAsAdministrator
$ErrorActionPreference = 'Stop'
$roompickIp = '192.168.0.44'
$roompickSubnet = '192.168.0.0/24'
$roompickPython = 'C:\Users\playdata\AppData\Local\Programs\Python\Python314\python.exe'
$roompickRuleName = 'RoomPick-LAN-8003'
Start-Transcript -Path (Join-Path $PSScriptRoot 'lan-firewall.log') -Force | Out-Null
try {
    $roompickRule = Get-NetFirewallRule -Name $roompickRuleName -ErrorAction SilentlyContinue
    if ($roompickRule) {
        $roompickRule | Set-NetFirewallRule -Enabled True -Direction Inbound -Action Allow -Profile Any
        $roompickRule | Get-NetFirewallPortFilter | Set-NetFirewallPortFilter -Protocol TCP -LocalPort 8003
        $roompickRule | Get-NetFirewallAddressFilter | Set-NetFirewallAddressFilter -LocalAddress $roompickIp -RemoteAddress $roompickSubnet
        $roompickRule | Get-NetFirewallApplicationFilter | Set-NetFirewallApplicationFilter -Program $roompickPython
    } else {
        New-NetFirewallRule -Name $roompickRuleName -DisplayName 'RoomPick LAN 8003' -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8003 -LocalAddress $roompickIp -RemoteAddress $roompickSubnet -Program $roompickPython -Profile Any | Out-Null
    }
    # Windows user-denial rules override allow rules; keep their other TCP ports blocked.
    $roompickBlocks = Get-NetFirewallApplicationFilter | Where-Object { $_.Program -eq $roompickPython } | Get-NetFirewallRule | Where-Object { $_.Enabled -eq 'True' -and $_.Direction -eq 'Inbound' -and $_.Action -eq 'Block' -and $_.Profile -eq 'Public' }
    foreach ($roompickBlock in $roompickBlocks) {
        $roompickPort = $roompickBlock | Get-NetFirewallPortFilter
        if ($roompickPort.Protocol -eq 'TCP' -and $roompickPort.LocalPort -contains 'Any') {
            $roompickPort | Set-NetFirewallPortFilter -LocalPort '1-8002','8004-65535'
        }
    }
    Get-NetFirewallRule -Name $roompickRuleName | Format-List DisplayName,Enabled,Direction,Action,Profile
    Get-NetFirewallRule -Name $roompickRuleName | Get-NetFirewallPortFilter | Format-List Protocol,LocalPort
    Get-NetFirewallRule -Name $roompickRuleName | Get-NetFirewallAddressFilter | Format-List LocalAddress,RemoteAddress
} finally {
    Stop-Transcript | Out-Null
}
