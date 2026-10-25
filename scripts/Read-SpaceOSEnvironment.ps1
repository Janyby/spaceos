# Read-only x64 Windows diagnostic. Prints only an explicit non-secret allowlist.
$ErrorActionPreference = 'Stop'
Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
public static class SpaceOSEnvironment {
 [DllImport("kernel32.dll", SetLastError=true)] static extern IntPtr OpenProcess(uint a,bool i,int p);
 [DllImport("kernel32.dll")] static extern bool CloseHandle(IntPtr p);
 [DllImport("kernel32.dll", SetLastError=true)] static extern bool ReadProcessMemory(IntPtr p,IntPtr a,byte[] b,int n,out IntPtr r);
 [DllImport("ntdll.dll")] static extern int NtQueryInformationProcess(IntPtr p,int c,IntPtr[] b,int n,out int r);
 static byte[] Read(IntPtr p,long a,int n) {
  var b=new byte[n]; IntPtr r;
  if(!ReadProcessMemory(p,new IntPtr(a),b,n,out r)||r.ToInt64()!=n) throw new Exception("ReadProcessMemory failed");
  return b;
 }
 public static Dictionary<string,string> ReadAllowed(int pid) {
  if(IntPtr.Size!=8) throw new Exception("Use 64-bit PowerShell");
  IntPtr p=OpenProcess(0x410,false,pid);
  if(p==IntPtr.Zero) throw new Exception("OpenProcess denied");
  try {
   var info=new IntPtr[6]; int ret;
   if(NtQueryInformationProcess(p,0,info,48,out ret)!=0) throw new Exception("Process information unavailable");
   long parameters=BitConverter.ToInt64(Read(p,info[1].ToInt64()+0x20,8),0);
   long env=BitConverter.ToInt64(Read(p,parameters+0x80,8),0);
   var names=new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
    "SPACEOS_DB","SPACEOS_SEMANTIC_WORLD_ID","SPACEOS_BRIDGE_CORE_PIPE",
    "SPACEOS_ROOT","SPACEOS_ALLOWED_ROOTS","SPACEOS_PROVIDER_ID","SPACEOS_BROWSER_CDP_BASE",
    "OPENCODE_SERVER_URL","OPENCODE_SERVER_USERNAME","OPENCODE_SERVER_PASSWORD",
    "XDG_STATE_HOME","USERPROFILE","SPACEOS_CORE_PIPE"
   };
   var result=new Dictionary<string,string>(); var entry=new StringBuilder();
   for(int n=0;n<1048576;n+=2) {
    char ch=(char)BitConverter.ToUInt16(Read(p,env+n,2),0);
    if(ch==0) {
     if(entry.Length==0) return result;
     string line=entry.ToString(); int eq=line.IndexOf('=');
     if(eq>0&&names.Contains(line.Substring(0,eq))) result[line.Substring(0,eq)]=line.Substring(eq+1);
     entry.Clear();
    } else entry.Append(ch);
   }
   throw new Exception("Environment exceeded bound");
  } finally { CloseHandle(p); }
 }
}
'@
Get-CimInstance Win32_Process |
 Where-Object { $_.Name -match '^(space-core|space-bridge|browser-provider|filesystem-provider)\.exe$' } |
 ForEach-Object {
    $actual = [SpaceOSEnvironment]::ReadAllowed([int]$_.ProcessId)
    $gateway = @{}
    foreach ($key in @('OPENCODE_SERVER_URL','OPENCODE_SERVER_USERNAME','OPENCODE_SERVER_PASSWORD','XDG_STATE_HOME','USERPROFILE')) {
      $value = $actual[$key]
      $gateway[$key] = @{ Present = ![string]::IsNullOrEmpty($value); MatchesCaller = $value -ceq [Environment]::GetEnvironmentVariable($key, 'Process') }
      $actual.Remove($key) | Out-Null
    }
    [pscustomobject]@{
      ProcessId=$_.ProcessId
      ExecutablePath=$_.ExecutablePath
      Environment=$actual
      GatewayEnvironment=$gateway
    }
 } | ConvertTo-Json -Depth 4
