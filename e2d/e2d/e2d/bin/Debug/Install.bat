%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\installutil.exe e2d.exe
Net Start e2dTest
sc config e2dTest start= auto