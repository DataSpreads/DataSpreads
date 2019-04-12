@echo off

for /f "tokens=2 delims==" %%a in ('wmic OS Get localdatetime /value') do set "dt=%%a"
set "YY=%dt:~2,2%" & set "YYYY=%dt:~0,4%" & set "MM=%dt:~4,2%" & set "DD=%dt:~6,2%"
set "HH=%dt:~8,2%" & set "Min=%dt:~10,2%" & set "Sec=%dt:~12,2%"

set "timestamp=%HH%%Min%%Sec%"

set "build=build%timestamp%"
echo build: "%build%"

dotnet restore ..\src\DataSpreads.Security\DataSpreads.Security.csproj
dotnet pack ..\src\DataSpreads.Security\DataSpreads.Security.csproj -c Release -o C:\transient\LocalNuget  --version-suffix "%build%" -p:AutoSuffix=True

dotnet restore ..\src\DataSpreads.Core\DataSpreads.Core.csproj
dotnet pack ..\src\DataSpreads.Core\DataSpreads.Core.csproj -c Release -o C:\transient\LocalNuget  --version-suffix "%build%" -p:AutoSuffix=True

dotnet restore ..\src\DataSpreads\DataSpreads.csproj
dotnet pack ..\src\DataSpreads\DataSpreads.csproj -c Release -o C:\transient\LocalNuget --version-suffix "%build%" -p:AutoSuffix=True

pause
