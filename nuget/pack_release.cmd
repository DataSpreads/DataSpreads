del ..\artifacts\*.nupkg

dotnet restore ..\src\DataSpreads.Security\DataSpreads.Security.csproj
dotnet pack ..\src\DataSpreads.Security\DataSpreads.Security.csproj -c Release -o ..\artifacts -p:AutoSuffix=False

dotnet restore ..\src\DataSpreads.Core\DataSpreads.Core.csproj
dotnet pack ..\src\DataSpreads.Core\DataSpreads.Core.csproj -c Release -o ..\artifacts -p:AutoSuffix=False

dotnet restore ..\src\DataSpreads\DataSpreads.csproj
dotnet pack ..\src\DataSpreads\DataSpreads.csproj -c Release -o ..\artifacts -p:AutoSuffix=False


pause