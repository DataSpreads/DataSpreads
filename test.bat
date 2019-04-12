@echo off
dotnet test test/DataSpreads.Core.Tests/DataSpreads.Tests.csproj -c Release --filter TestCategory=CI -v n
REM dotnet test test/DataSpreads.Security.Tests/DataSpreads.Tests.csproj -c Release --filter TestCategory=CI -v n
REM dotnet test test/DataSpreads.Tests/DataSpreads.Tests.csproj -c Release --filter TestCategory=CI -v n

