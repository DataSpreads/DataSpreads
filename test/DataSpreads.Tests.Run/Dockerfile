FROM mcr.microsoft.com/dotnet/core-nightly/runtime:3.0.0-preview4-stretch-slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/core-nightly/sdk:3.0.0-preview4-stretch-slim AS build
WORKDIR /src
COPY NuGet.Config /
COPY build/common.props build/
COPY tools/DataSpreadsKey.snk tools/
COPY test/DataSpreads.Tests.Run/DataSpreads.Tests.Run.csproj test/DataSpreads.Tests.Run/
COPY test/DataSpreads.Tests/DataSpreads.Tests.csproj test/DataSpreads.Tests/
COPY src/DataSpreads/DataSpreads.Core.csproj src/DataSpreads/
RUN dotnet restore test/DataSpreads.Tests.Run/DataSpreads.Tests.Run.csproj
COPY . .
WORKDIR /src/test/DataSpreads.Tests.Run
RUN dotnet build DataSpreads.Tests.Run.csproj -c Release -o /app

FROM build AS publish
RUN dotnet publish DataSpreads.Tests.Run.csproj -c Release -o /app

FROM base AS final
WORKDIR /app
COPY --from=publish /app .
ENTRYPOINT ["dotnet", "DataSpreads.Tests.Run.dll"]
