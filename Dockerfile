# First stage
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /build

COPY kafka-dedup-dotnet.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c Release -o /app --no-restore

# Final stage
FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app ./

USER app

ENTRYPOINT ["dotnet", "kafka-dedup-dotnet.dll"]