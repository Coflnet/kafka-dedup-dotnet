# First stage
FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /build

COPY kafka-dedup-dotnet.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c release -o /app

# Final stage
FROM mcr.microsoft.com/dotnet/runtime:5.0
WORKDIR /app
COPY --from=build /app ./

RUN useradd --uid $(shuf -i 2000-65000 -n 1) app
USER app

ENTRYPOINT ["dotnet", "kafka-dedup-dotnet.dll"]