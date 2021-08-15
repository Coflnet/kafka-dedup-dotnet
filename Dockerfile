# First stage
FROM mcr.microsoft.com/dotnet/sdk:5.0 AS build
WORKDIR /build

# Copy everything else and build website
COPY . .
RUN dotnet publish -c release -o /app

# Final stage
FROM mcr.microsoft.com/dotnet/runtime:5.0
WORKDIR /app
COPY --from=build /app ./
ENTRYPOINT ["dotnet", "kafka-dedup-dotnet.dll"]