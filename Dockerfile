# First stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /build

COPY kafka-dedup-dotnet.csproj .
RUN dotnet restore

COPY . .
RUN dotnet publish -c release -o /app

# Final stage — chiseled-extra: distroless, rootless ($APP_UID), no shell/package manager.
# "extra" adds ICU/tzdata + ca-certificates over the base chiseled image. Console app, so the
# runtime image (not aspnet). useradd is gone because chiseled ships a non-root user already.
FROM mcr.microsoft.com/dotnet/runtime:10.0-noble-chiseled-extra
WORKDIR /app
COPY --from=build --chown=$APP_UID:$APP_UID /app ./

ENV DOTNET_RUNNING_IN_CONTAINER=true \
    HOME=/tmp \
    TMPDIR=/tmp

USER $APP_UID

ENTRYPOINT ["dotnet", "kafka-dedup-dotnet.dll"]
