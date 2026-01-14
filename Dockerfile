FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy csproj and restore as distinct layers
COPY *.sln .
COPY *.csproj .
RUN dotnet restore --disable-parallel

# Copy everything else and build
COPY . .
RUN dotnet publish apiNET8_UploadItemsCosmos.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .

ENV DOTNET_RUNNING_IN_CONTAINER=true
ENV DOTNET_USE_POLLING_FILE_WATCHER=false

# Listen on port 5008 inside the container
ENV ASPNETCORE_URLS=http://+:5008
EXPOSE 5008

ENTRYPOINT ["dotnet", "apiNET8_UploadItemsCosmos.dll"]
