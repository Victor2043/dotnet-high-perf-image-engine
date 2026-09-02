FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libfontconfig1 \
    libfreetype6 \
    libgl1-mesa-glx \
    libgomp1 \
    && rm -rf /var/lib/apt-get/lists/*

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore
RUN dotnet publish -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "HighPerfImageEngine.dll"]