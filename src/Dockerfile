# Стейдж 1: Сборка
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Копируем файлы проектов, используя относительные пути.
# Важно: На вашем скрине в папке WhisperApp.Appication нет буквы 'l'
COPY WhisperApp.Api/WhisperApp.Api.csproj WhisperApp.Api/
COPY WhisperApp.Application/WhisperApp.Application.csproj WhisperApp.Application/

# Восстановление зависимостей
RUN dotnet restore WhisperApp.Api/WhisperApp.Api.csproj

# Копируем все остальное
COPY . .

# Собираем проект
WORKDIR /src/WhisperApp.Api
RUN dotnet build WhisperApp.Api.csproj -c Release -o /app/build

# Стейдж 2: Публикация
FROM build AS publish
RUN dotnet publish WhisperApp.Api.csproj -c Release -o /app/publish /p:UseAppHost=false

# Стейдж 3: Финальный образ
FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app

# Установка FFmpeg
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/temp_audio && chmod 777 /app/temp_audio

COPY --from=publish /app/publish .

EXPOSE 8080
ENTRYPOINT ["dotnet", "WhisperApp.Api.dll"]