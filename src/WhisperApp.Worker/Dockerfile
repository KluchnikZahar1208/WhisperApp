# ========================
# 1. Сборка whisper.cpp (CLI)
# ========================
FROM ubuntu:22.04 AS whisper-build
RUN apt-get update && apt-get install -y \
    git cmake build-essential g++ libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt
RUN git clone --depth=1 https://github.com/ggerganov/whisper.cpp.git
WORKDIR /opt/whisper.cpp
RUN cmake -B build -DWHISPER_SHARED=OFF -DBUILD_SHARED_LIBS=OFF -DGGML_OPENBLAS=ON
RUN cmake --build build -j$(nproc)

# Скачиваем модель именно здесь, чтобы использовать кэш Docker
RUN bash ./models/download-ggml-model.sh large-v3-turbo
# ========================
# 2. Сборка .NET Worker
# ========================
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS dotnet-build
WORKDIR /src
COPY ["WhisperApp.Worker/WhisperApp.Worker.csproj", "WhisperApp.Worker/"]
RUN dotnet restore "WhisperApp.Worker/WhisperApp.Worker.csproj"
COPY . .
WORKDIR "/src/WhisperApp.Worker"
RUN dotnet publish -c Release -o /app/publish

# ========================
# 3. Финальный Runtime
# ========================
FROM mcr.microsoft.com/dotnet/runtime:8.0
RUN apt-get update && apt-get install -y \
    ffmpeg libopenblas-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем бинарник из первого этапа
COPY --from=whisper-build /opt/whisper.cpp/build/bin/whisper-cli /usr/local/bin/whisper

# Копируем приложение
WORKDIR /app
COPY --from=dotnet-build /app/publish .

# Настройка путей (как в вашем примере)
ENV WHISPER_MODELS_PATH=/models
RUN mkdir -p /models /app/data
COPY --from=whisper-build /opt/whisper.cpp/models/ggml-large-v3-turbo.bin /models/

VOLUME ["/models", "/app/data"]

ENTRYPOINT ["dotnet", "WhisperApp.Worker.dll"]