FROM python:3.13-slim-bullseye

ENV DEBIAN_FRONTEND=noninteractive
ENV EXIFTOOL_PATH=/usr/bin/exiftool
ENV FFMPEG_PATH=/usr/bin/ffmpeg

# Runtime dependency
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg exiftool

ARG INSTALL_GIT=false
RUN if [ "$INSTALL_GIT" = "true" ]; then \
    apt-get install -y --no-install-recommends git; \
    fi

RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app
# Копирует все из текущей директории сборки (где Dockerfile) в /app
COPY . /app

# Установка зависимостей для API-сервиса
# python-multipart для поддержки загрузки файлов в FastAPI
# RUN pip --no-cache-dir install fastapi uvicorn python-multipart httpx
RUN pip --no-cache-dir install fastapi uvicorn[standard] python-multipart httpx

# Установка markitdown (как и было)
RUN pip --no-cache-dir install \
    /app/packages/markitdown[all] \
    /app/packages/markitdown-sample-plugin

# USER nobody:nogroup # для запуска от non-root пользователя.
# Убедитесь, что у пользователя есть права на запись во временные папки (FastAPI может их использовать).
# Default USERID and GROUPID
ARG USERID=nobody
ARG GROUPID=nogroup
USER $USERID:$GROUPID

EXPOSE 8181

# CMD для запуска API-сервиса через Uvicorn
# api_wrapper - имя файла (api_wrapper.py)
# app - имя экземпляра FastAPI в этом файле
# --workers 1 для простоты; для продакшена можно настроить Gunicorn + Uvicorn workers.
CMD ["uvicorn", "api_wrapper:app", "--host", "0.0.0.0", "--port", "8181", "--workers", "1"]