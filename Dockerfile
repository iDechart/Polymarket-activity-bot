FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir httpx sqlalchemy aiosqlite

COPY app/ /app/
RUN mkdir -p /data

EXPOSE 8080

CMD ["python", "main.py"]
