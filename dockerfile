FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# зависимости
RUN pip install --no-cache-dir httpx sqlalchemy aiosqlite

COPY app/ /app/

# папка под volume с БД
RUN mkdir -p /data

CMD ["python", "main.py"]
