FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (cache layer)
COPY pyproject.toml README.md /app/
COPY src /app/src

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy web scraper (isolated module, not installed as package)
COPY sonxeber_scraper /app/sonxeber_scraper
COPY main.py /app/main.py

# Ensure both packages are importable
ENV PYTHONPATH=/app:/app/src

# Create required directories
RUN mkdir -p /app/state/telethon /app/data

# Default command (overridden by docker-compose per service)
CMD ["python", "-m", "news_ingestor.cli", "run-telegram-worker"]
