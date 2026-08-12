FROM python:3.12-slim

# System deps for aiohttp[speedups] (C-extensions: aiodns, cchardet, brotli)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (Docker layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY app/ ./app/

# Copy storage_state.json (if present) for initial cookies
# The user can update this file and trigger a new Railway deploy
COPY storage_state.json* ./

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--no-access-log"]
