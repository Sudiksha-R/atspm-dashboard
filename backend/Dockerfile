FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY atspm ./atspm
RUN pip install --no-cache-dir -e ./atspm

COPY backend ./backend
COPY bottleneck_detection ./bottleneck_detection
COPY corridor_correlation_cliques ./corridor_correlation_cliques
COPY ["Correlation_Feature (1)", "./Correlation_Feature (1)"]

ENV PYTHONPATH="/app:/app/backend:/app/atspm/src:/app/Correlation_Feature (1)"
ENV ATSPM_BACKEND_HOST=0.0.0.0 \
    ATSPM_BACKEND_PORT=8000 \
    ATSPM_RAW_DIR=/app/derived/bee_ridge_atspm_feb_to_may

EXPOSE 8000

CMD ["python", "-u", "backend/server.py"]
