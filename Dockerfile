FROM python:3.11-slim

WORKDIR /app

# Build arg for version tracking
ARG GIT_COMMIT_SHA=dev
ENV GIT_COMMIT_SHA=${GIT_COMMIT_SHA}

COPY requirements.txt constraints.txt ./
RUN pip install --no-cache-dir -r requirements.txt -c constraints.txt

COPY app/ app/

# Run as an unprivileged user
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8010
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8010/health')" || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010"]
