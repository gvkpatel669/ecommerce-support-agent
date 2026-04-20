FROM python:3.11-slim

WORKDIR /app

# Build arg for version tracking
ARG GIT_COMMIT_SHA=dev
ENV GIT_COMMIT_SHA=${GIT_COMMIT_SHA}

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ app/

EXPOSE 8010
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8010"]
