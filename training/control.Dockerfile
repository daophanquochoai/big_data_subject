FROM docker:27.5.1-cli AS dockercli
FROM node:22.12.0-alpine AS ui
WORKDIR /ui
COPY frontend/package*.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
WORKDIR /training
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY control ./control
COPY --from=ui /ui/dist ./static
ENV PYTHONPATH=/training PYTHONUNBUFFERED=1
CMD ["uvicorn", "--factory", "control.main:build", "--host", "0.0.0.0", "--port", "8000"]
