FROM node:22.12.0-alpine AS ui
WORKDIR /ui
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim-bookworm
WORKDIR /app
COPY backend/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY backend/app ./app
COPY backend/engine ./engine
COPY --from=ui /ui/dist ./static
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "--factory", "app.main:build", "--host", "0.0.0.0", "--port", "8000"]
