# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Requirements first, as their own layer: application code changes far more
# often than dependencies, and this keeps the pip install cached across rebuilds.
COPY hub/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY hub/ .

# The database lives on a volume mounted here, not in the image layer.
ENV DATABASE_URL=sqlite+aiosqlite:////data/sysfleet.db
RUN mkdir -p /data

EXPOSE 8000

# Migrations run at container start rather than at build time: the image has no
# access to the volume during build, and the schema must be current against
# whatever database the volume actually holds.
CMD ["sh", "-c", "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"]
