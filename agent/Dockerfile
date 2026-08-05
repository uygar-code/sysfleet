# syntax=docker/dockerfile:1
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY agent/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copied as a package directory so `python -m agent.main` resolves the same way
# it does when run from the repository root.
COPY agent/ ./agent/

# Run as a non-root user. The agent only reads /proc, which needs no privileges,
# and a monitoring process should hold the least authority that still works.
RUN useradd --create-home --uid 10001 sysfleet
USER sysfleet

CMD ["python", "-m", "agent.main"]
