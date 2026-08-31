# syntax=docker/dockerfile:1
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /workspace
COPY . .
RUN python -m pip install --no-cache-dir ".[http]"

CMD ["python", "-m", "tests.tailnet.probe", "--help"]
