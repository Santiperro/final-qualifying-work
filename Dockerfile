FROM python:3.11-slim

WORKDIR /github_patterns

RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    libnss3 libxss1 libasound2 libgbm1 libu2f-udev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock ./

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root

COPY github_patterns .

RUN mkdir -p github_patterns_app/migrations && \
    touch github_patterns_app/migrations/__init__.py

EXPOSE 8000