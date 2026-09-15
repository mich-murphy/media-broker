FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.12.5 \
    && uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev

USER 65532:65532
EXPOSE 8000
ENV MEDIA_BROKER_BIND_HOST=127.0.0.1
CMD ["/app/.venv/bin/media-broker"]
