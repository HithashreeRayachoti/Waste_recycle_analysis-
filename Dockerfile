FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 NYRT_DATA_DIR=/data

# Editable install: nyrt finds sql/ and seeds/ relative to the source tree in /app.
COPY pyproject.toml ./
COPY src ./src
COPY sql ./sql
COPY seeds ./seeds
COPY dashboard ./dashboard
RUN pip install --no-cache-dir -e .

RUN useradd --create-home nyrt && mkdir -p /data && chown -R nyrt /data /app/dashboard
USER nyrt

VOLUME ["/data"]
ENTRYPOINT ["nyrt"]
CMD ["run", "--incremental"]
