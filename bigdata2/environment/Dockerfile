FROM python:3.11-slim-bookworm

RUN python -m pip install --no-cache-dir \
    duckdb==1.4.5 \
    boto3==1.34.131 \
    jsonschema==4.26.0 \
    && python -c "import duckdb,boto3,jsonschema; print('Lab 2 runtime ready')" \
    && groupadd --gid 1000 lab \
    && useradd --uid 1000 --gid 1000 --create-home lab \
    && mkdir -p /work && chown lab:lab /work

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /work
USER 1000:1000
CMD ["sleep", "infinity"]
