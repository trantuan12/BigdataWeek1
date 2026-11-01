FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PYSPARK_SUBMIT_ARGS="--driver-memory 1536m pyspark-shell" \
    LAB3_PRELOADED_JARS=1 \
    PYTHONUNBUFFERED=1 \
    MPLCONFIGDIR=/tmp/matplotlib \
    TZ=UTC
ENV PATH="${SPARK_HOME}/bin:${PATH}"
COPY infrastructure/requirements.txt /opt/lab3/requirements.txt
RUN python -m pip install --no-cache-dir -r /opt/lab3/requirements.txt \
    && python -m pip freeze > /opt/lab3/requirements.lock.txt
COPY infrastructure/cache_jars.py /opt/lab3/cache_jars.py
RUN python /opt/lab3/cache_jars.py
WORKDIR /workspace
CMD ["sleep", "infinity"]
