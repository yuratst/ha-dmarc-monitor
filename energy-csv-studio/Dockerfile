ARG BUILD_FROM=ghcr.io/home-assistant/base:3.19
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    bash \
    ca-certificates \
    python3 \
    py3-pip \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir \
    flask \
    requests

ENV PATH="/opt/venv/bin:${PATH}"

COPY app.py /opt/energy-csv-studio/app.py
COPY energy_csv_tool.py /opt/energy-csv-studio/energy_csv_tool.py
COPY templates /opt/energy-csv-studio/templates
COPY run.sh /run.sh
COPY rootfs /

RUN chmod +x /run.sh /etc/services.d/energy-csv-studio/run

