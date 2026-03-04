ARG BUILD_FROM=ghcr.io/home-assistant/base:3.19
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    ca-certificates \
    bash \
    python3 \
    py3-pip \
    libxml2 \
    libxslt \
 && apk add --no-cache --virtual .build-deps \
    cargo \
    gcc \
    linux-headers \
    musl-dev \
    rust \
    python3-dev \
    libffi-dev \
    openssl-dev \
    libxml2-dev \
    libxslt-dev \
 && python3 -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
 && /opt/venv/bin/pip install --no-cache-dir --prefer-binary \
    parsedmarc \
    imapclient \
    lxml \
    python-dateutil \
 && apk del .build-deps

ENV PATH="/opt/venv/bin:${PATH}"

COPY run.sh /run.sh
COPY rootfs /
RUN chmod +x /run.sh /etc/services.d/dmarc-monitor/run && mkdir -p /config/dmarc

VOLUME ["/config/dmarc"]
