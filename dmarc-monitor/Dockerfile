FROM python:3.11-alpine

RUN apk add --no-cache \
    bash \
    ca-certificates \
    tzdata \
    gcc \
    musl-dev \
    libffi-dev \
    openssl-dev \
    libxml2-dev \
    libxslt-dev

RUN pip install --no-cache-dir \
    parsedmarc \
    imapclient \
    lxml \
    python-dateutil

COPY run.sh /run.sh
RUN chmod +x /run.sh && mkdir -p /config/dmarc

VOLUME ["/config/dmarc"]
ENTRYPOINT ["/run.sh"]
