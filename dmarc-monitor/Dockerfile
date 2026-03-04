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

WORKDIR /app
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh && mkdir -p /config/dmarc

VOLUME ["/config/dmarc"]
CMD ["/app/run.sh"]
