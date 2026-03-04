FROM ghcr.io/home-assistant/base:3.19

RUN apk add --no-cache \
    python3 \
    py3-pip

RUN pip install --no-cache-dir \
    parsedmarc \
    imapclient \
    lxml \
    python-dateutil

COPY run.sh /run.sh
COPY rootfs /
RUN chmod +x /run.sh && mkdir -p /config/dmarc

VOLUME ["/config/dmarc"]
