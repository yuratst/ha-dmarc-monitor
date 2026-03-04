#!/usr/bin/env python3
import json
import socket
import time
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_FILE = Path("/config/dmarc/smtp_status.json")
HOST = "smtp.strato.com"
PORT = 587
TIMEOUT_SECONDS = 10


def main():
    started = time.perf_counter()
    status = "offline"
    error = None

    try:
        with socket.create_connection((HOST, PORT), timeout=TIMEOUT_SECONDS):
            status = "online"
    except Exception as exc:
        status = "offline"
        error = str(exc)

    latency_ms = int((time.perf_counter() - started) * 1000)

    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "smtp_host": HOST,
        "smtp_port": PORT,
        "smtp_status": status,
        "latency_ms": latency_ms,
    }
    if error:
        payload["error"] = error

    OUTPUT_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
