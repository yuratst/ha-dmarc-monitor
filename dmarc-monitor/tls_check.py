#!/usr/bin/env python3
import json
import smtplib
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_FILE = Path("/config/dmarc/tls_report.json")
TIMEOUT_SECONDS = 10
PORT = 587

TARGETS = [
    {"name": "gmail.com", "host": "smtp.gmail.com"},
    {"name": "outlook.com", "host": "smtp.office365.com"},
    {"name": "yahoo.com", "host": "smtp.mail.yahoo.com"},
    {"name": "smtp.strato.com", "host": "smtp.strato.com"},
]


def parse_certificate_expiry(not_after):
    if not not_after:
        return None, None
    try:
        expires = datetime.strptime(not_after, "%b %d %H:%M:%S %Y %Z").replace(
            tzinfo=timezone.utc
        )
    except Exception:
        return None, None
    remaining = int((expires - datetime.now(timezone.utc)).total_seconds() // 86400)
    return expires.isoformat(), remaining


def check_target(item):
    result = {
        "server": item["name"],
        "host": item["host"],
        "port": PORT,
        "starttls": False,
        "tls_version": None,
        "certificate_expires": None,
        "certificate_days_remaining": None,
        "status": "offline",
        "latency_ms": None,
        "error": None,
    }

    started = time.perf_counter()
    try:
        with smtplib.SMTP(item["host"], PORT, timeout=TIMEOUT_SECONDS) as client:
            client.ehlo_or_helo_if_needed()
            result["starttls"] = bool(client.has_extn("starttls"))
            if result["starttls"]:
                context = ssl.create_default_context()
                client.starttls(context=context)
                client.ehlo()
                ssl_sock = client.sock
                if ssl_sock is not None and hasattr(ssl_sock, "version"):
                    result["tls_version"] = ssl_sock.version()
                if ssl_sock is not None and hasattr(ssl_sock, "getpeercert"):
                    cert = ssl_sock.getpeercert() or {}
                    expires, remaining = parse_certificate_expiry(cert.get("notAfter"))
                    result["certificate_expires"] = expires
                    result["certificate_days_remaining"] = remaining
            result["status"] = "online"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
    finally:
        result["latency_ms"] = int((time.perf_counter() - started) * 1000)

    return result


def main():
    rows = [check_target(target) for target in TARGETS]
    failed = sum(1 for row in rows if row.get("status") != "online")
    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "failed_servers": failed,
        "servers": rows,
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
