#!/usr/bin/env python3
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

AGGREGATE_FILE = Path("/config/dmarc/aggregate.json")
OUTPUT_FILE = Path("/config/dmarc/ip_locations.json")
IPINFO_TOKEN = os.environ.get("IPINFO_TOKEN", "").strip()
HTTP_TIMEOUT = 8


def load_reports():
    if not AGGREGATE_FILE.exists():
        return []
    try:
        loaded = json.loads(AGGREGATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(loaded, list):
        return loaded
    if isinstance(loaded, dict):
        reports = loaded.get("reports")
        if isinstance(reports, list):
            return reports
    return []


def parse_count(record, row):
    for value in (record.get("count"), row.get("count"), 1):
        try:
            return max(int(value), 0)
        except Exception:
            continue
    return 1


def extract_ip_counts(reports):
    counts = Counter()
    for report in reports:
        if not isinstance(report, dict):
            continue
        records = report.get("records") or []
        if isinstance(records, dict):
            records = [records]
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            row = record.get("row") if isinstance(record.get("row"), dict) else {}
            source_ip = record.get("source_ip") or row.get("source_ip")
            if not source_ip:
                continue
            counts[str(source_ip)] += parse_count(record, row)
    return counts


def geolocate_ip(ip_address):
    endpoint = f"https://ipinfo.io/{quote(ip_address)}/json"
    if IPINFO_TOKEN:
        endpoint = f"{endpoint}?token={quote(IPINFO_TOKEN)}"
    request = Request(endpoint, headers={"User-Agent": "ha-dmarc-monitor/1.1.0"})
    try:
        with urlopen(request, timeout=HTTP_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (URLError, TimeoutError, ValueError, OSError):
        return {
            "ip": ip_address,
            "country": "unknown",
            "city": "unknown",
            "lat": None,
            "lon": None,
        }

    lat = None
    lon = None
    loc = payload.get("loc")
    if isinstance(loc, str) and "," in loc:
        parts = loc.split(",", 1)
        try:
            lat = float(parts[0].strip())
            lon = float(parts[1].strip())
        except Exception:
            lat = None
            lon = None

    return {
        "ip": ip_address,
        "country": payload.get("country") or "unknown",
        "city": payload.get("city") or "unknown",
        "lat": lat,
        "lon": lon,
    }


def main():
    reports = load_reports()
    counts = extract_ip_counts(reports)

    results = []
    for ip, count in counts.most_common(100):
        row = geolocate_ip(ip)
        row["count"] = count
        results.append(row)

    output = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "ips": results,
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
