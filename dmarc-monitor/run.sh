#!/usr/bin/env bash
set -euo pipefail

OPTIONS_FILE="/data/options.json"
SECRETS_FILE="/config/secrets.yaml"
OUT_DIR="/config/dmarc"
RAW_DIR="$OUT_DIR/raw"
PROCESSED_DIR="$OUT_DIR/processed"
UID_DB="$OUT_DIR/processed_uids.json"
SUMMARY_FILE="$OUT_DIR/summary.json"
DOMAIN="tsutsylivskyy.nl"

mkdir -p "$RAW_DIR" "$PROCESSED_DIR"

run_once() {
python3 - <<'PY'
import gzip
import hashlib
import io
import json
import zipfile
from collections import Counter
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from pathlib import Path

from imapclient import IMAPClient
from parsedmarc import parse_report_file

OPTIONS_FILE = Path("/data/options.json")
SECRETS_FILE = Path("/config/secrets.yaml")
OUT_DIR = Path("/config/dmarc")
RAW_DIR = OUT_DIR / "raw"
PROCESSED_DIR = OUT_DIR / "processed"
UID_DB = OUT_DIR / "processed_uids.json"
SUMMARY_FILE = OUT_DIR / "summary.json"
DOMAIN = "tsutsylivskyy.nl"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def read_secrets(path: Path) -> dict:
    data = {}
    if not path.exists():
        return data

    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        data[key] = value

    return data


def resolve_option(value, secrets: dict) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        return str(value)

    cleaned = value.strip()
    if cleaned.startswith("!secret "):
        secret_key = cleaned.split(" ", 1)[1].strip()
        return str(secrets.get(secret_key, "")).strip()

    return cleaned


if OPTIONS_FILE.exists():
    options = json.loads(OPTIONS_FILE.read_text(encoding="utf-8"))
else:
    options = {}

secrets = read_secrets(SECRETS_FILE)

imap_host = resolve_option(options.get("imap_host", "imap.strato.com"), secrets)
imap_user = resolve_option(options.get("imap_user", "dmarc@tsutsylivskyy.nl"), secrets)
imap_password = resolve_option(options.get("imap_password", ""), secrets)

# Fallback secret names (when option is empty)
if not imap_host:
    imap_host = secrets.get("strato_imap_host", "imap.strato.com")
if not imap_user:
    imap_user = secrets.get("dmarc_imap_user", "dmarc@tsutsylivskyy.nl")
if not imap_password:
    imap_password = secrets.get("dmarc_imap_password", "")

processed_uids = set()
if UID_DB.exists():
    try:
        processed_uids = set(json.loads(UID_DB.read_text(encoding="utf-8")))
    except Exception:
        processed_uids = set()


def extract_xml_payloads(filename: str, payload: bytes):
    filename = (filename or "attachment").lower()
    if filename.endswith(".xml"):
        return [payload]
    if filename.endswith(".gz"):
        try:
            return [gzip.decompress(payload)]
        except Exception:
            return []
    if filename.endswith(".zip"):
        results = []
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".xml"):
                        results.append(zf.read(name))
        except Exception:
            return []
        return results
    return []


def save_xml(uid: str, index: int, xml_bytes: bytes) -> Path:
    digest = hashlib.sha256(xml_bytes).hexdigest()[:16]
    name = f"uid_{uid}_{index}_{digest}.xml"
    path = RAW_DIR / name
    path.write_bytes(xml_bytes)
    return path


def parse_records(report_obj):
    records = report_obj.get("records") or []
    for record in records:
        count = int(record.get("count") or 0)
        pol = record.get("policy_evaluated") or {}

        spf_result = str(pol.get("spf", "fail")).lower()
        dkim_result = str(pol.get("dkim", "fail")).lower()

        header_from = (
            (record.get("identifiers") or {}).get("header_from")
            or (report_obj.get("policy_published") or {}).get("domain")
            or ""
        )
        header_from = str(header_from).lower()

        dmarc_pass = spf_result == "pass" or dkim_result == "pass"
        spoof_attempt = (not dmarc_pass) and (header_from == DOMAIN)

        src = (record.get("source") or {}).get("ip_address", "unknown")
        yield {
            "count": count,
            "spf_pass": spf_result == "pass",
            "dkim_pass": dkim_result == "pass",
            "dmarc_pass": dmarc_pass,
            "spoof_attempt": spoof_attempt,
            "source_ip": str(src),
        }


summary = {
    "reports_total": 0,
    "messages_total": 0,
    "spf_pass": 0,
    "spf_fail": 0,
    "dkim_pass": 0,
    "dkim_fail": 0,
    "dmarc_pass": 0,
    "dmarc_fail": 0,
    "spoof_attempts": 0,
    "spf_pass_rate": 0,
    "dkim_pass_rate": 0,
    "dmarc_pass_rate": 0,
}

ip_counter = Counter()
errors = []
new_uids = set(processed_uids)

if not imap_password:
    errors.append("imap_password is empty (set add-on option or !secret)")
else:
    try:
        with IMAPClient(imap_host, ssl=True) as client:
            client.login(imap_user, imap_password)
            client.select_folder("INBOX", readonly=False)
            uids = client.search(["ALL"])
            pending = [str(uid) for uid in uids if str(uid) not in processed_uids]

            for uid in pending:
                fetched = client.fetch([int(uid)], [b"RFC822"])
                message_bytes = fetched.get(int(uid), {}).get(b"RFC822")
                if not message_bytes:
                    continue

                msg = BytesParser(policy=policy.default).parsebytes(message_bytes)
                xml_paths = []
                idx = 0

                for part in msg.walk():
                    if part.get_content_disposition() != "attachment":
                        continue
                    filename = part.get_filename() or "attachment"
                    payload = part.get_payload(decode=True) or b""
                    for xml_payload in extract_xml_payloads(filename, payload):
                        idx += 1
                        xml_paths.append(save_xml(uid, idx, xml_payload))

                if not xml_paths:
                    new_uids.add(uid)
                    continue

                for xml_path in xml_paths:
                    try:
                        parsed = parse_report_file(str(xml_path), offline=True, nameservers=[])
                        summary["reports_total"] += 1
                        for rec in parse_records(parsed):
                            cnt = rec["count"]
                            summary["messages_total"] += cnt
                            if rec["spf_pass"]:
                                summary["spf_pass"] += cnt
                            else:
                                summary["spf_fail"] += cnt
                            if rec["dkim_pass"]:
                                summary["dkim_pass"] += cnt
                            else:
                                summary["dkim_fail"] += cnt
                            if rec["dmarc_pass"]:
                                summary["dmarc_pass"] += cnt
                            else:
                                summary["dmarc_fail"] += cnt
                            if rec["spoof_attempt"]:
                                summary["spoof_attempts"] += cnt
                            ip_counter[rec["source_ip"]] += cnt

                        done_name = PROCESSED_DIR / xml_path.name
                        xml_path.replace(done_name)
                    except Exception as exc:
                        errors.append(f"parse_failed:{xml_path.name}:{exc}")

                new_uids.add(uid)

            client.logout()
    except Exception as exc:
        errors.append(f"imap_error:{exc}")

if summary["messages_total"] > 0:
    total = summary["messages_total"]
    summary["spf_pass_rate"] = round((summary["spf_pass"] / total) * 100, 2)
    summary["dkim_pass_rate"] = round((summary["dkim_pass"] / total) * 100, 2)
    summary["dmarc_pass_rate"] = round((summary["dmarc_pass"] / total) * 100, 2)

summary["top_sending_ips"] = [
    {"ip": ip, "messages": count}
    for ip, count in ip_counter.most_common(10)
]
summary["updated_at"] = datetime.now(timezone.utc).isoformat()
summary["errors"] = errors

SUMMARY_FILE.write_text(json.dumps(summary, indent=2), encoding="utf-8")
UID_DB.write_text(json.dumps(sorted(new_uids)), encoding="utf-8")
PY
}

echo "[dmarc-monitor] Started"
while true; do
  run_once || true
  sleep 1800
done
