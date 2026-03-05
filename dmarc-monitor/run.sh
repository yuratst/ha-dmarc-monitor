#!/usr/bin/with-contenv bash
set -euo pipefail

DMARC_DIR="/config/dmarc"
RUNTIME_CONFIG="/data/dmarc.ini"
RAW_AGGREGATE="$DMARC_DIR/aggregate_raw.json"
AGGREGATE_JSON="$DMARC_DIR/aggregate.json"
SLEEP_SECONDS=1800

mkdir -p "$DMARC_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [dmarc-monitor] $*"
}

load_options() {
  eval "$(python3 - <<'PY'
import json
import shlex
from pathlib import Path

OPTIONS_FILE = Path("/data/options.json")
SECRETS_FILE = Path("/config/secrets.yaml")


def read_secrets(path: Path):
    result = {}
    if not path.exists():
        return result
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
        result[key] = value
    return result


def resolve_option(value, secrets):
    if value is None:
        return ""
    if not isinstance(value, str):
        return str(value)
    cleaned = value.strip()
    if (cleaned.startswith('"') and cleaned.endswith('"')) or (
        cleaned.startswith("'") and cleaned.endswith("'")
    ):
        cleaned = cleaned[1:-1].strip()
    if cleaned.startswith("!secret "):
        secret_key = cleaned.split(" ", 1)[1].strip()
        return str(secrets.get(secret_key, "")).strip()
    return cleaned


options = {}
if OPTIONS_FILE.exists():
    try:
        options = json.loads(OPTIONS_FILE.read_text(encoding="utf-8"))
    except Exception:
        options = {}

secrets = read_secrets(SECRETS_FILE)

imap_host = resolve_option(options.get("imap_host", ""), secrets) or secrets.get(
    "strato_imap_host", ""
)
imap_user = resolve_option(options.get("imap_user", ""), secrets) or secrets.get(
    "dmarc_imap_user", ""
)
imap_password = resolve_option(options.get("imap_password", ""), secrets) or secrets.get(
    "dmarc_imap_password", ""
)
imap_reports_folder = resolve_option(options.get("imap_reports_folder", ""), secrets) or "INBOX"
ipinfo_token = secrets.get("ipinfo_token", "")

print(f"IMAP_HOST={shlex.quote(imap_host)}")
print(f"IMAP_USER={shlex.quote(imap_user)}")
print(f"IMAP_PASSWORD={shlex.quote(imap_password)}")
print(f"IMAP_REPORTS_FOLDER={shlex.quote(imap_reports_folder)}")
print(f"IPINFO_TOKEN={shlex.quote(ipinfo_token)}")
PY
)"

  export IMAP_HOST IMAP_USER IMAP_PASSWORD IMAP_REPORTS_FOLDER IPINFO_TOKEN
}

render_config() {
  python3 - <<'PY'
from pathlib import Path
import os

template_path = Path("/opt/dmarc-monitor/dmarc.ini.template")
runtime_path = Path("/data/dmarc.ini")

template = template_path.read_text(encoding="utf-8")

replacements = {
    "{{IMAP_HOST}}": os.environ.get("IMAP_HOST", "").replace("%", "%%"),
    "{{IMAP_USER}}": os.environ.get("IMAP_USER", "").replace("%", "%%"),
    "{{IMAP_PASSWORD}}": os.environ.get("IMAP_PASSWORD", "").replace("%", "%%"),
    "{{IMAP_REPORTS_FOLDER}}": os.environ.get("IMAP_REPORTS_FOLDER", "INBOX").replace("%", "%%"),
}

for needle, value in replacements.items():
    template = template.replace(needle, value)

runtime_path.write_text(template, encoding="utf-8")
PY
}

update_aggregate_json() {
  python3 - <<'PY'
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

raw_path = Path("/config/dmarc/aggregate_raw.json")
aggregate_path = Path("/config/dmarc/aggregate.json")
summary_path = Path("/config/dmarc/summary.json")


def load_reports():
    candidates = [raw_path, aggregate_path]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            loaded = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(loaded, list):
            return loaded
        if isinstance(loaded, dict):
            reports = loaded.get("reports")
            if isinstance(reports, list):
                return reports
    return []


def to_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


reports = load_reports()

messages_total = 0
spf_passes = 0
spf_failures = 0
dkim_passes = 0
dkim_failures = 0
dmarc_failures = 0
spoof_attempts = 0
ip_counter = Counter()

for report in reports:
    records = report.get("records") if isinstance(report, dict) else []
    if isinstance(records, dict):
        records = [records]
    if not isinstance(records, list):
        continue

    for record in records:
        if not isinstance(record, dict):
            continue
        row = record.get("row") if isinstance(record.get("row"), dict) else {}
        count = to_int(record.get("count", row.get("count", 1)), default=1)
        if count < 0:
            count = 0

        policy = (
            record.get("policy_evaluated")
            if isinstance(record.get("policy_evaluated"), dict)
            else row.get("policy_evaluated", {})
        )
        if not isinstance(policy, dict):
            policy = {}

        spf_ok = str(policy.get("spf", "")).lower() == "pass"
        dkim_ok = str(policy.get("dkim", "")).lower() == "pass"
        dmarc_ok = spf_ok or dkim_ok

        messages_total += count
        if spf_ok:
            spf_passes += count
        if not spf_ok:
            spf_failures += count
        if dkim_ok:
            dkim_passes += count
        if not dkim_ok:
            dkim_failures += count
        if not dmarc_ok:
            dmarc_failures += count

        # Spoof detection rule: increment on SPF fail OR DMARC fail.
        if (not spf_ok) or (not dmarc_ok):
            spoof_attempts += count

        source_ip = record.get("source_ip") or row.get("source_ip")
        if source_ip:
            ip_counter[str(source_ip)] += count

dmarc_passes = messages_total - dmarc_failures
spf_pass_rate = round((spf_passes / messages_total) * 100, 2) if messages_total > 0 else 0.0
dkim_pass_rate = round((dkim_passes / messages_total) * 100, 2) if messages_total > 0 else 0.0
dmarc_pass_rate = round(
    (dmarc_passes / messages_total) * 100
    if messages_total > 0
    else 0.0,
    2,
)

payload = {
    "updated_at": datetime.now(timezone.utc).isoformat(),
    "reports_total": len(reports),
    "total_messages": messages_total,
    "spf_pass_rate": spf_pass_rate,
    "spf_failures": spf_failures,
    "dkim_pass_rate": dkim_pass_rate,
    "dkim_failures": dkim_failures,
    "dmarc_failures": dmarc_failures,
    "spoof_attempts": spoof_attempts,
    "dmarc_pass_rate": dmarc_pass_rate,
    "top_sending_ips": [
        {"ip": ip, "count": count} for ip, count in ip_counter.most_common(10)
    ],
    "reports": reports,
}

summary = {
    "updated_at": payload["updated_at"],
    "reports_total": payload["reports_total"],
    "total_messages": payload["total_messages"],
    "spf_pass_rate": payload["spf_pass_rate"],
    "spf_failures": payload["spf_failures"],
    "dkim_pass_rate": payload["dkim_pass_rate"],
    "dkim_failures": payload["dkim_failures"],
    "dmarc_failures": payload["dmarc_failures"],
    "spoof_attempts": payload["spoof_attempts"],
    "dmarc_pass_rate": payload["dmarc_pass_rate"],
}

aggregate_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
PY
}

aggregate_has_reports() {
  local src="$1"
  python3 - "$src" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(1)
try:
    loaded = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    raise SystemExit(1)

reports = []
if isinstance(loaded, list):
    reports = loaded
elif isinstance(loaded, dict):
    report_list = loaded.get("reports")
    if isinstance(report_list, list):
        reports = report_list

raise SystemExit(0 if len(reports) > 0 else 1)
PY
}

run_once() {
  log "checking mailbox"
  load_options

  if [[ -z "${IMAP_HOST}" || -z "${IMAP_USER}" || -z "${IMAP_PASSWORD}" ]]; then
    log "IMAP options missing; skipping parsedmarc cycle"
    log "updating sensors"
    update_aggregate_json
    log "updating map data"
    python3 /opt/dmarc-monitor/ip_extractor.py >/dev/null 2>&1 || true
    python3 /opt/dmarc-monitor/tls_check.py >/dev/null 2>&1 || true
    python3 /opt/dmarc-monitor/smtp_monitor.py >/dev/null 2>&1 || true
    return 0
  fi
  if [[ "${IMAP_HOST}" == *"!secret"* || "${IMAP_USER}" == *"!secret"* || "${IMAP_PASSWORD}" == *"!secret"* ]]; then
    log "Unresolved !secret value detected in add-on options; check option formatting."
    return 0
  fi

  log "using imap host=${IMAP_HOST} user=${IMAP_USER} folder=${IMAP_REPORTS_FOLDER}"

  render_config

  log "parsing reports"
  set +e
  parsedmarc -c "${RUNTIME_CONFIG}"
  parsedmarc_rc=$?
  set -e
  if [[ ${parsedmarc_rc} -ne 0 ]]; then
    log "parsedmarc exited with code ${parsedmarc_rc}"
  fi

  if [[ -f "${AGGREGATE_JSON}" ]]; then
    if aggregate_has_reports "${AGGREGATE_JSON}"; then
      cp "${AGGREGATE_JSON}" "${RAW_AGGREGATE}"
      log "parsedmarc returned one or more reports"
    else
      log "no new reports returned; keeping previous aggregate history"
    fi
  fi

  log "updating sensors"
  update_aggregate_json

  log "updating map data"
  python3 /opt/dmarc-monitor/ip_extractor.py >/dev/null 2>&1 || log "ip map update failed"
  python3 /opt/dmarc-monitor/tls_check.py >/dev/null 2>&1 || log "tls check failed"
  python3 /opt/dmarc-monitor/smtp_monitor.py >/dev/null 2>&1 || log "smtp monitor failed"
}

echo "[dmarc-monitor] Starting parsedmarc"
log "service started"

while true; do
  run_once || log "unexpected cycle error"
  echo "[dmarc-monitor] parsedmarc finished"
  sleep "${SLEEP_SECONDS}"
done
