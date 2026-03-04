#!/usr/bin/with-contenv bash
set -euo pipefail

OUT_DIR="/config/dmarc"
INBOX_DIR="$OUT_DIR/inbox"
AGG_FILE="$OUT_DIR/aggregate.json"
SUMMARY_FILE="$OUT_DIR/summary.json"
DOMAIN="tsutsylivskyy.nl"
SLEEP_SECONDS=1800

mkdir -p "$OUT_DIR" "$INBOX_DIR"
echo "[dmarc-monitor] Version 1.0.3"
echo "[dmarc-monitor] Service started"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [dmarc-monitor] $*"
}

write_summary() {
  SUMMARY_INFO="$(python3 - <<'PY'
import json
from pathlib import Path

agg_file = Path('/config/dmarc/aggregate.json')
summary_file = Path('/config/dmarc/summary.json')
domain = 'tsutsylivskyy.nl'

summary = {
    'reports_total': 0,
    'messages_total': 0,
    'spf_pass': 0,
    'spf_fail': 0,
    'dkim_pass': 0,
    'dkim_fail': 0,
    'dmarc_pass': 0,
    'dmarc_fail': 0,
    'spoof_attempts': 0,
    'spf_pass_rate': 0,
    'dkim_pass_rate': 0,
    'dmarc_pass_rate': 0,
}

reports = []
if agg_file.exists():
    try:
        loaded = json.loads(agg_file.read_text(encoding='utf-8'))
        if isinstance(loaded, list):
            reports = loaded
        elif isinstance(loaded, dict):
            reports = loaded.get('reports') or []
    except Exception:
        reports = []

summary['reports_total'] = len(reports)

for report in reports:
    records = report.get('records') or []
    for record in records:
        try:
            cnt = int(record.get('count') or 0)
        except Exception:
            cnt = 0

        pol = record.get('policy_evaluated') or {}
        spf_ok = str(pol.get('spf', '')).lower() == 'pass'
        dkim_ok = str(pol.get('dkim', '')).lower() == 'pass'
        dmarc_ok = spf_ok or dkim_ok

        identifiers = record.get('identifiers') or {}
        header_from = (identifiers.get('header_from') or '').lower()
        if not header_from:
            header_from = ((report.get('policy_published') or {}).get('domain') or '').lower()

        spoof = (not dmarc_ok) and bool(domain) and header_from == domain

        summary['messages_total'] += cnt
        summary['spf_pass'] += cnt if spf_ok else 0
        summary['spf_fail'] += cnt if not spf_ok else 0
        summary['dkim_pass'] += cnt if dkim_ok else 0
        summary['dkim_fail'] += cnt if not dkim_ok else 0
        summary['dmarc_pass'] += cnt if dmarc_ok else 0
        summary['dmarc_fail'] += cnt if not dmarc_ok else 0
        summary['spoof_attempts'] += cnt if spoof else 0

if summary['messages_total'] > 0:
    total = summary['messages_total']
    summary['spf_pass_rate'] = round((summary['spf_pass'] / total) * 100, 2)
    summary['dkim_pass_rate'] = round((summary['dkim_pass'] / total) * 100, 2)
    summary['dmarc_pass_rate'] = round((summary['dmarc_pass'] / total) * 100, 2)

summary_file.write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(f"reports={summary['reports_total']} messages={summary['messages_total']}")
PY
)"

  log "Writing summary.json (${SUMMARY_INFO})"
}

run_once() {
  log "Checking inbox directory (${INBOX_DIR})"

  set +e
  parsedmarc \
    "${INBOX_DIR}" \
    -o "${OUT_DIR}"
  PARSEDMARC_RC=$?
  set -e

  if [[ ${PARSEDMARC_RC} -ne 0 ]]; then
    log "parsedmarc exited with code ${PARSEDMARC_RC}"
  else
    if [[ -f "${AGG_FILE}" ]]; then
      log "Found DMARC reports"
    else
      log "No aggregate.json generated (no new reports or parse issue)"
    fi
  fi

  write_summary
}

echo "[dmarc-monitor] Started"
log "Started"

while true; do
  run_once || log "Unexpected error in run_once; continuing"
  log "Sleeping ${SLEEP_SECONDS} seconds"
  sleep "${SLEEP_SECONDS}"
done
