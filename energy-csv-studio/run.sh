#!/usr/bin/with-contenv bash
set -euo pipefail

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [energy-csv-studio] $*"
}

read_options() {
  eval "$(python3 - <<'PY'
import json
import shlex
from pathlib import Path

options_file = Path('/data/options.json')
opts = {}
if options_file.exists():
    try:
        opts = json.loads(options_file.read_text(encoding='utf-8'))
    except Exception:
        opts = {}

db_path = str(opts.get('db_path') or '/config/home-assistant_v2.db')
workspace_dir = str(opts.get('workspace_dir') or '/config/_tmp_energy_restore')
timezone = str(opts.get('timezone') or 'Europe/Amsterdam')

print(f"DB_PATH={shlex.quote(db_path)}")
print(f"WORKSPACE_DIR={shlex.quote(workspace_dir)}")
print(f"TIMEZONE_NAME={shlex.quote(timezone)}")
PY
)"
}

read_options

export DB_PATH WORKSPACE_DIR TIMEZONE_NAME
export FLASK_APP=/opt/energy-csv-studio/app.py
export FLASK_ENV=production

mkdir -p "$WORKSPACE_DIR" "$WORKSPACE_DIR/uploads" "$WORKSPACE_DIR/exports"

log "Service started"
log "DB path: $DB_PATH"
log "Workspace: $WORKSPACE_DIR"

exec python3 /opt/energy-csv-studio/app.py
