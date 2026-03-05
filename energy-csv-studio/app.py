#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import List, Tuple

import requests
from flask import Flask, flash, redirect, render_template, request, send_file, url_for

APP_NAME = "Energy CSV Studio"
ROOT = Path("/opt/energy-csv-studio")
TOOL = ROOT / "energy_csv_tool.py"
DB_PATH = Path(os.environ.get("DB_PATH", "/config/home-assistant_v2.db"))
WORKSPACE = Path(os.environ.get("WORKSPACE_DIR", "/config/_tmp_energy_restore"))
TIMEZONE = os.environ.get("TIMEZONE_NAME", "Europe/Amsterdam")
SUPERVISOR_TOKEN = os.environ.get("SUPERVISOR_TOKEN", "")
CSV_COLUMNS = ["date", "gas_m3", "stroom_t1_kwh", "stroom_t2_kwh", "water_l", "notes"]

app = Flask(__name__, template_folder=str(ROOT / "templates"))
app.secret_key = os.environ.get("ENERGY_CSV_STUDIO_SECRET", "energy-csv-studio")


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [energy-csv-studio] {msg}", flush=True)


def _ingress_prefix() -> str:
    for key in ("X-Ingress-Path", "X-Forwarded-Prefix"):
        raw = (request.headers.get(key) or "").strip()
        if raw:
            return "/" + raw.strip("/")
    script_root = (request.script_root or "").strip()
    if script_root:
        return "/" + script_root.strip("/")
    return ""


def _ingress_url(endpoint: str, **values) -> str:
    path = url_for(endpoint, **values)
    prefix = _ingress_prefix()
    if not prefix:
        return path
    if path == prefix or path.startswith(prefix + "/"):
        return path
    if path.startswith("/"):
        return prefix + path
    return prefix + "/" + path


@app.before_request
def _set_script_name_from_ingress_headers():
    # Ensure url_for() includes ingress prefix when HA proxies under /api/hassio_ingress/<token>.
    prefix = (request.headers.get("X-Ingress-Path") or request.headers.get("X-Forwarded-Prefix") or "").strip()
    if prefix:
        request.environ["SCRIPT_NAME"] = "/" + prefix.strip("/")


@app.context_processor
def _inject_template_helpers():
    return {"ingress_url": _ingress_url}


def _ensure_workspace() -> None:
    WORKSPACE.mkdir(parents=True, exist_ok=True)
    (WORKSPACE / "uploads").mkdir(parents=True, exist_ok=True)
    (WORKSPACE / "exports").mkdir(parents=True, exist_ok=True)


def _run_tool(args: List[str]) -> Tuple[int, str]:
    cmd = ["python3", str(TOOL), *args]
    _log("Running: " + " ".join(cmd))
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    output = proc.stdout.strip()
    if output:
        _log(output)
    return proc.returncode, output


def _list_csv_files() -> List[str]:
    files: List[str] = []
    if not WORKSPACE.exists():
        return files
    for p in WORKSPACE.rglob("*.csv"):
        if p.is_file():
            files.append(str(p.relative_to(WORKSPACE)))
    files.sort(key=lambda rel: (WORKSPACE / rel).stat().st_mtime, reverse=True)
    return files


def _list_backups() -> List[str]:
    pattern = f"{DB_PATH.name}.energy_csv_studio_*.bak"
    backups = [p.name for p in DB_PATH.parent.glob(pattern) if p.is_file()]
    backups.sort(reverse=True)
    return backups


def _resolve_under_workspace(rel_or_abs: str) -> Path:
    candidate = Path(rel_or_abs)
    if candidate.is_absolute():
        resolved = candidate.resolve()
    else:
        resolved = (WORKSPACE / candidate).resolve()
    workspace_resolved = WORKSPACE.resolve()
    if not str(resolved).startswith(str(workspace_resolved)):
        raise ValueError("Path outside workspace is not allowed")
    return resolved


def _preview_csv(path: Path, max_rows: int = 20):
    headers = []
    rows = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            headers = next(reader, [])
            for i, row in enumerate(reader):
                rows.append(row)
                if i + 1 >= max_rows:
                    break
    except Exception as exc:
        return [], [], str(exc)
    return headers, rows, ""


def _read_csv_dict_rows(path: Path) -> Tuple[List[str], List[dict]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        if not headers:
            headers = CSV_COLUMNS[:]
        for col in CSV_COLUMNS:
            if col not in headers:
                headers.append(col)
        rows = []
        for row in reader:
            normalized = {k: (row.get(k, "") or "").strip() for k in headers}
            rows.append(normalized)
    return headers, rows


def _write_csv_dict_rows(path: Path, headers: List[str], rows: List[dict]) -> None:
    for col in CSV_COLUMNS:
        if col not in headers:
            headers.append(col)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _normalize_decimal(raw: str) -> str:
    txt = (raw or "").strip()
    if txt == "":
        return ""
    txt = txt.replace(",", ".")
    float(txt)  # validate
    return txt


def _upsert_csv_date(path: Path, d: date, updates: dict) -> Tuple[bool, str]:
    headers, rows = _read_csv_dict_rows(path)
    target = d.isoformat()
    found = False

    for row in rows:
        if (row.get("date") or "").strip() != target:
            continue
        found = True
        for key, value in updates.items():
            if value is None:
                continue
            row[key] = value
        break

    if not found:
        new_row = {h: "" for h in headers}
        for col in CSV_COLUMNS:
            new_row.setdefault(col, "")
        new_row["date"] = target
        for key, value in updates.items():
            if value is None:
                continue
            new_row[key] = value
        rows.append(new_row)

    def sort_key(row: dict):
        raw = (row.get("date") or "").strip()
        try:
            return (0, datetime.strptime(raw, "%Y-%m-%d").date())
        except Exception:
            return (1, raw)

    rows.sort(key=sort_key)
    _write_csv_dict_rows(path, headers, rows)
    return found, target


def _supervisor_post(path: str) -> Tuple[bool, str]:
    if not SUPERVISOR_TOKEN:
        return False, "SUPERVISOR_TOKEN missing"
    try:
        resp = requests.post(
            f"http://supervisor{path}",
            headers={"Authorization": f"Bearer {SUPERVISOR_TOKEN}"},
            timeout=20,
        )
        if resp.status_code >= 300:
            return False, f"HTTP {resp.status_code}: {resp.text[:200]}"
        return True, resp.text[:200]
    except Exception as exc:
        return False, str(exc)


def _stop_core() -> Tuple[bool, str]:
    return _supervisor_post("/core/stop")


def _start_core() -> Tuple[bool, str]:
    return _supervisor_post("/core/start")


def _backup_db() -> Tuple[Path, List[Path]]:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_db = DB_PATH.parent / f"{DB_PATH.name}.energy_csv_studio_{ts}.bak"
    shutil.copy2(DB_PATH, backup_db)
    extras: List[Path] = []
    wal = Path(str(DB_PATH) + "-wal")
    shm = Path(str(DB_PATH) + "-shm")
    if wal.exists():
        wal_bak = DB_PATH.parent / f"{wal.name}.energy_csv_studio_{ts}.bak"
        shutil.copy2(wal, wal_bak)
        extras.append(wal_bak)
    if shm.exists():
        shm_bak = DB_PATH.parent / f"{shm.name}.energy_csv_studio_{ts}.bak"
        shutil.copy2(shm, shm_bak)
        extras.append(shm_bak)
    return backup_db, extras


def _restore_backup(backup_name: str) -> str:
    backup_db = (DB_PATH.parent / backup_name).resolve()
    if not backup_db.exists():
        raise FileNotFoundError(f"Backup not found: {backup_db}")
    if not str(backup_db).startswith(str(DB_PATH.parent.resolve())):
        raise ValueError("Invalid backup path")

    shutil.copy2(backup_db, DB_PATH)

    stamp = backup_db.name.split(".energy_csv_studio_")[-1].replace(".bak", "")
    wal_src = DB_PATH.parent / f"{DB_PATH.name}-wal.energy_csv_studio_{stamp}.bak"
    shm_src = DB_PATH.parent / f"{DB_PATH.name}-shm.energy_csv_studio_{stamp}.bak"

    wal_dst = Path(str(DB_PATH) + "-wal")
    shm_dst = Path(str(DB_PATH) + "-shm")

    if wal_src.exists():
        shutil.copy2(wal_src, wal_dst)
    elif wal_dst.exists():
        wal_dst.unlink(missing_ok=True)

    if shm_src.exists():
        shutil.copy2(shm_src, shm_dst)
    elif shm_dst.exists():
        shm_dst.unlink(missing_ok=True)

    return stamp


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}


@app.get("/")
def index():
    _ensure_workspace()
    csv_files = _list_csv_files()
    backups = _list_backups()
    preview_file = request.args.get("preview", "")
    preview_headers: List[str] = []
    preview_rows: List[List[str]] = []
    preview_error = ""

    if preview_file:
        try:
            p = _resolve_under_workspace(preview_file)
            if p.exists():
                preview_headers, preview_rows, preview_error = _preview_csv(p)
        except Exception as exc:
            preview_error = str(exc)

    db_size_mb = 0.0
    if DB_PATH.exists():
        db_size_mb = DB_PATH.stat().st_size / (1024 * 1024)

    return render_template(
        "index.html",
        app_name=APP_NAME,
        db_path=str(DB_PATH),
        db_size_mb=f"{db_size_mb:.1f}",
        workspace=str(WORKSPACE),
        timezone=TIMEZONE,
        csv_files=csv_files,
        backups=backups,
        preview_file=preview_file,
        preview_headers=preview_headers,
        preview_rows=preview_rows,
        preview_error=preview_error,
    )


@app.post("/export")
def export_csv():
    _ensure_workspace()
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    out = WORKSPACE / "exports" / f"energy_daily_edit_{ts}.csv"
    rc, output = _run_tool(
        [
            "export",
            "--db",
            str(DB_PATH),
            "--out",
            str(out),
            "--timezone",
            TIMEZONE,
        ]
    )
    if rc == 0:
        flash(f"Export OK: {out}", "success")
    else:
        flash(f"Export failed: {output}", "error")
    return redirect(_ingress_url("index", preview=str(out.relative_to(WORKSPACE))))


@app.post("/upload")
def upload_csv():
    _ensure_workspace()
    f = request.files.get("csv_file")
    if f is None or not f.filename:
        flash("No file uploaded", "error")
        return redirect(_ingress_url("index"))

    if not f.filename.lower().endswith(".csv"):
        flash("Only .csv files are allowed", "error")
        return redirect(_ingress_url("index"))

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in Path(f.filename).name)
    target = WORKSPACE / "uploads" / f"{ts}_{safe}"
    f.save(target)
    flash(f"Uploaded: {target.name}", "success")
    return redirect(_ingress_url("index", preview=str(target.relative_to(WORKSPACE))))


@app.post("/validate")
def validate_csv():
    selected = (request.form.get("csv_path") or "").strip()
    if not selected:
        flash("Select a CSV file first", "error")
        return redirect(_ingress_url("index"))

    try:
        path = _resolve_under_workspace(selected)
    except Exception as exc:
        flash(f"Invalid file: {exc}", "error")
        return redirect(_ingress_url("index"))

    rc, output = _run_tool(["validate", "--csv", str(path)])
    if rc == 0:
        flash("Validation OK", "success")
    else:
        flash("Validation failed", "error")
    flash(output or "(no output)", "log")
    return redirect(_ingress_url("index", preview=selected))


@app.post("/set-date")
def set_date():
    selected = (request.form.get("csv_path") or "").strip()
    raw_date = (request.form.get("edit_date") or "").strip()
    if not selected:
        flash("Select a CSV file first", "error")
        return redirect(_ingress_url("index"))
    if not raw_date:
        flash("Date is required", "error")
        return redirect(_ingress_url("index", preview=selected))

    try:
        edit_date = datetime.strptime(raw_date, "%Y-%m-%d").date()
    except ValueError:
        flash("Invalid date format (expected YYYY-MM-DD)", "error")
        return redirect(_ingress_url("index", preview=selected))

    try:
        path = _resolve_under_workspace(selected)
    except Exception as exc:
        flash(f"Invalid file: {exc}", "error")
        return redirect(_ingress_url("index"))

    if not path.exists():
        flash("CSV file not found", "error")
        return redirect(_ingress_url("index"))

    updates = {}
    try:
        for key in ("gas_m3", "stroom_t1_kwh", "stroom_t2_kwh", "water_l"):
            raw = request.form.get(key, "")
            # Empty input means: do not change existing value.
            updates[key] = _normalize_decimal(raw) if raw.strip() != "" else None
        notes_raw = request.form.get("notes", "")
        updates["notes"] = notes_raw.strip() if notes_raw.strip() != "" else None
    except ValueError:
        flash("Numeric field has invalid value", "error")
        return redirect(_ingress_url("index", preview=selected))

    try:
        existed, target = _upsert_csv_date(path, edit_date, updates)
        action = "Updated" if existed else "Inserted"
        flash(f"{action} row for {target} in {selected}", "success")
    except Exception as exc:
        flash(f"Date edit failed: {exc}", "error")

    return redirect(_ingress_url("index", preview=selected))


@app.post("/import")
def import_csv():
    selected = (request.form.get("csv_path") or "").strip()
    auto_core = request.form.get("auto_core") == "on"
    strict = request.form.get("strict") == "on"

    if not selected:
        flash("Select a CSV file first", "error")
        return redirect(_ingress_url("index"))

    try:
        path = _resolve_under_workspace(selected)
    except Exception as exc:
        flash(f"Invalid file: {exc}", "error")
        return redirect(_ingress_url("index"))

    core_stopped = False
    try:
        if auto_core:
            ok, msg = _stop_core()
            if ok:
                core_stopped = True
                flash("Core stopped", "success")
            else:
                flash(f"Core stop failed: {msg}", "warn")
            time.sleep(2)

        backup_db, extra = _backup_db()
        flash(f"Backup created: {backup_db.name}", "success")
        for e in extra:
            flash(f"Backup created: {e.name}", "log")

        dry_args = ["import", "--db", str(DB_PATH), "--csv", str(path), "--dry-run", "--timezone", TIMEZONE]
        if strict:
            dry_args.append("--strict")
        rc_dry, out_dry = _run_tool(dry_args)
        flash(out_dry or "(no output)", "log")
        if rc_dry != 0:
            flash("Dry-run failed; import aborted", "error")
            return redirect(_ingress_url("index", preview=selected))

        run_args = ["import", "--db", str(DB_PATH), "--csv", str(path), "--timezone", TIMEZONE]
        if strict:
            run_args.append("--strict")
        rc, output = _run_tool(run_args)
        flash(output or "(no output)", "log")
        if rc == 0:
            flash("Import committed", "success")
        else:
            flash("Import failed", "error")

    except Exception as exc:
        flash(f"Import error: {exc}", "error")
    finally:
        if auto_core and core_stopped:
            ok, msg = _start_core()
            if ok:
                flash("Core started", "success")
            else:
                flash(f"Core start failed: {msg}", "warn")

    return redirect(_ingress_url("index", preview=selected))


@app.post("/rollback")
def rollback():
    backup = (request.form.get("backup_name") or "").strip()
    auto_core = request.form.get("auto_core") == "on"

    if not backup:
        flash("Select backup first", "error")
        return redirect(_ingress_url("index"))

    core_stopped = False
    try:
        if auto_core:
            ok, msg = _stop_core()
            if ok:
                core_stopped = True
                flash("Core stopped", "success")
            else:
                flash(f"Core stop failed: {msg}", "warn")
            time.sleep(2)

        stamp = _restore_backup(backup)
        flash(f"Rollback restored backup timestamp {stamp}", "success")

    except Exception as exc:
        flash(f"Rollback failed: {exc}", "error")
    finally:
        if auto_core and core_stopped:
            ok, msg = _start_core()
            if ok:
                flash("Core started", "success")
            else:
                flash(f"Core start failed: {msg}", "warn")

    return redirect(_ingress_url("index"))


@app.get("/download")
def download_csv():
    selected = (request.args.get("file") or "").strip()
    if not selected:
        flash("No file selected", "error")
        return redirect(_ingress_url("index"))
    try:
        path = _resolve_under_workspace(selected)
    except Exception as exc:
        flash(f"Invalid file: {exc}", "error")
        return redirect(_ingress_url("index"))
    if not path.exists() or not path.is_file():
        flash("File not found", "error")
        return redirect(_ingress_url("index"))
    return send_file(path, as_attachment=True, download_name=path.name)


if __name__ == "__main__":
    _ensure_workspace()
    _log(f"Starting web UI on 0.0.0.0:8099 (db={DB_PATH}, workspace={WORKSPACE})")
    app.run(host="0.0.0.0", port=8099, debug=False)
