#!/usr/bin/env python3
"""Energy CSV tool for Home Assistant statistics.

Features:
- export: export daily cumulative gas/stroom/water to one editable CSV
- validate: validate edited CSV before import
- import: upsert CSV values back into statistics tables

Target statistic_ids:
- sensor.gas_meter_gas
- sensor.p1_meter_energie_import_tarief_1
- sensor.p1_meter_energie_import_tarief_2
- sensor.watermeter_total_water_usage
"""

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo


STAT_MAP = {
    "gas_m3": "sensor.gas_meter_gas",
    "stroom_t1_kwh": "sensor.p1_meter_energie_import_tarief_1",
    "stroom_t2_kwh": "sensor.p1_meter_energie_import_tarief_2",
    "water_l": "sensor.watermeter_total_water_usage",
}

CSV_COLUMNS = [
    "date",
    "gas_m3",
    "stroom_t1_kwh",
    "stroom_t2_kwh",
    "water_l",
    "notes",
]


@dataclass
class ValidationResult:
    errors: List[str]
    warnings: List[str]
    infos: List[str]


def _connect_readonly_immutable(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?immutable=1"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _connect_write(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=120.0)
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


def _parse_float(value: str) -> Optional[float]:
    txt = (value or "").strip()
    if txt == "":
        return None
    try:
        return float(txt.replace(",", "."))
    except ValueError:
        return None


def _fmt_float(value: Optional[float], decimals: int = 3) -> str:
    if value is None:
        return ""
    txt = f"{value:.{decimals}f}"
    txt = txt.rstrip("0").rstrip(".")
    return txt if txt else "0"


def _date_to_start_ts_utc(d: date, local_tz: ZoneInfo) -> float:
    local_dt = datetime(d.year, d.month, d.day, 0, 0, tzinfo=local_tz)
    return local_dt.astimezone(timezone.utc).timestamp()


def _start_ts_to_local_date(start_ts: float, local_tz: ZoneInfo) -> date:
    dt_utc = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    return dt_utc.astimezone(local_tz).date()


def _metadata_ids(conn: sqlite3.Connection) -> Dict[str, int]:
    cur = conn.cursor()
    result: Dict[str, int] = {}
    for col, sid in STAT_MAP.items():
        cur.execute("SELECT id FROM statistics_meta WHERE statistic_id = ?", (sid,))
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"statistics_meta not found for {sid}")
        result[col] = int(row[0])
    return result


def _load_daily_states(
    conn: sqlite3.Connection, metadata_id: int, local_tz: ZoneInfo
) -> Dict[date, float]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT start_ts, state
        FROM statistics
        WHERE metadata_id = ?
        ORDER BY start_ts ASC
        """,
        (metadata_id,),
    )
    per_day: Dict[date, float] = {}
    for start_ts, state in cur.fetchall():
        if state is None:
            continue
        d = _start_ts_to_local_date(float(start_ts), local_tz)
        per_day[d] = float(state)
    return per_day


def cmd_export(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    out_csv = Path(args.out)
    tz = ZoneInfo(args.timezone)

    conn = _connect_readonly_immutable(db_path)
    try:
        mids = _metadata_ids(conn)
        data: Dict[str, Dict[date, float]] = {}
        all_dates: set[date] = set()

        for col, mid in mids.items():
            m = _load_daily_states(conn, mid, tz)
            data[col] = m
            all_dates.update(m.keys())

        if not all_dates:
            print("No statistics found for export.")
            return 1

        min_d = min(all_dates)
        max_d = max(all_dates)
        rows = []
        d = min_d
        while d <= max_d:
            row = {
                "date": d.isoformat(),
                "gas_m3": _fmt_float(data["gas_m3"].get(d), 3),
                "stroom_t1_kwh": _fmt_float(data["stroom_t1_kwh"].get(d), 3),
                "stroom_t2_kwh": _fmt_float(data["stroom_t2_kwh"].get(d), 3),
                "water_l": _fmt_float(data["water_l"].get(d), 1),
                "notes": "",
            }
            rows.append(row)
            d += timedelta(days=1)

        out_csv.parent.mkdir(parents=True, exist_ok=True)
        with out_csv.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Exported {len(rows)} days to {out_csv}")
        print(f"Range: {min_d.isoformat()} -> {max_d.isoformat()}")
        return 0
    finally:
        conn.close()


def _iter_csv_rows(csv_path: Path) -> Iterable[dict]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = [c for c in CSV_COLUMNS if c not in reader.fieldnames]
        if missing:
            raise RuntimeError(f"CSV missing columns: {', '.join(missing)}")
        for row in reader:
            yield row


def _validate_rows(rows: List[dict], strict: bool = False) -> ValidationResult:
    errors: List[str] = []
    warnings: List[str] = []
    infos: List[str] = []

    if not rows:
        errors.append("CSV has no data rows.")
        return ValidationResult(errors, warnings, infos)

    parsed_dates: List[date] = []
    parsed_values: Dict[str, List[Tuple[date, Optional[float]]]] = {
        "gas_m3": [],
        "stroom_t1_kwh": [],
        "stroom_t2_kwh": [],
        "water_l": [],
    }

    for idx, row in enumerate(rows, start=2):
        raw_date = (row.get("date") or "").strip()
        try:
            d = datetime.strptime(raw_date, "%Y-%m-%d").date()
            parsed_dates.append(d)
        except ValueError:
            errors.append(f"line {idx}: invalid date '{raw_date}' (expected YYYY-MM-DD)")
            continue

        for key in parsed_values.keys():
            v = _parse_float(row.get(key, ""))
            if row.get(key, "").strip() != "" and v is None:
                errors.append(f"line {idx}: {key} is not numeric: '{row.get(key, '')}'")
            parsed_values[key].append((d, v))

    if errors:
        return ValidationResult(errors, warnings, infos)

    # Date order + duplicates + gaps
    if parsed_dates != sorted(parsed_dates):
        errors.append("dates are not sorted ascending")
    if len(set(parsed_dates)) != len(parsed_dates):
        errors.append("duplicate dates found")

    if parsed_dates:
        min_d = parsed_dates[0]
        max_d = parsed_dates[-1]
        expected_count = (max_d - min_d).days + 1
        if expected_count != len(parsed_dates):
            warnings.append(
                f"date gaps found: expected {expected_count} rows between {min_d} and {max_d}, got {len(parsed_dates)}"
            )

    # Cumulative monotonic checks
    for key, seq in parsed_values.items():
        prev: Optional[float] = None
        prev_d: Optional[date] = None
        for d, v in seq:
            if v is None:
                continue
            if v < 0:
                errors.append(f"{key}: negative value {v} on {d}")
            if prev is not None and v < prev:
                msg = f"{key}: non-monotonic drop on {d} ({v} < {prev})"
                if strict:
                    errors.append(msg)
                else:
                    warnings.append(msg)
            if prev is not None and prev_d is not None:
                delta = v - prev
                if delta > 10000:
                    warnings.append(f"{key}: very large jump on {d} (+{delta:.3f})")
            prev = v
            prev_d = d

    infos.append(f"rows checked: {len(rows)}")
    return ValidationResult(errors, warnings, infos)


def cmd_validate(args: argparse.Namespace) -> int:
    csv_path = Path(args.csv)
    rows = list(_iter_csv_rows(csv_path))
    result = _validate_rows(rows, strict=bool(args.strict))

    for msg in result.infos:
        print(f"[INFO] {msg}")
    for msg in result.warnings:
        print(f"[WARN] {msg}")
    for msg in result.errors:
        print(f"[ERROR] {msg}")

    if result.errors:
        print("Validation failed.")
        return 2
    print("Validation OK.")
    return 0


def _sum_offset(conn: sqlite3.Connection, metadata_id: int) -> float:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT state, sum
        FROM statistics
        WHERE metadata_id = ?
        ORDER BY start_ts ASC
        LIMIT 1
        """,
        (metadata_id,),
    )
    row = cur.fetchone()
    if row is None:
        return 0.0
    state = float(row[0] or 0.0)
    ssum = float(row[1] or 0.0)
    return state - ssum


def _collect_points_for_import(
    rows: Iterable[dict], local_tz: ZoneInfo
) -> Dict[str, List[Tuple[float, float]]]:
    out: Dict[str, List[Tuple[float, float]]] = {k: [] for k in STAT_MAP.keys()}
    for row in rows:
        d = datetime.strptime((row["date"] or "").strip(), "%Y-%m-%d").date()
        ts = _date_to_start_ts_utc(d, local_tz)
        for key in out.keys():
            v = _parse_float(row.get(key, ""))
            if v is None:
                continue
            out[key].append((ts, v))
    return out


def _upsert_points(
    cur: sqlite3.Cursor, metadata_id: int, offset: float, points: List[Tuple[float, float]]
) -> Tuple[int, int]:
    now_ts = datetime.now(timezone.utc).timestamp()
    n_stats = 0
    n_short = 0
    for ts, state in points:
        ssum = state - offset
        cur.execute(
            """
            INSERT INTO statistics (created_ts, metadata_id, start_ts, state, sum)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(metadata_id, start_ts) DO UPDATE SET
              created_ts = excluded.created_ts,
              state = excluded.state,
              sum = excluded.sum
            """,
            (now_ts, metadata_id, ts, state, ssum),
        )
        n_stats += 1
        cur.execute(
            """
            INSERT INTO statistics_short_term (created_ts, metadata_id, start_ts, state, sum)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(metadata_id, start_ts) DO UPDATE SET
              created_ts = excluded.created_ts,
              state = excluded.state,
              sum = excluded.sum
            """,
            (now_ts, metadata_id, ts, state, ssum),
        )
        n_short += 1
    return n_stats, n_short


def cmd_import(args: argparse.Namespace) -> int:
    db_path = Path(args.db)
    csv_path = Path(args.csv)
    tz = ZoneInfo(args.timezone)

    rows = list(_iter_csv_rows(csv_path))
    result = _validate_rows(rows, strict=bool(args.strict))
    if result.errors:
        for msg in result.errors:
            print(f"[ERROR] {msg}")
        print("Import aborted: CSV validation failed.")
        return 2
    for msg in result.warnings:
        print(f"[WARN] {msg}")

    points = _collect_points_for_import(rows, tz)
    for key, pts in points.items():
        if not pts:
            print(f"[WARN] {key}: no points in CSV")

    if args.dry_run:
        for key, sid in STAT_MAP.items():
            pts = points[key]
            if not pts:
                print(f"{sid}: 0 points")
                continue
            first = datetime.fromtimestamp(pts[0][0], tz=timezone.utc).isoformat()
            last = datetime.fromtimestamp(pts[-1][0], tz=timezone.utc).isoformat()
            print(f"{sid}: {len(pts)} points ({first} -> {last})")
        print("Dry-run OK.")
        return 0

    conn = _connect_write(db_path)
    try:
        mids = _metadata_ids(conn)
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.cursor()
        for key, sid in STAT_MAP.items():
            pts = points[key]
            if not pts:
                continue
            metadata_id = mids[key]
            offset = _sum_offset(conn, metadata_id)
            n_stats, n_short = _upsert_points(cur, metadata_id, offset, pts)
            print(
                f"{sid}: upserted statistics={n_stats}, short_term={n_short}, offset={offset:.6f}"
            )
        conn.commit()
        print("Import committed.")
        return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export/validate/import Home Assistant energy statistics via CSV."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_export = sub.add_parser("export", help="Export daily cumulative values to CSV")
    p_export.add_argument("--db", required=True, help="Path to home-assistant_v2.db")
    p_export.add_argument(
        "--out",
        required=True,
        help="Output CSV path (editable export)",
    )
    p_export.add_argument("--timezone", default="Europe/Amsterdam")
    p_export.set_defaults(func=cmd_export)

    p_validate = sub.add_parser("validate", help="Validate editable CSV")
    p_validate.add_argument("--csv", required=True, help="CSV path")
    p_validate.add_argument(
        "--strict",
        action="store_true",
        help="Treat cumulative drops as errors.",
    )
    p_validate.set_defaults(func=cmd_validate)

    p_import = sub.add_parser("import", help="Import editable CSV into statistics")
    p_import.add_argument("--db", required=True, help="Path to home-assistant_v2.db")
    p_import.add_argument("--csv", required=True, help="CSV path")
    p_import.add_argument("--timezone", default="Europe/Amsterdam")
    p_import.add_argument("--dry-run", action="store_true")
    p_import.add_argument(
        "--strict",
        action="store_true",
        help="Treat cumulative drops as errors before import.",
    )
    p_import.set_defaults(func=cmd_import)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
