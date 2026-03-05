# Energy CSV Studio (Home Assistant Add-on)

Visual tool for repairing Home Assistant energy history using CSV.

## What it does

- Export energy statistics to editable CSV
- Upload edited CSV
- Validate CSV before import
- Import into existing statistics IDs (no helper/template rename)
- Auto backup before every import
- Rollback to previous DB backup

## Targeted statistics

- `sensor.gas_meter_gas`
- `sensor.p1_meter_energie_import_tarief_1`
- `sensor.p1_meter_energie_import_tarief_2`
- `sensor.watermeter_total_water_usage`

## UI actions

1. **Export latest from DB**
2. Edit CSV locally in Excel/Numbers/Sheets
3. Upload edited CSV
4. Validate
5. Import (backup + dry-run + commit)
6. Rollback if needed

## Notes

- CSV uses cumulative values.
- `strict` mode blocks imports when values drop.
- Non-strict mode allows historical resets and logs warnings.

## Security

- No passwords required.
- Add-on only reads/writes under `/config`.
