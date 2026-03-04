# Mail Security Monitor (Home Assistant Add-on)

This add-on connects to an IMAP mailbox, processes DMARC reports with `parsedmarc`, and writes JSON files for Home Assistant dashboards and sensors.

Data flow:

IMAP mailbox -> parsedmarc -> `/config/dmarc/*.json` -> Home Assistant sensors -> dashboard

## Generated files

- `/config/dmarc/aggregate_raw.json` (raw parsedmarc output)
- `/config/dmarc/aggregate.json` (dashboard-ready summary + reports)
- `/config/dmarc/summary.json`
- `/config/dmarc/ip_locations.json`
- `/config/dmarc/tls_report.json`
- `/config/dmarc/smtp_status.json`

## Add-on options

- `imap_host`
- `imap_user`
- `imap_password`

No passwords are stored in this repository.

## Use Home Assistant secrets.yaml

You can reference Home Assistant secrets from add-on options with `!secret` values.

Example `/config/secrets.yaml`:

```yaml
dmarc_imap_user: YOUR_IMAP_USER
dmarc_imap_password: YOUR_IMAP_PASSWORD
strato_imap_host: YOUR_IMAP_HOST
```

Example add-on options:

```yaml
imap_host: "!secret strato_imap_host"
imap_user: "!secret dmarc_imap_user"
imap_password: "!secret dmarc_imap_password"
```

## Home Assistant files in this repo

- `homeassistant/dmarc_sensors.yaml`
- `homeassistant/dashboard_mail_security.yaml`

The monitor checks mailbox reports every 30 minutes.
