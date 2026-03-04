# Mail Security Monitor (Home Assistant Add-on)

This add-on reads DMARC aggregate reports from an IMAP mailbox, parses reports with `parsedmarc`, and writes a summary to:

- `/config/dmarc/summary.json`

## Add-on options

- `imap_host`
- `imap_user`
- `imap_password` (required)

Passwords are **not** stored in this repository.

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

## Output JSON keys

- `reports_total`
- `messages_total`
- `spf_pass`
- `spf_fail`
- `dkim_pass`
- `dkim_fail`
- `dmarc_pass`
- `dmarc_fail`
- `spoof_attempts`
- `spf_pass_rate`
- `dkim_pass_rate`
- `dmarc_pass_rate`
- `top_sending_ips`
- `updated_at`
- `errors`

The monitor checks mailbox reports every 30 minutes.
