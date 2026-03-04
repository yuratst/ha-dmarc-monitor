# Mail Security Monitor (Home Assistant Add-on)

This add-on reads DMARC aggregate reports from an IMAP mailbox, parses reports with `parsedmarc`, and writes a summary to:

- `/config/dmarc/summary.json`

## Options

- `imap_host` (default: `imap.strato.com`)
- `imap_user` (default: `dmarc@tsutsylivskyy.nl`)
- `imap_password` (required)

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
