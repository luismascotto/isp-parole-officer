# isp-parole-officer

A small Go utility that monitors ISP connectivity by probing a list of hosts over TCP (port 443). It logs latency, retries on failure, and can optionally track public IP changes.

## Requirements

- Go 1.25+

## Build & run

```bash
go build
./isp-parole-officer
```

On Windows, use the included scripts:

- `build.bat` — compile the binary
- `run.bat` — build and start the monitor

## Configuration

Create a `config.json` in the project directory:

```json
{
  "hosts": ["1.1.1.1", "8.8.8.8", "cloudflare.com"],
  "dns_servers": [],
  "round_interval_seconds": 15,
  "round_timeout_seconds": 3,
  "round_retry_interval_seconds": 5,
  "ip_check_interval_seconds": 120,
  "ip_check_timeout_seconds": 10,
  "ip_check_url": "https://api.ipify.org"
}
```

| Field | Description |
|---|---|
| `hosts` | Hostnames or IPs to probe (majority must succeed per round) |
| `dns_servers` | Optional custom DNS resolvers; empty uses system default |
| `round_interval_seconds` | Delay between successful rounds |
| `round_timeout_seconds` | Probe host timeout |
| `retry_interval_seconds` | Delay after a failed round |
| `ip_check_interval_seconds` | Public IP check interval; `0` disables |
| `ip_check_timeout_seconds` | Public IP check timeout |
| `ip_check_url` | HTTP endpoint that returns your public IP |

## Output

Logs are printed to the console and written hourly under `Results/<session-id>/`. Press Ctrl+C to stop gracefully.

