# vmbackuppy

Backup scheduler for VictoriaMetrics in k8s.

Runs as a Kubernetes sidecar, performs scheduled incremental backups via [vmbackup](https://docs.victoriametrics.com/vmbackup/) with automatic retention management and restore support via [vmrestore](https://docs.victoriametrics.com/vmrestore/).

## Features

- Scheduled incremental backups with configurable interval
- Hourly / daily / weekly / monthly retention policies
- Restore via restore marks (JSON file in S3) + initContainer
- HTTP API for backup/restore management
- Prometheus metrics

## Quick start

```yaml
# k8s sidecar
containers:
  - name: vmbackuppy
    image: vmbackuppy:latest
    env:
      - name: BACKUP_DESTINATION
        value: s3://my-bucket/vm-backups
      - name: VM_URL
        value: http://localhost:8428
```

See [examples/example-vmsingle.yaml](examples/example-vmsingle.yaml) for a full example with initContainer for restore.

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKUP_DESTINATION` | *required* | S3 backup destination (`s3://bucket/path`) |
| `VM_URL` | `http://localhost:8428` | VictoriaMetrics URL |
| `STORAGE_DATA_PATH` | `/victoria-metrics-data` | VM data path |
| `BACKUP_SCHEDULE` | | Cron expression (`0 * * * *`), overrides `BACKUP_INTERVAL` |
| `BACKUP_INTERVAL` | `1h` | Interval between backups (`1h`, `30m`, `3600`) |
| `KEEP_LAST_HOURLY` | `24` | Hourly backups to keep (0 = disable) |
| `KEEP_LAST_DAILY` | `7` | Daily backups to keep (0 = disable) |
| `KEEP_LAST_WEEKLY` | `4` | Weekly backups to keep (0 = disable) |
| `KEEP_LAST_MONTHLY` | `12` | Monthly backups to keep (0 = disable) |
| `BACKUP_CONCURRENCY` | `10` | vmbackup concurrency |
| `S3_ENDPOINT` | | Custom S3 endpoint (MinIO, etc.) |
| `S3_FORCE_PATH_STYLE` | `true` | Use path-style S3 addressing |
| `HTTP_ADDR` | `0.0.0.0` | HTTP server listen address |
| `HTTP_PORT` | `8491` | HTTP server port |
| `LOG_LEVEL` | `INFO` | Log level |
| `RUN_AT_STARTUP` | `true` | Run backup immediately on start |

## HTTP API

### Service endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Health check |
| GET | `/metrics` | Prometheus metrics |

### Backup

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/backups` | List all backups |
| POST | `/api/v1/backups` | Trigger backup |

### Restore

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/restore` | Create restore mark |
| GET | `/api/v1/restore` | Get current restore mark |
| DELETE | `/api/v1/restore` | Delete restore mark |


API совместим с [vmbackupmanager](https://docs.victoriametrics.com/vmbackupmanager/).

## Restore workflow

1. List available backups:
   ```bash
   curl http://localhost:8491/api/v1/backups
   ```

2. Create restore mark:
   ```bash
   curl -X POST http://localhost:8491/api/v1/restore \
     -H 'Content-Type: application/json' \
     -d '{"backup": "daily/2026-03-14"}'
   ```

3. Restart the pod — initContainer finds the mark, runs vmrestore, deletes the mark:
   ```bash
   kubectl delete pod <pod-name>
   ```

4. VictoriaMetrics starts with restored data.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `vmbackuppy_backup_success_total` | counter | Successful backups |
| `vmbackuppy_backup_errors_total` | counter | Failed backups |
| `vmbackuppy_backup_duration_seconds` | gauge | Duration of last backup |
| `vmbackuppy_backup_last_success_timestamp` | gauge | Unix timestamp of last success |

## Backup structure in S3

```
s3://bucket/vm-backups/
  latest/          # always the most recent backup
  hourly/
    2026-03-14:09/
    2026-03-14:10/
  daily/
    2026-03-13/
    2026-03-14/
  weekly/
    2026-W11/
  monthly/
    2026-03/
  restore-mark.json  # exists only when restore is pending
```

## License

MIT
