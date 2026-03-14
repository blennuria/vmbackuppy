import os
from dataclasses import dataclass


def parse_duration(value: str) -> int:
    """Parse duration string (e.g., '1h', '30m', '3600') to seconds."""
    value = value.strip()
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("s"):
        return int(value[:-1])
    return int(value)


@dataclass
class Config:
    vm_url: str
    storage_data_path: str
    dst: str
    backup_schedule: str
    backup_interval: int
    keep_last_hourly: int
    keep_last_daily: int
    keep_last_weekly: int
    keep_last_monthly: int
    vmbackup_bin: str
    vmrestore_bin: str
    concurrency: int
    s3_endpoint: str
    s3_force_path_style: bool
    http_addr: str
    http_port: int
    log_level: str
    run_at_startup: bool

    @classmethod
    def from_env(cls) -> "Config":
        cfg = cls(
            vm_url=os.getenv("VM_URL", "http://localhost:8428"),
            storage_data_path=os.getenv("STORAGE_DATA_PATH", "/victoria-metrics-data"),
            dst=os.getenv("BACKUP_DESTINATION", ""),
            backup_schedule=os.getenv("BACKUP_SCHEDULE", ""),
            backup_interval=parse_duration(os.getenv("BACKUP_INTERVAL", "1h")),
            keep_last_hourly=int(os.getenv("KEEP_LAST_HOURLY", "24")),
            keep_last_daily=int(os.getenv("KEEP_LAST_DAILY", "7")),
            keep_last_weekly=int(os.getenv("KEEP_LAST_WEEKLY", "4")),
            keep_last_monthly=int(os.getenv("KEEP_LAST_MONTHLY", "12")),
            vmbackup_bin=os.getenv("VMBACKUP_PATH", "/usr/local/bin/vmbackup"),
            vmrestore_bin=os.getenv("VMRESTORE_PATH", "/usr/local/bin/vmrestore"),
            concurrency=int(os.getenv("BACKUP_CONCURRENCY", "10")),
            s3_endpoint=os.getenv("S3_ENDPOINT", ""),
            s3_force_path_style=os.getenv("S3_FORCE_PATH_STYLE", "true").lower() == "true",
            http_addr=os.getenv("HTTP_ADDR", "0.0.0.0"),
            http_port=int(os.getenv("HTTP_PORT", "8491")),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            run_at_startup=os.getenv("RUN_AT_STARTUP", "true").lower() == "true",
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.dst:
            raise ValueError("BACKUP_DESTINATION environment variable is required")
        if not self.dst.startswith(("s3://", "gs://", "azblob://", "fs://")):
            raise ValueError(f"Unsupported destination scheme: {self.dst}")

    @property
    def snapshot_create_url(self) -> str:
        return f"{self.vm_url.rstrip('/')}/snapshot/create"

    @property
    def dst_normalized(self) -> str:
        return self.dst.rstrip("/")
