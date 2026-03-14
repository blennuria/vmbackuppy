import json
import logging
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone

from vmbackuppy.config import Config
from vmbackuppy.storage import S3Storage

log = logging.getLogger(__name__)

VALID_PERIODS = ("latest", "hourly", "daily", "weekly", "monthly")


class RestoreError(Exception):
    pass


@dataclass
class RestoreMark:
    backup: str
    created_at: str


class RestoreManager:
    def __init__(self, storage: S3Storage) -> None:
        self.storage = storage

    def create_mark(self, backup: str) -> RestoreMark:
        """Validate backup exists and write restore mark to S3."""
        if backup != "latest":
            parts = backup.split("/", 1)
            if len(parts) != 2 or parts[0] not in VALID_PERIODS:
                raise RestoreError(
                    f"Invalid backup name '{backup}'. "
                    f"Expected 'latest' or '<period>/<name>' "
                    f"where period is one of: {', '.join(VALID_PERIODS)}"
                )
            period, name = parts
            if not self.storage.backup_exists(period, name):
                raise RestoreError(f"Backup '{backup}' not found in S3")

        mark = RestoreMark(
            backup=backup,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        data = json.dumps(
            {"backup": mark.backup, "created_at": mark.created_at}
        ).encode()
        self.storage.put_restore_mark(data)
        return mark

    def get_mark(self) -> RestoreMark | None:
        """Read restore mark from S3."""
        data = self.storage.get_restore_mark()
        if data is None:
            return None
        obj = json.loads(data)
        return RestoreMark(
            backup=obj["backup"], created_at=obj["created_at"]
        )

    def delete_mark(self) -> None:
        """Delete restore mark from S3."""
        self.storage.delete_restore_mark()

    def run_restore(self, config: Config) -> None:
        """Check for restore mark, run vmrestore, delete mark on success."""
        mark = self.get_mark()
        if mark is None:
            log.info("No restore mark found, nothing to do")
            return

        log.info(
            "Found restore mark: backup=%s, created_at=%s",
            mark.backup,
            mark.created_at,
        )

        src = f"{config.dst_normalized}/{mark.backup}/"
        cmd = [
            config.vmrestore_bin,
            f"-src={src}",
            f"-storageDataPath={config.storage_data_path}",
        ]
        if config.s3_endpoint:
            cmd.append(f"-customS3Endpoint={config.s3_endpoint}")
        if config.s3_force_path_style:
            cmd.append("-s3ForcePathStyle=true")

        log.info("Running vmrestore: %s", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                log.info("[vmrestore] %s", line)
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                log.info("[vmrestore] %s", line)

        if result.returncode != 0:
            raise RestoreError(
                f"vmrestore exited with code {result.returncode}: {result.stderr}"
            )

        log.info("vmrestore completed successfully, removing restore mark")
        self.delete_mark()
