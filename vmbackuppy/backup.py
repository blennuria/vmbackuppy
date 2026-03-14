import logging
import subprocess

from vmbackuppy.config import Config

log = logging.getLogger(__name__)


class BackupError(Exception):
    pass


def run_vmbackup(config: Config) -> None:
    """Run vmbackup to create incremental backup to <dst>/latest/."""
    cmd = [
        config.vmbackup_bin,
        f"-snapshot.createURL={config.snapshot_create_url}",
        f"-dst={config.dst_normalized}/latest/",
        f"-storageDataPath={config.storage_data_path}",
        f"-concurrency={config.concurrency}",
    ]
    if config.s3_endpoint:
        cmd.append(f"-customS3Endpoint={config.s3_endpoint}")
    if config.s3_force_path_style:
        cmd.append("-s3ForcePathStyle=true")

    log.info("Running vmbackup: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info("[vmbackup] %s", line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.info("[vmbackup] %s", line)

    if result.returncode != 0:
        raise BackupError(
            f"vmbackup exited with code {result.returncode}: {result.stderr}"
        )

    log.info("vmbackup completed successfully")
