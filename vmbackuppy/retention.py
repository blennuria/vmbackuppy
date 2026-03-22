import logging

from vmbackuppy.config import Config
from vmbackuppy.storage import S3Storage

log = logging.getLogger(__name__)


def enforce_retention(config: Config, storage: S3Storage) -> None:
    """Delete old backups that exceed retention limits."""
    policies = {
        "hourly": config.keep_last_hourly,
        "daily": config.keep_last_daily,
        "weekly": config.keep_last_weekly,
        "monthly": config.keep_last_monthly,
    }

    for period, keep in policies.items():
        if keep < 0:
            continue

        names = storage.list_backup_names(period)
        if not names:
            continue

        if keep == 0:
            to_delete = names
        elif len(names) <= keep:
            continue
        else:
            to_delete = names[:-keep]
        log.info(
            "Retention %s: %d backups found, keeping %d, deleting %d",
            period, len(names), keep, len(to_delete),
        )
        for name in to_delete:
            storage.delete_prefix(period, name)
