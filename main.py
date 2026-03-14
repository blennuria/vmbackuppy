import logging
import sys

from vmbackuppy.config import Config
from vmbackuppy.restore import RestoreError, RestoreManager
from vmbackuppy.scheduler import Scheduler
from vmbackuppy.storage import S3Storage


def main() -> None:
    try:
        config = Config.from_env()
    except ValueError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
        sys.exit(1)

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # initContainer entrypoint: python main.py restore run
    if sys.argv[1:] == ["restore"]:
        storage = S3Storage(config)
        manager = RestoreManager(storage)
        try:
            manager.run_restore(config)
        except RestoreError as e:
            print(f"Restore error: {e}", file=sys.stderr)
            sys.exit(1)
        return

    log = logging.getLogger("vmbackuppy")
    log.info("Starting vmbackuppy")
    log.info("Destination: %s", config.dst)
    if config.backup_schedule:
        log.info("Backup schedule: %s", config.backup_schedule)
    else:
        log.info("Backup interval: %ds", config.backup_interval)
    log.info(
        "Retention: hourly=%d, daily=%d, weekly=%d, monthly=%d",
        config.keep_last_hourly,
        config.keep_last_daily,
        config.keep_last_weekly,
        config.keep_last_monthly,
    )

    scheduler = Scheduler(config)
    scheduler.run()


if __name__ == "__main__":
    main()
