import json
import logging
import signal
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import urllib.request
import urllib.error

from croniter import croniter

from vmbackuppy.backup import BackupError, run_vmbackup
from vmbackuppy.config import Config
from vmbackuppy.restore import RestoreError, RestoreManager
from vmbackuppy.retention import enforce_retention
from vmbackuppy.storage import S3Storage

log = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.storage = S3Storage(config)
        self._running = True
        self._last_success: datetime | None = None
        self._backup_requested = threading.Event()
        self._backup_lock = threading.Lock()
        self._backup_success_total = 0
        self._backup_errors_total = 0
        self._backup_duration_seconds = 0.0
        self._setup_signals()

    def _setup_signals(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame: object) -> None:
        log.info("Received signal %d, shutting down", signum)
        self._running = False

    def run(self) -> None:
        self._start_health_server()
        self._wait_for_vm()

        if self.config.backup_schedule:
            self._run_cron()
        else:
            self._run_interval()

    def _run_cron(self) -> None:
        cron = croniter(self.config.backup_schedule, datetime.now(timezone.utc))
        log.info("Using cron schedule: %s", self.config.backup_schedule)

        if self.config.run_at_startup:
            self._backup_cycle()

        while self._running:
            next_time = cron.get_next(datetime)
            delay = (next_time - datetime.now(timezone.utc)).total_seconds()
            log.info("Next backup at %s", next_time.strftime("%Y-%m-%dT%H:%M:%S"))
            if delay > 0:
                self._sleep(int(delay))
            if self._running:
                self._backup_cycle()
                self._backup_requested.clear()

    def _run_interval(self) -> None:
        initial_delay = self._seconds_until_next_backup()
        if initial_delay > 0:
            next_at = datetime.now(timezone.utc) + timedelta(seconds=initial_delay)
            log.info(
                "Last backup is recent, next backup at %s", next_at.strftime("%Y-%m-%dT%H:%M:%S")
            )
            self._sleep(initial_delay)
        elif self.config.run_at_startup:
            self._backup_cycle()

        prev_cycle_duration = 0
        while self._running:
            sleep_time = self.config.backup_interval - prev_cycle_duration
            if sleep_time > 0:
                self._sleep(sleep_time)
            else:
                log.warning("Backup cycle took %.0fs longer than interval", -sleep_time)
            if self._running:
                start = time.monotonic()
                self._backup_cycle()
                self._backup_requested.clear()
                prev_cycle_duration = int(time.monotonic() - start)

    def _seconds_until_next_backup(self) -> int:
        """Check S3 for last backup time, return seconds to wait (0 = run now)."""
        try:
            last_backup = self.storage.get_latest_backup_time()
        except Exception:
            log.exception("Failed to check last backup time")
            return 0

        if last_backup is None:
            log.info("No previous backup found")
            return 0

        age = (datetime.now(timezone.utc) - last_backup).total_seconds()
        remaining = self.config.backup_interval - age
        log.info(
            "Last backup: %s (%.0fs ago)", last_backup.isoformat(), age
        )

        if remaining > 0:
            self._last_success = last_backup
            return int(remaining)
        return 0

    def _wait_for_vm(self) -> None:
        """Wait for VictoriaMetrics to become reachable."""
        url = f"{self.config.vm_url.rstrip('/')}/health"
        log.info("Waiting for VictoriaMetrics at %s", url)
        while self._running:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    if resp.status == 200:
                        log.info("VictoriaMetrics is ready")
                        return
            except (urllib.error.URLError, OSError):
                pass
            self._sleep(5)

    def _sleep(self, seconds: int) -> None:
        """Interruptible sleep — wakes on shutdown or backup request."""
        end = time.monotonic() + seconds
        while self._running and time.monotonic() < end:
            remaining = min(1, end - time.monotonic())
            if self._backup_requested.wait(timeout=remaining):
                break

    def _backup_cycle(self) -> None:
        if not self._backup_lock.acquire(blocking=False):
            log.warning("Backup cycle already running, skipping")
            return
        try:
            self._run_backup_cycle()
        finally:
            self._backup_lock.release()

    def _run_backup_cycle(self) -> None:
        now = datetime.now(timezone.utc)
        log.info("Starting backup cycle at %s", now.isoformat())
        start = time.monotonic()

        # Step 1: incremental backup to latest/
        try:
            run_vmbackup(self.config)
        except BackupError:
            log.exception("Backup failed, skipping this cycle")
            self._backup_errors_total += 1
            return

        self._backup_duration_seconds = time.monotonic() - start
        self._backup_success_total += 1
        self._last_success = datetime.now(timezone.utc)

        # Step 2: copy latest/ to period folders if needed
        self._maybe_create_period_backup("hourly", now.strftime("%Y-%m-%d:%H"))
        self._maybe_create_period_backup("daily", now.strftime("%Y-%m-%d"))
        self._maybe_create_period_backup("weekly", now.strftime("%G-W%V"))
        self._maybe_create_period_backup("monthly", now.strftime("%Y-%m"))

        # Step 3: enforce retention
        try:
            enforce_retention(self.config, self.storage)
        except Exception:
            log.exception("Retention enforcement failed")

        log.info("Backup cycle completed")

    def _maybe_create_period_backup(self, period: str, name: str) -> None:
        keep = getattr(self.config, f"keep_last_{period}")
        if keep <= 0:
            return

        if self.storage.backup_exists(period, name):
            log.debug("Backup %s/%s already exists, skipping", period, name)
            return

        log.info("Creating %s backup: %s", period, name)
        try:
            self.storage.copy_prefix("latest", period, name)
        except Exception:
            log.exception("Failed to create %s backup %s", period, name)

    def _start_health_server(self) -> None:
        scheduler = self
        restore_mgr = RestoreManager(self.storage)

        class Handler(BaseHTTPRequestHandler):
            server_version = "vmbackuppy"
            sys_version = ""

            def _respond(self, code: int, body: bytes, content_type: str = "text/plain") -> None:
                self.send_response(code)
                self.send_header("Content-Type", content_type)
                self.end_headers()
                self.wfile.write(body)

            def _read_json(self) -> dict:
                length = int(self.headers.get("Content-Length", 0))
                return json.loads(self.rfile.read(length))

            def do_GET(self) -> None:
                try:
                    self._handle_get()
                except Exception:
                    log.exception("Error handling GET %s", self.path)
                    self._respond(500, b'{"error": "internal server error"}\n', "application/json")

            def _handle_get(self) -> None:
                if self.path == "/healthz":
                    self._respond(200, b"ok\n")
                elif self.path == "/metrics":
                    last_ts = scheduler._last_success.timestamp() if scheduler._last_success else 0
                    lines = [
                        "# HELP vmbackuppy_backup_success_total Total number of successful backups.",
                        "# TYPE vmbackuppy_backup_success_total counter",
                        f"vmbackuppy_backup_success_total {scheduler._backup_success_total}",
                        "# HELP vmbackuppy_backup_errors_total Total number of failed backups.",
                        "# TYPE vmbackuppy_backup_errors_total counter",
                        f"vmbackuppy_backup_errors_total {scheduler._backup_errors_total}",
                        "# HELP vmbackuppy_backup_duration_seconds Duration of last backup in seconds.",
                        "# TYPE vmbackuppy_backup_duration_seconds gauge",
                        f"vmbackuppy_backup_duration_seconds {scheduler._backup_duration_seconds:.3f}",
                        "# HELP vmbackuppy_backup_last_success_timestamp Unix timestamp of last successful backup.",
                        "# TYPE vmbackuppy_backup_last_success_timestamp gauge",
                        f"vmbackuppy_backup_last_success_timestamp {last_ts:.3f}",
                        "",
                    ]
                    self._respond(200, "\n".join(lines).encode(), "text/plain; version=0.0.4; charset=utf-8")
                elif self.path == "/api/v1/backups":
                    backups: list[dict] = []
                    if scheduler.storage.get_latest_backup_time() is not None:
                        backups.append({"name": "latest"})
                    for period in ("hourly", "daily", "weekly", "monthly"):
                        for name in scheduler.storage.list_backup_names(period):
                            backups.append({"name": f"{period}/{name}"})
                    self._respond(200, json.dumps(backups, indent=2).encode() + b"\n", "application/json")
                elif self.path == "/api/v1/restore":
                    mark = restore_mgr.get_mark()
                    if mark is None:
                        self._respond(200, b"{}\n", "application/json")
                    else:
                        body = json.dumps({"backup": mark.backup, "created_at": mark.created_at}, indent=2)
                        self._respond(200, body.encode() + b"\n", "application/json")
                else:
                    log.warning("Unmatched %s path: %r", self.command, self.path)
                    self.send_response(404)
                    self.end_headers()

            def do_POST(self) -> None:
                try:
                    self._handle_post()
                except Exception:
                    log.exception("Error handling POST %s", self.path)
                    self._respond(500, b'{"error": "internal server error"}\n', "application/json")

            def do_DELETE(self) -> None:
                try:
                    self._handle_delete()
                except Exception:
                    log.exception("Error handling DELETE %s", self.path)
                    self._respond(500, b'{"error": "internal server error"}\n', "application/json")

            def _handle_post(self) -> None:
                if self.path == "/api/v1/backups":
                    if scheduler._backup_lock.locked():
                        self._respond(400, b'{"error": "backup is in progress"}\n', "application/json")
                        return
                    scheduler._backup_requested.set()
                    self._respond(201, b"{}\n", "application/json")
                    log.info("Manual backup requested via API")
                elif self.path == "/api/v1/restore":
                    try:
                        data = self._read_json()
                        backup_name = data["backup"]
                    except (json.JSONDecodeError, KeyError):
                        self._respond(400, b'{"error": "JSON body with backup field required"}\n', "application/json")
                        return
                    try:
                        mark = restore_mgr.create_mark(backup_name)
                    except RestoreError as e:
                        body = json.dumps({"error": str(e)})
                        self._respond(400, body.encode() + b"\n", "application/json")
                        return
                    body = json.dumps({"backup": mark.backup, "created_at": mark.created_at}, indent=2)
                    self._respond(200, body.encode() + b"\n", "application/json")
                else:
                    log.warning("Unmatched %s path: %r", self.command, self.path)
                    self.send_response(404)
                    self.end_headers()

            def _handle_delete(self) -> None:
                if self.path == "/api/v1/restore":
                    restore_mgr.delete_mark()
                    self._respond(200, b"{}\n", "application/json")
                else:
                    log.warning("Unmatched %s path: %r", self.command, self.path)
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format: str, *args: object) -> None:
                log.debug(format, *args)

        server = HTTPServer((self.config.http_addr, self.config.http_port), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        log.info("Health server started on port %d", self.config.http_port)
