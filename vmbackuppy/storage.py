import logging
from datetime import datetime
from urllib.parse import urlparse

import boto3
from botocore.config import Config as BotoConfig

from vmbackuppy.config import Config

log = logging.getLogger(__name__)


class S3Storage:
    """S3-compatible storage operations for backup retention management."""

    def __init__(self, config: Config) -> None:
        kwargs: dict = {}
        if config.s3_endpoint:
            kwargs["endpoint_url"] = config.s3_endpoint
        if config.s3_force_path_style:
            kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

        self.s3 = boto3.client("s3", **kwargs)

        parsed = urlparse(config.dst)
        self.bucket = parsed.netloc
        self.base_prefix = parsed.path.strip("/")

    def list_backup_names(self, period: str) -> list[str]:
        """List backup folder names under a period prefix, sorted ascending."""
        prefix = f"{self.base_prefix}/{period}/"
        names: list[str] = []

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.bucket, Prefix=prefix, Delimiter="/"
        ):
            for cp in page.get("CommonPrefixes", []):
                name = cp["Prefix"].removeprefix(prefix).rstrip("/")
                if name:
                    names.append(name)

        return sorted(names)

    def copy_prefix(self, src_period: str, dst_period: str, name: str) -> None:
        """Copy all objects from latest/ to <period>/<name>/ via server-side copy."""
        src_prefix = f"{self.base_prefix}/{src_period}/"
        dst_prefix = f"{self.base_prefix}/{dst_period}/{name}/"

        count = 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=src_prefix):
            for obj in page.get("Contents", []):
                src_key = obj["Key"]
                relative = src_key.removeprefix(src_prefix)
                dst_key = f"{dst_prefix}{relative}"
                self.s3.copy_object(
                    Bucket=self.bucket,
                    CopySource={"Bucket": self.bucket, "Key": src_key},
                    Key=dst_key,
                )
                count += 1

        log.info("Copied %d objects to %s/%s", count, dst_period, name)

    def delete_prefix(self, period: str, name: str) -> None:
        """Delete all objects under <period>/<name>/."""
        prefix = f"{self.base_prefix}/{period}/{name}/"
        count = 0

        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                self.s3.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": objects}
                )
                count += len(objects)

        log.info("Deleted %d objects from %s/%s", count, period, name)

    def get_latest_backup_time(self) -> datetime | None:
        """Get the modification time of the most recent object in latest/."""
        prefix = f"{self.base_prefix}/latest/"
        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=1
        )
        contents = resp.get("Contents", [])
        if not contents:
            return None
        return contents[0]["LastModified"]

    def put_restore_mark(self, data: bytes) -> None:
        """Write restore mark JSON to S3."""
        key = f"{self.base_prefix}/restore-mark.json"
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        log.info("Wrote restore mark to %s", key)

    def get_restore_mark(self) -> bytes | None:
        """Read restore mark from S3. Returns None if not found."""
        key = f"{self.base_prefix}/restore-mark.json"
        try:
            resp = self.s3.get_object(Bucket=self.bucket, Key=key)
            return resp["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def delete_restore_mark(self) -> None:
        """Delete restore mark from S3."""
        key = f"{self.base_prefix}/restore-mark.json"
        self.s3.delete_object(Bucket=self.bucket, Key=key)
        log.info("Deleted restore mark %s", key)

    def backup_exists(self, period: str, name: str) -> bool:
        """Check if a backup with the given name exists under the period prefix."""
        prefix = f"{self.base_prefix}/{period}/{name}/"
        resp = self.s3.list_objects_v2(
            Bucket=self.bucket, Prefix=prefix, MaxKeys=1
        )
        return resp.get("KeyCount", 0) > 0
