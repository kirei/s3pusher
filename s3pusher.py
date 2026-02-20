import argparse
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import boto3
import structlog
from watchdog.events import DirModifiedEvent, FileModifiedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

STABLE_FILE_DELAY_SECONDS = 1
EXCEPTION_DELAY_SECONDS = 60

logger = structlog.get_logger()


class ThePusher(FileSystemEventHandler):
    def __init__(self, bucket: str | None, hostname: str | None) -> None:
        self.bucket = bucket
        self.hostname = hostname
        self.logger = structlog.get_logger()

    def on_any_event(self, event: FileSystemEvent) -> None:
        if isinstance(event, DirModifiedEvent):
            self.logger.debug("Directory modified", directory=event.src_path)
            directory = Path(event.src_path)
            if directory.is_dir():
                self.upload_directory_to_s3(directory)
        elif isinstance(event, FileModifiedEvent):
            self.logger.debug("File modified", filename=event.src_path)
            filename = Path(event.src_path)
            if filename.is_file():
                self.upload_file_to_s3(filename)
        else:
            self.logger.debug("Ignoring unsupported event type", event_type=type(event).__name__)

    def upload_directory_to_s3(self, directory: Path) -> None:
        """Upload all files in a directory to S3 and delete them if upload is successful"""

        for filename in directory.glob("*"):
            if filename.is_file():
                self.upload_file_to_s3(filename)

    def upload_file_to_s3(self, filename: Path) -> None:
        """Upload file to S3 and delete it if upload is successful"""

        with structlog.contextvars.bound_contextvars(filename=str(filename)):
            if not self.wait_for_stable_file(filename):
                return

            try:
                s3_client = boto3.client("s3")
                s3_object_key = self.get_s3_object_key(hostname=self.hostname, filename=filename)
                with structlog.contextvars.bound_contextvars(s3_bucket=self.bucket, s3_object_key=s3_object_key):
                    self.logger.debug("Uploading file")
                    s3_client = boto3.client("s3")
                    t1 = time.time()
                    with open(filename, "rb") as fp:
                        s3_client.upload_fileobj(fp, self.bucket, s3_object_key)
                    t2 = time.time()
                    self.logger.info("File uploaded", s3_upload_seconds=round(t2 - t1, 3))
                filename.unlink()
                self.logger.info("File deleted")
            except Exception as exc:
                self.logger.error("Failed to upload", exception=str(exc))
                time.sleep(EXCEPTION_DELAY_SECONDS)

    @staticmethod
    def get_s3_object_key(filename: Path | None = None, hostname: str | None = None) -> str:
        """Get S3 object key from filename"""
        dt = datetime.now(tz=UTC)
        fields_dict = {
            "year": f"{dt.year:04}",
            "month": f"{dt.month:02}",
            "day": f"{dt.day:02}",
            "hour": f"{dt.hour:02}",
            "minute": f"{dt.minute:02}",
            "second": f"{dt.second:02}",
            **({"hostname": hostname} if hostname else {}),
            "uuid": str(uuid.uuid4()),
        }
        fields_list = [f"{k}={v}" for k, v in fields_dict.items() if v is not None]
        if filename:
            fields_list.append(filename.name)
        return "/".join(fields_list)

    def wait_for_stable_file(self, filename: Path, timeout: int = 60) -> bool:
        """Wait for a file to become stable (not modified for a certain period)"""

        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = filename.stat().st_size
                if current_size == last_size:
                    self.logger.debug("File is stable")
                    return True
                last_size = current_size
            except FileNotFoundError:
                return False
            self.logger.debug("Waiting for file to become stable", filename=str(filename))
            time.sleep(STABLE_FILE_DELAY_SECONDS)

        self.logger.warning("File not stable, timeout reached")

        return False


def main():

    parser = argparse.ArgumentParser(description="S3 Pusher")

    parser.add_argument("directory", nargs="+", help="Directory to watch for changes")
    parser.add_argument("--bucket", default=None, help="S3 bucket name")
    parser.add_argument(
        "--hostname",
        required=False,
        default=None,
        help="Hostname to include in S3 object key",
    )
    parser.add_argument("--log-json", action="store_true", help="Log in JSON format")
    parser.add_argument("--debug", action="store_true", help="Enable debugging")

    args = parser.parse_args()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="%Y-%m-%dT%H:%M:%SZ", utc=True),
            structlog.processors.JSONRenderer() if args.log_json else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG if args.debug else logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )

    if not args.bucket:
        logger.warning("No S3 bucket specified, file upload will be skipped")

    logger.info("Watching directories for changes", directories=args.directory)

    event_handler = ThePusher(bucket=args.bucket, hostname=args.hostname)

    observer = Observer()

    for directory in args.directory:
        event_handler.upload_directory_to_s3(Path(directory))
        observer.schedule(event_handler, directory)
    observer.start()

    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
