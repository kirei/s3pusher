import argparse
import logging
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import boto3
from watchdog.events import FileModifiedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

STABLE_FILE_DELAY_SECONDS = 1
EXCEPTION_DELAY_SECONDS = 60


class FileEventHandler(FileSystemEventHandler):
    def __init__(self, bucket: str | None, hostname: str | None) -> None:
        self.bucket = bucket
        self.hostname = hostname

    def on_any_event(self, event: FileSystemEvent) -> None:
        logging.debug("Event detected: %s", event)

        if isinstance(event, FileModifiedEvent):
            modified_file = Path(event.src_path)

            if not modified_file.is_file():
                logging.debug("%s does not exist, skipped", modified_file)
                return

            logging.info("File modified: %s", modified_file)

            if self.wait_for_stable_file(modified_file):
                logging.debug("File is stable: %s", modified_file)

                try:
                    s3_object_key = self.get_s3_object_key(hostname=self.hostname, filename=modified_file)
                    if self.bucket:
                        logging.info("Uploading %s as %s/%s", modified_file, self.bucket, s3_object_key)
                        s3_client = boto3.client("s3")
                        with open(modified_file, "rb") as fp:
                            s3_client.upload_fileobj(fp, self.bucket, s3_object_key)
                        modified_file.unlink()
                        logging.info("File uploaded and deleted: %s", modified_file)
                    else:
                        logging.warning("File upload skipped: %s", modified_file)
                except Exception as exc:
                    logging.error("Failed to upload %s: %s", modified_file, exc)
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

    @staticmethod
    def wait_for_stable_file(filename: Path, timeout: int = 60) -> bool:
        """Wait for a file to become stable (not modified for a certain period)"""

        start_time = time.time()
        last_size = -1

        while time.time() - start_time < timeout:
            try:
                current_size = filename.stat().st_size
                if current_size == last_size:
                    return True
                last_size = current_size
            except FileNotFoundError:
                return False
            time.sleep(STABLE_FILE_DELAY_SECONDS)

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
    parser.add_argument("--debug", action="store_true", help="Enable debugging")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not args.bucket:
        logging.warning("No S3 bucket specified, file upload will be skipped")

    logging.info("Watching directory: %s", args.directory)

    event_handler = FileEventHandler(bucket=args.bucket, hostname=args.hostname)

    observer = Observer()
    for directory in args.directory:
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
