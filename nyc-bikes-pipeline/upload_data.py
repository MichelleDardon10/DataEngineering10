# final/upload_all_sequential.py
import os, time, logging
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET   = "city-data-25"
DATA_DIR = Path("/Users/michelledardon/Documents/GitHub/DataEngineering10/citibike_data")
RETRIES  = 5
PAUSE_S  = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("uploader")

s3 = boto3.client("s3", config=Config(retries={"max_attempts": 10, "mode": "standard"}))

def upload_one(file_path: Path, key: str) -> None:
    size = file_path.stat().st_size
    for attempt in range(1, RETRIES + 1):
        try:
            log.info(f"[{attempt}/{RETRIES}] Uploading {file_path} → s3://{BUCKET}/{key} ({size} bytes)")
            s3.upload_file(str(file_path), BUCKET, key)
            # verify by size
            obj = s3.head_object(Bucket=BUCKET, Key=key)
            if obj["ContentLength"] == size:
                log.info(f"Verified on S3: {key} ({obj['ContentLength']} bytes)")
                return
            else:
                log.warning(f"Size mismatch (local {size} vs S3 {obj['ContentLength']}); retrying…")
        except ClientError as e:
            log.warning(f"Upload error: {e}; retrying…")
        time.sleep(PAUSE_S)
    raise RuntimeError(f"Failed to upload after {RETRIES} attempts: {file_path}")

def delete_local(file_path: Path) -> None:
    for attempt in range(1, RETRIES + 1):
        try:
            os.remove(file_path)
            log.info(f"Deleted local: {file_path}")
            return
        except OSError as e:
            log.warning(f"Delete error: {e}; retrying…")
            time.sleep(PAUSE_S)
    raise RuntimeError(f"Failed to delete after {RETRIES} attempts: {file_path}")

def main():
    if not DATA_DIR.exists():
        log.error(f"No existe la carpeta: {DATA_DIR}")
        return
    files = [p for p in sorted(DATA_DIR.rglob("*")) if p.is_file()]
    if not files:
        log.info(f"No hay archivos en: {DATA_DIR}")
        return

    log.info(f"Found {len(files)} files under {DATA_DIR}")
    for idx, path in enumerate(files, 1):
        key = str(path.relative_to(DATA_DIR)).replace("\\", "/")
        log.info(f"({idx}/{len(files)}) Processing: {path.name} → {key}")
        upload_one(path, key)
        delete_local(path)   # ⬅️ only after delete we continue to the next
    log.info("✅ Finished: all files uploaded and removed locally.")

if __name__ == "__main__":
    main()