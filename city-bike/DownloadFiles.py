#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple
from urllib.parse import quote, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

BUCKET_URL_DEFAULT = "https://s3.amazonaws.com/tripdata"
# Ahora descargamos directamente a la carpeta "zips/"
DEFAULT_DEST = "./citibike_data/zips"
DEFAULT_WORKERS = 8
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

@dataclass
class FileEntry:
    key: str
    url: str
    size: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None

def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "citibike-downloader/1.0"})
    return s

def list_bucket_objects(base_url: str) -> List[FileEntry]:
    entries: List[FileEntry] = []
    token: Optional[str] = None
    s = make_session()
    while True:
        params = {"list-type": "2", "max-keys": "1000", "encoding-type": "url"}
        if token:
            params["continuation-token"] = token
        r = s.get(base_url, params=params, timeout=60)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for contents in root.findall("s3:Contents", S3_NS):
            key_enc = contents.findtext("s3:Key", default="", namespaces=S3_NS)
            size_txt = contents.findtext("s3:Size", default="", namespaces=S3_NS)
            etag = contents.findtext("s3:ETag", default=None, namespaces=S3_NS)
            last_mod = contents.findtext("s3:LastModified", default=None, namespaces=S3_NS)
            if not key_enc:
                continue
            key = unquote(key_enc)
            if not key.lower().endswith(".zip"):
                continue
            name = os.path.basename(key)
            if name.upper().startswith("JC-"):
                continue
            url = f"{base_url}/{quote(key)}"
            size = int(size_txt) if size_txt.isdigit() else None
            entries.append(FileEntry(key=key, url=url, size=size, etag=etag, last_modified=last_mod))
        next_token = root.findtext("s3:NextContinuationToken", default=None, namespaces=S3_NS)
        is_truncated = root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS)
        if not (is_truncated and is_truncated.lower() == "true" and next_token):
            break
        token = next_token
    entries.sort(key=lambda e: e.key.lower())
    return entries

def filter_includes(entries: Iterable[FileEntry], include_regex: Optional[str]) -> List[FileEntry]:
    if not include_regex:
        return list(entries)
    rx = re.compile(include_regex)
    return [e for e in entries if rx.search(os.path.basename(e.key))]

def local_size(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except OSError:
        return None

def should_skip(final_path: str, remote_size: Optional[int]) -> bool:
    if not os.path.exists(final_path):
        return False
    if remote_size is None:
        return True
    ls = local_size(final_path)
    return ls is not None and ls == remote_size

def head_size(s: requests.Session, url: str) -> Optional[int]:
    try:
        hr = s.head(url, timeout=20)
        if hr.ok:
            cl = hr.headers.get("Content-Length")
            if cl and cl.isdigit():
                return int(cl)
    except requests.RequestException:
        pass
    return None

def download_one(entry: FileEntry, dest_dir: str, max_retries: int = 4) -> Tuple[str, str]:
    os.makedirs(dest_dir, exist_ok=True)
    name = os.path.basename(entry.key)
    final_path = os.path.join(dest_dir, name)
    part_path = final_path + ".part"

    s = make_session()
    remote_size = entry.size if entry.size is not None else head_size(s, entry.url)

    if should_skip(final_path, remote_size):
        return (name, "skipped")

    resume_from = 0
    if remote_size and os.path.exists(part_path):
        ps = local_size(part_path)
        if ps and 0 < ps < remote_size:
            resume_from = ps

    backoff = 1.6
    for attempt in range(1, max_retries + 1):
        try:
            headers = {}
            if resume_from:
                headers["Range"] = f"bytes={resume_from}-"

            with s.get(entry.url, stream=True, timeout=120, headers=headers) as r:
                r.raise_for_status()
                mode = "ab" if resume_from else "wb"
                total = remote_size - resume_from if remote_size else None

                with open(part_path, mode) as f, tqdm(
                    total=total, initial=0, unit="B", unit_scale=True, unit_divisor=1024,
                    desc=name, leave=False
                ) as pbar:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        f.write(chunk)
                        pbar.update(len(chunk))

            if remote_size is not None:
                if local_size(part_path) != remote_size:
                    try:
                        os.remove(part_path)
                    except OSError:
                        pass
                    raise IOError("size mismatch after download")

            os.replace(part_path, final_path)
            return (name, "ok")

        except (requests.RequestException, IOError) as e:
            try:
                if os.path.exists(part_path) and not resume_from:
                    os.remove(part_path)
            except OSError:
                pass
            if attempt == max_retries:
                return (name, f"failed: {e}")
            time.sleep((backoff ** attempt) + 0.25 * attempt)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket-url", default=BUCKET_URL_DEFAULT)
    ap.add_argument("--dest", default=DEFAULT_DEST)
    ap.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    ap.add_argument("--include-regex", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    all_entries = list_bucket_objects(args.bucket_url)
    entries = filter_includes(all_entries, args.include_regex)

    if not entries:
        print("No matching files found.")
        return

    if args.dry_run:
        for e in entries:
            print(os.path.basename(e.key))
        print(f"DRY-RUN: {len(entries)} archivos coinciden.")
        return

    statuses = {"ok": 0, "skipped": 0, "failed": 0}
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(download_one, e, args.dest) for e in entries]
        for fut in tqdm(cf.as_completed(futures), total=len(futures), desc="Overall", unit="file"):
            name, status = fut.result()
            if status.startswith("failed"):
                statuses["failed"] += 1
            else:
                statuses[status] += 1

    print(f"Done. ok={statuses['ok']}, skipped={statuses['skipped']}, failed={statuses['failed']}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
        raise SystemExit(130)
