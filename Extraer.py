#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Extrae CSVs faltantes desde todos los .zip bajo la carpeta zips/.
from __future__ import annotations
import argparse, os, sys, zipfile, shutil, concurrent.futures as cf
from pathlib import Path

# === NUEVO: carpeta base + subcarpetas =========================
DEFAULT_BASE_DIR = "./citibike_data"
SUBDIR_ZIPS = "zips"
SUBDIR_CSV  = "csv"
# ===============================================================

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

def find_zips(zips_root: Path):
    for dirpath, dirnames, filenames in os.walk(zips_root):
        dirnames[:] = [d for d in dirnames if d != "__MACOSX"]
        for fn in filenames:
            if fn.lower().endswith(".zip"):
                yield Path(dirpath) / fn

def _normalize_name(name: str) -> str:
    n = name.lower()
    while n and (n[0] in "._"):
        n = n[1:]
    return n

def _equivalent_exists(dirpath: Path, basename: str) -> bool:
    want = _normalize_name(basename)
    try:
        for f in dirpath.iterdir():
            if f.is_file() and f.suffix.lower() == ".csv":
                if _normalize_name(f.name) == want and f.stat().st_size > 0:
                    return True
    except FileNotFoundError:
        return False
    return False

def extract_missing_csvs(zippath: Path, csv_dir: Path) -> tuple[int, int, int]:
    csv_dir.mkdir(parents=True, exist_ok=True)
    extracted = skipped = 0
    try:
        with zipfile.ZipFile(zippath) as zf:
            for zi in zf.infolist():
                if zi.is_dir() or not zi.filename.lower().endswith(".csv"):
                    continue
                base = Path(zi.filename).name
                if _equivalent_exists(csv_dir, base):
                    skipped += 1
                    continue
                out_path = csv_dir / base
                with zf.open(zi) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
                extracted += 1
        return extracted, skipped, 0
    except zipfile.BadZipFile:
        print(f"[WARN] Bad zip, skipping: {zippath}", file=sys.stderr)
        return 0, 0, 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default=DEFAULT_BASE_DIR)
    ap.add_argument("--zips-dir", default=None, help="Por defecto <base>/zips")
    ap.add_argument("--csv-dir", default=None, help="Por defecto <base>/csv")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    base = Path(os.path.expanduser(args.base_dir))
    zips_dir = Path(args.zips_dir) if args.zips_dir else base / SUBDIR_ZIPS
    csv_dir  = Path(args.csv_dir)  if args.csv_dir  else base / SUBDIR_CSV

    if not zips_dir.exists():
        print(f"[ERROR] Carpeta de zips no encontrada: {zips_dir}")
        sys.exit(2)

    zips = list(find_zips(zips_dir))
    if not zips:
        print("No .zip files found.")
        return

    total_extracted = total_skipped = total_bad = 0
    iterator = zips
    if tqdm:
        iterator = tqdm(zips, desc="Scanning zips", unit="zip")

    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(extract_missing_csvs, zp, csv_dir) for zp in iterator]
        progress = tqdm(cf.as_completed(futures), total=len(futures), desc="Extracting", unit="zip") if tqdm else cf.as_completed(futures)
        for fut in progress:
            e, s, b = fut.result()
            total_extracted += e
            total_skipped += s
            total_bad += b

    print(f"Done. Extracted: {total_extracted}, skipped existing: {total_skipped}, bad zips: {total_bad}")
    print(f"CSVs en: {csv_dir}")

if __name__ == "__main__":
    main()
