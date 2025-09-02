#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Extrae CSVs faltantes desde todos los .zip bajo la carpeta zips/.
from __future__ import annotations
import argparse, os, sys, zipfile, shutil, concurrent.futures as cf, tempfile
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

def extract_csv_from_zip(zf, zi, csv_dir: Path) -> bool:
    """Extrae un archivo CSV de un objeto zipfile.ZipFile"""
    base = Path(zi.filename).name
    if _equivalent_exists(csv_dir, base):
        return False
    
    out_path = csv_dir / base
    with zf.open(zi) as src, open(out_path, "wb") as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)
    return True

def process_zip_file(zip_path: Path, csv_dir: Path) -> tuple[int, int, int]:
    """Procesa un archivo ZIP que puede contener CSVs o otros ZIPs mensuales"""
    extracted = skipped = 0
    
    try:
        with zipfile.ZipFile(zip_path) as zf:
            for zi in zf.infolist():
                if zi.is_dir():
                    continue
                
                filename_lower = zi.filename.lower()
                
                # Procesar archivos CSV directamente
                if filename_lower.endswith(".csv"):
                    if extract_csv_from_zip(zf, zi, csv_dir):
                        extracted += 1
                    else:
                        skipped += 1
                
                # Procesar archivos ZIP mensuales (como 202301-citibike-tripdata.zip)
                elif (filename_lower.endswith(".zip") and 
                      "citibike-tripdata" in filename_lower and
                      len(Path(zi.filename).name) == 28):  # Ej: 202301-citibike-tripdata.zip
                    try:
                        # Extraer el ZIP mensual a un archivo temporal
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                            with zf.open(zi) as src:
                                shutil.copyfileobj(src, temp_zip, length=1024 * 1024)
                            temp_zip_path = temp_zip.name
                        
                        # Procesar el ZIP mensual
                        with zipfile.ZipFile(temp_zip_path) as monthly_zip:
                            for monthly_zi in monthly_zip.infolist():
                                if (monthly_zi.is_dir() or 
                                    not monthly_zi.filename.lower().endswith(".csv")):
                                    continue
                                if extract_csv_from_zip(monthly_zip, monthly_zi, csv_dir):
                                    extracted += 1
                                else:
                                    skipped += 1
                        
                        # Limpiar archivo temporal
                        os.unlink(temp_zip_path)
                        
                    except zipfile.BadZipFile:
                        print(f"[WARN] Bad monthly zip {zi.filename} in: {zip_path}", file=sys.stderr)
                    except Exception as e:
                        print(f"[ERROR] Processing monthly zip {zi.filename} in {zip_path}: {e}", file=sys.stderr)
        
        return extracted, skipped, 0
        
    except zipfile.BadZipFile:
        print(f"[WARN] Bad zip, skipping: {zip_path}", file=sys.stderr)
        return 0, 0, 1
    except Exception as e:
        print(f"[ERROR] Unexpected error processing {zip_path}: {e}", file=sys.stderr)
        return extracted, skipped, 1

def extract_missing_csvs(zippath: Path, csv_dir: Path) -> tuple[int, int, int]:
    """Wrapper para mantener compatibilidad con el código existente"""
    return process_zip_file(zippath, csv_dir)

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