from __future__ import annotations
import argparse, os, sys, zipfile
from pathlib import Path

DEFAULT_ROOT = "citibike_data"

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

def find_zips(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
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

def extract_missing_csvs(zippath: Path) -> tuple[int, int, int]:
    extracted = skipped = 0
    try:
        with zipfile.ZipFile(zippath) as zf:
            for zi in zf.infolist():
                if zi.is_dir() or not zi.filename.lower().endswith(".csv"):
                    continue
                base = Path(zi.filename).name 
                if _equivalent_exists(zippath.parent, base):
                    skipped += 1
                    continue
                out_path = zippath.parent / base
                # Extract
                with zf.open(zi) as src, open(out_path, "wb") as dst:
                    dst.write(src.read())
                extracted += 1
        return extracted, skipped, 0
    except zipfile.BadZipFile:
        print(f"[WARN] Bad zip, skipping: {zippath}", file=sys.stderr)
        return 0, 0, 1

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=DEFAULT_ROOT, help="Root folder with downloads/zips (optional)")
    args = ap.parse_args()
    root = Path(args.root)

    if not root.exists():
        print(f"[ERROR] Root folder not found: {root}")
        sys.exit(2)

    zips = list(find_zips(root))
    if not zips:
        print("No .zip files found.")
        return

    total_extracted = total_skipped = total_bad = 0
    iterator = tqdm(zips, desc="Processing zips", unit="zip") if tqdm else zips
    for zp in iterator:
        e, s, b = extract_missing_csvs(zp)
        total_extracted += e
        total_skipped += s
        total_bad += b

    print(f"Done. Extracted: {total_extracted}, skipped existing: {total_skipped}, bad zips: {total_bad}")

if __name__ == "__main__":
    main()
