#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse, os, re, hashlib
from pathlib import Path
from typing import Iterable
from datetime import datetime
import pandas as pd
from tqdm import tqdm
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_ROOT = "citibike_data"
DEFAULT_OUT  = "citibike_clean"
CHUNK_ROWS   = 250_000

STANDARD_COLS = [
    "ride_id","rideable_type",
    "started_at","ended_at",
    "start_station_id","start_station_name",
    "end_station_id","end_station_name",
    "start_lat","start_lng","end_lat","end_lng",
    "member_casual","birth_year","gender",
    "duration_seconds","source_file"
]

pd.options.mode.copy_on_write = True

def iter_csvs(root: Path) -> Iterable[Path]:
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d != "__MACOSX"]
        for fn in filenames:
            if fn in (".DS_Store",) or fn.startswith("._"):
                continue
            if fn.upper().startswith("JC-"):
                continue
            if fn.lower().endswith(".csv"):
                yield Path(dirpath) / fn

def stable_hash(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", "ignore")).hexdigest()

def _canon(name: str) -> str:
    n = name.replace("\ufeff", "")
    n = n.strip().strip('"').strip("'").lower()
    n = re.sub(r"[_\s]+", " ", n)
    return n

def to_ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce", utc=False)

def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def normalize_chunk(df: pd.DataFrame, source_file: str, today_year: int) -> pd.DataFrame:
    cols = {_canon(c): c for c in df.columns}
    def col(*cands):
        for c in cands:
            if c in cols:
                return df[cols[c]]
        return pd.Series([pd.NA] * len(df))
    started = to_ts(col("started at", "starttime", "start time"))
    ended   = to_ts(col("ended at",   "stoptime",  "end time"))
    s_id   = col("start station id","start_station_id").astype("string")
    s_name = col("start station name","start_station_name").astype("string")
    e_id   = col("end station id","end_station_id").astype("string")
    e_name = col("end station name","end_station_name").astype("string")
    s_lat = num(col("start lat","start station latitude")).astype("Float64")
    s_lng = num(col("start lng","start station longitude")).astype("Float64")
    e_lat = num(col("end lat","end station latitude")).astype("Float64")
    e_lng = num(col("end lng","end station longitude")).astype("Float64")
    member_raw = col("member casual","usertype").astype("string").str.lower()
    member = member_raw.map({
        "member": "member", "casual": "casual",
        "subscriber": "member", "customer": "casual",
        "registered": "member", "non-member": "casual"
    }).fillna(member_raw)
    rideable = col("rideable type").astype("string")
    by = num(col("birth year","birth_year")).astype("Float64")
    by = by.where((by >= 1900) & (by <= max(1900, today_year - 12))).astype("Int64")
    g = col("gender").astype("string")
    if g.dtype == "string":
        g_map = g.str.strip().str.lower().map({
            "0": 0, "1": 1, "2": 2,
            "unknown": 0, "male": 1, "female": 2
        })
        g_num = pd.to_numeric(g, errors="coerce")
        g = g_map.where(g_map.notna(), g_num).astype("Int64")
    else:
        g = pd.Series([pd.NA] * len(df), dtype="Int64")
    dur_ts = (ended - started).dt.total_seconds().astype("Float64")
    dur_csv = num(col("tripduration","duration")).astype("Float64")
    duration = dur_ts.where(dur_ts.notna(), dur_csv)
    ride_id = col("ride id").astype("string")
    if ride_id.isna().all():
        bikeid = col("bikeid").astype("string")
        ride_id = (
            started.astype("string").fillna("") + "|" +
            ended.astype("string").fillna("") + "|" +
            s_id.fillna("") + "|" + e_id.fillna("") + "|" +
            bikeid.fillna("")
        ).map(stable_hash).astype("string")
    out = pd.DataFrame({
        "ride_id": ride_id,
        "rideable_type": rideable,
        "started_at": started,
        "ended_at": ended,
        "start_station_id": s_id,
        "start_station_name": s_name,
        "end_station_id": e_id,
        "end_station_name": e_name,
        "start_lat": s_lat,
        "start_lng": s_lng,
        "end_lat": e_lat,
        "end_lng": e_lng,
        "member_casual": member,
        "birth_year": by,
        "gender": g,
        "duration_seconds": duration.astype("Float64"),
        "source_file": pd.Series([source_file]*len(df), dtype="string"),
    })
    out = out.dropna(subset=["started_at","ended_at"])
    out = out[(out["duration_seconds"].notna()) & (out["duration_seconds"] > 0) & (out["duration_seconds"] <= 86400)]
    out["duration_seconds"] = out["duration_seconds"].round().astype("Int64")
    out = out.drop_duplicates(subset=["ride_id"], keep="first")
    return out[STANDARD_COLS]

def write_parquet_incremental(out_path: Path, generator):
    writer = None
    try:
        for cleaned in generator:
            if cleaned.empty:
                continue
            table = pa.Table.from_pandas(cleaned, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(str(out_path), table.schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
        elif not out_path.exists():
            empty = pd.DataFrame(columns=STANDARD_COLS)
            pq.write_table(pa.Table.from_pandas(empty, preserve_index=False), str(out_path))

def process_one(csv_path: Path, out_dir: Path, chunksize: int = CHUNK_ROWS):
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / (csv_path.stem + ".parquet")
    if out_path.exists() and out_path.stat().st_size > 0:
        return
    today_year = datetime.now().year
    def gen():
        for chunk in pd.read_csv(csv_path, chunksize=chunksize, low_memory=False, dtype=str):
            yield normalize_chunk(chunk, str(csv_path), today_year)
    write_parquet_incremental(out_path, gen())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=DEFAULT_ROOT)
    ap.add_argument("--out",  default=DEFAULT_OUT)
    ap.add_argument("--chunksize", type=int, default=CHUNK_ROWS)
    args = ap.parse_args()
    root = Path(args.root).expanduser()
    out_dir = Path(args.out).expanduser()
    csvs = sorted(iter_csvs(root))
    if not csvs:
        print(f"No CSVs found under: {root}")
        return
    for p in tqdm(csvs, desc="Cleaning CSVs", unit="file"):
        try:
            process_one(p, out_dir, chunksize=args.chunksize)
        except Exception as e:
            print(f"[ERROR] {p} -> {e}")
    print(f"Done. Clean Parquet files are in: {out_dir}")

if __name__ == "__main__":
    main()