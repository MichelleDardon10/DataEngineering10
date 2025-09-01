import os, re, sys, glob
from datetime import datetime
import pandas as pd

# Por defecto: tus carpetas reales en Windows. En contenedor se sobreescriben a /raw y /clean.
RAW_DIR   = os.getenv("RAW_DIR",   "./citibike_data/csv")
CLEAN_DIR = os.getenv("CLEAN_DIR", "./citibike_data/clean")
WRITE_PARQUET = os.getenv("WRITE_PARQUET", "1") == "1"

os.makedirs(CLEAN_DIR, exist_ok=True)

FINAL_COLS = [
    "ride_id","rideable_type","started_at","ended_at",
    "start_station_id","start_station_name","start_lat","start_lng",
    "end_station_id","end_station_name","end_lat","end_lng",
    "member_casual","tripduration_seconds","bikeid","usertype",
    "birth_year","gender","source_file",
]

def norm(s:str)->str:
    s = s.strip().replace("/", " ").replace("-", " ").replace(".", " ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    return s.lower()

SYN = {
    # legacy
    "tripduration":"tripduration_seconds","trip_duration":"tripduration_seconds",
    "starttime":"started_at","start_time":"started_at",
    "stoptime":"ended_at","stop_time":"ended_at",
    "start_station_id":"start_station_id","start_station_name":"start_station_name",
    "start_station_latitude":"start_lat","start_station_longitude":"start_lng",
    "end_station_id":"end_station_id","end_station_name":"end_station_name",
    "end_station_latitude":"end_lat","end_station_longitude":"end_lng",
    "bike_id":"bikeid","bikeid":"bikeid",
    "user_type":"usertype","usertype":"usertype",
    "birth_year":"birth_year","birth_year_":"birth_year",
    "gender":"gender",
    # modern
    "ride_id":"ride_id","rideable_type":"rideable_type",
    "started_at":"started_at","ended_at":"ended_at",
    "start_lat":"start_lat","start_lng":"start_lng",
    "end_lat":"end_lat","end_lng":"end_lng",
    "start_station_id_":"start_station_id","start_station_name_":"start_station_name",
    "end_station_id_":"end_station_id","end_station_name_":"end_station_name",
    "member_casual":"member_casual",
    # variantes con mayúsculas/espacios
    "trip_duration":"tripduration_seconds",
    "start_station_id_":"start_station_id",
    "start_time":"started_at","stop_time":"ended_at",
    "start_station_id":"start_station_id","start_station_name":"start_station_name",
    "end_station_id":"end_station_id","end_station_name":"end_station_name",
    "start_station_latitude_":"start_lat","start_station_longitude_":"start_lng",
    "end_station_latitude_":"end_lat","end_station_longitude_":"end_lng",
    "bike_id_":"bikeid","user_type_":"usertype",
    "birth_year__":"birth_year"
}

def rename_columns(cols):
    out = {}
    for c in cols:
        key = norm(c)
        out[c] = SYN.get(key, key)
    return out

def to_ts(s):  return pd.to_datetime(s, errors="coerce", utc=False)
def to_num(s): return pd.to_numeric(s, errors="coerce")

YEAR_NOW = datetime.utcnow().year

def process_file(path):
    base = os.path.basename(path)
    stem = re.sub(r"\.csv$", "", base)
    out_csv = os.path.join(CLEAN_DIR, f"{stem}__clean.csv")
    out_par = os.path.join(CLEAN_DIR, f"{stem}__clean.parquet")

    first_chunk = True
    parquet_written = False

    try:
        for chunk in pd.read_csv(
            path, chunksize=200_000, dtype=str, low_memory=False,
            na_values=["", "NULL", "null", "NaN"], keep_default_na=True, encoding="utf-8"
        ):
            # headers
            chunk = chunk.rename(columns=rename_columns(list(chunk.columns)))
            # ensure all cols
            for col in FINAL_COLS:
                if col not in chunk.columns:
                    chunk[col] = pd.NA

            df = chunk[FINAL_COLS].copy()

            # tipos
            df["started_at"] = to_ts(df["started_at"])
            df["ended_at"]   = to_ts(df["ended_at"])

            # duración
            dur = to_num(df["tripduration_seconds"])
            need_calc = dur.isna()
            calc = (df["ended_at"] - df["started_at"]).dt.total_seconds()
            df.loc[need_calc, "tripduration_seconds"] = calc.round()
            df["tripduration_seconds"] = to_num(df["tripduration_seconds"])

            # filtros básicos
            ok = (
                df["started_at"].notna() & df["ended_at"].notna() &
                df["tripduration_seconds"].notna() &
                (df["tripduration_seconds"] > 0) &
                (df["tripduration_seconds"] <= 86400)  # <= 24h
            )
            df = df.loc[ok]

            # numéricos
            for col in ["start_lat","start_lng","end_lat","end_lng"]:
                df[col] = to_num(df[col])

            # birth_year
            by = to_num(df["birth_year"])
            by = by.where((by >= 1900) & (by <= YEAR_NOW))
            df["birth_year"] = by.astype("Int64")

            # gender
            df["gender"] = to_num(df["gender"]).astype("Int64")

            # strings clave
            for col in ["ride_id","rideable_type","start_station_id","start_station_name",
                        "end_station_id","end_station_name","member_casual","bikeid","usertype"]:
                df[col] = df[col].astype("string")

            # legacy → member_casual
            fill_mc = df["member_casual"].isna()
            ut = df["usertype"].str.lower()
            df.loc[fill_mc & ut.eq("subscriber"), "member_casual"] = "member"
            df.loc[fill_mc & ut.eq("customer"),   "member_casual"] = "casual"

            # rideable_type default in legacy
            df.loc[df["rideable_type"].isna(), "rideable_type"] = "classic_bike"

            # source
            df["source_file"] = base

            # dedupe chunk
            if df["ride_id"].notna().any():
                df = df.drop_duplicates(subset=["ride_id"])
            else:
                df = df.drop_duplicates(subset=[
                    "started_at","ended_at","bikeid","start_station_id","end_station_id"
                ])

            # salida CSV (para COPY)
            df_csv = df.copy()
            df_csv["started_at"] = df_csv["started_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df_csv["ended_at"]   = df_csv["ended_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df_csv.to_csv(out_csv, index=False,
                          mode="w" if first_chunk else "a",
                          header=first_chunk)

            # salida Parquet por archivo (opcional) — rápido para analytics
            if WRITE_PARQUET:
                # Parquet no soporta append simple en pandas; escribimos por archivo completo al finalizar chunks
                # acumulando temporalmente en una lista pequeña es arriesgado en memoria; así que escribimos por chunk a CSV (para DB)
                # y al terminar, juntamos con un segundo pase a partir del CSV limpio (barato y secuencial).
                pass

            first_chunk = False

        # Si pediste parquet, hacemos un pase secuencial sobre el CSV limpio ya generado.
        if WRITE_PARQUET and os.path.exists(out_csv):
            df_all = pd.read_csv(out_csv, dtype={
                "ride_id":"string","rideable_type":"string",
                "start_station_id":"string","start_station_name":"string",
                "end_station_id":"string","end_station_name":"string",
                "member_casual":"string","bikeid":"string","usertype":"string",
                "source_file":"string"
            }, parse_dates=["started_at","ended_at"])
            df_all.to_parquet(out_par, index=False)
            parquet_written = True

        print(f"[OK] {base} → {os.path.basename(out_csv)}" + (" + parquet" if parquet_written else ""))

    except Exception as e:
        print(f"[ERROR] {base}: {e}", file=sys.stderr)

def main():
    pattern = os.path.join(RAW_DIR, "**", "*.csv")
    files = glob.glob(pattern, recursive=True)
    if not files:
        print(f"Sin CSV en {RAW_DIR}")
        return
    files.sort()
    for f in files:
        process_file(f)

if __name__ == "__main__":
    main()
