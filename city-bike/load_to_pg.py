# -*- coding: utf-8 -*-
import os, re
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import psycopg2
import pandas as pd

# =========================================================
# Config & util
# =========================================================
def get_raw_dir() -> Path:
    base_dir = Path(__file__).resolve().parent
    env_dir = os.getenv("RAW_DIR", "citibike_data").strip()
    if env_dir:
        return Path(os.path.expandvars(os.path.expanduser(env_dir))).resolve()
    return (base_dir / "clean").resolve()

RAW_DIR = get_raw_dir()

# paralelismo (ajústalo si quieres)
MAX_WORKERS = min(os.cpu_count() or 4, 8)
PARALLEL = True

# columnas staging (sin ride_id)
STAGING_COLS = [
    "rideable_type","started_at","ended_at",
    "start_station_id","start_station_name","start_lat","start_lng",
    "end_station_id","end_station_name","end_lat","end_lng",
    "member_casual","tripduration_seconds","bikeid","usertype",
    "birth_year","gender","source_file","year_month"
]

def norm(s: str) -> str:
    s = s.strip().replace("/", " ").replace("-", " ").replace(".", " ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    return s.lower()

SYN = {
    "tripduration": "tripduration_seconds", "trip_duration": "tripduration_seconds",
    "starttime": "started_at", "start_time": "started_at",
    "stoptime": "ended_at", "stop_time": "ended_at",
    "start_station_id": "start_station_id", "start_station_name": "start_station_name",
    "start_station_latitude": "start_lat", "start_station_longitude": "start_lng",
    "end_station_id": "end_station_id", "end_station_name": "end_station_name",
    "end_station_latitude": "end_lat", "end_station_longitude": "end_lng",
    "bike_id": "bikeid", "bikeid": "bikeid", "user_type": "usertype", "usertype": "usertype",
    "birth_year": "birth_year", "birth_year_": "birth_year", "gender": "gender",
    "ride_id": "ride_id", "rideable_type": "rideable_type",
    "started_at": "started_at", "ended_at": "ended_at",
    "member_casual": "member_casual",
    "trip_duration": "tripduration_seconds",
}

def rename_columns(cols):
    return {c: SYN.get(norm(c), norm(c)) for c in cols}

YEAR_NOW = datetime.now(timezone.utc).year

# =========================================================
# SQL
# =========================================================
DDL_SQL = """
CREATE SCHEMA IF NOT EXISTS citibike;

CREATE TABLE IF NOT EXISTS citibike.stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
);

CREATE TABLE IF NOT EXISTS citibike.trips (
    ride_id TEXT NOT NULL,
    rideable_type TEXT,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP NOT NULL,
    start_station_id TEXT,
    start_station_name TEXT,
    start_lat FLOAT4,
    start_lng FLOAT4,
    end_station_id TEXT,
    end_station_name TEXT,
    end_lat FLOAT4,
    end_lng FLOAT4,
    member_casual TEXT,
    tripduration_seconds INTEGER NOT NULL,
    bikeid TEXT,
    usertype TEXT,
    birth_year SMALLINT,
    gender SMALLINT,
    source_file TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    year_month TEXT NOT NULL,
    UNIQUE (ride_id, started_at)
) PARTITION BY RANGE (started_at);

CREATE TABLE IF NOT EXISTS citibike.load_metadata (
    source_file TEXT PRIMARY KEY,
    records_loaded INTEGER,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Función mejorada que maneja el error de partición existente y agrega constraint única
CREATE OR REPLACE FUNCTION citibike.create_trip_partition_if_not_exists(partition_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    partition_start DATE;
    partition_end DATE;
BEGIN
    partition_name := 'trips_' || TO_CHAR(partition_date, 'YYYY_MM');
    partition_start := DATE_TRUNC('MONTH', partition_date);
    partition_end := partition_start + INTERVAL '1 month';
    
    -- Verificar si la partición ya existe
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND n.nspname = 'citibike' AND c.relname = partition_name
    ) THEN
        BEGIN
            EXECUTE format('
                CREATE TABLE citibike.%I PARTITION OF citibike.trips
                FOR VALUES FROM (%L) TO (%L)
            ', partition_name, partition_start, partition_end);
            
            -- Agregar constraint única a la partición recién creada
            EXECUTE format('
                ALTER TABLE citibike.%I ADD CONSTRAINT %I UNIQUE (ride_id)
            ', partition_name, partition_name || '_unique_ride_id');
            
        EXCEPTION WHEN duplicate_table THEN
            -- La partición ya existe, simplemente ignorar
            RAISE NOTICE 'Partition % already exists', partition_name;
        END;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- staging rápida para COPY
CREATE UNLOGGED TABLE IF NOT EXISTS citibike.trips_staging (
    rideable_type TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_id TEXT,
    start_station_name TEXT,
    start_lat FLOAT4,
    start_lng FLOAT4,
    end_station_id TEXT,
    end_station_name TEXT,
    end_lat FLOAT4,
    end_lng FLOAT4,
    member_casual TEXT,
    tripduration_seconds INTEGER,
    bikeid TEXT,
    usertype TEXT,
    birth_year SMALLINT,
    gender SMALLINT,
    source_file TEXT,
    year_month TEXT
);
"""

COPY_STAGING_SQL = """
COPY citibike.trips_staging (
  rideable_type, started_at, ended_at,
  start_station_id, start_station_name, start_lat, start_lng,
  end_station_id, end_station_name, end_lat, end_lng,
  member_casual, tripduration_seconds, bikeid, usertype,
  birth_year, gender, source_file, year_month
) FROM STDIN WITH (FORMAT CSV, NULL '')
"""

INSERT_FINAL_SQL = """
INSERT INTO citibike.trips (
  ride_id, rideable_type, started_at, ended_at,
  start_station_id, start_station_name, start_lat, start_lng,
  end_station_id, end_station_name, end_lat, end_lng,
  member_casual, tripduration_seconds, bikeid, usertype,
  birth_year, gender, source_file, year_month
)
SELECT
  md5(
    s.source_file || '|' ||
    to_char(s.started_at,'YYYY-MM-DD HH24:MI:SS') || '|' ||
    to_char(s.ended_at,'YYYY-MM-DD HH24:MI:SS') || '|' ||
    coalesce(s.bikeid,'')
  ) AS ride_id,
  s.rideable_type, s.started_at, s.ended_at,
  s.start_station_id, s.start_station_name, s.start_lat, s.start_lng,
  s.end_station_id, s.end_station_name, s.end_lat, s.end_lng,
  s.member_casual, s.tripduration_seconds, s.bikeid, s.usertype,
  s.birth_year, s.gender, s.source_file, s.year_month
FROM citibike.trips_staging s
ON CONFLICT (ride_id, started_at) DO NOTHING;
"""

# =========================================================
# DB helpers
# =========================================================
def connect():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        dbname=os.getenv("PGDATABASE", "citibike"),
        port=int(os.getenv("PGPORT", "5432")),
    )

def set_perf_settings(conn):
    cmds = [
        "SET synchronous_commit TO OFF",
        "SET maintenance_work_mem TO '1024MB'",
        "SET work_mem TO '256MB'",
        "SET temp_buffers TO '256MB'",
    ]
    with conn.cursor() as cur:
        for sql in cmds:
            try:
                cur.execute(sql)
            except Exception as e:
                print(f"⚠️  No se pudo aplicar '{sql}': {e}")
    conn.commit()

def init_db(conn):
    with conn.cursor() as cur:
        cur.execute(DDL_SQL)
    conn.commit()

def get_loaded_files(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source_file FROM citibike.load_metadata")
            return {r[0] for r in cur.fetchall()}
    except:
        return set()

def list_parquet(raw_dir: Path):
    print(f"🔎 Buscando Parquet en: {raw_dir}")
    if not raw_dir.exists():
        print("⚠️  La carpeta no existe.")
        return []
    files = sorted(str(p) for p in raw_dir.rglob("*.parquet"))
    print(f"📁 Encontrados {len(files)} archivos Parquet")
    if files:
        for ex in files[:3]:
            print("   •", ex)
    return files

# =========================================================
# Transform (vectorizado, sin ride_id)
# =========================================================
def transform(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    df = df.rename(columns=rename_columns(df.columns))

    for c in STAGING_COLS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[STAGING_COLS].copy()

    # fechas (naive)
    df["started_at"] = pd.to_datetime(df["started_at"], errors="coerce", utc=False)
    df["ended_at"]   = pd.to_datetime(df["ended_at"],   errors="coerce", utc=False)

    # duración
    td = pd.to_numeric(df["tripduration_seconds"], errors="coerce")
    calc = (df["ended_at"] - df["started_at"]).dt.total_seconds()
    df["tripduration_seconds"] = td.fillna(calc).fillna(0).astype("int32")

    # filtra inválidos
    df = df[
        df["started_at"].notna() &
        df["ended_at"].notna() &
        df["tripduration_seconds"].between(1, 86400)
    ].copy()

    # partición
    df["year_month"] = df["started_at"].dt.strftime("%Y-%m")

    # coordenadas
    for c in ["start_lat","start_lng","end_lat","end_lng"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("float32")

    # birth_year & gender
    by = pd.to_numeric(df["birth_year"], errors="coerce").astype("float32")
    by = by.where((by>=1900)&(by<=YEAR_NOW)).fillna(0).astype("Int16")
    df["birth_year"] = by
    df["gender"] = pd.to_numeric(df["gender"], errors="coerce").fillna(0).astype("Int8")

    # texto
    for c in ["rideable_type","start_station_id","start_station_name",
              "end_station_id","end_station_name","member_casual","bikeid","usertype"]:
        df[c] = df[c].astype("string").fillna("")

    # legacy -> member_casual
    mc_empty = df["member_casual"].eq("")
    ut = df["usertype"].str.lower()
    df.loc[mc_empty & ut.eq("subscriber"), "member_casual"] = "member"
    df.loc[mc_empty & ut.eq("customer"),   "member_casual"] = "casual"
    df.loc[df["member_casual"].eq(""),     "member_casual"] = "unknown"

    df["source_file"] = source_file
    return df

# =========================================================
# Estaciones: bulk upsert
# =========================================================
def upsert_stations(conn, df: pd.DataFrame):
    cols = []
    if "start_station_id" in df.columns: cols.append("start")
    if "end_station_id"   in df.columns: cols.append("end")
    if not cols: return

    frames = []
    now = datetime.now()
    
    if "start" in cols:
        st = df[["start_station_id","start_station_name","start_lat","start_lng"]].copy()
        st.columns = ["station_id","station_name","latitude","longitude"]
        st["first_seen"] = now
        st["last_seen"] = now
        # Limpieza más agresiva
        st = st[st["station_id"].notna()]
        st["station_id"] = st["station_id"].astype(str).str.strip()
        st = st[st["station_id"] != ""]
        st = st[st["station_id"] != "nan"]
        frames.append(st)
    
    if "end" in cols:
        et = df[["end_station_id","end_station_name","end_lat","end_lng"]].copy()
        et.columns = ["station_id","station_name","latitude","longitude"]
        et["first_seen"] = now
        et["last_seen"] = now
        # Limpieza más agresiva
        et = et[et["station_id"].notna()]
        et["station_id"] = et["station_id"].astype(str).str.strip()
        et = et[et["station_id"] != ""]
        et = et[et["station_id"] != "nan"]
        frames.append(et)

    if not frames:
        return
    
    all_st = pd.concat(frames, ignore_index=True)
    
    # Limpieza final exhaustiva
    all_st = all_st[all_st["station_id"].notna()]
    all_st["station_id"] = all_st["station_id"].astype(str).str.strip()
    all_st = all_st[all_st["station_id"] != ""]
    all_st = all_st[all_st["station_id"] != "nan"]
    
    if len(all_st) == 0:
        return
    
    all_st = all_st.drop_duplicates("station_id")

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _stations_stage (
              station_id TEXT PRIMARY KEY,
              station_name TEXT,
              latitude DOUBLE PRECISION,
              longitude DOUBLE PRECISION,
              first_seen TIMESTAMP,
              last_seen TIMESTAMP
            ) ON COMMIT DROP;
        """)
    
    buf = StringIO()
    all_st.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    
    try:
        with conn.cursor() as cur:
            cur.copy_expert(
                "COPY _stations_stage (station_id,station_name,latitude,longitude,first_seen,last_seen) FROM STDIN WITH (FORMAT CSV, NULL '')",
                buf
            )
            cur.execute("""
                INSERT INTO citibike.stations AS t
                (station_id, station_name, latitude, longitude, first_seen, last_seen)
                SELECT station_id, station_name, latitude, longitude, MIN(first_seen), MAX(last_seen)
                FROM _stations_stage
                GROUP BY 1,2,3,4
                ON CONFLICT (station_id) DO UPDATE SET
                  station_name = COALESCE(EXCLUDED.station_name, t.station_name),
                  latitude     = COALESCE(EXCLUDED.latitude,     t.latitude),
                  longitude    = COALESCE(EXCLUDED.longitude,    t.longitude),
                  last_seen    = GREATEST(EXCLUDED.last_seen,    t.last_seen);
            """)
        conn.commit()
        print("✅ Estaciones actualizadas correctamente")
    except Exception as e:
        print(f"❌ Error en upsert_stations: {e}")
        conn.rollback()
        raise

# =========================================================
# COPY helpers
# =========================================================
def copy_df(conn, df: pd.DataFrame, copy_sql: str):
    buf = StringIO()
    df.to_csv(buf, index=False, header=False, na_rep='')
    buf.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)

def precreate_partitions(conn, year_month_list):
    with conn.cursor() as cur:
        for ym in sorted(set(year_month_list)):
            try:
                cur.execute("SELECT citibike.create_trip_partition_if_not_exists(%s)", (ym+'-01',))
            except psycopg2.errors.DuplicateTable:
                # Si la partición ya existe, simplemente continuar
                print(f"ℹ️  La partición {ym} ya existe, continuando...")
                conn.rollback()
                continue
            except Exception as e:
                print(f"⚠️  Error al crear partición {ym}: {e}")
                conn.rollback()
                continue
    conn.commit()

# =========================================================
# Procesamiento por archivo
# =========================================================
def process_file_fast(file_path: str):
    import pyarrow.dataset as ds
    dataset = ds.dataset(file_path, format="parquet")
    table = dataset.to_table()

    # Usar la ruta completa como identificador único, no solo el nombre base
    source_file_id = file_path
    start_time = datetime.now()
    print(f"⚡ Procesando: {source_file_id}")

    # clave: NO Arrow-backed
    df = table.to_pandas()

    conn = connect()
    try:
        set_perf_settings(conn)

        # Pasar el identificador único a transform
        df = transform(df, source_file_id)

        precreate_partitions(conn, df["year_month"].unique())

        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE citibike.trips_staging")
            copy_df(conn, df[STAGING_COLS], COPY_STAGING_SQL)
            upsert_stations(conn, df)
            with conn.cursor() as cur:
                cur.execute(INSERT_FINAL_SQL)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO citibike.load_metadata (source_file, records_loaded)
                    SELECT %s, COUNT(*) FROM citibike.trips_staging
                    ON CONFLICT (source_file) DO UPDATE
                    SET records_loaded = EXCLUDED.records_loaded,
                        load_date = CURRENT_TIMESTAMP
                """, (source_file_id,))
        elapsed = datetime.now() - start_time
        print(f"✅ {source_file_id}: OK en {elapsed}")
    except Exception as e:
        import traceback
        print(f"❌ Error procesando {source_file_id}: {e}")
        traceback.print_exc()
        conn.rollback()
        raise
    finally:
        conn.close()

# =========================================================
# Main
# =========================================================
def main():
    conn = connect()
    try:
        init_db(conn)
        set_perf_settings(conn)
    finally:
        conn.close()

    files = list_parquet(RAW_DIR)
    if not files:
        print("❌ No hay archivos Parquet para procesar")
        return

    conn = connect()
    try:
        loaded = get_loaded_files(conn)
    finally:
        conn.close()

    # Usar rutas completas para la verificación
    new_files = [f for f in files if f not in loaded]
    if not new_files:
        print("✅ Todos los archivos ya están cargados")
        return

    print(f"📊 Archivos nuevos: {len(new_files)}/{len(files)}")

    if PARALLEL and len(new_files) > 1:
        print(f"🧵 Paralelizando con {MAX_WORKERS} procesos…")
        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(process_file_fast, f) for f in new_files]
            for _ in as_completed(futs):
                pass
    else:
        for i, fp in enumerate(new_files, 1):
            print(f"📁 {i}/{len(new_files)}")
            process_file_fast(fp)

    print("🎉 Carga completada")

if __name__ == "__main__":
    main()