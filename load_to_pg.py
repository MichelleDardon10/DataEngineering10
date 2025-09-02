import os, re, sys, glob
from datetime import datetime, timezone
from io import StringIO
import hashlib
import psycopg2
import pandas as pd
import numpy as np

# ------------------ Config ------------------
RAW_DIR = os.getenv("RAW_DIR", "citibike_data/clean")

# ACTUALIZADO: Incluir todas las columnas que existen en los Parquet
FINAL_COLS = [
    "ride_id", "rideable_type", "started_at", "ended_at",
    "start_station_id", "start_station_name", "start_lat", "start_lng",
    "end_station_id", "end_station_name", "end_lat", "end_lng",
    "member_casual", "tripduration_seconds", "bikeid", "usertype",
    "birth_year", "gender", "source_file", "year_month"
]

def norm(s: str) -> str:
    s = s.strip().replace("/", " ").replace("-", " ").replace(".", " ")
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)
    return s.lower()

# ACTUALIZADO: Incluir todas las columnas de estaciones
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
    out = {}
    for c in cols:
        key = norm(c)
        out[c] = SYN.get(key, key)
    return out

to_ts = lambda s: pd.to_datetime(s, errors="coerce", utc=False)
to_num = lambda s: pd.to_numeric(s, errors="coerce")
YEAR_NOW = datetime.now(timezone.utc).year

# ------------------ SQL ------------------
# ACTUALIZADO: Estructura completa con todas las columnas
CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS citibike;

-- Tabla maestra de estaciones
CREATE TABLE IF NOT EXISTS citibike.stations (
    station_id TEXT PRIMARY KEY,
    station_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
);

-- Tabla principal particionada - ESTRUCTURA COMPLETA
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
    year_month TEXT NOT NULL
) PARTITION BY RANGE (started_at);

-- Tabla de metadata simplificada
CREATE TABLE IF NOT EXISTS citibike.load_metadata (
    source_file TEXT PRIMARY KEY,
    records_loaded INTEGER,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Función para crear particiones
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
    
    IF NOT EXISTS (SELECT 1 FROM pg_tables WHERE tablename = partition_name AND schemaname = 'citibike') THEN
        EXECUTE format('
            CREATE TABLE citibike.%I PARTITION OF citibike.trips
            FOR VALUES FROM (%L) TO (%L)
        ', partition_name, partition_start, partition_end);
    END IF;
END;
$$ LANGUAGE plpgsql;
"""

# ACTUALIZADO: Incluir todas las columnas en el COPY
COPY_SQL = """
COPY citibike.trips (
    ride_id, rideable_type, started_at, ended_at,
    start_station_id, start_station_name, start_lat, start_lng,
    end_station_id, end_station_name, end_lat, end_lng,
    member_casual, tripduration_seconds, bikeid, usertype,
    birth_year, gender, source_file, year_month
) FROM STDIN WITH (FORMAT CSV, NULL '')
"""

# ------------------ Funciones auxiliares ------------------
def generate_ride_id(row, source_file):
    """Genera ID único con hash SHA256"""
    unique_string = f"{source_file}_{row.get('started_at', '')}_{row.get('ended_at', '')}_{row.get('bikeid', '')}"
    return hashlib.sha256(unique_string.encode()).hexdigest()

def get_loaded_files(conn):
    """Obtiene archivos ya cargados"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source_file FROM citibike.load_metadata")
            return {row[0] for row in cur.fetchall()}
    except:
        return set()

def database_exists():
    """Verifica si la base de datos existe"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            dbname=os.getenv("PGDATABASE", "citibike"),
            port=int(os.getenv("PGPORT", "5432")),
        )
        conn.close()
        return True
    except:
        return False

def reset_database():
    """Elimina y recrea completamente la base de datos"""
    try:
        # Conectar a la base de datos postgres para poder eliminar citibike
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            dbname="postgres",
            port=int(os.getenv("PGPORT", "5432")),
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # Terminar todas las conexiones a la base de datos citibike
            cur.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'citibike'
                AND pid <> pg_backend_pid();
            """)
            
            # Eliminar y recrear la base de datos
            cur.execute("DROP DATABASE IF EXISTS citibike")
            cur.execute("CREATE DATABASE citibike")
        
        conn.close()
        print("✅ Base de datos citibike recreada completamente")
        return True
        
    except Exception as e:
        print(f"❌ Error recreando base de datos: {e}")
        return False

def list_files(raw_dir):
    """Lista archivos disponibles - SOLO Parquet"""
    pq_files = glob.glob(os.path.join(raw_dir, "**", "*.parquet"), recursive=True)
    print(f"📁 Encontrados {len(pq_files)} archivos Parquet")
    return "parquet", sorted(pq_files)

def ensure_partition_exists(conn, date_str):
    """Asegura que exista la partición"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT citibike.create_trip_partition_if_not_exists(%s)", (date_str + '-01',))
        conn.commit()
    except Exception as e:
        print(f"⚠️  Error creando partición: {e}")

def update_stations_table(conn, df):
    """Actualiza tabla maestra de estaciones CON TODOS LOS DATOS"""
    try:
        # Verificar qué columnas tenemos disponibles
        available_cols = df.columns.tolist()
        
        # Solo procesar si tenemos datos de estaciones
        if 'start_station_id' in available_cols:
            # Preparar datos de estaciones de inicio
            start_cols = ['start_station_id']
            if 'start_station_name' in available_cols:
                start_cols.append('start_station_name')
            if 'start_lat' in available_cols:
                start_cols.append('start_lat')
            if 'start_lng' in available_cols:
                start_cols.append('start_lng')
                
            start_stations = df[start_cols].copy()
            start_stations.columns = ['station_id'] + [col.replace('start_', '') for col in start_cols[1:]]
            
            # Preparar datos de estaciones de fin
            end_cols = ['end_station_id']
            if 'end_station_name' in available_cols:
                end_cols.append('end_station_name')
            if 'end_lat' in available_cols:
                end_cols.append('end_lat')
            if 'end_lng' in available_cols:
                end_cols.append('end_lng')
                
            end_stations = df[end_cols].copy()
            end_stations.columns = ['station_id'] + [col.replace('end_', '') for col in end_cols[1:]]
            
            # Combinar todas las estaciones
            all_stations = pd.concat([start_stations, end_stations], ignore_index=True)
            all_stations = all_stations.dropna(subset=['station_id'])
            all_stations = all_stations.drop_duplicates('station_id')
            
            # Insertar o actualizar en BD
            with conn.cursor() as cur:
                for _, row in all_stations.iterrows():
                    values = [
                        row['station_id'],
                        row['station_name'] if 'station_name' in row and pd.notna(row['station_name']) else None,
                        row['lat'] if 'lat' in row and pd.notna(row['lat']) else None,
                        row['lng'] if 'lng' in row and pd.notna(row['lng']) else None,
                        datetime.now(), datetime.now()
                    ]
                    
                    cur.execute("""
                        INSERT INTO citibike.stations 
                        (station_id, station_name, latitude, longitude, first_seen, last_seen)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (station_id) DO UPDATE SET
                        station_name = COALESCE(EXCLUDED.station_name, stations.station_name),
                        latitude = COALESCE(EXCLUDED.latitude, stations.latitude),
                        longitude = COALESCE(EXCLUDED.longitude, stations.longitude),
                        last_seen = EXCLUDED.last_seen
                    """, values)
            
            conn.commit()
            print(f"✅ Actualizadas {len(all_stations)} estaciones en la tabla maestra")
            
    except Exception as e:
        print(f"⚠️  Error actualizando estaciones: {e}")
        conn.rollback()

def safe_to_int(s, default=0):
    """Convierte seguro a entero, manejando strings vacíos y nulos"""
    if pd.isna(s) or s == '' or s == ' ':
        return default
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return default

def safe_to_float(s, default=0.0):
    """Convierte seguro a float, manejando strings vacíos y nulos"""
    if pd.isna(s) or s == '' or s == ' ':
        return default
    try:
        return float(s)
    except (ValueError, TypeError):
        return default

def bulk_insert_data(conn, df, batch_size=50000):
    """Bulk insert con manejo robusto de errores"""
    if df.empty:
        return 0
    
    total_inserted = 0
    
    for partition, partition_df in df.groupby('year_month'):
        ensure_partition_exists(conn, partition)
        
        for i in range(0, len(partition_df), batch_size):
            batch = partition_df[i:i + batch_size]
            
            try:
                with conn.cursor() as cur:
                    buf = StringIO()
                    # Convertir a CSV manteniendo los tipos correctos
                    batch.to_csv(buf, index=False, header=False, na_rep='')
                    buf.seek(0)
                    
                    cur.copy_expert(COPY_SQL, buf)
                    conn.commit()
                    
                    inserted = len(batch)
                    total_inserted += inserted
                    print(f"📦 Part {partition}: {inserted} records (total: {total_inserted})")
                    
            except Exception as e:
                conn.rollback()
                print(f"❌ Error en lote: {e}")
    
    return total_inserted

# ------------------ Transform simplificada ------------------
def transform(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    # Normalizar nombres
    df = df.rename(columns=rename_columns(list(df.columns)))

    # Crear columnas faltantes - TODAS LAS COLUMNAS
    for col in FINAL_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[FINAL_COLS].copy()

    # Generar ride_id ÚNICO
    df["ride_id"] = df.apply(lambda row: generate_ride_id(row, source_file), axis=1)

    # Eliminar duplicados por ride_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=["ride_id"])
    if len(df) < initial_count:
        print(f"✅ Eliminados {initial_count - len(df)} duplicados internos")

    # Convertir timestamps
    df["started_at"] = to_ts(df["started_at"])
    df["ended_at"] = to_ts(df["ended_at"])

    # Calcular duración si falta - CON MANEJO DE NULOS
    dur = to_num(df["tripduration_seconds"])
    need_calc = dur.isna()
    if need_calc.any():
        calc = (df["ended_at"] - df["started_at"]).dt.total_seconds()
        df.loc[need_calc, "tripduration_seconds"] = calc.round()
    df["tripduration_seconds"] = to_num(df["tripduration_seconds"]).fillna(0).astype("int64")

    # Filtrar datos inválidos
    ok = (
        df["started_at"].notna() &
        df["ended_at"].notna() &
        (df["tripduration_seconds"] > 0) &
        (df["tripduration_seconds"] <= 86400)
    )
    if not ok.all():
        print(f"✅ Filtrando {(~ok).sum()} registros inválidos")
        df = df.loc[ok].copy()

    # Crear columna de particionado
    df["year_month"] = df["started_at"].dt.strftime("%Y-%m")

    # Convertir coordenadas - CON MANEJO DE NULOS
    for col in ["start_lat", "start_lng", "end_lat", "end_lng"]:
        df[col] = to_num(df[col]).fillna(0.0).astype("float32")

    # Convertir birth_year - MANEJO SEGURO DE ENTEROS
    df["birth_year"] = df["birth_year"].apply(lambda x: safe_to_int(x, 0))
    df["birth_year"] = df["birth_year"].where((df["birth_year"] >= 1900) & (df["birth_year"] <= YEAR_NOW), 0)
    df["birth_year"] = df["birth_year"].astype("Int16")

    # Convertir gender - MANEJO SEGURO DE ENTEROS
    df["gender"] = df["gender"].apply(lambda x: safe_to_int(x, 0))
    df["gender"] = df["gender"].astype("Int8")

    # Columnas de texto - manejar nulos
    text_cols = ["ride_id", "rideable_type", "start_station_id", "start_station_name",
                 "end_station_id", "end_station_name", "member_casual", "bikeid", "usertype"]
    for col in text_cols:
        df[col] = df[col].astype("string").fillna("")

    # Mapear tipos de usuario legacy
    fill_mc = df["member_casual"].isna()
    ut = df["usertype"].str.lower().fillna("")
    df.loc[fill_mc & ut.eq("subscriber"), "member_casual"] = "member"
    df.loc[fill_mc & ut.eq("customer"), "member_casual"] = "casual"
    df.loc[fill_mc & (ut == ""), "member_casual"] = "unknown"

    df.loc[df["rideable_type"].isna(), "rideable_type"] = "classic_bike"
    df["source_file"] = source_file

    # Formatear timestamps para COPY
    df["started_at"] = df["started_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["ended_at"] = df["ended_at"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # Reemplazar NaN por valores por defecto según el tipo
    for col in df.columns:
        if df[col].dtype == 'string' or df[col].dtype == 'object':
            df[col] = df[col].fillna("")
        elif 'int' in str(df[col].dtype):
            df[col] = df[col].fillna(0)
        elif 'float' in str(df[col].dtype):
            df[col] = df[col].fillna(0.0)

    return df[FINAL_COLS]

# ------------------ Procesamiento ------------------
def process_file(conn, path, file_type):
    """Procesa un archivo completo"""
    base = os.path.basename(path)
    print(f"🔄 Procesando: {base}")
    start_time = datetime.now()
    
    try:
        if file_type == "csv":
            chunks = pd.read_csv(
                path, 
                chunksize=100000, 
                dtype=str, 
                low_memory=False,
                na_values=["", "NULL", "null", "NaN", "NA", "N/A", "n/a", " "],
                keep_default_na=False
            )
        else:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(path)
            chunks = (pf.read_row_group(rg).to_pandas() for rg in range(pf.num_row_groups))
        
        total_rows = 0
        for chunk in chunks:
            transformed = transform(chunk, base)
            if not transformed.empty:
                # Siempre intentar actualizar estaciones (ahora tenemos todos los datos)
                update_stations_table(conn, transformed)
                inserted = bulk_insert_data(conn, transformed)
                total_rows += inserted
        
        # Registrar en metadata
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO citibike.load_metadata 
                (source_file, records_loaded)
                VALUES (%s, %s)
                ON CONFLICT (source_file) DO UPDATE SET
                records_loaded = EXCLUDED.records_loaded,
                load_date = CURRENT_TIMESTAMP
            """, (base, total_rows))
            conn.commit()
        
        processing_time = datetime.now() - start_time
        print(f"✅ {base}: {total_rows} registros en {processing_time}")
        
    except Exception as e:
        print(f"❌ Error procesando {base}: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()

# ------------------ Main ------------------
def main():
    """Función principal"""
    # Resetear completamente la base de datos para empezar desde cero
    print("🔄 Reiniciando base de datos...")
    if not reset_database():
        print("❌ No se pudo reiniciar la base de datos")
        return

    # Listar archivos (solo Parquet)
    kind, files = list_files(RAW_DIR)
    if not files:
        print("❌ No hay archivos Parquet para procesar")
        return

    # Conectar
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            dbname=os.getenv("PGDATABASE", "citibike"),
            port=int(os.getenv("PGPORT", "5432")),
        )
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return

    # Crear estructura CON LA DEFINICIÓN CORREGIDA
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
        conn.commit()
        print("🏗️  Estructura de BD creada con definiciones corregidas")
    except Exception as e:
        print(f"❌ Error creando estructura: {e}")
        conn.close()
        return

    # Cargar solo archivos nuevos
    loaded_files = get_loaded_files(conn)
    new_files = [f for f in files if os.path.basename(f) not in loaded_files]
    
    if not new_files:
        print("✅ Todos los archivos ya están cargados")
        conn.close()
        return

    print(f"📊 Archivos nuevos: {len(new_files)}/{len(files)}")
    
    # Procesar archivos nuevos
    for i, file_path in enumerate(new_files, 1):
        print(f"📁 Procesando archivo {i}/{len(new_files)}")
        process_file(conn, file_path, kind)
    
    conn.close()
    print("🎉 Carga completada")

if __name__ == "__main__":
    main()