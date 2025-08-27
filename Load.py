#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Cargador ultra-rápido CitiBike → PostgreSQL (optimizado para BI y dashboards).
"""

from __future__ import annotations
import argparse, os, tempfile, multiprocessing as mp, sys
from pathlib import Path
from typing import Tuple, List
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm
import time

# ======= Defaults =======
CLEAN_DIR_DEFAULT = "citibike_clean"
DB = dict(host="localhost", port=5432, user="postgres", password="postgres", dbname="citibike")

# DDL básica inicial (sin columnas calculadas)
DDL_BASIC_TABLE = """
CREATE TABLE IF NOT EXISTS public.citibike_trips (
  ride_id            TEXT PRIMARY KEY,
  rideable_type      TEXT,
  started_at         TIMESTAMP,
  ended_at           TIMESTAMP,
  start_station_id   TEXT,
  start_station_name TEXT,
  end_station_id     TEXT,
  end_station_name   TEXT,
  start_lat          DOUBLE PRECISION,
  start_lng          DOUBLE PRECISION,
  end_lat            DOUBLE PRECISION,
  end_lng            DOUBLE PRECISION,
  member_casual      TEXT,
  birth_year         SMALLINT,
  gender             SMALLINT,
  duration_seconds   INTEGER,
  source_file        TEXT
);
"""

# Índices temporales para carga
TEMPORARY_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_temp_started_at ON public.citibike_trips(started_at);",
]

ORDER = ["ride_id","rideable_type","started_at","ended_at",
         "start_station_id","start_station_name","end_station_id","end_station_name",
         "start_lat","start_lng","end_lat","end_lng",
         "member_casual","birth_year","gender","duration_seconds","source_file"]

# ---------- Helpers Optimizados ----------
def ensure_database(host:str, port:int, user:str, password:str, dbname:str):
    """Crea la DB si no existe con configuración optimizada."""
    try:
        psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname).close()
        return
    except Exception:
        pass
    
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname="postgres")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s;", (dbname,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{dbname}";')
                print(f"[INFO] Base de datos {dbname} creada")
    finally:
        conn.close()

def list_parquets(root: Path) -> List[Path]:
    """Lista parquets ordenados por fecha para mejor performance."""
    files = []
    for dp, _, files_in_dir in os.walk(root):
        for fn in files_in_dir:
            if fn.lower().endswith(".parquet"):
                files.append(Path(dp) / fn)
    # Ordenar por nombre (asumiendo que tienen fecha en el nombre)
    files.sort()
    return files

def configure_database_performance(dsn: str):
    """Configura PostgreSQL para máxima velocidad de carga (usando psycopg2 directo)."""
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True  # Necesario para cambiar parámetros
        with conn.cursor() as cur:
            # Solo parámetros que se pueden cambiar en runtime
            cur.execute("SET synchronous_commit = off;")
            cur.execute("SET work_mem = '256MB';")
            cur.execute("SET maintenance_work_mem = '1GB';")
            cur.execute("SET effective_cache_size = '4GB';")
            print("[CONFIG] Parámetros de performance configurados")
        conn.close()
    except Exception as e:
        print(f"[WARN] No se pudieron configurar parámetros: {e}")

def create_table_if_needed(engine):
    """Crea tabla básica para carga inicial."""
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql(DDL_BASIC_TABLE)
            # Índices temporales para la carga
            for sql in TEMPORARY_INDEXES:
                try:
                    conn.exec_driver_sql(sql)
                except:
                    pass  # Ignorar si ya existen
        except AttributeError:
            conn.execute(text(DDL_BASIC_TABLE))
            for sql in TEMPORARY_INDEXES:
                try:
                    conn.execute(text(sql))
                except:
                    pass

def finalize_table_optimization(engine):
    """Añade columnas calculadas, índices finales y optimiza."""
    print("[OPTIMIZANDO] Añadiendo columnas calculadas...")
    
    # Primero dropear índices temporales si existen
    with engine.begin() as conn:
        try:
            conn.exec_driver_sql("DROP INDEX IF EXISTS idx_temp_started_at;")
        except:
            pass
    
    # Añadir columnas generadas e índices permanentes
    optimization_sql = """
    -- Añadir columnas generadas
    ALTER TABLE public.citibike_trips 
    ADD COLUMN IF NOT EXISTS date_key DATE GENERATED ALWAYS AS (DATE(started_at)) STORED,
    ADD COLUMN IF NOT EXISTS hour_of_day SMALLINT GENERATED ALWAYS AS (EXTRACT(HOUR FROM started_at)) STORED,
    ADD COLUMN IF NOT EXISTS day_of_week SMALLINT GENERATED ALWAYS AS (EXTRACT(DOW FROM started_at)) STORED,
    ADD COLUMN IF NOT EXISTS month_of_year SMALLINT GENERATED ALWAYS AS (EXTRACT(MONTH FROM started_at)) STORED,
    ADD COLUMN IF NOT EXISTS year SMALLINT GENERATED ALWAYS AS (EXTRACT(YEAR FROM started_at)) STORED,
    ADD COLUMN IF NOT EXISTS is_member BOOLEAN GENERATED ALWAYS AS (member_casual = 'member') STORED,
    ADD COLUMN IF NOT EXISTS is_weekend BOOLEAN GENERATED ALWAYS AS (EXTRACT(DOW FROM started_at) IN (0, 6)) STORED;

    -- Crear índices estratégicos para BI
    CREATE INDEX IF NOT EXISTS idx_citibike_date_key ON public.citibike_trips(date_key);
    CREATE INDEX IF NOT EXISTS idx_citibike_hour ON public.citibike_trips(hour_of_day);
    CREATE INDEX IF NOT EXISTS idx_citibike_member ON public.citibike_trips(is_member);
    CREATE INDEX IF NOT EXISTS idx_citibike_station_start ON public.citibike_trips(start_station_id);
    CREATE INDEX IF NOT EXISTS idx_citibike_station_end ON public.citibike_trips(end_station_id);
    CREATE INDEX IF NOT EXISTS idx_citibike_weekend ON public.citibike_trips(is_weekend);
    CREATE INDEX IF NOT EXISTS idx_citibike_started_at ON public.citibike_trips(started_at);

    -- Optimizar tabla
    VACUUM ANALYZE public.citibike_trips;
    """
    
    # Ejecutar en partes para mejor manejo de errores
    with engine.begin() as conn:
        statements = optimization_sql.split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                try:
                    conn.exec_driver_sql(stmt)
                except Exception as e:
                    print(f"[WARN] Error ejecutando: {stmt[:50]}... -> {e}")
                    continue

def copy_from_parquet_fast(pq_path: Path, dsn: str) -> Tuple[str, int]:
    """Worker optimizado para carga masiva."""
    print(f"[PROCESANDO] {pq_path.name}", flush=True)
    
    raw = psycopg2.connect(dsn)
    raw.autocommit = False
    
    try:
        with raw.cursor() as cur:
            # Configuración de performance por conexión
            cur.execute("SET work_mem = '256MB';")
            cur.execute("SET synchronous_commit = off;")
            
            # Leer parquet
            table = pq.read_table(pq_path)
            total_rows = table.num_rows
            
            # Preparar datos
            cols = []
            for c in ORDER:
                if c in table.column_names:
                    # Optimizar tipos de datos
                    if c in ["birth_year", "gender"]:
                        # Convertir a SMALLINT para ahorrar espacio
                        col_data = table[c]
                        if col_data.type != pa.int16():
                            col_data = col_data.cast(pa.int16())
                        cols.append(col_data)
                    else:
                        cols.append(table[c])
                else:
                    # Valores por defecto optimizados
                    if c in ["birth_year", "gender"]:
                        null_col = pa.nulls(table.num_rows, type=pa.int16())
                    else:
                        null_col = pa.nulls(table.num_rows, type=pa.string())
                    cols.append(null_col)
            
            table_ordered = pa.table(cols, names=ORDER)
            
            # Crear CSV temporal
            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
                tmp_path = tmp.name

            try:
                # Escribir CSV sin headers (más rápido)
                pacsv.write_csv(table_ordered, tmp_path, write_options=pacsv.WriteOptions(include_header=False))
                
                # COPY directo
                with open(tmp_path, "r", encoding="utf-8") as f:
                    cur.copy_expert(
                        "COPY public.citibike_trips FROM STDIN WITH CSV",
                        f
                    )
                
                raw.commit()
                print(f"[OK] {pq_path.name} - {total_rows} filas", flush=True)
                return (f"{pq_path.name} - OK: {total_rows} filas", 0)
                
            finally:
                try: 
                    os.remove(tmp_path)
                except OSError: 
                    pass

    except Exception as e:
        raw.rollback()
        msg = f"{pq_path.name} -> ERROR: {str(e)}"
        print(f"[ERROR] {msg}", flush=True)
        return (msg, 1)
    finally:
        raw.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean",  default=CLEAN_DIR_DEFAULT)
    ap.add_argument("--dbhost", default=DB["host"])
    ap.add_argument("--dbport", type=int, default=DB["port"])
    ap.add_argument("--dbuser", default=DB["user"])
    ap.add_argument("--dbpass", default=DB["password"])
    ap.add_argument("--dbname", default=DB["dbname"])
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--test", action="store_true", help="Procesar solo un archivo de prueba")
    ap.add_argument("--skip-optimize", action="store_true", help="Saltar optimización final")
    args = ap.parse_args()

    clean_dir = Path(args.clean)
    files = list_parquets(clean_dir)
    
    if not files:
        print(f"[ERROR] No se encontraron .parquet en {clean_dir}")
        sys.exit(3)

    print(f"[INFO] Encontrados {len(files)} archivos .parquet")
    
    # 1) Configuración de base de datos
    ensure_database(args.dbhost, args.dbport, args.dbuser, args.dbpass, args.dbname)
    
    # 2) Configurar parámetros de performance
    dsn = f"host={args.dbhost} port={args.dbport} dbname={args.dbname} user={args.dbuser} password={args.dbpass}"
    configure_database_performance(dsn)
    
    # 3) Crear engine y tabla básica
    engine = create_engine(f"postgresql+psycopg2://{args.dbuser}:{args.dbpass}@{args.dbhost}:{args.dbport}/{args.dbname}")
    create_table_if_needed(engine)
    
    # 4) Carga de datos
    if args.test:
        files = files[:1]
        print("[MODO TEST] Procesando solo el primer archivo...")
    
    print(f"[INICIANDO] Carga de {len(files)} archivos...")
    
    # Carga secuencial para mejor control
    results = []
    successful_files = 0
    
    for i, file_path in enumerate(files, 1):
        print(f"\n[{i}/{len(files)}] ", end="", flush=True)
        result = copy_from_parquet_fast(file_path, dsn)
        results.append(result)
        
        if result[1] == 0:
            successful_files += 1
        
        # Pequeña pausa para no saturar
        time.sleep(0.1)

    # 5) Optimización final (opcional)
    if not args.skip_optimize and successful_files > 0:
        print("\n[OPTIMIZANDO] Aplicando optimizaciones finales...")
        finalize_table_optimization(engine)
    
    # 6) Reporte
    errors = [r for r in results if r[1] != 0]
    print(f"\n{'='*60}")
    print(f"Termindo")
    print(f"Archivos exitosos: {successful_files}/{len(files)}")
    print(f"Archivos con error: {len(errors)}")
    
    if errors:
        print("\n[ERRORES] Archivos que fallaron:")
        for msg, _ in errors[:5]:  # Mostrar solo primeros 5 errores
            print(f"  - {msg}")
        if len(errors) > 5:
            print(f"  ... y {len(errors) - 5} más")
    
    if successful_files > 0:
        print("\nBase de datos optimizada para BI y dashboards rápidos")
        print("   • Columnas calculadas pregeneradas")
        print("   • Índices estratégicos creados")
        print("   • Estructura optimizada para consultas")
    else:
        print("\nNo se cargaron archivos exitosamente")

if __name__ == "__main__":
    mp.freeze_support()
    main()