import os
import psycopg2
import sys

def nuke_database():
    """Elimina y recrea completamente la base de datos citibike"""
    print("💣 Iniciando limpieza completa de la base de datos...")
    
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
            print("🔌 Terminando conexiones activas a la base de datos citibike...")
            
            # Terminar todas las conexiones a la base de datos citibike
            cur.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'citibike'
                AND pid <> pg_backend_pid();
            """)
            
            print("🗑️  Eliminando base de datos citibike...")
            # Eliminar la base de datos si existe
            cur.execute("DROP DATABASE IF EXISTS citibike")
            
            print("🆕 Creando nueva base de datos citibike...")
            # Crear nueva base de datos
            cur.execute("CREATE DATABASE citibike")
            
            print("✅ Esquema eliminado y recreado exitosamente")
        
        conn.close()
        
        # Ahora conectar a la nueva base de datos para crear el esquema completo
        print("🏗️  Creando estructura completa del esquema citibike...")
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            dbname="citibike",
            port=int(os.getenv("PGPORT", "5432")),
        )
        conn.autocommit = True
        
        with conn.cursor() as cur:
            # SQL completo con todas las 19 columnas
            create_sql = """
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

            -- Tabla principal particionada - ESTRUCTURA COMPLETA (19 columnas)
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
            
            cur.execute(create_sql)
            print("✅ Estructura de tablas creada exitosamente")
            
            # Verificar la estructura creada
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'citibike' AND table_name = 'trips' 
                ORDER BY ordinal_position;
            """)
            
            columns = cur.fetchall()
            print(f"📋 Estructura de la tabla 'trips' ({len(columns)} columnas):")
            for col in columns:
                print(f"   - {col[0]}: {col[1]}")
        
        conn.close()
        print("🎉 Base de datos completamente resetada y lista para usar!")
        return True
        
    except Exception as e:
        print(f"❌ Error durante el reset de la base de datos: {e}")
        import traceback
        traceback.print_exc()
        return False

def verify_structure():
    """Verifica que la estructura de la BD coincida con los archivos Parquet"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            user=os.getenv("PGUSER", "postgres"),
            password=os.getenv("PGPASSWORD", "postgres"),
            dbname="citibike",
            port=int(os.getenv("PGPORT", "5432")),
        )
        
        with conn.cursor() as cur:
            # Verificar columnas de la tabla trips
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'citibike' AND table_name = 'trips' 
                ORDER BY ordinal_position;
            """)
            
            db_columns = [col[0] for col in cur.fetchall()]
            expected_columns = [
                "ride_id", "rideable_type", "started_at", "ended_at",
                "start_station_id", "start_station_name", "start_lat", "start_lng",
                "end_station_id", "end_station_name", "end_lat", "end_lng",
                "member_casual", "tripduration_seconds", "bikeid", "usertype",
                "birth_year", "gender", "source_file", "load_timestamp", "year_month"
            ]
            
            print("🔍 Verificando coincidencia de columnas:")
            print(f"   - Columnas en BD: {len(db_columns)}")
            print(f"   - Columnas esperadas: {len(expected_columns)}")
            
            missing_in_db = set(expected_columns) - set(db_columns)
            extra_in_db = set(db_columns) - set(expected_columns)
            
            if missing_in_db:
                print("❌ Faltan columnas en la BD:")
                for col in missing_in_db:
                    print(f"   - {col}")
            
            if extra_in_db:
                print("❌ Columnas extra en la BD:")
                for col in extra_in_db:
                    print(f"   - {col}")
            
            if not missing_in_db and not extra_in_db:
                print("✅ Todas las columnas coinciden perfectamente!")
                return True
            else:
                return False
                
    except Exception as e:
        print(f"❌ Error verificando estructura: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    print("=" * 60)
    print("NUKE DATABASE SCRIPT")
    print("=" * 60)
    
    # Preguntar confirmación por seguridad
    confirm = input("⚠️  ¿Estás seguro de que quieres ELIMINAR COMPLETAMENTE la base de datos? (yes/NO): ")
    
    if confirm.lower() == 'yes':
        success = nuke_database()
        
        if success:
            print("\n" + "=" * 60)
            print("VERIFICACIÓN FINAL")
            print("=" * 60)
            verify_structure()
    else:
        print("Operación cancelada.")
    
    print("\nScript finalizado.")