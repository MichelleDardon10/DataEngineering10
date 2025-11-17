# test_complete_system.py
import requests
import time
import json
import boto3
import psycopg2
import pandas as pd

def test_api():
    """Probar que la API responde"""
    print("1. Testing API...")
    try:
        response = requests.get("http://localhost:8082/health", timeout=5)
        if response.status_code == 200:
            print("   API saludable")
            return True
        else:
            print("   API no responde")
            return False
    except:
        print("   API no disponible")
        return False

def test_send_trips():
    """Enviar viajes de prueba"""
    print("2. Enviando viajes de prueba...")
    
    trips = [
        {
            "trip_id": "test_perfect_001",
            "bike_id": 1001,
            "start_time": "2024-01-15T10:00:00Z",
            "end_time": "2024-01-15T10:30:00Z", 
            "start_station_id": 1,
            "end_station_id": 2,
            "rider_age": 25,
            "trip_duration": 1800,
            "bike_type": "electric",
            "member_casual": "member"  # NUEVO CAMPO
        },
        {
            "trip_id": "test_bad_age_002", 
            "bike_id": 1002,
            "start_time": "2024-01-15T11:00:00Z",
            "end_time": "2024-01-15T11:30:00Z",
            "start_station_id": 3,
            "end_station_id": 4,
            "rider_age": 150,  # Edad inválida
            "trip_duration": 1800,
            "bike_type": "classic",
            "member_casual": "casual"  # NUEVO CAMPO
        }
    ]
    
    
    success_count = 0
    for trip in trips:
        try:
            response = requests.post(
                "http://localhost:8082/api/v1/trips",
                json=trip,
                timeout=5
            )
            if response.status_code == 202:
                success_count += 1
                print(f"   ✅ {trip['trip_id']} enviado")
            else:
                print(f"   ❌ {trip['trip_id']} falló: {response.text}")
        except Exception as e:
            print(f"   ❌ Error enviando {trip['trip_id']}: {e}")
    
    print(f"   {success_count}/{len(trips)} viajes enviados exitosamente")
    return success_count > 0

def test_worker_processing():
    """Verificar que el worker procesó los datos"""
    print("3. Verificando procesamiento del worker...")
    time.sleep(5)  # Dar tiempo al worker
    
    try:
        # Verificar S3
        s3 = boto3.client('s3', endpoint_url='http://localhost:4566')
        response = s3.list_objects_v2(Bucket='city-data-25', Prefix='bronze/trips/')
        
        if 'Contents' in response:
            print(f"   ✅ {len(response['Contents'])} archivos en Bronze")
            
            # Verificar que tienen quality_score
            for obj in response['Contents'][:2]:  # Primeros 2 archivos
                file_obj = s3.get_object(Bucket='city-data-25', Key=obj['Key'])
                trip_data = json.loads(file_obj['Body'].read().decode('utf-8'))
                if 'quality_score' in trip_data:
                    print(f"   ✅ {trip_data['trip_id']} tiene score: {trip_data['quality_score']}")
                else:
                    print(f"   ⚠️  {trip_data['trip_id']} sin score (será procesado por Mage)")
        else:
            print("   ❌ No hay archivos en Bronze")
            
    except Exception as e:
        print(f"   ❌ Error verificando S3: {e}")

def test_database():
    """Verificar datos en base de datos"""
    print("4. Verificando base de datos...")
    
    try:
        conn = psycopg2.connect(
            host='localhost',
            database='nyc_bikes',
            user='bikes_user',
            password='bikes_pass'
        )
        
        count = pd.read_sql("SELECT COUNT(*) as total FROM trip_metadata", conn)
        print(f"   ✅ {count['total'].iloc[0]} viajes en base de datos")
        
        quality_stats = pd.read_sql("""
            SELECT 
                AVG(quality_score) as avg_score,
                COUNT(CASE WHEN is_valid = true THEN 1 END) as valid_count,
                COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_count
            FROM trip_metadata
        """, conn)
        
        print(f"   📊 Calidad promedio: {quality_stats['avg_score'].iloc[0]:.1f}")
        print(f"   📊 Válidos: {quality_stats['valid_count'].iloc[0]}, Inválidos: {quality_stats['invalid_count'].iloc[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Error con base de datos: {e}")
        return False

def test_mage_pipelines():
    """Verificar que Mage puede ejecutar pipelines"""
    print("5. Verificando Mage...")
    
    try:
        # Verificar que Mage está arriba
        response = requests.get("http://localhost:6789", timeout=10)
        if response.status_code == 200:
            print("   ✅ Mage UI disponible")
            
            # Aquí podrías usar la API de Mage para ejecutar pipelines
            # Por ahora solo verificamos que esté arriba
            return True
        else:
            print("   ❌ Mage no disponible")
            return False
            
    except Exception as e:
        print(f"   ❌ Error con Mage: {e}")
        return False

def test_dashboard():
    """Verificar dashboard"""
    print("6. Verificando dashboard...")
    
    try:
        response = requests.get("http://localhost:8501", timeout=10)
        if response.status_code == 200:
            print("   ✅ Dashboard disponible")
            return True
        else:
            print("   ❌ Dashboard no disponible")
            return False
    except:
        print("   ❌ Dashboard no responde")
        return False

def main():
    print("🧪 INICIANDO PRUEBAS DEL SISTEMA NYC BIKES 🧪\n")
    
    tests = [
        test_api,
        test_send_trips, 
        test_worker_processing,
        test_database,
        test_mage_pipelines,
        test_dashboard
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
            print()
        except Exception as e:
            print(f"    Test falló con error: {e}\n")
            results.append(False)
    
    # Resumen
    print(" RESUMEN DE PRUEBAS:")
    print(f"   Tests pasados: {sum(results)}/{len(results)}")
    
    if all(results):
        print(" ¡Todos los tests pasaron! El sistema está funcionando correctamente.")
    else:
        print("  Algunos tests fallaron. Revisa los logs arriba.")

if __name__ == "__main__":
    main()