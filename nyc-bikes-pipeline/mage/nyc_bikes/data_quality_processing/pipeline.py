import boto3
import json
import pandas as pd
from datetime import datetime, timedelta
import os

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_loader
def check_new_realtime_data(*args, **kwargs):
    """
    Cargar nuevos datos desde Bronze layer
    """
    bucket_name = os.getenv('BRONZE_BUCKET', 'city-data-25')
    prefix = 'bronze/trips/'
    
    execution_date = kwargs.get('execution_date', datetime.utcnow())
    start_time = execution_date - timedelta(minutes=30)
    end_time = execution_date - timedelta(minutes=15)
    
    print(f"Buscando datos desde {start_time} hasta {end_time}")
    
    s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
        endpoint_url=os.getenv('AWS_ENDPOINT_URL')
    )
    
    new_files = []
    current_date = start_time.date()
    
    while current_date <= end_time.date():
        date_str = current_date.strftime('%Y-%m-%d')
        hour_start = start_time.hour if current_date == start_time.date() else 0
        hour_end = end_time.hour if current_date == end_time.date() else 23
        
        for hour in range(hour_start, hour_end + 1):
            hour_str = f"{hour:02d}"
            folder_prefix = f"{prefix}date={date_str}/hour={hour_str}/"
            
            try:
                response = s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=folder_prefix
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        file_time = obj['LastModified'].replace(tzinfo=None)
                        if start_time <= file_time <= end_time:
                            new_files.append(obj['Key'])
                            
            except Exception as e:
                print(f"Error listando {folder_prefix}: {e}")
        
        current_date += timedelta(days=1)
    
    print(f"Encontrados {len(new_files)} archivos nuevos")
    
    # Cargar datos
    all_data = []
    for file_key in new_files:
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=file_key)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            all_data.append(data)
        except Exception as e:
            print(f"Error cargando {file_key}: {e}")
    
    df = pd.DataFrame(all_data) if all_data else pd.DataFrame()
    
    return {
        'data': df,
        'file_count': len(new_files),
        'new_files': new_files
    }

@transformer
def comprehensive_quality_checks(output, *args, **kwargs):
    """
    Aplicar validaciones completas de calidad
    """
    if output['data'].empty:
        print("No hay datos para procesar")
        return output
    
    df = output['data'].copy()
    
    def calculate_quality_score(row):
        base_score = 100
        deductions = 0
        issues = []
        
        # SCHEMA & COMPLETENESS (40 puntos)
        required_fields = ['trip_id', 'bike_id', 'start_time', 'end_time', 
                          'start_station_id', 'end_station_id', 'trip_duration', 'bike_type']
        
        for field in required_fields:
            if field not in row or pd.isna(row[field]) or row[field] in ['', None]:
                deductions += 10
                issues.append(f'missing_{field}')
        
        # DATA VALIDITY (40 puntos)
        try:
            start_time = pd.to_datetime(row['start_time'])
            end_time = pd.to_datetime(row['end_time'])
            
            if start_time > end_time:
                deductions += 25
                issues.append('invalid_time_sequence')
            
            # Verificar duración
            calculated_duration = (end_time - start_time).total_seconds()
            provided_duration = float(row['trip_duration'])
            
            if abs(calculated_duration - provided_duration) > 60:
                deductions += 15
                issues.append('duration_mismatch')
                
        except:
            deductions += 25
            issues.append('time_parsing_error')
        
        # Validar rider_age
        try:
            rider_age = float(row.get('rider_age', 0))
            if rider_age < 16 or rider_age > 100:
                deductions += 20
                issues.append('invalid_age')
        except:
            deductions += 10
            issues.append('invalid_age_format')
        
        # Validar station_ids
        try:
            start_station = int(float(row['start_station_id']))
            if start_station <= 0:
                deductions += 10
                issues.append('invalid_start_station')
        except:
            deductions += 10
            issues.append('invalid_start_station')
            
        try:
            end_station = int(float(row['end_station_id']))
            if end_station <= 0:
                deductions += 10
                issues.append('invalid_end_station')
        except:
            deductions += 10
            issues.append('invalid_end_station')
        
        # BUSINESS LOGIC (20 puntos)
        try:
            trip_duration = float(row['trip_duration'])
            if trip_duration < 60 or trip_duration > 86400:
                deductions += 15
                issues.append('invalid_duration')
            
            # Calcular velocidad
            distance_km = 2.0
            duration_hours = trip_duration / 3600
            if duration_hours > 0:
                speed_kmh = distance_km / duration_hours
                if speed_kmh > 30:
                    deductions += 10
                    issues.append('impossible_speed')
            
            # Misma estación con duración > 1 hora
            if (str(row.get('start_station_id')) == str(row.get('end_station_id')) and 
                trip_duration > 3600):
                deductions += 5
                issues.append('same_station_long_duration')
                
        except:
            deductions += 15
            issues.append('duration_calculation_error')
        
        # Calcular score final
        final_score = max(0, (base_score - deductions))
        
        return pd.Series({
            'quality_score': final_score,
            'quality_issues': issues,
            'is_valid_quality': final_score >= 60
        })
    
    # Aplicar scoring
    quality_data = df.apply(calculate_quality_score, axis=1)
    
    # Combinar con datos originales
    for col in quality_data.columns:
        df[col] = quality_data[col]
    
    output['data'] = df
    return output

@transformer
def calculate_quality_scores(output, *args, **kwargs):
    """
    Calcular métricas agregadas de calidad
    """
    if output['data'].empty:
        return {
            'quality_bands': {'EXCELLENT': 0, 'GOOD': 0, 'FAIR': 0, 'POOR': 0},
            'total_records': 0,
            'avg_score': 0
        }
    
    df = output['data']
    
    # Calcular bands de calidad
    excellent = len(df[df['quality_score'] >= 90])
    good = len(df[(df['quality_score'] >= 75) & (df['quality_score'] < 90)])
    fair = len(df[(df['quality_score'] >= 60) & (df['quality_score'] < 75)])
    poor = len(df[df['quality_score'] < 60])
    
    quality_bands = {
        'EXCELLENT': excellent,
        'GOOD': good,
        'FAIR': fair,
        'POOR': poor
    }
    
    stats = {
        'quality_bands': quality_bands,
        'total_records': len(df),
        'valid_records': len(df[df['is_valid_quality']]),
        'invalid_records': len(df[~df['is_valid_quality']]),
        'avg_score': df['quality_score'].mean(),
        'timestamp': datetime.utcnow().isoformat()
    }
    
    print(f"Resumen de calidad: {stats}")
    
    # Guardar en base de datos o archivo para reporting
    output['quality_stats'] = stats
    return output

@data_exporter
def move_to_silver(output, *args, **kwargs):
    """
    Mover datos validados a Silver layer
    """
    if output['data'].empty:
        print("No hay datos para exportar a Silver")
        return
    
    df = output['data']
    bucket_name = os.getenv('BRONZE_BUCKET', 'city-data-25')
    
    # Filtrar solo datos válidos
    silver_data = df[df['is_valid_quality']].copy()
    
    if not silver_data.empty:
        s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL')
        )
        
        # Guardar en Silver layer
        for _, row in silver_data.iterrows():
            try:
                silver_key = f"silver/trips/date={datetime.utcnow().strftime('%Y-%m-%d')}/{row['trip_id']}.json"
                
                s3.put_object(
                    Bucket=bucket_name,
                    Key=silver_key,
                    Body=json.dumps(row.to_dict(), default=str),
                    ContentType='application/json'
                )
                
            except Exception as e:
                print(f"Error guardando {row['trip_id']} en Silver: {e}")
    
    print(f"Procesados {len(silver_data)} registros válidos a Silver layer")
    
    # Métricas de calidad
    quality_stats = {
        'total_records': len(df),
        'valid_records': len(silver_data),
        'invalid_records': len(df) - len(silver_data),
        'avg_quality_score': df['quality_score'].mean()
    }
    
    print(f"Estadísticas de calidad: {quality_stats}")
    return quality_stats