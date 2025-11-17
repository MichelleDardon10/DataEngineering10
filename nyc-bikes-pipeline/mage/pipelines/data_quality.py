from mage_ai.data_preparation.decorators import data_loader, transformer, exporter
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta

s3 = boto3.client('s3', endpoint_url='http://localstack:4566')

@data_loader
def load_bronze_data(**kwargs):
    """
    Solo cargar datos que NO tengan quality_score (datos nuevos sin procesar)
    """
    bucket = 'city-data-25'
    time_threshold = datetime.utcnow() - timedelta(minutes=15)
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix='bronze/trips/')
    trips_data = []
    
    for obj in response.get('Contents', []):
        if obj['LastModified'].replace(tzinfo=None) >= time_threshold:
            file_obj = s3.get_object(Bucket=bucket, Key=obj['Key'])
            trip = json.loads(file_obj['Body'].read().decode('utf-8'))
            
            # Solo procesar si NO tiene quality_score (no procesado por worker)
            if 'quality_score' not in trip:
                trips_data.append(trip)
    
    print(f"Found {len(trips_data)} unprocessed trips")
    return pd.DataFrame(trips_data) if trips_data else pd.DataFrame()

@transformer
def calculate_quality_scores(df: pd.DataFrame, **kwargs):
    """
    Calcular calidad EXACTA según fórmula del proyecto (para datos que el worker no procesó)
    """
    if df.empty:
        return df
    
    def convert_types(row):
        """Convertir tipos flexibles a los esperados"""
        try:
            # Bike ID
            try:
                row['bike_id'] = int(float(row['bike_id']))
            except:
                row['bike_id'] = 0
            
            # Station IDs  
            try:
                row['start_station_id'] = int(float(row['start_station_id']))
            except:
                row['start_station_id'] = 0
                
            try:
                row['end_station_id'] = int(float(row['end_station_id']))
            except:
                row['end_station_id'] = 0
            
            # Rider age
            try:
                if pd.notna(row.get('rider_age')) and row['rider_age'] not in ['', None]:
                    row['rider_age'] = int(float(row['rider_age']))
                else:
                    row['rider_age'] = 0
            except:
                row['rider_age'] = 0
            
            # Trip duration
            try:
                row['trip_duration'] = int(float(row['trip_duration']))
            except:
                row['trip_duration'] = 0
                
        except Exception as e:
            print(f"Type conversion error for trip {row.get('trip_id')}: {e}")
        
        return row
    
    # Aplicar conversión de tipos
    df = df.apply(convert_types, axis=1)
    
    def score_trip_exact(row):
        base_score = 100
        deductions = 0
        issues = []
        
        # SCHEMA & COMPLETENESS (40 puntos total)
        required_fields = ['trip_id', 'bike_id', 'start_time', 'end_time', 
                          'start_station_id', 'end_station_id', 'rider_age', 
                          'trip_duration', 'bike_type']
        
        for field in required_fields:
            if field not in row or pd.isna(row[field]):
                deductions += 10
                issues.append(f'missing_{field}')
        
        # DATA VALIDITY (40 puntos total)
        try:
            start = pd.to_datetime(row['start_time'])
            end = pd.to_datetime(row['end_time'])
            
            # start_time > end_time: -25 points
            if start >= end:
                deductions += 25
                issues.append('invalid_time_sequence')
            
            # trip_duration mismatch: -15 points
            actual_duration = (end - start).total_seconds()
            if abs(actual_duration - row['trip_duration']) > 60:
                deductions += 15
                issues.append('duration_mismatch')
                
        except:
            deductions += 25
            issues.append('time_parsing_error')
        
        # rider_age < 16 or > 100: -20 points
        if row['rider_age'] < 16 or row['rider_age'] > 100:
            deductions += 20
            issues.append('invalid_age')
        
        # BUSINESS LOGIC (20 points total)
        # Trip duration < 60 seconds or > 24 hours: -15 points
        if row['trip_duration'] < 60 or row['trip_duration'] > 86400:
            deductions += 15
            issues.append('invalid_duration')
        
        # Impossible speed (> 30 km/h): -10 points
        try:
            distance_km = 2.0  # approximate
            duration_hours = row['trip_duration'] / 3600
            speed_kmh = distance_km / duration_hours if duration_hours > 0 else 0
            
            if speed_kmh > 30:
                deductions += 10
                issues.append('impossible_speed')
        except:
            pass
        
        # Same start/end station with duration > 1 hour: -5 points
        if (row['start_station_id'] == row['end_station_id'] and 
            row['trip_duration'] > 3600):
            deductions += 5
            issues.append('same_station_long_duration')
        
        final_score = max(0, base_score - deductions)
        
        if final_score >= 90:
            band = 'EXCELLENT'
        elif final_score >= 75:
            band = 'GOOD'
        elif final_score >= 60:
            band = 'FAIR'
        else:
            band = 'POOR'
            
        return pd.Series({
            'quality_score': final_score,
            'quality_band': band,
            'quality_issues': issues,
            'is_valid': final_score >= 60
        })
    
    quality_data = df.apply(score_trip_exact, axis=1)
    result_df = pd.concat([df, quality_data], axis=1)
    
    print(f"Quality distribution: {result_df['quality_band'].value_counts().to_dict()}")
    return result_df

@exporter
def save_to_silver_gold(df: pd.DataFrame, **kwargs):
    """
    Mover datos validados a Silver y crear agregados Gold
    """
    if df.empty:
        return
    
    now = datetime.utcnow()
    date_str = now.strftime('%Y-%m-%d')
    hour_str = now.strftime('%H')
    
    valid_trips = df[df['is_valid'] == True]
    
    # Guardar en Silver layer
    for _, trip in valid_trips.iterrows():
        silver_key = f"silver/trips/date={date_str}/hour={hour_str}/{trip['trip_id']}.json"
        trip_data = trip.to_dict()
        trip_data['mage_processed_at'] = now.isoformat()
        
        s3.put_object(
            Bucket='city-data-25',
            Key=silver_key,
            Body=json.dumps(trip_data, default=str)
        )
    
    # Crear agregados Gold layer
    if not valid_trips.empty:
        # Agregados por estación
        station_stats = valid_trips.groupby('start_station_id').agg({
            'trip_id': 'count',
            'trip_duration': 'mean',
            'quality_score': 'mean'
        }).reset_index()
        station_stats.columns = ['station_id', 'departures', 'avg_duration', 'avg_quality']
        
        # Agregados por hora
        valid_trips['hour'] = pd.to_datetime(valid_trips['start_time']).dt.hour
        hourly_stats = valid_trips.groupby('hour').agg({
            'trip_id': 'count',
            'quality_score': 'mean'
        }).reset_index()
        hourly_stats.columns = ['hour', 'trips', 'avg_quality']
        
        gold_data = {
            'timestamp': now.isoformat(),
            'total_trips': len(valid_trips),
            'avg_quality_score': valid_trips['quality_score'].mean(),
            'station_stats': station_stats.to_dict('records'),
            'hourly_stats': hourly_stats.to_dict('records'),
            'quality_distribution': valid_trips['quality_band'].value_counts().to_dict()
        }
        
        s3.put_object(
            Bucket='city-data-25',
            Key=f"gold/aggregates/{date_str}/{hour_str}/stats.json",
            Body=json.dumps(gold_data, default=str)
        )
    
    print(f"Mage processed {len(df)} trips, {len(valid_trips)} valid -> Silver/Gold")