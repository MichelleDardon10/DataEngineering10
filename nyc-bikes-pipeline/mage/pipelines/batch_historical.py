from mage_ai.data_preparation.decorators import data_loader, transformer, exporter
import pandas as pd
import boto3
import json
from datetime import datetime, timedelta

s3 = boto3.client('s3', endpoint_url='http://localstack:4566')

@data_loader
def load_historical_data(**kwargs):
    """
    Cargar datos históricos que YA fueron procesados por el worker
    """
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    
    historical_data = []
    
    # Cargar de Bronze (datos ya procesados con calidad)
    for hour in range(24):
        prefix = f"bronze/trips/date={date_str}/hour={hour:02d}/"
        response = s3.list_objects_v2(Bucket='city-data-25', Prefix=prefix)
        
        for obj in response.get('Contents', []):
            file_obj = s3.get_object(Bucket='city-data-25', Key=obj['Key'])
            trip = json.loads(file_obj['Body'].read().decode('utf-8'))
            
            # Solo usar datos que YA tienen quality_score (procesados por worker)
            if 'quality_score' in trip:
                historical_data.append(trip)
    
    print(f"Loaded {len(historical_data)} historical trips with quality scores")
    return pd.DataFrame(historical_data) if historical_data else pd.DataFrame()

@exporter
def save_historical_aggregates(df: pd.DataFrame, **kwargs):
    """
    Crear agregados históricos usando datos YA procesados
    """
    if df.empty:
        return
    
    yesterday = datetime.utcnow() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')
    
    valid_trips = df[df['is_valid'] == True]
    
    daily_stats = {
        'date': date_str,
        'total_trips': len(df),
        'valid_trips': len(valid_trips),
        'avg_duration': valid_trips['trip_duration'].mean() if not valid_trips.empty else 0,
        'avg_quality': df['quality_score'].mean(),
        'peak_hour': pd.to_datetime(df['start_time']).dt.hour.mode().iloc[0] if not df.empty else None,
        'most_used_station': df['start_station_id'].mode().iloc[0] if not df.empty else None
    }
    
    s3.put_object(
        Bucket='city-data-25',
        Key=f"gold/historical/{date_str}/daily_stats.json",
        Body=json.dumps(daily_stats, default=str)
    )
    
    print(f"Created historical aggregates for {date_str}: {len(valid_trips)} valid trips")