from mage_ai.data_preparation.decorators import data_loader, transformer, exporter
import pandas as pd
import psycopg2
import json
from datetime import datetime

@data_loader
def load_system_metrics(**kwargs):
    conn = psycopg2.connect(
        host='db',
        database='nyc_bikes',
        user='bikes_user',
        password='bikes_pass'
    )
    
    queries = {
        'hourly_metrics': """
            SELECT COUNT(*) as trip_count, 
                   AVG(quality_score) as avg_quality,
                   COUNT(CASE WHEN is_valid = false THEN 1 END) as invalid_count
            FROM trip_metadata 
            WHERE ingested_at >= NOW() - INTERVAL '1 hour'
        """,
        'quality_dist': """
            SELECT quality_band, COUNT(*) as count
            FROM trip_metadata 
            WHERE processed_at >= NOW() - INTERVAL '1 hour'
            GROUP BY quality_band
        """
    }
    
    metrics = {}
    for name, query in queries.items():
        metrics[name] = pd.read_sql(query, conn)
    
    conn.close()
    return metrics

@transformer
def check_alerts(metrics: dict, **kwargs):
    hourly = metrics['hourly_metrics']
    quality_dist = metrics['quality_dist']
    
    alerts = []
    
    total_trips = hourly['trip_count'].iloc[0] if not hourly.empty else 0
    poor_trips = quality_dist[quality_dist['quality_band'] == 'POOR']['count'].iloc[0] if not quality_dist.empty else 0
    
    if total_trips > 0:
        poor_pct = (poor_trips / total_trips) * 100
        
        if poor_pct > 20:
            alerts.append({
                'level': 'HIGH',
                'message': f'High poor quality rate: {poor_pct:.1f}%',
                'timestamp': datetime.utcnow().isoformat()
            })
    
    if total_trips == 0:
        alerts.append({
            'level': 'CRITICAL',
            'message': 'No trips processed in last hour',
            'timestamp': datetime.utcnow().isoformat()
        })
    
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'total_trips': total_trips,
        'avg_quality': hourly['avg_quality'].iloc[0] if not hourly.empty else 0,
        'alerts': alerts
    }

@exporter
def save_monitoring_data(monitoring_data: dict, **kwargs):
    import boto3
    s3 = boto3.client('s3', endpoint_url='http://localstack:4566')
    
    now = datetime.utcnow()
    key = f"gold/monitoring/{now.strftime('%Y-%m-%d/%H')}/metrics.json"
    
    s3.put_object(
        Bucket='city-data-25',
        Key=key,
        Body=json.dumps(monitoring_data, default=str)
    )
    
    for alert in monitoring_data['alerts']:
        print(f"ALERT [{alert['level']}]: {alert['message']}")