import boto3
import psycopg2
import os
from datetime import datetime

@data_loader
def check_system_health(*args, **kwargs):
    """
    Verificar salud de todos los componentes del sistema
    """
    print("Verificando salud del sistema...")
    
    health_checks = {}
    
    # 1. Verificar S3
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL')
        )
        bucket_name = os.getenv('BRONZE_BUCKET', 'city-data-25')
        s3.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
        health_checks['s3'] = {'status': 'healthy', 'bucket': bucket_name}
    except Exception as e:
        health_checks['s3'] = {'status': 'unhealthy', 'error': str(e)}
    
    # 2. Verificar PostgreSQL
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT', '5432')
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM trip_metadata")
            count = cur.fetchone()[0]
        conn.close()
        health_checks['postgresql'] = {'status': 'healthy', 'trip_count': count}
    except Exception as e:
        health_checks['postgresql'] = {'status': 'unhealthy', 'error': str(e)}
    
    # 3. Verificar SQS
    try:
        sqs = boto3.client(
            'sqs',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            endpoint_url=os.getenv('AWS_ENDPOINT_URL')
        )
        queue_url = os.getenv('SQS_QUEUE_URL')
        if queue_url:
            response = sqs.get_queue_attributes(
                QueueUrl=queue_url, 
                AttributeNames=['ApproximateNumberOfMessages']
            )
            message_count = int(response['Attributes']['ApproximateNumberOfMessages'])
            health_checks['sqs'] = {'status': 'healthy', 'message_count': message_count}
        else:
            health_checks['sqs'] = {'status': 'unknown', 'error': 'No SQS_QUEUE_URL'}
    except Exception as e:
        health_checks['sqs'] = {'status': 'unhealthy', 'error': str(e)}
    
    print(f"Chequeo de salud completado")
    return health_checks