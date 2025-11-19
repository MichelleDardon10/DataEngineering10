import boto3
import json
import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timezone
import time
import logging
import threading
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
sqs = boto3.client(
    "sqs",
    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)

QUEUE_URL = os.getenv("SQS_QUEUE_URL")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "city-data-25")

# Batch configuration
BATCH_SIZE = 1
BATCH_TIMEOUT = 5
batch_buffer = defaultdict(list)  # {(year, month): [trips]}
batch_lock = threading.Lock()
last_flush_time = {}  # {(year, month): timestamp}

# Database connection pool
db_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=5,
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

def validate_trip_quality(trip: dict) -> dict:
    """
    Validate trip data and return quality score.
    Returns: {score: float, issues: list, is_valid: bool}
    """
    issues = []
    score = 100.0
    
    # CONVERTIR TIPOS FLEXIBLES A LOS ESPERADOS
    try:
        # Convertir bike_id a int
        try:
            trip["bike_id"] = int(float(trip["bike_id"]))
        except (ValueError, TypeError):
            issues.append("invalid_bike_id")
            score -= 10
        
        # Convertir station_ids a int
        try:
            trip["start_station_id"] = int(float(trip["start_station_id"]))
        except (ValueError, TypeError):
            issues.append("invalid_start_station")
            score -= 10
            
        try:
            trip["end_station_id"] = int(float(trip["end_station_id"]))
        except (ValueError, TypeError):
            issues.append("invalid_end_station") 
            score -= 10
        
        # Convertir rider_age a int (si existe)
        if "rider_age" in trip and trip["rider_age"] not in [None, ""]:
            try:
                trip["rider_age"] = int(float(trip["rider_age"]))
            except (ValueError, TypeError):
                issues.append("invalid_rider_age")
                score -= 10
        else:
            trip["rider_age"] = 0  # Default value
        
        # Convertir trip_duration a int
        try:
            trip["trip_duration"] = int(float(trip["trip_duration"]))
        except (ValueError, TypeError):
            issues.append("invalid_trip_duration")
            score -= 10
        
        # Validar member_casual (nuevo campo)
        if "member_casual" in trip and trip["member_casual"] not in [None, ""]:
            if trip["member_casual"].lower() not in ["member", "casual"]:
                issues.append("invalid_member_type")
                score -= 5
        else:
            trip["member_casual"] = "casual"  # Default value
    
    except Exception as e:
        issues.append("type_conversion_error")
        score -= 20
    
    # Check 1: Duration consistency (con tipos ya convertidos)
    try:
        start = datetime.fromisoformat(trip["start_time"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(trip["end_time"].replace("Z", "+00:00"))
        actual_duration = (end - start).total_seconds()
        
        if abs(actual_duration - trip["trip_duration"]) > 60:  # 1 minute tolerance
            issues.append("duration_mismatch")
            score -= 20
    except Exception as e:
        issues.append("time_parsing_error")
        score -= 25
    
    # Check 2: End time after start time
    try:
        if end <= start:
            issues.append("invalid_time_sequence")
            score -= 30
    except:
        pass  # Ya se restó puntos arriba
    
    # Check 3: Reasonable duration (< 24 hours)
    if trip["trip_duration"] > 86400:
        issues.append("excessive_duration")
        score -= 15
    
    # Check 4: Reasonable age (solo si age > 0)
    if trip["rider_age"] > 0:  # Solo validar si tiene edad
        if trip["rider_age"] < 16 or trip["rider_age"] > 100:
            issues.append("suspicious_age")
            score -= 10
    
    # Check 5: Same station trips (suspicious)
    if trip["start_station_id"] == trip["end_station_id"] and trip["trip_duration"] < 300:
        issues.append("same_station_short_trip")
        score -= 5
    
    # Check 6: Valid bike type
    valid_types = ["electric", "classic", "docked"]
    if trip["bike_type"].lower() not in valid_types:
        issues.append("invalid_bike_type")
        score -= 25
    
    return {
        "score": max(0, score),
        "issues": issues,
        "is_valid": score >= 50  # Threshold for "valid" data
    }

def persist_to_bronze(trip: dict, quality: dict):
    """Add trip to batch buffer for bronze layer persistence"""
    try:
        # Extraer año/mes del start_time del trip
        start_time = datetime.fromisoformat(trip["start_time"].replace("Z", "+00:00"))
        year = start_time.strftime("%Y")
        month = start_time.strftime("%m")
        
        # Add quality metadata
        trip["quality_score"] = quality["score"]
        trip["quality_issues"] = quality["issues"]
        trip["is_valid"] = quality["is_valid"]
        trip["processed_at"] = datetime.now(timezone.utc).isoformat()
        
        # Add to batch buffer
        partition_key = (year, month)
        
        with batch_lock:
            batch_buffer[partition_key].append(trip)
            
            # Initialize flush time if needed
            if partition_key not in last_flush_time:
                last_flush_time[partition_key] = time.time()
            
            # Check if we need to flush this partition
            should_flush = (
                len(batch_buffer[partition_key]) >= BATCH_SIZE or
                (time.time() - last_flush_time[partition_key]) >= BATCH_TIMEOUT
            )
            
            if should_flush:
                flush_batch(partition_key)
        
        return True
    
    except Exception as e:
        logger.error(f"Failed to add trip to batch: {e}")
        return False

def flush_batch(partition_key):
    """Flush a batch of trips to S3 (must be called with batch_lock held)"""
    try:
        year, month = partition_key
        trips = batch_buffer[partition_key]
        
        if not trips:
            return
        
        # Generate unique batch filename with timestamp
        timestamp = int(time.time() * 1000)  # milliseconds

        key = f"bronze/year={year}/month={month}/trip_{timestamp}.json"
        
        # Write batch to S3
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(trips, default=str),
            ContentType="application/json"
        )
        
        logger.info(f"Flushed batch of {len(trips)} trips to {key}")
        
        # Clear buffer and update flush time
        batch_buffer[partition_key] = []
        last_flush_time[partition_key] = time.time()
        
    except Exception as e:
        logger.error(f"Failed to flush batch: {e}")
        raise

def flush_all_batches():
    """Flush all pending batches (called on shutdown or periodically)"""
    with batch_lock:
        for partition_key in list(batch_buffer.keys()):
            if batch_buffer[partition_key]:
                flush_batch(partition_key)

def log_to_database(trip: dict, quality: dict):
    """Log trip metadata and quality to PostgreSQL"""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO trip_metadata (
                    trip_id, bike_id, start_time, end_time,
                    start_station_id, end_station_id, rider_age,
                    trip_duration, bike_type, member_casual, quality_score,
                    quality_issues, is_valid, ingested_at, processed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (trip_id) DO UPDATE SET
                    quality_score = EXCLUDED.quality_score,
                    quality_issues = EXCLUDED.quality_issues,
                    processed_at = EXCLUDED.processed_at
            """, (
                trip["trip_id"],
                trip["bike_id"],
                trip["start_time"],
                trip["end_time"],
                trip["start_station_id"],
                trip["end_station_id"],
                trip["rider_age"],
                trip["trip_duration"],
                trip["bike_type"],
                trip.get("member_casual", "casual"),
                quality["score"],
                json.dumps(quality["issues"]),
                quality["is_valid"],
                trip.get("ingested_at"),
                datetime.now(timezone.utc)
            ))
        conn.commit()
        logger.info(f"Logged trip {trip['trip_id']} to database")
    except Exception as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
    finally:
        db_pool.putconn(conn)

def process_message(message):
    """Process a single SQS message"""
    try:
        trip = json.loads(message["Body"])
        
        # Validate and score quality
        quality = validate_trip_quality(trip)
        
        # Persist to bronze layer (adds to batch)
        s3_success = persist_to_bronze(trip, quality)
        
        # Log metadata to database
        if s3_success:
            log_to_database(trip, quality)
        
        # Delete message from queue (only if successful)
        if s3_success:
            sqs.delete_message(
                QueueUrl=QUEUE_URL,
                ReceiptHandle=message["ReceiptHandle"]
            )
            logger.info(f"Successfully processed trip {trip['trip_id']}")
        else:
            # Message will become visible again for retry
            logger.warning(f"Failed to process trip {trip['trip_id']}, will retry")
    
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Don't delete message - it will be retried

def main():
    """Main worker loop"""
    logger.info("Worker started, polling for messages...")
    last_periodic_flush = time.time()
    
    try:
        while True:
            try:
                # Long polling (20 seconds)
                response = sqs.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=60  # 60 seconds to process
                )
                
                messages = response.get("Messages", [])
                
                if messages:
                    logger.info(f"Received {len(messages)} messages")
                    for message in messages:
                        process_message(message)
                else:
                    logger.debug("No messages, waiting...")
                
                # Periodic flush every 30 seconds
                if time.time() - last_periodic_flush >= 30:
                    flush_all_batches()
                    last_periodic_flush = time.time()
            
            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(5)  # Brief pause before retrying
    
    finally:
        # Flush any remaining batches on shutdown
        logger.info("Shutting down, flushing remaining batches...")
        flush_all_batches()

if __name__ == "__main__":
    main()