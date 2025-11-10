from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, conint, constr
from datetime import datetime, timezone
import boto3
import json
import os
from contextlib import asynccontextmanager

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize connections
    app.state.sqs = boto3.client(
        "sqs",
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    )
    app.state.queue_url = os.getenv("SQS_QUEUE_URL")
    yield
    # Shutdown: cleanup if needed

app = FastAPI(
    title="NYC Bikes Ingestion API",
    version="1.0.0",
    lifespan=lifespan
)

# Validation schema
class TripEvent(BaseModel):
    trip_id: constr(strip_whitespace=True, min_length=1)
    bike_id: conint(ge=1)
    start_time: datetime
    end_time: datetime
    start_station_id: conint(ge=1)
    end_station_id: conint(ge=1)
    rider_age: conint(ge=0, le=120)
    trip_duration: conint(ge=0)
    bike_type: constr(strip_whitespace=True)

    class Config:
        json_schema_extra = {
            "example": {
                "trip_id": "trip_12345",
                "bike_id": 101,
                "start_time": "2025-11-10T10:00:00Z",
                "end_time": "2025-11-10T10:30:00Z",
                "start_station_id": 1,
                "end_station_id": 2,
                "rider_age": 25,
                "trip_duration": 1800,
                "bike_type": "electric"
            }
        }

@app.post("/api/v1/trips", status_code=202)
async def ingest_trip(event: TripEvent, request: Request):
    """
    Ingest a bike trip event.
    Returns 202 Accepted immediately after queuing.
    """
    evt = event.model_dump()
    
    # Add metadata
    evt["ingested_at"] = datetime.now(timezone.utc).isoformat()
    evt["source_ip"] = request.client.host
    
    try:
        # Send to SQS - this is fast and reliable
        response = app.state.sqs.send_message(
            QueueUrl=app.state.queue_url,
            MessageBody=json.dumps(evt, default=str),
            MessageAttributes={
                "trip_id": {
                    "StringValue": evt["trip_id"],
                    "DataType": "String"
                },
                "bike_type": {
                    "StringValue": evt["bike_type"],
                    "DataType": "String"
                }
            }
        )
        
        return {
            "status": "accepted",
            "trip_id": evt["trip_id"],
            "message_id": response["MessageId"],
            "message": "Event queued for processing"
        }
    
    except Exception as e:
        # If SQS fails, return 503 so client knows to retry
        raise HTTPException(
            status_code=503,
            detail=f"Unable to queue message: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test SQS connection
        app.state.sqs.get_queue_attributes(
            QueueUrl=app.state.queue_url,
            AttributeNames=["ApproximateNumberOfMessages"]
        )
        return {"status": "healthy", "service": "ingestion-api"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {str(e)}")

@app.get("/")
async def root():
    return {
        "service": "NYC Bikes Ingestion API",
        "version": "1.0.0",
        "endpoints": {
            "ingest": "POST /api/v1/trips",
            "health": "GET /health"
        }
    }