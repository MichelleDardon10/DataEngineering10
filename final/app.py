from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, conint, constr
from datetime import datetime, timezone
import boto3, json, os

app = FastAPI(title="NYC Bikes Ingestion")

#validacion de JSON
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

s3 = boto3.client("s3")
BRONZE_BUCKET = "city-data-25"
BRONZE_PREFIX = "zips/"

def persist_to_bronze(evt: dict):
    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    key = f"{BRONZE_PREFIX}trips/date={date}/hour={hour}/{evt['trip_id']}.jsonl"

@app.post("/api/v1/trips")
async def ingest_trip(event: TripEvent, request: Request):
    # Respuesta rápida: validación ya ocurrió; persistimos de inmediato.
    evt = event.model_dump()
    try:
        persist_to_bronze(evt)  # I/O corto; <100ms en VPC con S3. Si no, use SQS (próximo paso).
    except Exception:
        # Manejo básico de conectividad: 400 para que el emisor reintente.
        raise HTTPException(status_code=400, detail="Persistence error")

    return {
        "status": "received",
        "trip_id": evt["trip_id"],
        "message": "Event accepted for processing"
    }