# DataEngineering10
pip install fastapi uvicorn boto3 --break-system-packages
pip install psycopg2-binary --break-system-packages
pip install streamlit --break-system-packages

# NYC Bikes Real-Time Analytics Pipeline

## 🚀 Quick Start
```bash
# Start system
cd ~/Documents/GitHub/DataEngineering10/nyc-bikes-pipeline
docker-compose up -d
sleep 60
docker-compose ps
```

## ✅ Verify System
```bash
# Check API
curl http://localhost:8082/health

# Send test trip
python3 test-client.py single

# Check worker logs
docker-compose logs worker --tail=20

# Check S3
aws s3 ls s3://city-data-25/bronze/trips/ --recursive

# Check database
docker-compose exec db psql -U bikes_user -d nyc_bikes -c "SELECT trip_id, quality_score, is_valid FROM trip_metadata ORDER BY processed_at DESC LIMIT 5;"
```

## 🧪 Load Test
```bash
# Send 100 trips
python3 test-client.py load 100 10

# Verify data loss = 0%
docker-compose exec db psql -U bikes_user -d nyc_bikes -c "SELECT COUNT(*) FROM trip_metadata WHERE DATE(processed_at) = CURRENT_DATE;"
```

## 📊 Access Interfaces
```bash
# Dashboard
open http://localhost:8501

# Mage
open http://localhost:6789

# API Docs
open http://localhost:8082/docs
```

## 🔍 Monitoring
```bash
# View all logs
docker-compose logs -f

# View API logs
docker-compose logs -f api

# View worker logs
docker-compose logs -f worker

# Check status
docker-compose ps

# Restart service
docker-compose restart api
```

## 🛑 Stop System
```bash
# Stop (keep data)
docker-compose down

# Stop and remove all data
docker-compose down -v
```

## 📋 System Architecture
```
Client → API (8082) → SQS → Worker → S3 Bronze + PostgreSQL
                                   ↓
                            Quality Score (0-100)
                                   ↓
                            Mage Pipelines → Silver/Gold
```

## 🎯 Key Metrics

- **Data Loss**: 0%
- **API Response**: ~150ms
- **Quality Scoring**: Implemented per project spec
- **Storage**: S3 `city-data-25` + PostgreSQL

## 🔧 Configuration Files

- `.env` - AWS credentials (DO NOT commit)
- `docker-compose.yml` - Service orchestration
- `app.py` - FastAPI ingestion endpoint
- `worker.py` - SQS consumer with quality scoring
- `init.sql` - PostgreSQL schema

## ⚠️ Important Notes

- Ensure `.env` is in `.gitignore`
- AWS credentials required in `.env`
- SQS queue: `nyc-bike-trips`
- S3 bucket: `city-data-25`