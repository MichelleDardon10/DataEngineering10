-- Create schema for bike trip data
CREATE SCHEMA IF NOT EXISTS nyc_bikes;

-- Trip metadata and quality tracking table
CREATE TABLE IF NOT EXISTS trip_metadata (
    trip_id VARCHAR(255) PRIMARY KEY,
    bike_id INTEGER NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE NOT NULL,
    start_station_id INTEGER NOT NULL,
    end_station_id INTEGER NOT NULL,
    rider_age INTEGER NOT NULL,
    trip_duration INTEGER NOT NULL,
    bike_type VARCHAR(50) NOT NULL,
    member_casual VARCHAR(10) DEFAULT 'casual',
    quality_score DECIMAL(5,2) NOT NULL,
    quality_issues JSONB,
    is_valid BOOLEAN NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Indexes for common queries
CREATE INDEX idx_trip_start_time ON trip_metadata(start_time);
CREATE INDEX idx_trip_quality_score ON trip_metadata(quality_score);
CREATE INDEX idx_trip_is_valid ON trip_metadata(is_valid);
CREATE INDEX idx_trip_bike_type ON trip_metadata(bike_type);
CREATE INDEX idx_trip_processed_at ON trip_metadata(processed_at);
CREATE INDEX IF NOT EXISTS idx_trip_metadata_ingested_at ON trip_metadata(ingested_at);
CREATE INDEX IF NOT EXISTS idx_trip_metadata_quality_band ON trip_metadata(quality_band);
CREATE INDEX IF NOT EXISTS idx_trip_metadata_processed_at ON trip_metadata(processed_at);

-- Quality summary view
CREATE OR REPLACE VIEW quality_summary AS
SELECT 
    DATE(processed_at) as date,
    COUNT(*) as total_trips,
    COUNT(*) FILTER (WHERE is_valid) as valid_trips,
    COUNT(*) FILTER (WHERE NOT is_valid) as invalid_trips,
    ROUND(AVG(quality_score), 2) as avg_quality_score,
    ROUND(100.0 * COUNT(*) FILTER (WHERE is_valid) / COUNT(*), 2) as valid_percentage
FROM trip_metadata
GROUP BY DATE(processed_at)
ORDER BY date DESC;

-- Quality issues frequency
CREATE OR REPLACE VIEW quality_issues_frequency AS
SELECT 
    issue,
    COUNT(*) as frequency,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM trip_metadata), 2) as percentage
FROM trip_metadata,
     jsonb_array_elements_text(quality_issues) as issue
GROUP BY issue
ORDER BY frequency DESC;

-- Hourly processing stats
CREATE OR REPLACE VIEW hourly_stats AS
SELECT 
    DATE_TRUNC('hour', processed_at) as hour,
    COUNT(*) as trips_processed,
    ROUND(AVG(quality_score), 2) as avg_quality,
    COUNT(*) FILTER (WHERE is_valid) as valid_count,
    COUNT(*) FILTER (WHERE NOT is_valid) as invalid_count
FROM trip_metadata
GROUP BY DATE_TRUNC('hour', processed_at)
ORDER BY hour DESC;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO bikes_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO bikes_user;