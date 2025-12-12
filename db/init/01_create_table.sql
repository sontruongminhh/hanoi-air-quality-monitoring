
CREATE TABLE IF NOT EXISTS aqi_readings (
    id BIGSERIAL PRIMARY KEY,
    location_id BIGINT NOT NULL,
    location_name TEXT,
    locality TEXT,
    country TEXT,
    country_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    provider TEXT DEFAULT 'OpenAQ',

    parameter TEXT NOT NULL,                
    unit TEXT,                              
    value DOUBLE PRECISION,                
    measurement_time TIMESTAMPTZ NOT NULL,  

    alerted BOOLEAN DEFAULT FALSE,
    alert_sent_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    
    CONSTRAINT uq_reading UNIQUE (location_id, parameter, created_at)
);

CREATE INDEX IF NOT EXISTS idx_aqi_readings_loc_param_time
    ON aqi_readings (location_id, parameter, measurement_time DESC);

CREATE INDEX IF NOT EXISTS idx_aqi_readings_alerted
    ON aqi_readings (alerted) WHERE alerted = TRUE;

