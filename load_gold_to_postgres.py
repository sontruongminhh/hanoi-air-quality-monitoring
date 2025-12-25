#!/usr/bin/env python3
"""
Script để load dữ liệu từ Gold layer (S3) vào PostgreSQL
Cho phép phân tích và kết nối với BI tools (Grafana, Superset, Metabase)
"""

import os
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from io import BytesIO
from datetime import datetime

# Configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'aqi-hanoi-bronze')
GOLD_PREFIX = 'gold/openaq/marts'

POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),  # Default to localhost for local execution
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'aqi'),
    'user': os.getenv('POSTGRES_USER', 'aqi_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'aqi_password'),
}


def get_s3_client():
    """Tạo S3 client"""
    return boto3.client('s3')


def get_postgres_connection():
    """Tạo PostgreSQL connection"""
    return psycopg2.connect(**POSTGRES_CONFIG)


def create_gold_tables(conn):
    """Tạo các bảng Gold trong PostgreSQL"""
    cursor = conn.cursor()
    
    # Fact table: AQI Hourly
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_fact_aqi_hourly (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            hour INTEGER NOT NULL,
            parameter VARCHAR(50),
            value_min FLOAT,
            value_max FLOAT,
            value_avg FLOAT,
            value_median FLOAT,
            value_count INTEGER,
            value_std FLOAT,
            location_id VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, hour, parameter, location_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_gold_hourly_date ON gold_fact_aqi_hourly(date);
        CREATE INDEX IF NOT EXISTS idx_gold_hourly_parameter ON gold_fact_aqi_hourly(parameter);
    """)
    
    # Fact table: AQI Daily
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_fact_aqi_daily (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            parameter VARCHAR(50),
            value_min FLOAT,
            value_max FLOAT,
            value_avg FLOAT,
            value_median FLOAT,
            value_count INTEGER,
            value_std FLOAT,
            location_id VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date, parameter, location_id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_gold_daily_date ON gold_fact_aqi_daily(date);
        CREATE INDEX IF NOT EXISTS idx_gold_daily_parameter ON gold_fact_aqi_daily(parameter);
    """)
    
    # Dimension table: Location
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_dim_location (
            id SERIAL PRIMARY KEY,
            location_id VARCHAR(50) UNIQUE NOT NULL,
            location_name VARCHAR(255),
            city VARCHAR(100),
            country VARCHAR(100),
            latitude FLOAT,
            longitude FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Dimension table: Parameter
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_dim_parameter (
            id SERIAL PRIMARY KEY,
            parameter VARCHAR(50) UNIQUE NOT NULL,
            parameter_name VARCHAR(100),
            unit VARCHAR(50),
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Dimension table: Time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_dim_time (
            id SERIAL PRIMARY KEY,
            date DATE UNIQUE NOT NULL,
            day INTEGER,
            month INTEGER,
            year INTEGER,
            day_of_week VARCHAR(20),
            is_weekend BOOLEAN,
            quarter INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Summary view for BI
    cursor.execute("""
        CREATE OR REPLACE VIEW gold_aqi_summary AS
        SELECT 
            d.date,
            d.parameter,
            d.value_avg as daily_avg,
            d.value_max as daily_max,
            d.value_min as daily_min,
            d.value_count as measurement_count,
            l.location_name,
            l.city,
            t.day_of_week,
            t.is_weekend,
            CASE 
                WHEN d.parameter = 'pm25' AND d.value_avg <= 12 THEN 'Good'
                WHEN d.parameter = 'pm25' AND d.value_avg <= 35.4 THEN 'Moderate'
                WHEN d.parameter = 'pm25' AND d.value_avg <= 55.4 THEN 'Unhealthy for Sensitive'
                WHEN d.parameter = 'pm25' AND d.value_avg <= 150.4 THEN 'Unhealthy'
                WHEN d.parameter = 'pm25' AND d.value_avg <= 250.4 THEN 'Very Unhealthy'
                WHEN d.parameter = 'pm25' AND d.value_avg > 250.4 THEN 'Hazardous'
                ELSE 'N/A'
            END as aqi_level
        FROM gold_fact_aqi_daily d
        LEFT JOIN gold_dim_location l ON d.location_id = l.location_id
        LEFT JOIN gold_dim_time t ON d.date = t.date
        ORDER BY d.date DESC;
    """)
    
    conn.commit()
    print("[OK] Created Gold tables in PostgreSQL")


def load_parquet_from_s3(s3_client, bucket, key):
    """Load Parquet file từ S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(BytesIO(response['Body'].read()))
        return df
    except Exception as e:
        print(f"  [WARN] Error loading {key}: {e}")
        return None


def load_fact_hourly(conn, s3_client):
    """Load fact_aqi_hourly từ S3 vào PostgreSQL"""
    print("\n[INFO] Loading fact_aqi_hourly...")
    
    prefix = f"{GOLD_PREFIX}/fact_aqi_hourly/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_loaded = 0
    cursor = conn.cursor()
    
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet'):
            continue
        
        df = load_parquet_from_s3(s3_client, S3_BUCKET, obj['Key'])
        if df is None or df.empty:
            continue
        
        # Extract location_id from path
        parts = obj['Key'].split('/')
        location_id = None
        for part in parts:
            if part.startswith('locationid='):
                location_id = part.split('=')[1]
                break
        
        df['location_id'] = location_id
        
        # Prepare data for insert
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO gold_fact_aqi_hourly 
                    (date, hour, parameter, value_min, value_max, value_avg, value_median, value_count, value_std, location_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, hour, parameter, location_id) DO UPDATE SET
                        value_min = EXCLUDED.value_min,
                        value_max = EXCLUDED.value_max,
                        value_avg = EXCLUDED.value_avg,
                        value_median = EXCLUDED.value_median,
                        value_count = EXCLUDED.value_count,
                        value_std = EXCLUDED.value_std
                """, (
                    row.get('date'),
                    row.get('hour'),
                    row.get('parameter'),
                    row.get('value_min'),
                    row.get('value_max'),
                    row.get('value_avg'),
                    row.get('value_median'),
                    row.get('value_count'),
                    row.get('value_std'),
                    location_id
                ))
                total_loaded += 1
            except Exception as e:
                print(f"  [WARN] Error inserting row: {e}")
    
    conn.commit()
    print(f"  [OK] Loaded {total_loaded} hourly records")
    return total_loaded


def load_fact_daily(conn, s3_client):
    """Load fact_aqi_daily từ S3 vào PostgreSQL"""
    print("\n[INFO] Loading fact_aqi_daily...")
    
    prefix = f"{GOLD_PREFIX}/fact_aqi_daily/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_loaded = 0
    cursor = conn.cursor()
    
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet'):
            continue
        
        df = load_parquet_from_s3(s3_client, S3_BUCKET, obj['Key'])
        if df is None or df.empty:
            continue
        
        # Extract location_id from path
        parts = obj['Key'].split('/')
        location_id = None
        for part in parts:
            if part.startswith('locationid='):
                location_id = part.split('=')[1]
                break
        
        df['location_id'] = location_id
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO gold_fact_aqi_daily 
                    (date, parameter, value_min, value_max, value_avg, value_median, value_count, value_std, location_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date, parameter, location_id) DO UPDATE SET
                        value_min = EXCLUDED.value_min,
                        value_max = EXCLUDED.value_max,
                        value_avg = EXCLUDED.value_avg,
                        value_median = EXCLUDED.value_median,
                        value_count = EXCLUDED.value_count,
                        value_std = EXCLUDED.value_std
                """, (
                    row.get('date'),
                    row.get('parameter'),
                    row.get('value_min'),
                    row.get('value_max'),
                    row.get('value_avg'),
                    row.get('value_median'),
                    row.get('value_count'),
                    row.get('value_std'),
                    location_id
                ))
                total_loaded += 1
            except Exception as e:
                print(f"  [WARN] Error inserting row: {e}")
    
    conn.commit()
    print(f"  [OK] Loaded {total_loaded} daily records")
    return total_loaded


def load_dim_location(conn, s3_client):
    """Load dim_location từ S3 vào PostgreSQL"""
    print("\n[INFO] Loading dim_location...")
    
    prefix = f"{GOLD_PREFIX}/dim_location/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_loaded = 0
    cursor = conn.cursor()
    
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet'):
            continue
        
        df = load_parquet_from_s3(s3_client, S3_BUCKET, obj['Key'])
        if df is None or df.empty:
            continue
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO gold_dim_location 
                    (location_id, location_name, city, country)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (location_id) DO UPDATE SET
                        location_name = EXCLUDED.location_name,
                        city = EXCLUDED.city,
                        country = EXCLUDED.country
                """, (
                    str(row.get('location_id')),
                    row.get('location_name'),
                    row.get('city'),
                    row.get('country')
                ))
                total_loaded += 1
            except Exception as e:
                print(f"  [WARN] Error inserting row: {e}")
    
    conn.commit()
    print(f"  [OK] Loaded {total_loaded} location records")
    return total_loaded


def load_dim_parameter(conn, s3_client):
    """Load dim_parameter từ S3 vào PostgreSQL"""
    print("\n[INFO] Loading dim_parameter...")
    
    prefix = f"{GOLD_PREFIX}/dim_parameter/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_loaded = 0
    cursor = conn.cursor()
    
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet'):
            continue
        
        df = load_parquet_from_s3(s3_client, S3_BUCKET, obj['Key'])
        if df is None or df.empty:
            continue
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO gold_dim_parameter 
                    (parameter, parameter_name, unit)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (parameter) DO UPDATE SET
                        parameter_name = EXCLUDED.parameter_name,
                        unit = EXCLUDED.unit
                """, (
                    row.get('parameter'),
                    row.get('parameter_name'),
                    row.get('unit')
                ))
                total_loaded += 1
            except Exception as e:
                print(f"  [WARN] Error inserting row: {e}")
    
    conn.commit()
    print(f"  [OK] Loaded {total_loaded} parameter records")
    return total_loaded


def load_dim_time(conn, s3_client):
    """Load dim_time từ S3 vào PostgreSQL"""
    print("\n[INFO] Loading dim_time...")
    
    prefix = f"{GOLD_PREFIX}/dim_time/"
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    total_loaded = 0
    cursor = conn.cursor()
    
    for obj in response.get('Contents', []):
        if not obj['Key'].endswith('.parquet'):
            continue
        
        df = load_parquet_from_s3(s3_client, S3_BUCKET, obj['Key'])
        if df is None or df.empty:
            continue
        
        for _, row in df.iterrows():
            try:
                # Parse date
                date_val = row.get('date')
                if isinstance(date_val, str):
                    date_val = date_val.zfill(2)  # Pad if needed
                
                year = row.get('year')
                month = row.get('month')
                day = row.get('date')
                
                # Create proper date
                if year and month and day:
                    try:
                        date_obj = datetime(int(year), int(month), int(day)).date()
                    except:
                        continue
                else:
                    continue
                
                cursor.execute("""
                    INSERT INTO gold_dim_time 
                    (date, day, month, year, day_of_week, is_weekend, quarter)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date) DO UPDATE SET
                        day_of_week = EXCLUDED.day_of_week,
                        is_weekend = EXCLUDED.is_weekend
                """, (
                    date_obj,
                    int(day),
                    int(month),
                    int(year),
                    row.get('day_of_week'),
                    row.get('is_weekend'),
                    (int(month) - 1) // 3 + 1
                ))
                total_loaded += 1
            except Exception as e:
                print(f"  [WARN] Error inserting row: {e}")
    
    conn.commit()
    print(f"  [OK] Loaded {total_loaded} time records")
    return total_loaded


def show_summary(conn):
    """Hiển thị tóm tắt dữ liệu đã load"""
    cursor = conn.cursor()
    
    print("\n" + "=" * 60)
    print("  GOLD DATA SUMMARY IN POSTGRESQL")
    print("=" * 60)
    
    tables = [
        ('gold_fact_aqi_hourly', 'Hourly Facts'),
        ('gold_fact_aqi_daily', 'Daily Facts'),
        ('gold_dim_location', 'Locations'),
        ('gold_dim_parameter', 'Parameters'),
        ('gold_dim_time', 'Time Dimension'),
    ]
    
    for table, name in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {name}: {count} records")
    
    # Sample data
    print("\n" + "-" * 60)
    print("  SAMPLE: Daily AQI Summary")
    print("-" * 60)
    
    cursor.execute("""
        SELECT date, parameter, value_avg, value_max, location_id 
        FROM gold_fact_aqi_daily 
        ORDER BY date DESC 
        LIMIT 5
    """)
    
    for row in cursor.fetchall():
        print(f"  {row[0]} | {row[1]}: avg={row[2]:.1f}, max={row[3]:.1f}")
    
    print("=" * 60)


def main():
    print("=" * 60)
    print("  LOAD GOLD DATA FROM S3 TO POSTGRESQL")
    print("=" * 60)
    print(f"  S3 Bucket: {S3_BUCKET}")
    print(f"  Gold Prefix: {GOLD_PREFIX}")
    print(f"  PostgreSQL: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
    print("=" * 60)
    
    # Initialize clients
    s3_client = get_s3_client()
    conn = get_postgres_connection()
    
    try:
        # Create tables
        create_gold_tables(conn)
        
        # Load dimensions first
        load_dim_location(conn, s3_client)
        load_dim_parameter(conn, s3_client)
        load_dim_time(conn, s3_client)
        
        # Load facts
        load_fact_hourly(conn, s3_client)
        load_fact_daily(conn, s3_client)
        
        # Show summary
        show_summary(conn)
        
        print("\n[OK] GOLD DATA LOADED SUCCESSFULLY!")
        print("\n[INFO] You can now connect BI tools to PostgreSQL:")
        print(f"   Host: localhost")
        print(f"   Port: 5432")
        print(f"   Database: aqi")
        print(f"   User: aqi_user")
        print(f"   Tables: gold_fact_aqi_hourly, gold_fact_aqi_daily, gold_dim_*")
        print(f"   View: gold_aqi_summary")
        
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()

