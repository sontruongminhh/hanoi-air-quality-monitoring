#!/usr/bin/env python3
"""
Script để kiểm tra xem Gold data có bao nhiêu locations
"""

import psycopg2

# PostgreSQL config
POSTGRES_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'aqi',
    'user': 'aqi_user',
    'password': 'aqi_password',
}

def check_gold_locations():
    """Kiểm tra locations trong Gold tables"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        print("=" * 60)
        print("  CHECKING GOLD DATA LOCATIONS")
        print("=" * 60)
        
        # Check gold_fact_aqi_hourly
        print("\n1. gold_fact_aqi_hourly:")
        cursor.execute("SELECT DISTINCT location_id FROM gold_fact_aqi_hourly ORDER BY location_id")
        locations = cursor.fetchall()
        print(f"   Locations found: {[loc[0] for loc in locations]}")
        
        # Count records per location
        cursor.execute("""
            SELECT 
                location_id, 
                COUNT(*) as total_records,
                MIN(date) as first_date,
                MAX(date) as last_date,
                COUNT(DISTINCT parameter) as num_parameters
            FROM gold_fact_aqi_hourly
            GROUP BY location_id
            ORDER BY location_id
        """)
        counts = cursor.fetchall()
        
        print("\n   Details:")
        for row in counts:
            print(f"   - Location {row[0]}:")
            print(f"     Records: {row[1]:,}")
            print(f"     Date range: {row[2]} to {row[3]}")
            print(f"     Parameters: {row[4]}")
        
        # Check gold_dim_location
        print("\n2. gold_dim_location:")
        cursor.execute("SELECT * FROM gold_dim_location ORDER BY location_id")
        dim_locations = cursor.fetchall()
        print(f"   Locations found: {len(dim_locations)}")
        for row in dim_locations:
            print(f"   - {row}")
        
        # Check gold_fact_aqi_daily
        print("\n3. gold_fact_aqi_daily:")
        cursor.execute("SELECT DISTINCT location_id FROM gold_fact_aqi_daily ORDER BY location_id")
        daily_locations = cursor.fetchall()
        print(f"   Locations found: {[loc[0] for loc in daily_locations]}")
        
        print("\n" + "=" * 60)
        print("SUMMARY:")
        print("=" * 60)
        print(f"Total locations in hourly: {len(locations)}")
        print(f"Total locations in daily: {len(daily_locations)}")
        print(f"Total locations in dim: {len(dim_locations)}")
        
        if len(locations) < 2:
            print("\n⚠️ WARNING: Only 1 location found!")
            print("   Expected: 2 locations (4946813, 4946812)")
            print("\n   Possible reasons:")
            print("   1. Airflow DAG chưa chạy cho location 4946812")
            print("   2. Data chưa được load vào PostgreSQL")
            print("   3. Backfill script chưa chạy")
        else:
            print("\n✅ OK: Found multiple locations!")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nMake sure:")
        print("1. PostgreSQL is running")
        print("2. psycopg2 is installed: pip install psycopg2-binary")

if __name__ == '__main__':
    check_gold_locations()
