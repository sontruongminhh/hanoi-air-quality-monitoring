#!/usr/bin/env python3
"""
Script de trigger DAG cho TAT CA cac tram va tat ca cac ngay co du lieu Bronze
Chay trong container Airflow

Usage:
    python backfill_multi_locations.py
"""

import boto3
import time
import sys
from datetime import datetime

# Danh sach cac location ID can xu ly
LOCATION_IDS = ['4946813', '4946812']

S3_BUCKET = 'aqi-hanoi-bronze'


def get_all_bronze_days_for_location(location_id):
    """Lay danh sach tat ca cac ngay co du lieu trong Bronze cho location ID"""
    s3 = boto3.client('s3')
    
    all_days = []
    
    # Cau truc 1: bronze/openaq/backfill/year=2025/locationid=XXX/month=MM/day=DD/
    for month in range(1, 13):
        prefix = f'bronze/openaq/backfill/year=2025/locationid={location_id}/month={month:02d}/'
        
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/')
            
            # Check for day= folders
            for p in response.get('CommonPrefixes', []):
                if 'day=' in p['Prefix']:
                    day_part = p['Prefix'].split('day=')[1].rstrip('/')
                    all_days.append({
                        'year': '2025', 
                        'month': f'{month:02d}', 
                        'day': day_part,
                        'location_id': location_id
                    })
            
            # Cau truc 2: Files truc tiep trong month/ (khong co day folder)
            # VD: location-4946812-20250703.csv.gz
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.csv.gz'):
                    # Extract date from filename: location-XXXXXX-YYYYMMDD.csv.gz
                    filename = key.split('/')[-1]
                    if '-' in filename:
                        parts = filename.replace('.csv.gz', '').split('-')
                        if len(parts) >= 3:
                            date_str = parts[-1]  # YYYYMMDD
                            if len(date_str) == 8:
                                day = date_str[6:8]
                                all_days.append({
                                    'year': '2025', 
                                    'month': f'{month:02d}', 
                                    'day': day,
                                    'location_id': location_id
                                })
        except Exception as e:
            print(f"  [WARN] Error scanning month {month}: {e}")
    
    # Cau truc 3: bronze/openaq/backfill/year=2025/month=MM/day=DD/locationid=XXX/
    for month in range(1, 13):
        prefix = f'bronze/openaq/backfill/year=2025/month={month:02d}/'
        
        try:
            response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/')
            
            for p in response.get('CommonPrefixes', []):
                if 'day=' in p['Prefix']:
                    # Check if locationid folder exists
                    day_prefix = p['Prefix']
                    loc_check = s3.list_objects_v2(
                        Bucket=S3_BUCKET, 
                        Prefix=f"{day_prefix}locationid={location_id}/",
                        MaxKeys=1
                    )
                    if 'Contents' in loc_check:
                        day_part = p['Prefix'].split('day=')[1].split('/')[0]
                        all_days.append({
                            'year': '2025', 
                            'month': f'{month:02d}', 
                            'day': day_part,
                            'location_id': location_id
                        })
        except Exception as e:
            print(f"  [WARN] Error scanning old structure month {month}: {e}")
    
    # Remove duplicates
    seen = set()
    unique_days = []
    for d in all_days:
        key = (d['year'], d['month'], d['day'], d['location_id'])
        if key not in seen:
            seen.add(key)
            unique_days.append(d)
    
    return sorted(unique_days, key=lambda x: (x['month'], x['day']))


def trigger_dag_for_day(year, month, day, location_id):
    """Trigger DAG cho mot ngay cu the voi unique run_id"""
    from airflow.api.common.trigger_dag import trigger_dag
    
    conf = {'year': year, 'month': month, 'day': day, 'location_id': location_id}
    
    # Tao unique run_id
    run_id = f"backfill_{location_id}_{year}_{month}_{day}_{datetime.utcnow().strftime('%H%M%S%f')}"
    
    try:
        result = trigger_dag(
            dag_id='aqi_etl_pipeline_v2',
            run_id=run_id,
            conf=conf
        )
        return True, str(result)
    except Exception as e:
        return False, str(e)


def main():
    print("=" * 70)
    print("  BACKFILL ALL LOCATIONS - Bronze -> Silver -> Gold")
    print("=" * 70)
    print()
    
    all_tasks = []
    
    # Scan du lieu cho tung location
    for loc_id in LOCATION_IDS:
        print(f"[INFO] Scanning location {loc_id}...")
        days = get_all_bronze_days_for_location(loc_id)
        print(f"  Found {len(days)} days")
        all_tasks.extend(days)
    
    print()
    print(f"[INFO] Total tasks to process: {len(all_tasks)}")
    print()
    
    # Summary by location
    by_location = {}
    for t in all_tasks:
        loc = t['location_id']
        by_location[loc] = by_location.get(loc, 0) + 1
    
    print("[INFO] Summary by location:")
    for loc, count in by_location.items():
        print(f"   Location {loc}: {count} days")
    print()
    
    # Summary by month
    by_month = {}
    for t in all_tasks:
        key = f"{t['location_id']}-{t['month']}"
        by_month[key] = by_month.get(key, 0) + 1
    
    print("[INFO] Summary by month:")
    for key in sorted(by_month.keys()):
        print(f"   {key}: {by_month[key]} days")
    print()
    
    if not all_tasks:
        print("[WARN] Khong tim thay du lieu nao!")
        return
    
    # Bat dau backfill
    print("[INFO] Starting backfill...")
    print("-" * 70)
    
    success_count = 0
    fail_count = 0
    
    for i, t in enumerate(all_tasks):
        date_str = f"{t['location_id']}/{t['year']}-{t['month']}-{t['day']}"
        
        success, result = trigger_dag_for_day(t['year'], t['month'], t['day'], t['location_id'])
        
        if success:
            success_count += 1
            print(f"[OK] [{i+1}/{len(all_tasks)}] Triggered: {date_str}")
        else:
            fail_count += 1
            print(f"[ERROR] [{i+1}/{len(all_tasks)}] Failed: {date_str} - {result}")
        
        # Delay giua moi trigger
        time.sleep(0.5)
        
        # Pause dai hon sau moi 20 triggers
        if (i + 1) % 20 == 0:
            print(f"   ... pausing 3s after {i+1} triggers ...")
            time.sleep(3)
    
    print()
    print("=" * 70)
    print(f"  BACKFILL COMPLETE!")
    print(f"  [OK] Success: {success_count}")
    print(f"  [ERROR] Failed: {fail_count}")
    print("=" * 70)
    print()
    print("[INFO] Check Airflow UI at http://localhost:8080 to monitor progress")
    print()
    print("[NEXT STEPS]")
    print("  1. Doi cac DAG runs hoan thanh")
    print("  2. Chay: docker-compose --profile load-gold up")
    print("     De load Gold data vao PostgreSQL")


if __name__ == '__main__':
    main()

