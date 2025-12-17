#!/usr/bin/env python3
"""
Script để trigger DAG cho tất cả các ngày có dữ liệu Bronze
Chạy trong container Airflow
"""

import boto3
import time
from datetime import datetime

def get_all_bronze_days():
    """Lấy danh sách tất cả các ngày có dữ liệu trong Bronze"""
    s3 = boto3.client('s3')
    bucket = 'aqi-hanoi-bronze'
    
    all_days = []
    
    for month in range(7, 13):  # Jul to Dec
        prefix = f'bronze/openaq/backfill/year=2025/month={month:02d}/'
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter='/')
        
        for p in response.get('CommonPrefixes', []):
            # Extract day from path like 'bronze/openaq/backfill/year=2025/month=07/day=03/'
            day_part = p['Prefix'].split('day=')[1].rstrip('/')
            all_days.append({
                'year': '2025', 
                'month': f'{month:02d}', 
                'day': day_part
            })
    
    return all_days


def trigger_dag_for_day(year, month, day):
    """Trigger DAG cho một ngày cụ thể với unique run_id"""
    from airflow.api.common.trigger_dag import trigger_dag
    from datetime import datetime
    
    conf = {'year': year, 'month': month, 'day': day}
    
    # Tạo unique run_id dựa trên ngày dữ liệu
    run_id = f"backfill_{year}_{month}_{day}_{datetime.utcnow().strftime('%H%M%S%f')}"
    
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
    print("  BACKFILL ALL BRONZE DATA: Jul-Dec 2025")
    print("=" * 70)
    print()
    
    # Get all days with data
    print("[INFO] Scanning Bronze layer for available data...")
    all_days = get_all_bronze_days()
    
    print(f"[OK] Found {len(all_days)} days with data")
    print()
    
    # Show summary by month
    by_month = {}
    for d in all_days:
        m = d['month']
        by_month[m] = by_month.get(m, 0) + 1
    
    print("[INFO] Summary by month:")
    for m in sorted(by_month.keys()):
        print(f"   2025-{m}: {by_month[m]} days")
    print()
    
    # Confirm
    print("[INFO] Starting backfill...")
    print("-" * 70)
    
    success_count = 0
    fail_count = 0
    
    for i, d in enumerate(all_days):
        date_str = f"{d['year']}-{d['month']}-{d['day']}"
        
        success, result = trigger_dag_for_day(d['year'], d['month'], d['day'])
        
        if success:
            success_count += 1
            print(f"[OK] [{i+1}/{len(all_days)}] Triggered: {date_str}")
        else:
            fail_count += 1
            print(f"[ERROR] [{i+1}/{len(all_days)}] Failed: {date_str} - {result}")
        
        # Delay 0.5s giữa mỗi trigger để tránh conflict
        time.sleep(0.5)
        
        # Longer pause every 20 triggers
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


if __name__ == '__main__':
    main()
