#!/usr/bin/env python3
"""
Script de kiem tra du lieu Gold trong S3 cho location 4946812
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv('S3_BUCKET', 'aqi-hanoi-bronze')
GOLD_PREFIX = 'gold/openaq/marts'
LOCATION_ID = '4946812'

def get_s3_client():
    """Tao S3 client"""
    s3_kwargs = {
        'region_name': os.getenv('S3_REGION', 'us-east-1'),
    }
    
    if os.getenv('AWS_ACCESS_KEY_ID') and os.getenv('AWS_SECRET_ACCESS_KEY'):
        s3_kwargs['aws_access_key_id'] = os.getenv('AWS_ACCESS_KEY_ID')
        s3_kwargs['aws_secret_access_key'] = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if os.getenv('S3_ENDPOINT_URL'):
        s3_kwargs['endpoint_url'] = os.getenv('S3_ENDPOINT_URL')
    
    return boto3.client('s3', **s3_kwargs)

def check_gold_data():
    """Kiem tra Gold data trong S3"""
    print("=" * 80)
    print("KIEM TRA GOLD DATA TRONG S3 CHO LOCATION 4946812")
    print("=" * 80)
    print(f"Bucket: {S3_BUCKET}")
    print(f"Gold Prefix: {GOLD_PREFIX}")
    print("=" * 80)
    
    s3_client = get_s3_client()
    
    # Kiem tra dim_location
    print("\n1. gold/openaq/marts/dim_location/:")
    prefix = f"{GOLD_PREFIX}/dim_location/"
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = response.get('Contents', [])
        print(f"   Tim thay {len(files)} file(s)")
        
        location_found = False
        for obj in files:
            if LOCATION_ID in obj['Key']:
                print(f"   [OK] Tim thay location 4946812: {obj['Key']}")
                location_found = True
            elif 'locationid=4946812' in obj['Key']:
                print(f"   [OK] Tim thay location 4946812: {obj['Key']}")
                location_found = True
        
        if not location_found:
            print(f"   [ERROR] KHONG tim thay location 4946812 trong dim_location")
            print(f"   Cac file co san:")
            for obj in files[:10]:
                print(f"     - {obj['Key']}")
    except Exception as e:
        print(f"   [ERROR] Loi khi kiem tra: {e}")
    
    # Kiem tra fact_aqi_hourly
    print("\n2. gold/openaq/marts/fact_aqi_hourly/:")
    prefix = f"{GOLD_PREFIX}/fact_aqi_hourly/"
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = response.get('Contents', [])
        print(f"   Tim thay {len(files)} file(s)")
        
        location_found = False
        count = 0
        for obj in files:
            if LOCATION_ID in obj['Key'] or 'locationid=4946812' in obj['Key']:
                count += 1
                if not location_found:
                    print(f"   [OK] Tim thay location 4946812: {obj['Key']}")
                    location_found = True
        
        if location_found:
            print(f"   [OK] Tong cong {count} file(s) cho location 4946812")
        else:
            print(f"   [ERROR] KHONG tim thay location 4946812 trong fact_aqi_hourly")
            print(f"   Cac file co san (sample):")
            for obj in files[:10]:
                print(f"     - {obj['Key']}")
    except Exception as e:
        print(f"   [ERROR] Loi khi kiem tra: {e}")
    
    # Kiem tra fact_aqi_daily
    print("\n3. gold/openaq/marts/fact_aqi_daily/:")
    prefix = f"{GOLD_PREFIX}/fact_aqi_daily/"
    try:
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        files = response.get('Contents', [])
        print(f"   Tim thay {len(files)} file(s)")
        
        location_found = False
        count = 0
        for obj in files:
            if LOCATION_ID in obj['Key'] or 'locationid=4946812' in obj['Key']:
                count += 1
                if not location_found:
                    print(f"   [OK] Tim thay location 4946812: {obj['Key']}")
                    location_found = True
        
        if location_found:
            print(f"   [OK] Tong cong {count} file(s) cho location 4946812")
        else:
            print(f"   [ERROR] KHONG tim thay location 4946812 trong fact_aqi_daily")
            print(f"   Cac file co san (sample):")
            for obj in files[:10]:
                print(f"     - {obj['Key']}")
    except Exception as e:
        print(f"   [ERROR] Loi khi kiem tra: {e}")
    
    print("\n" + "=" * 80)
    print("KET LUAN:")
    print("=" * 80)
    print("Neu co du lieu trong S3 Gold nhung khong co trong PostgreSQL,")
    print("can chay script load_gold_to_postgres.py de load du lieu vao PostgreSQL")
    print("\nLenh chay:")
    print("  docker-compose --profile load-gold run --rm load-gold-to-postgres")
    print("  hoac")
    print("  python load_gold_to_postgres.py")

if __name__ == '__main__':
    check_gold_data()

