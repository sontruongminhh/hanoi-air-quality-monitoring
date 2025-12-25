#!/usr/bin/env python3
"""
Script để kiểm tra Bronze data trong S3 cho cả 2 locations
"""

import boto3

S3_BUCKET = 'aqi-hanoi-bronze'
BRONZE_PREFIX = 'bronze/openaq/backfill'
LOCATIONS = ['4946813', '4946812']

def check_bronze_data():
    """Kiểm tra Bronze data cho cả 2 locations"""
    s3 = boto3.client('s3')
    
    print("=" * 60)
    print("  CHECKING BRONZE DATA IN S3")
    print("=" * 60)
    print(f"Bucket: {S3_BUCKET}")
    print(f"Prefix: {BRONZE_PREFIX}")
    print("=" * 60)
    
    for location_id in LOCATIONS:
        print(f"\nLocation {location_id}:")
        
        # Check structure 1: year/locationid/month/
        prefix = f"{BRONZE_PREFIX}/year=2025/locationid={location_id}/"
        
        try:
            response = s3.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=prefix,
                MaxKeys=10
            )
            
            files = response.get('Contents', [])
            print(f"  Prefix: {prefix}")
            print(f"  Files found: {len(files)}")
            
            if files:
                print(f"  Sample files:")
                for obj in files[:5]:
                    print(f"    - {obj['Key']}")
            else:
                print(f"  No files found!")
        
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\n" + "=" * 60)

if __name__ == '__main__':
    check_bronze_data()
