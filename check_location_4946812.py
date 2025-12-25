#!/usr/bin/env python3
"""
Script để kiểm tra tại sao location 4946812 không có trong PostgreSQL
"""

import psycopg2
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import json

load_dotenv()

# PostgreSQL config
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'aqi'),
    'user': os.getenv('POSTGRES_USER', 'aqi_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'aqi_password'),
}

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'aqi.hanoi.raw')

def check_raw_data():
    """Kiem tra du lieu trong bang aqi_readings (raw data tu consumer)"""
    print("\n" + "=" * 80)
    print("1. KIEM TRA RAW DATA (aqi_readings table)")
    print("=" * 80)
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Kiểm tra tất cả location_id
        cursor.execute("""
            SELECT DISTINCT location_id, location_name, COUNT(*) as count
            FROM aqi_readings
            GROUP BY location_id, location_name
            ORDER BY location_id
        """)
        locations = cursor.fetchall()
        
        print(f"\nTat ca locations trong aqi_readings:")
        for loc in locations:
            print(f"  - Location ID: {loc[0]}, Name: {loc[1]}, Records: {loc[2]}")
        
        # Kiem tra cu the location 4946812
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(created_at) as first_record,
                   MAX(created_at) as last_record,
                   MIN(measurement_time) as first_measurement,
                   MAX(measurement_time) as last_measurement
            FROM aqi_readings
            WHERE location_id = '4946812' OR location_id::text LIKE '%4946812%'
        """)
        result = cursor.fetchone()
        
        if result and result[0] > 0:
            print(f"\n[OK] Tim thay location 4946812 trong aqi_readings:")
            print(f"   - So ban ghi: {result[0]}")
            print(f"   - Ban ghi dau tien (created_at): {result[1]}")
            print(f"   - Ban ghi cuoi cung (created_at): {result[2]}")
            print(f"   - Measurement dau tien: {result[3]}")
            print(f"   - Measurement cuoi cung: {result[4]}")
        else:
            print(f"\n[ERROR] KHONG tim thay location 4946812 trong aqi_readings")
            
            # Kiem tra cac location_id tuong tu
            cursor.execute("""
                SELECT DISTINCT location_id
                FROM aqi_readings
                WHERE location_id::text LIKE '%494681%'
                ORDER BY location_id
            """)
            similar = cursor.fetchall()
            if similar:
                print(f"\n   Cac location_id tuong tu:")
                for loc in similar:
                    print(f"     - {loc[0]}")
        
        # Kiem tra location_id dang string
        cursor.execute("""
            SELECT DISTINCT location_id, location_name
            FROM aqi_readings
            WHERE location_id::text LIKE '%iqair%' OR location_id::text LIKE '%hanoi%'
            ORDER BY location_id
        """)
        iqair_locs = cursor.fetchall()
        if iqair_locs:
            print(f"\n   Cac location tu IQAir:")
            for loc in iqair_locs:
                print(f"     - {loc[0]}: {loc[1]}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] ERROR khi kiem tra raw data: {e}")
        import traceback
        traceback.print_exc()

def check_gold_data():
    """Kiem tra du lieu trong cac bang Gold layer"""
    print("\n" + "=" * 80)
    print("2. KIEM TRA GOLD DATA")
    print("=" * 80)
    
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cursor = conn.cursor()
        
        # Kiểm tra gold_dim_location
        print("\n2.1. gold_dim_location:")
        cursor.execute("""
            SELECT id, location_id, location_name, city, country
            FROM gold_dim_location
            WHERE location_id::text LIKE '%4946812%' OR location_id::text = '4946812'
            ORDER BY location_id
        """)
        dim_locs = cursor.fetchall()
        if dim_locs:
            print(f"   [OK] Tim thay {len(dim_locs)} location(s):")
            for loc in dim_locs:
                print(f"      - ID: {loc[0]}, Location ID: {loc[1]}, Name: {loc[2]}, City: {loc[3]}, Country: {loc[4]}")
        else:
            print(f"   [ERROR] KHONG tim thay location 4946812 trong gold_dim_location")
            
            # Liet ke tat ca locations trong dim
            cursor.execute("SELECT location_id, location_name FROM gold_dim_location ORDER BY location_id")
            all_dim = cursor.fetchall()
            print(f"\n   Tat ca locations trong gold_dim_location:")
            for loc in all_dim:
                print(f"      - {loc[0]}: {loc[1]}")
        
        # Kiem tra gold_fact_aqi_hourly
        print("\n2.2. gold_fact_aqi_hourly:")
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(date) as first_date,
                   MAX(date) as last_date
            FROM gold_fact_aqi_hourly
            WHERE location_id::text LIKE '%4946812%' OR location_id::text = '4946812'
        """)
        hourly_result = cursor.fetchone()
        if hourly_result and hourly_result[0] > 0:
            print(f"   [OK] Tim thay {hourly_result[0]} ban ghi")
            print(f"      - Tu ngay: {hourly_result[1]} den {hourly_result[2]}")
        else:
            print(f"   [ERROR] KHONG tim thay du lieu cho location 4946812")
        
        # Kiem tra gold_fact_aqi_daily
        print("\n2.3. gold_fact_aqi_daily:")
        cursor.execute("""
            SELECT COUNT(*) as count,
                   MIN(date) as first_date,
                   MAX(date) as last_date
            FROM gold_fact_aqi_daily
            WHERE location_id::text LIKE '%4946812%' OR location_id::text = '4946812'
        """)
        daily_result = cursor.fetchone()
        if daily_result and daily_result[0] > 0:
            print(f"   [OK] Tim thay {daily_result[0]} ban ghi")
            print(f"      - Tu ngay: {daily_result[1]} den {daily_result[2]}")
        else:
            print(f"   [ERROR] KHONG tim thay du lieu cho location 4946812")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"\n[ERROR] ERROR khi kiem tra Gold data: {e}")
        import traceback
        traceback.print_exc()

def check_kafka_messages():
    """Kiem tra messages trong Kafka topic"""
    print("\n" + "=" * 80)
    print("3. KIEM TRA KAFKA MESSAGES (sample)")
    print("=" * 80)
    
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',  # Chi xem messages moi
            consumer_timeout_ms=5000,  # Timeout sau 5 giay
            enable_auto_commit=False
        )
        
        print(f"\nDang kiem tra topic: {KAFKA_TOPIC}")
        print("(Chi kiem tra messages moi nhat, timeout sau 5 giay)\n")
        
        location_ids_found = set()
        message_count = 0
        max_messages = 20  # Giới hạn số messages để kiểm tra
        
        for message in consumer:
            if message_count >= max_messages:
                break
                
            data = message.value
            location_id = data.get('location_id', 'N/A')
            location_name = data.get('location_name', 'N/A')
            location_ids_found.add(str(location_id))
            
            if '4946812' in str(location_id):
                print(f"   [OK] Tim thay location 4946812 trong Kafka!")
                print(f"      - Location ID: {location_id}")
                print(f"      - Location Name: {location_name}")
                print(f"      - Timestamp: {data.get('timestamp', 'N/A')}")
                print(f"      - Sensors: {len(data.get('sensors', []))}")
            
            message_count += 1
        
        consumer.close()
        
        print(f"\n   Da kiem tra {message_count} messages")
        print(f"   Cac location_id tim thay: {sorted(location_ids_found)}")
        
        if '4946812' not in ' '.join(location_ids_found):
            print(f"\n   [ERROR] KHONG tim thay location 4946812 trong Kafka messages gan day")
            print(f"   (Co the messages da cu hoac chua duoc gui)")
        
    except Exception as e:
        print(f"\n[ERROR] ERROR khi kiem tra Kafka: {e}")
        print("   (Co the Kafka chua chay hoac topic khong ton tai)")
        import traceback
        traceback.print_exc()

def check_producer_config():
    """Kiem tra cau hinh producer"""
    print("\n" + "=" * 80)
    print("4. KIEM TRA CAU HINH PRODUCER")
    print("=" * 80)
    
    print(f"\n   KAFKA_TOPIC: {KAFKA_TOPIC}")
    print(f"   KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
    
    # Kiem tra producer code
    print("\n   Kiem tra producer.py:")
    try:
        with open('producer.py', 'r', encoding='utf-8') as f:
            content = f.read()
            if 'location_id' in content:
                # Tim cach producer tao location_id
                if 'iqair_' in content:
                    print("      - Producer su dung format: iqair_{city}_{state}")
                    print("      - Location ID se KHONG phai la so 4946812")
                    print("      - Location ID se co dang: iqair_hanoi_hanoi")
    except Exception as e:
        print(f"      [ERROR] Khong doc duoc producer.py: {e}")

def main():
    import sys
    import io
    # Fix encoding for Windows console
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    
    print("\n" + "=" * 80)
    print("KIEM TRA TAI SAO LOCATION 4946812 KHONG CO TRONG POSTGRESQL")
    print("=" * 80)
    
    check_raw_data()
    check_gold_data()
    check_kafka_messages()
    check_producer_config()
    
    print("\n" + "=" * 80)
    print("KET LUAN VA GOI Y")
    print("=" * 80)
    print("""
Neu location 4946812 khong co trong PostgreSQL, co the do:

1. Producer dang su dung IQAir API va tao location_id dang string (iqair_hanoi_hanoi)
   thay vi so 4946812. Kiem tra producer.py de xem format location_id.

2. Consumer chua nhan duoc messages tu Kafka cho location nay.

3. Airflow DAG chua chay de xu ly du lieu tu Bronze -> Silver -> Gold.

4. Backfill script chua chay cho location 4946812.

De khac phuc:
- Kiem tra logs cua producer: docker logs air-quality-producer
- Kiem tra logs cua consumer: docker logs air-quality-consumer  
- Kiem tra Airflow DAG runs cho location nay
- Chay backfill script neu can
    """)

if __name__ == '__main__':
    main()

