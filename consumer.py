
"""
Kafka Consumer - Nhận dữ liệu chất lượng không khí từ Kafka
"""

import json
import os
from datetime import datetime, timezone, timedelta
import time
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
from util_email import send_aqi_alert_email


load_dotenv()


class AirQualityConsumer:
    def __init__(self):
        """
        Khởi tạo Kafka Consumer với cấu hình từ file .env
        """
    
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        self.topic = os.getenv('KAFKA_TOPIC', 'aqi.hanoi.raw')
        group_id = 'air-quality-consumer-group'

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print(f"[OK] Consumer da ket noi toi Kafka: {self.bootstrap_servers}")
        print(f"[OK] Topic: {self.topic}")
        print(f"[OK] Group ID: {group_id}")

        
        self.pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.pg_db = os.getenv('POSTGRES_DB', 'aqi')
        self.pg_user = os.getenv('POSTGRES_USER', 'aqi_user')
        self.pg_password = os.getenv('POSTGRES_PASSWORD', 'aqi_password')

        
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.alert_email_to = os.getenv('ALERT_EMAIL_TO')
        self.alert_email_from = os.getenv('ALERT_EMAIL_FROM', 'alerts@example.com')

      
        self.aqi_threshold = float(os.getenv('THRESHOLD_AQI', '150'))

        self.pg_conn = self._init_pg()
        print("[OK] Ket noi Postgres thanh cong")

    def _init_pg(self):
        retries = 30
        delay = 2
        last_err = None
        for _ in range(retries):
            try:
                conn = psycopg2.connect(
                    host=self.pg_host,
                    port=self.pg_port,
                    dbname=self.pg_db,
                    user=self.pg_user,
                    password=self.pg_password,
                )
                conn.autocommit = True
                return conn
            except Exception as e:
                last_err = e
                print(f"Postgres chưa sẵn sàng, thử lại sau {delay}s...")
                time.sleep(delay)
        raise last_err

    def _normalize_param(self, raw_param: str, display: str) -> str:
        text = (raw_param or display or '').strip().lower()
        text = text.replace(' mass', '').replace(' concentration', '').replace('_', ' ').replace('.', ' ').strip()
        
      
        if text in ['p2', 'pm25', 'pm2.5', 'pm 2.5']:
            return 'pm25'
        elif text in ['p1', 'pm10', 'pm 10']:
            return 'pm10'
        elif text in ['n2', 'no2', 'nitrogen dioxide']:
            return 'no2'
        elif text in ['o3', 'ozone']:
            return 'o3'
        elif text in ['co', 'carbon monoxide']:
            return 'co'
        elif text in ['s2', 'so2', 'sulfur dioxide']:
            return 'so2'
        elif text in ['aqi', 'aqius', 'aqi_us', 'aqi-us']:
            return 'aqi_us'
        
        cleaned = text.replace(' ', '').replace('.', '')
        return cleaned

    def _parse_utc(self, dt_str: str) -> datetime:
        if not dt_str:
            return None
        try:
            
            if dt_str.endswith('Z'):
                dt_clean = dt_str.rstrip('Z')
               
                dt = datetime.fromisoformat(dt_clean + '+00:00' if '+' not in dt_clean and dt_clean.count('-') <= dt_clean.count('T') else dt_clean)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
           
            if '+' in dt_str or (dt_str.count('-') > dt_str.count('T')):
                dt = datetime.fromisoformat(dt_str)
                if dt.tzinfo is None: 
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
           
            dt = datetime.fromisoformat(dt_str)
            return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
        except Exception as e:
            print(f"   [ERROR] Loi parse datetime '{dt_str}': {e}")
            return None

    def _evaluate_reading(self, cursor, row: dict, unit: str = None):
        """
        Kiểm tra điều kiện để quyết định việc gửi cảnh báo:
        - Chỉ cảnh báo trong 2 khung giờ: 7h-8h sáng và 17h-18h chiều (UTC+7)
        - So sánh với ngưỡng tiêu chuẩn (150) và gửi cảnh báo nếu vượt (chỉ 1 lần duy nhất mỗi khung giờ)
        - Ngoài 2 khung giờ trên: không cảnh báo

        Args:
            cursor: Database cursor
            row: Row data với location_id, parameter='aqi', value, measurement_time
            unit: Unit của giá trị (phải là 'AQI')

        Returns:
            tuple(bool, bool, datetime | None):
                - is_newer: luôn True (vì luôn insert mỗi lần gọi)
                - should_alert: có nên gửi cảnh báo (chỉ True trong khung giờ và chưa cảnh báo)
                - prev_time: measurement_time gần nhất trước đó (nếu có)
        """
        
        measurement_time = row['measurement_time']
        value = row['value']
        
        if value is None:
            return True, False, None
        
        value_float = float(value)
        threshold_standard = self.aqi_threshold  # 150 (ngưỡng tiêu chuẩn)
        
      
        current_time = datetime.now(timezone.utc)
        local_tz = timezone(timedelta(hours=7))
        current_time_local = current_time.astimezone(local_tz)
        current_hour_local = current_time_local.hour
        current_date_local = current_time_local.date()
        
      
        is_alert_hour = (7 <= current_hour_local < 8) or (17 <= current_hour_local < 18)
        
      
        print(
            f"  DEBUG: Gio hien tai (UTC) = {current_time.hour}:{current_time.minute}, "
            f"Gio dia phuong (UTC+7) = {current_hour_local}:{current_time_local.minute}, "
            f"is_alert_hour = {is_alert_hour}, AQI = {value_float}, threshold = {threshold_standard}"
        )
        
        should_alert = False
        
        # Chỉ cảnh báo trong 2 khung giờ: 7h-8h và 17h-18h (UTC+7)
        if is_alert_hour:
            if value_float > threshold_standard:
                # Xác định khung giờ địa phương (7-8h hoặc 17-18h)
                if 7 <= current_hour_local < 8:
                    hour_start_local, hour_end_local = 7, 8
                else:  # 17 <= current_hour_local < 18
                    hour_start_local, hour_end_local = 17, 18
                
                # Kiểm tra xem đã gửi cảnh báo trong khung giờ này (cùng ngày) chưa
                cursor.execute(
                    """
                    SELECT COUNT(*) 
                    FROM aqi_readings 
                    WHERE location_id = %s 
                      AND parameter = %s 
                      AND alerted = TRUE 
                      AND (created_at AT TIME ZONE '+07')::date = %s
                      AND EXTRACT(HOUR FROM created_at AT TIME ZONE '+07') >= %s
                      AND EXTRACT(HOUR FROM created_at AT TIME ZONE '+07') < %s
                    """,
                    (row['location_id'], row['parameter'], current_date_local, hour_start_local, hour_end_local),
                )
                alert_count = cursor.fetchone()[0]
                
                if alert_count == 0:
                    # Chua co canh bao nao trong khung gio nay -> gui canh bao
                    should_alert = True
                    print(f"  [ALERT] KHUNG GIO CANH BAO ({hour_start_local}h-{hour_end_local}h, gio dia phuong): AQI={value_float} > {threshold_standard} - CANH BAO LAN DAU TRONG KHUNG GIO")
                else:
                    # Da canh bao trong khung gio nay roi -> khong gui nua
                    print(f"  AQI={value_float} > {threshold_standard} nhung da canh bao trong khung gio {hour_start_local}h-{hour_end_local}h (gio dia phuong) hom nay roi (tranh spam)")
        else:
            # Ngoai khung gio: khong canh bao
            print(f"  Ngoai khung gio canh bao: khong gui canh bao (AQI={value_float})")
        
        return True, should_alert, None

    def _insert_reading(self, cursor, row: dict, alerted: bool) -> bool:
        """
        Insert reading vào database. Luôn insert mỗi lần gọi API (không có ON CONFLICT DO NOTHING).
        Unique constraint dựa trên (location_id, parameter, created_at) để cho phép nhiều bản ghi cùng measurement_time.
        """
        cursor.execute(
            """
            INSERT INTO aqi_readings (
                location_id, location_name, locality, country, country_code,
                latitude, longitude, provider,
                parameter, unit, value, measurement_time,
                main_pollutant,
                alerted, alert_sent_at
            ) VALUES (
                %(location_id)s, %(location_name)s, %(locality)s, %(country)s, %(country_code)s,
                %(latitude)s, %(longitude)s, %(provider)s,
                %(parameter)s, %(unit)s, %(value)s, %(measurement_time)s,
                %(main_pollutant)s,
                %(alerted)s, CASE WHEN %(alerted)s THEN NOW() ELSE NULL END
            )
            -- Không có ON CONFLICT vì mỗi lần gọi API sẽ có created_at khác nhau (độ chính xác đến microsecond)
            """,
            {**row, 'alerted': alerted},
        )
        return cursor.rowcount > 0

    def _send_email(self, subject: str, param_name: str, value: float, unit: str, 
                     threshold: float, location_name: str, locality: str, country: str,
                     latitude: float, longitude: float, measurement_time: datetime,
                     main_pollutant: str = None):
        """
        Gửi email cảnh báo - wrapper gọi hàm từ util_email.py
        """
        send_aqi_alert_email(
            smtp_host=self.smtp_host,
            smtp_port=self.smtp_port,
            smtp_user=self.smtp_user,
            smtp_password=self.smtp_password,
            alert_email_from=self.alert_email_from,
            alert_email_to=self.alert_email_to,
            subject=subject,
            param_name=param_name,
            value=value,
            unit=unit,
            threshold=threshold,
            location_name=location_name,
            locality=locality,
            country=country,
            latitude=latitude,
            longitude=longitude,
            measurement_time=measurement_time,
            main_pollutant=main_pollutant
        )

    def format_sensor_data(self, sensors):
        """
        Format dữ liệu sensors để hiển thị đẹp

        Args:
            sensors: Danh sách sensors

        Returns:
            str: Chuỗi đã format
        """
        if not sensors:
            return "    Không có dữ liệu sensors"

        result = []
        for sensor in sensors:
            param = sensor.get('parameter_display', sensor.get('parameter', 'N/A'))
            value = sensor.get('latest_value', 'N/A')
            unit = sensor.get('unit', '')
            datetime_str = sensor.get('latest_datetime', None)

         
            if datetime_str:
                result.append(f"    • {param}: {value} {unit} (Cập nhật: {datetime_str})")
            else:
                result.append(f"    • {param}: {value} {unit}")

        return '\n'.join(result)

    def start_consuming(self):
        """Bắt đầu nhận dữ liệu"""
        print(f"\n{'='*80}")
        print(f"BẮT ĐẦU NHẬN DỮ LIỆU CHẤT LƯỢNG KHÔNG KHÍ")
        print(f"{'='*80}\n")

        try:
            with self.pg_conn.cursor() as cursor:
                for message in self.consumer:
                    data = message.value

                    print(f"\n{'='*80}")
                    print(f" Location: {data.get('location_name', 'Unknown')}")
                    print(f" Địa điểm: {data.get('locality', 'N/A')}, {data.get('country', 'N/A')}")
                    print(f" Tọa độ: {data.get('coordinates', {}).get('latitude', 'N/A')}, "
                          f"{data.get('coordinates', {}).get('longitude', 'N/A')}")
                    print(f" Location ID: {data.get('location_id', 'N/A')}")
                    print(f" Timestamp: {data.get('timestamp', 'N/A')}")
                    print(f"\n Thông số chất lượng không khí:")
                    print(self.format_sensor_data(data.get('sensors', [])))

                    
                    base = {
                        'location_id': data.get('location_id'),
                        'location_name': data.get('location_name'),
                        'locality': data.get('locality'),
                        'country': data.get('country'),
                        'country_code': data.get('country_code'),
                        'latitude': (data.get('coordinates') or {}).get('latitude'),
                        'longitude': (data.get('coordinates') or {}).get('longitude'),
                        'provider': 'IQAir',
                    }

                    sensors = data.get('sensors', []) or []
                    inserted_count = 0
                    
                 
                    for sensor in sensors:
                        value = sensor.get('latest_value')
                        unit = sensor.get('unit')
                        
                       
                        if not unit or unit.upper() != 'AQI':
                            print(f"  Bo qua sensor {sensor.get('parameter')}: khong phai AQI (unit={unit})")
                            continue
                        
                        measurement_time = self._parse_utc(sensor.get('latest_datetime'))
                        if measurement_time is None:
                            print(f"  [WARN] latest_datetime tu sensor khong parse duoc: {sensor.get('latest_datetime')}")
                            measurement_time = self._parse_utc(data.get('timestamp'))
                            if measurement_time is None:
                                print(f"  [WARN] timestamp tong cung khong parse duoc, dung datetime.now()")
                                measurement_time = datetime.now(timezone.utc)
                            else:
                                print(f"  [INFO] Dung timestamp tong (thoi diem goi API): {measurement_time.isoformat()}")
                        else:
                            print(f"  [INFO] Dung latest_datetime tu sensor (thoi diem do tu API): {measurement_time.isoformat()}")
                        
                      
                        param_norm = 'aqi'
                        
                        
                        main_pollutant = sensor.get('main_pollutant_display') or sensor.get('main_pollutant', None)

                        row = {
                            **base,
                            'parameter': param_norm,
                            'unit': unit,
                            'value': value,
                            'measurement_time': measurement_time,
                            'main_pollutant': main_pollutant,  
                        }

                        try:
                           
                            is_newer, should_alert, prev_time = self._evaluate_reading(cursor, row, unit)
                            
                            value_float = float(value) if value is not None else None
                            
                            print(f"  AQI: {value_float}, should_alert={should_alert}")
                            print(f"  Chat o nhiem chinh: {main_pollutant}")
                            
                            
                            inserted = self._insert_reading(cursor, row, alerted=should_alert)
                            if inserted:
                                inserted_count += 1
                                print(f"  [OK] Da ghi vao DB (ID moi)")
                            else:
                                print(f"  [ERROR] Khong ghi duoc vao DB (co the do loi)")

                            if should_alert:
                                
                                aqi_value = value_float
                              
                                if aqi_value <= 50:
                                    aqi_level = 'Tốt'
                                elif aqi_value <= 100:
                                    aqi_level = 'Trung bình'
                                elif aqi_value <= 150:
                                    aqi_level = 'Kém (Unhealthy for Sensitive Groups)'
                                elif aqi_value <= 200:
                                    aqi_level = 'Xấu (Unhealthy)'
                                elif aqi_value <= 300:
                                    aqi_level = 'Rất xấu (Very Unhealthy)'
                                else:
                                    aqi_level = 'Nguy hiểm (Hazardous)'
                                
                               
                                param_display = f"AQI (Main: {main_pollutant})"
                                print(f"  [ALERT] PHAT HIEN VUOT NGUONG! {param_display}: {aqi_value} ({aqi_level})")

                              
                                try:
                                    self._send_email(
                                        subject=None,
                                        param_name=param_display,
                                        value=aqi_value,
                                        unit='AQI',
                                        threshold=float(self.aqi_threshold),
                                        location_name=base['location_name'],
                                        locality=base['locality'] or 'N/A',
                                        country=base['country'] or 'N/A',
                                        latitude=base['latitude'] or 0.0,
                                        longitude=base['longitude'] or 0.0,
                                        measurement_time=measurement_time,
                                        main_pollutant=main_pollutant
                                    )
                                    print("  [OK] Da gui email canh bao don")
                                except Exception as email_error:
                                    print(f"  [ERROR] Loi khi gui email: {email_error}")
                                    import traceback
                                    traceback.print_exc()
                        except Exception as e:
                            print(f"[ERROR] Loi khi ghi DB cho AQI: {e}")
                            import traceback
                            traceback.print_exc()

                    print(f"  Da ghi {inserted_count} sensors vao Postgres")

                    print(f"\n[INFO] Kafka Info:")
                    print(f"  Partition: {message.partition}, Offset: {message.offset}")
                    print(f"{'='*80}")

        except KeyboardInterrupt:
            print("\n[OK] Dung consumer.")

        finally:
            self.close()

    def close(self):
        """Dong ket noi Consumer"""
        print("\nDang dong Consumer...")
        self.consumer.close()
        print("[OK] Consumer da dong.")


def main():
    """Main function"""
   
    consumer = AirQualityConsumer()

  
    consumer.start_consuming()


if __name__ == '__main__':
    main()
