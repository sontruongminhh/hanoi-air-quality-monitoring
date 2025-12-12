
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
from util_email import send_aqi_alert_email, send_aqi_alert_email_summary, calculate_aqi_from_value

# Load environment variables
load_dotenv()


class AirQualityConsumer:
    def __init__(self):
        """
        Khởi tạo Kafka Consumer với cấu hình từ file .env
        """
    
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        # Đặt tên topic mặc định theo sơ đồ: aqi.hanoi.raw
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
        print(f"✓ Consumer đã kết nối tới Kafka: {self.bootstrap_servers}")
        print(f"✓ Topic: {self.topic}")
        print(f"✓ Group ID: {group_id}")

        # Postgres config
        self.pg_host = os.getenv('POSTGRES_HOST', 'localhost')
        self.pg_port = int(os.getenv('POSTGRES_PORT', '5432'))
        self.pg_db = os.getenv('POSTGRES_DB', 'aqi')
        self.pg_user = os.getenv('POSTGRES_USER', 'aqi_user')
        self.pg_password = os.getenv('POSTGRES_PASSWORD', 'aqi_password')

        # Email config
        self.smtp_host = os.getenv('SMTP_HOST')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.alert_email_to = os.getenv('ALERT_EMAIL_TO')
        self.alert_email_from = os.getenv('ALERT_EMAIL_FROM', 'alerts@example.com')

      
        self.aqi_threshold = float(os.getenv('THRESHOLD_AQI', '150'))

        self.pg_conn = self._init_pg()
        print("✓ Kết nối Postgres thành công")

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
            print(f"  ⚠ Lỗi parse datetime '{dt_str}': {e}")
            return None

    def _evaluate_reading(self, cursor, row: dict, unit: str = None):
        """
        Kiểm tra điều kiện để quyết định việc gửi cảnh báo theo logic mới:
        - Khung giờ 0h-1h sáng và 17h-18h chiều: so sánh với ngưỡng tiêu chuẩn (150) và gửi cảnh báo nếu vượt (chỉ 1 lần mỗi khung giờ)
        - Ngoài khung giờ: chỉ cảnh báo khi AQI > 201 VÀ tăng so với lần trước

        Args:
            cursor: Database cursor
            row: Row data với location_id, parameter='aqi', value, measurement_time
            unit: Unit của giá trị (phải là 'AQI')

        Returns:
            tuple(bool, bool, datetime | None):
                - is_newer: luôn True (vì luôn insert mỗi lần gọi)
                - should_alert: có nên gửi cảnh báo theo logic mới
                - prev_time: measurement_time gần nhất trước đó (nếu có)
        """
        
        cursor.execute(
            """
            SELECT measurement_time, value, created_at
            FROM aqi_readings
            WHERE location_id = %s AND parameter = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (row['location_id'], row['parameter']),
        )
        prev = cursor.fetchone()
        prev_time = prev[0] if prev else None
        prev_value = prev[1] if prev else None

        measurement_time = row['measurement_time']
        value = row['value']
        
        if value is None:
            return True, False, prev_time
        
        value_float = float(value)
        threshold_standard = self.aqi_threshold  
        
        threshold_very_unhealthy = 250
        
      
        current_time = datetime.now(timezone.utc)
        local_tz = timezone(timedelta(hours=7))
        current_time_local = current_time.astimezone(local_tz)
        current_hour_local = current_time_local.hour
        current_date_local = current_time_local.date()
        
      
        is_alert_hour = (7 <= current_hour_local < 8) or (17 <= current_hour_local < 18)
        
      
        print(
            f"  → DEBUG: Giờ hiện tại (UTC) = {current_time.hour}:{current_time.minute}, "
            f"Giờ địa phương (UTC+7) = {current_hour_local}:{current_time_local.minute}, "
            f"is_alert_hour = {is_alert_hour}, AQI = {value_float}, threshold = {threshold_standard}"
        )
        
        should_alert = False
        
        if is_alert_hour:
            
            if value_float > threshold_standard:
              
                if 7 <= current_hour_local < 8:
                    hour_start_local, hour_end_local = 7, 8
                else:  
                    hour_start_local, hour_end_local = 17, 18
                
               
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
                    
                    should_alert = True
                    print(f"  →  KHUNG GIỜ CẢNH BÁO ({hour_start_local}h-{hour_end_local}h, giờ địa phương): AQI={value_float} > {threshold_standard} - CẢNH BÁO LẦN ĐẦU TRONG KHUNG GIỜ")
                else:
                    
                    print(f"  → AQI={value_float} > {threshold_standard} nhưng đã cảnh báo trong khung giờ {hour_start_local}h-{hour_end_local}h (giờ địa phương) hôm nay rồi (tránh spam)")
        else:
           
            if value_float > threshold_very_unhealthy:
                if prev_value is not None:
                    prev_value_float = float(prev_value)
                   
                    if value_float > prev_value_float and value_float > threshold_very_unhealthy:
                        should_alert = True
                        print(f"  →  CẢNH BÁO NGOÀI KHUNG GIỜ: AQI={value_float} > {threshold_very_unhealthy} VÀ tăng so với lần trước ({prev_value_float})")
                    else:
                        print(f"  → AQI={value_float} > {threshold_very_unhealthy} nhưng không tăng so với lần trước ({prev_value_float}), không gửi cảnh báo (tránh spam)")
                else:
                  
                    if value_float > threshold_very_unhealthy:
                        should_alert = True
                        print(f"  →  CẢNH BÁO LẦN ĐẦU: AQI={value_float} > {threshold_very_unhealthy}")
        
        return True, should_alert, prev_time

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
                            print(f"  → Bỏ qua sensor {sensor.get('parameter')}: không phải AQI (unit={unit})")
                            continue
                        
                        measurement_time = self._parse_utc(sensor.get('latest_datetime'))
                        if measurement_time is None:
                            print(f"  →  latest_datetime từ sensor không parse được: {sensor.get('latest_datetime')}")
                            measurement_time = self._parse_utc(data.get('timestamp'))
                            if measurement_time is None:
                                print(f"  →  timestamp tổng cũng không parse được, dùng datetime.now()")
                                measurement_time = datetime.now(timezone.utc)
                            else:
                                print(f"  →  Dùng timestamp tổng (thời điểm gọi API): {measurement_time.isoformat()}")
                        else:
                            print(f"  →  Dùng latest_datetime từ sensor (thời điểm đo từ API): {measurement_time.isoformat()}")
                        
                      
                        param_norm = 'aqi'
                        
                        
                        main_pollutant = sensor.get('main_pollutant_display') or sensor.get('main_pollutant', None)

                        row = {
                            **base,
                            'parameter': param_norm,
                            'unit': unit,
                            'value': value,
                            'measurement_time': measurement_time,
                            'main_pollutant': main_pollutant,  # Thêm main_pollutant vào row
                        }

                        try:
                           
                            is_newer, should_alert, prev_time = self._evaluate_reading(cursor, row, unit)
                            
                            value_float = float(value) if value is not None else None
                            
                            print(f"  → AQI: {value_float}, should_alert={should_alert}")
                            print(f"  → Chất ô nhiễm chính: {main_pollutant}")
                            
                            
                            inserted = self._insert_reading(cursor, row, alerted=should_alert)
                            if inserted:
                                inserted_count += 1
                                print(f"  →  Đã ghi vào DB (ID mới)")
                            else:
                                print(f"  →  Không ghi được vào DB (có thể do lỗi)")

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
                                print(f"  →  PHÁT HIỆN VƯỢT NGƯỠNG! {param_display}: {aqi_value} ({aqi_level})")

                              
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
                                    print("  → ✓ Đã gửi email cảnh báo đơn")
                                except Exception as email_error:
                                    print(f"  → ✗ Lỗi khi gửi email: {email_error}")
                                    import traceback
                                    traceback.print_exc()
                        except Exception as e:
                            print(f"✗ Lỗi khi ghi DB cho AQI: {e}")
                            import traceback
                            traceback.print_exc()

                    print(f"  → Đã ghi {inserted_count} sensors vào Postgres")

                    print(f"\n Kafka Info:")
                    print(f"  → Partition: {message.partition}, Offset: {message.offset}")
                    print(f"{'='*80}")

        except KeyboardInterrupt:
            print("\n✓ Dừng consumer.")

        finally:
            self.close()

    def close(self):
        """Đóng kết nối Consumer"""
        print("\nĐang đóng Consumer...")
        self.consumer.close()
        print("✓ Consumer đã đóng.")


def main():
    """Main function"""
   
    consumer = AirQualityConsumer()

  
    consumer.start_consuming()


if __name__ == '__main__':
    main()
