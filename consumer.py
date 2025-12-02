
"""
Kafka Consumer - Nhận dữ liệu chất lượng không khí từ Kafka
"""

import json
import os
from datetime import datetime, timezone
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
        self.topic = os.getenv('KAFKA_TOPIC', 'air-quality-topic')
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

        # Thresholds (ug/m3)
        self.thresholds = {
            'pm25': float(os.getenv('THRESHOLD_PM25', '15')),
            'pm10': float(os.getenv('THRESHOLD_PM10', '45')),
            'no2': float(os.getenv('THRESHOLD_NO2', '25')),
            'o3': float(os.getenv('THRESHOLD_O3', '100')),
            'co': float(os.getenv('THRESHOLD_CO', '4000')),
            'so2': float(os.getenv('THRESHOLD_SO2', '40')),
        }

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
        # Loại bỏ các từ thừa như "mass", "concentration", etc.
        text = text.replace(' mass', '').replace(' concentration', '').replace('_', ' ').replace('.', ' ').strip()
        
        # Mapping các parameter - kiểm tra chính xác trước
        if text == 'pm25' or text == 'pm2.5' or text == 'pm 2.5':
            return 'pm25'
        elif text == 'pm10' or text == 'pm 10':
            return 'pm10'
        elif text == 'no2' or 'nitrogen dioxide' in text:
            return 'no2'
        elif text == 'o3' or 'ozone' in text:
            return 'o3'
        elif text == 'co' or 'carbon monoxide' in text:
            return 'co'
        elif text == 'so2' or 'sulfur dioxide' in text:
            return 'so2'
        
        # Fallback: trả về text đã clean (loại bỏ khoảng trắng)
        cleaned = text.replace(' ', '').replace('.', '')
        # Nếu cleaned là một trong các giá trị hợp lệ, trả về
        if cleaned in ['pm25', 'pm10', 'no2', 'o3', 'co', 'so2']:
            return cleaned
        return cleaned

    def _parse_utc(self, dt_str: str) -> datetime:
        if not dt_str:
            return None
        try:
            # OpenAQ returns ISO8601, sometimes with Z
            if dt_str.endswith('Z'):
                return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            return datetime.fromisoformat(dt_str).astimezone(timezone.utc)
        except Exception:
            return None

    def _evaluate_reading(self, cursor, row: dict):
        """
        Kiểm tra timestamp trước đó để quyết định việc gửi cảnh báo

        Returns:
            tuple(bool, bool, datetime | None):
                - is_newer: measurement_time có mới hơn lần trước không
                - should_alert: có nên gửi cảnh báo (chỉ khi mới và vượt ngưỡng)
                - prev_time: measurement_time gần nhất trước đó (nếu có)
        """
        cursor.execute(
            """
            SELECT measurement_time
            FROM aqi_readings
            WHERE location_id = %s AND parameter = %s
            ORDER BY measurement_time DESC
            LIMIT 1
            """,
            (row['location_id'], row['parameter']),
        )
        prev = cursor.fetchone()
        prev_time = prev[0] if prev else None

        measurement_time = row['measurement_time']
        is_newer = not prev_time or measurement_time > prev_time

        threshold = self.thresholds.get(row['parameter'])
        value = row['value']

        if threshold is None or value is None:
            return is_newer, False, prev_time

        exceeds = float(value) > float(threshold)
        should_alert = is_newer and exceeds
        return is_newer, should_alert, prev_time

    def _insert_reading(self, cursor, row: dict, alerted: bool) -> bool:
        cursor.execute(
            """
            INSERT INTO aqi_readings (
                location_id, location_name, locality, country, country_code,
                latitude, longitude, provider,
                parameter, unit, value, measurement_time,
                alerted, alert_sent_at
            ) VALUES (
                %(location_id)s, %(location_name)s, %(locality)s, %(country)s, %(country_code)s,
                %(latitude)s, %(longitude)s, %(provider)s,
                %(parameter)s, %(unit)s, %(value)s, %(measurement_time)s,
                %(alerted)s, CASE WHEN %(alerted)s THEN NOW() ELSE NULL END
            )
            ON CONFLICT (location_id, parameter, measurement_time) DO NOTHING
            """,
            {**row, 'alerted': alerted},
        )
        return cursor.rowcount > 0

    def _send_email(self, subject: str, param_name: str, value: float, unit: str, 
                     threshold: float, location_name: str, locality: str, country: str,
                     latitude: float, longitude: float, measurement_time: datetime):
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
            measurement_time=measurement_time
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

                    # Prepare common fields
                    base = {
                        'location_id': data.get('location_id'),
                        'location_name': data.get('location_name'),
                        'locality': data.get('locality'),
                        'country': data.get('country'),
                        'country_code': data.get('country_code'),
                        'latitude': (data.get('coordinates') or {}).get('latitude'),
                        'longitude': (data.get('coordinates') or {}).get('longitude'),
                        'provider': 'OpenAQ',
                    }

                    sensors = data.get('sensors', []) or []
                    inserted_count = 0
                    # Danh sách các sensor vượt ngưỡng để gửi email tổng hợp
                    alert_sensors = []  # List of (sensor_info, aqi_value)
                    
                    for sensor in sensors:
                        value = sensor.get('latest_value')
                        unit = sensor.get('unit')
                        measurement_time = self._parse_utc(sensor.get('latest_datetime'))
                        # Fallback: dùng timestamp tổng nếu sensor không có thời gian riêng
                        if measurement_time is None:
                            measurement_time = self._parse_utc(data.get('timestamp')) or datetime.now(timezone.utc)
                        param_norm = self._normalize_param(sensor.get('parameter'), sensor.get('parameter_display'))

                        row = {
                            **base,
                            'parameter': param_norm,
                            'unit': unit,
                            'value': value,
                            'measurement_time': measurement_time,
                        }

                        try:
                            is_newer, should_alert, prev_time = self._evaluate_reading(cursor, row)
                            
                            # Debug logging
                            threshold = self.thresholds.get(param_norm)
                            value_float = float(value) if value is not None else None
                            exceeds = threshold is not None and value_float is not None and value_float > float(threshold)
                            print(f"  → Sensor {param_norm}: value={value_float}, threshold={threshold}, exceeds={exceeds}, is_newer={is_newer}, should_alert={should_alert}")
                            
                            inserted = self._insert_reading(cursor, row, alerted=should_alert)
                            if inserted:
                                inserted_count += 1
                            else:
                                print(f"  → Bỏ qua ghi DB cho sensor {param_norm}: đã tồn tại bản ghi trùng")

                            if not is_newer:
                                prev_str = prev_time.isoformat() if prev_time else 'N/A'
                                print(f"  → Sensor {param_norm}: measurement_time {measurement_time.isoformat()} "
                                      f"không mới hơn lần trước ({prev_str}), chỉ lưu lịch sử")

                            if should_alert:
                                # Tính AQI để tìm sensor có mức độ nguy hiểm cao nhất
                                param_for_aqi = param_norm
                                aqi, aqi_level = calculate_aqi_from_value(value_float, param_for_aqi)
                                
                                # Lấy tên hiển thị từ sensor, nếu không có thì tạo từ param_norm
                                param_display = sensor.get('parameter_display')
                                if not param_display:
                                    # Tạo tên hiển thị đẹp từ param_norm
                                    display_map = {
                                        'pm25': 'PM2.5',
                                        'pm10': 'PM10',
                                        'no2': 'NO₂ mass',
                                        'o3': 'O₃ mass',
                                        'co': 'CO mass',
                                        'so2': 'SO₂ mass'
                                    }
                                    param_display = display_map.get(param_norm, param_norm.upper())
                                
                                alert_sensors.append({
                                    'param_display': param_display,
                                    'value': value_float,
                                    'unit': unit or 'µg/m³',
                                    'threshold': float(threshold),
                                    'aqi': aqi,
                                    'aqi_level': aqi_level,
                                    'measurement_time': measurement_time
                                })
                                print(f"  → ⚠️ PHÁT HIỆN VƯỢT NGƯỠNG! {param_display}: {value_float} {unit or 'µg/m³'} (AQI={aqi})")
                        except Exception as e:
                            print(f"✗ Lỗi khi ghi DB cho sensor {param_norm}: {e}")
                            import traceback
                            traceback.print_exc()

                    # Gửi 1 email tổng hợp cho tất cả các sensor vượt ngưỡng (nếu có)
                    if alert_sensors:
                        # Sắp xếp theo AQI giảm dần để hiển thị thông số nguy hiểm nhất trước
                        alert_sensors.sort(key=lambda x: x['aqi'], reverse=True)
                        
                        # Lấy measurement_time từ sensor đầu tiên (hoặc từ data chung)
                        measurement_time_for_email = alert_sensors[0]['measurement_time']
                        if not measurement_time_for_email:
                            measurement_time_for_email = self._parse_utc(data.get('timestamp')) or datetime.now(timezone.utc)
                        
                        num_exceeded = len(alert_sensors)
                        max_aqi = alert_sensors[0]['aqi']
                        
                        print(f"  → ⚠️ GỬI EMAIL CẢNH BÁO TỔNG HỢP: {num_exceeded} thông số vượt ngưỡng, AQI cao nhất = {max_aqi}")
                        try:
                            send_aqi_alert_email_summary(
                                alert_email_to=self.alert_email_to,
                                location_name=base['location_name'],
                                locality=base['locality'] or 'N/A',
                                country=base['country'] or 'N/A',
                                latitude=base['latitude'] or 0.0,
                                longitude=base['longitude'] or 0.0,
                                measurement_time=measurement_time_for_email,
                                alert_sensors=alert_sensors
                            )
                            print(f"  → ✓ Đã gửi email cảnh báo tổng hợp ({num_exceeded} thông số vượt ngưỡng)")
                        except Exception as email_error:
                            print(f"  → ✗ Lỗi khi gửi email: {email_error}")
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
