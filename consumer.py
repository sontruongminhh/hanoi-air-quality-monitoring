
"""
Kafka Consumer - Nhận dữ liệu chất lượng không khí từ Kafka
"""

import json
import os
from datetime import datetime, timezone
from email.mime.text import MIMEText
import smtplib
import time
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

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
        replacements = {
            'pm2.5': 'pm25', 'pm 2.5': 'pm25', 'pm_2_5': 'pm25',
            'pm10': 'pm10', 'pm 10': 'pm10',
            'nitrogen dioxide': 'no2', 'no2': 'no2',
            'ozone': 'o3', 'o3': 'o3',
            'carbon monoxide': 'co', 'co': 'co',
            'sulfur dioxide': 'so2', 'so2': 'so2'
        }
        return replacements.get(text, text)

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

    def _should_alert_and_record(self, cursor, row: dict) -> bool:
        # Dedup strict by timestamp: if any row exists for this key, skip alert
        cursor.execute(
            """
            SELECT 1 FROM aqi_readings
            WHERE location_id = %s AND parameter = %s AND measurement_time = %s
            LIMIT 1
            """,
            (row['location_id'], row['parameter'], row['measurement_time']),
        )
        if cursor.fetchone():
            return False
        threshold = self.thresholds.get(row['parameter'])
        if threshold is None or row['value'] is None:
            return False
        return float(row['value']) > float(threshold)

    def _insert_reading(self, cursor, row: dict, alerted: bool):
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

    def _send_email(self, subject: str, body: str):
        if not (self.smtp_host and self.smtp_user and self.smtp_password and self.alert_email_to):
            print("⚠ Bỏ qua gửi email: chưa cấu hình SMTP/EMAIL env vars")
            return
        msg = MIMEText(body, _charset='utf-8')
        msg['Subject'] = subject
        msg['From'] = self.alert_email_from
        msg['To'] = self.alert_email_to
        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.sendmail(self.alert_email_from, [self.alert_email_to], msg.as_string())
        print("✓ Đã gửi email cảnh báo")

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
                            should_alert = self._should_alert_and_record(cursor, row)
                            self._insert_reading(cursor, row, alerted=should_alert)
                            inserted_count += 1
                            if should_alert:
                                threshold = self.thresholds.get(param_norm)
                                subject = f"AQI Alert: {param_norm.upper()} vượt ngưỡng tại {base['location_name']}"
                                body = (
                                    f"Thông số: {param_norm.upper()}\n"
                                    f"Giá trị: {value} {unit} (ngưỡng: {threshold})\n"
                                    f"Thời gian (UTC): {measurement_time.isoformat()}\n"
                                    f"Vị trí: {base['location_name']} ({base['locality']}, {base['country']})\n"
                                    f"Tọa độ: {base['latitude']}, {base['longitude']}\n"
                                )
                                self._send_email(subject, body)
                        except Exception as e:
                            print(f"✗ Lỗi khi ghi DB cho sensor {param_norm}: {e}")

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
