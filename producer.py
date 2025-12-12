
"""
Kafka Producer - Đẩy dữ liệu chất lượng không khí từ IQAir API lên Kafka
"""

import json
import time
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from dotenv import load_dotenv


load_dotenv()


class IQAirProducer:
    def __init__(self):
        """
        Khởi tạo Kafka Producer với cấu hình từ file .env
        """
        
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
  
        self.topic = os.getenv('KAFKA_TOPIC', 'aqi.hanoi.raw')

     
        self.api_key = os.getenv('IQAIR_API_KEY', '')
        self.city = os.getenv('IQAIR_CITY', 'Hanoi')
        self.state = os.getenv('IQAIR_STATE', 'Hanoi')
        self.country = os.getenv('IQAIR_COUNTRY', 'Vietnam')
        self.api_url = "https://api.airvisual.com/v2/city"

       
        self.pollutant_map = {
            "p2": "pm25",
            "p1": "pm10",
            "o3": "o3",
            "n2": "no2",
            "s2": "so2",
            "co": "co",
        }
        
        self.pollutant_display_map = {
            "p2": "PM2.5",
            "p1": "PM10",
            "o3": "O₃ mass",
            "n2": "NO₂ mass",
            "s2": "SO₂ mass",
            "co": "CO mass",
        }

       
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )

        print(f" Producer đã kết nối tới Kafka: {self.bootstrap_servers}")
        print(f" Topic: {self.topic}")
        print(f" IQAir API: {self.api_url}")
        print(f" Location: {self.city}, {self.state}, {self.country}")

    def fetch_air_quality_data(self):
        """
        Gọi IQAir API để lấy dữ liệu chất lượng không khí (AQI)

        Returns:
            dict: Dữ liệu AQI từ IQAir API hoặc None
        """
        try:
            params = {
                "city": self.city,
                "state": self.state,
                "country": self.country,
                "key": self.api_key,
            }

            print(f" Đang gọi IQAir API...")
            print(f"URL: {self.api_url}")
            print(f"City: {self.city}, State: {self.state}, Country: {self.country}")
            
            response = requests.get(self.api_url, params=params, timeout=10)
            
            if response.status_code != 200:
                print(f" Status Code: {response.status_code}")
                print(f" Response: {response.text}")
                return None

            response.raise_for_status()
            data = response.json()

            if data.get("status") != "success":
                print(f" IQAir API error: {data}")
                return None

            print(f" Đã lấy dữ liệu AQI từ IQAir API")
            return data

        except requests.exceptions.RequestException as e:
            print(f" Lỗi khi gọi IQAir API: {e}")
            return None
        except Exception as e:
            print(f" Lỗi không xác định: {e}")
            import traceback
            traceback.print_exc()
            return None

    def process_iqair_data(self, iqair_data):
        """
        Xử lý dữ liệu từ IQAir API thành format phù hợp với consumer

        Args:
            iqair_data: Dữ liệu từ IQAir API

        Returns:
            dict: Dữ liệu đã được xử lý
        """
        data_info = iqair_data.get("data", {})
        pollution = data_info.get("current", {}).get("pollution", {})
        location_obj = data_info.get("location", {})
        coordinates = location_obj.get("coordinates", []) 
        
        
        ts = pollution.get("ts")
        current_time = datetime.now()
        if isinstance(ts, str):
            try:
                
                measurement_time = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                
                if measurement_time.tzinfo is None:
                    measurement_time = measurement_time.replace(tzinfo=timezone.utc)
                print(f"   → Timestamp từ API: {ts} (parsed: {measurement_time.isoformat()})")
                print(f"   → Thời điểm gọi API: {current_time.isoformat()}")
            except:
                measurement_time = datetime.now(timezone.utc)
                print(f"   ⚠ Không parse được timestamp từ API, dùng thời điểm hiện tại: {measurement_time.isoformat()}")
        else:
            # Nếu là Unix timestamp
            measurement_time = datetime.fromtimestamp(ts) if ts else datetime.now()
            print(f"   → Timestamp từ API (Unix): {ts} (parsed: {measurement_time.isoformat()})")
            print(f"   → Thời điểm gọi API: {current_time.isoformat()}")
        
        aqi_us = pollution.get("aqius")
        main_us = pollution.get("mainus")  
        
        
        main_param = self.pollutant_map.get(main_us, main_us)
        main_display = self.pollutant_display_map.get(main_us, main_us.upper())
        
       
        processed_data = {
            'timestamp': datetime.now().isoformat(),
            'location_id': f"iqair_{self.city}_{self.state}".lower().replace(' ', '_'),
            'location_name': f"{data_info.get('city', self.city)}, {data_info.get('state', self.state)}",
            'locality': data_info.get('city', self.city),
            'country': data_info.get('country', self.country),
            'country_code': self._get_country_code(data_info.get('country', self.country)),
            'coordinates': {
                'latitude': coordinates[1] if len(coordinates) >= 2 else None,
                'longitude': coordinates[0] if len(coordinates) >= 1 else None
            },
            'is_mobile': False,
            'is_monitor': True,
            'sensors': []
        }
        
        
        
        sensor_data = {
            'id': 'iqair_aqi',
            'name': 'AQI (Air Quality Index)',
            'parameter': 'aqi', 
            'parameter_display': 'AQI',
            'unit': 'AQI',
            'latest_value': aqi_us,
            'latest_datetime': measurement_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'latest_datetime_local': measurement_time.isoformat(),
            'main_pollutant': main_param,  
            'main_pollutant_display': main_display
        }
        processed_data['sensors'].append(sensor_data)
        
        print(f"    Đã xử lý dữ liệu: AQI={aqi_us}, Main pollutant={main_display}")
        
        return processed_data
    
    def _get_country_code(self, country_name):
        """Map country name sang country code"""
        country_map = {
            "Vietnam": "VN",
            "United States": "US",
            "China": "CN",
            "Thailand": "TH",
            "Japan": "JP",
        }
        return country_map.get(country_name, country_name[:2].upper())

    def send_message(self, data, key=None):
        """
        Gửi message lên Kafka

        Args:
            data: Dữ liệu cần gửi
            key: Key của message (optional)
        """
        try:
            future = self.producer.send(self.topic, key=key, value=data)
            record_metadata = future.get(timeout=10)

            print(f"✓ Đã gửi: {data.get('location_name', 'Unknown')} - {len(data.get('sensors', []))} sensors")
            print(f"  → Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        except KafkaError as e:
            print(f"✗ Lỗi khi gửi message: {e}")

    def start_streaming(self, interval=60, count=None):
        """
        Bắt đầu đẩy dữ liệu liên tục từ OpenAQ API

        Args:
            interval: Khoảng thời gian giữa các lần gọi API (giây)
            count: Số lần gọi API (None = vô hạn)
        """
        print(f"\n{'='*80}")
        print(f"BẮT ĐẦU STREAMING DỮ LIỆU TỪ IQAir API")
        print(f"{'='*80}")
        print(f"Interval: {interval} giây")
        print(f"Count: {'Vô hạn' if count is None else count}")
        print(f"{'='*80}\n")

        sent_count = 0

        try:
            while True:
                
                iqair_data = self.fetch_air_quality_data()

                if iqair_data:
                    processed_data = self.process_iqair_data(iqair_data)
                    key = str(processed_data['location_id'])
                    self.send_message(processed_data, key=key)

                    print(f"\n{'='*80}")
                    print(f"✓ Đã gửi dữ liệu AQI lên Kafka")
                    print(f"{'='*80}\n")
                else:
                    print(" Không có dữ liệu để gửi")

                sent_count += 1

               
                if count is not None and sent_count >= count:
                    break

                
                if count is None or sent_count < count:
                    print(f" Chờ {interval} giây trước lần gọi API tiếp theo...")
                    time.sleep(interval)

        except KeyboardInterrupt:
            print("\n✓ Dừng producer.")

        finally:
            self.close()

        print(f"\n✓ Hoàn thành! Đã gửi {sent_count} lần.")

    def close(self):
        """Đóng kết nối Producer"""
        print("\nĐang đóng Producer...")
        self.producer.flush()
        self.producer.close()
        print("✓ Producer đã đóng.")


def main():
    """Main function"""
 
    producer = IQAirProducer()

    
    
    producer.start_streaming(interval=600, count=None)


if __name__ == '__main__':
    main()
