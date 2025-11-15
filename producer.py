
"""
Kafka Producer - Đẩy dữ liệu chất lượng không khí từ OpenAQ API lên Kafka
"""

import json
import time
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from dotenv import load_dotenv


load_dotenv()


class OpenAQProducer:
    def __init__(self):
        """
        Khởi tạo Kafka Producer với cấu hình từ file .env
        """
        
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'air-quality-topic')

        self.api_url = os.getenv('OPENAQ_API_URL', 'https://api.openaq.org/v3/locations')
        self.api_key = os.getenv('OPENAQ_API_KEY', '')
        self.location_id = os.getenv('OPENAQ_LOCATION_ID', '')
        self.limit = int(os.getenv('OPENAQ_LIMIT', '100'))
        self.country = os.getenv('OPENAQ_COUNTRY', 'VN')
        self.radius = int(os.getenv('OPENAQ_RADIUS', '25000'))
        self.coordinates = os.getenv('OPENAQ_COORDINATES', '')

       
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )

        print(f"✓ Producer đã kết nối tới Kafka: {self.bootstrap_servers}")
        print(f"✓ Topic: {self.topic}")
        print(f"✓ OpenAQ API: {self.api_url}")
        if self.location_id:
            print(f"✓ Location ID: {self.location_id}")
        else:
            print(f"✓ Country filter: {self.country if self.country else 'None'}")

    def fetch_air_quality_data(self):
        """
        Gọi OpenAQ API v3 để lấy dữ liệu chất lượng không khí

        Returns:
            list: Danh sách dữ liệu từ API
        """
        try:
            
            headers = {}
            if self.api_key:
                headers['X-API-Key'] = self.api_key

            
            if self.location_id:
                api_url = f"{self.api_url}/{self.location_id}"
                params = {}

                print(f" Đang gọi OpenAQ API cho location ID: {self.location_id}...")
                print(f"URL: {api_url}")
                response = requests.get(api_url, headers=headers, params=params, timeout=30)

              
                if response.status_code != 200:
                    print(f"✗ Status Code: {response.status_code}")
                    print(f"✗ Response: {response.text}")

                response.raise_for_status()
                data = response.json()

                if 'results' in data and len(data['results']) > 0:
                    location = data['results'][0]
                    print(f"✓ Đã lấy dữ liệu từ trạm: {location.get('name')}")
                    return [location]
                else:
                    print("⚠ Không có dữ liệu từ API")
                    return []

            
            else:
              
                params = {
                    'limit': self.limit
                }

                
                if self.country:
                    params['countries'] = self.country

                
                if self.coordinates:
                    params['coordinates'] = self.coordinates
                    params['radius'] = self.radius

               
                print(f" Đang gọi OpenAQ API...")
                print(f"URL: {self.api_url}")
                print(f"Params: {params}")
                response = requests.get(self.api_url, headers=headers, params=params, timeout=30)

               
                if response.status_code != 200:
                    print(f"✗ Status Code: {response.status_code}")
                    print(f"✗ Response: {response.text}")

                response.raise_for_status()

                data = response.json()

                if 'results' in data and len(data['results']) > 0:
                    print(f"✓ Đã lấy {len(data['results'])} locations từ OpenAQ API")

                    
                    filtered_results = [
                        loc for loc in data['results']
                        if 'bách khoa' in loc.get('name', '').lower() or
                           'bach khoa' in loc.get('name', '').lower()
                    ]

                    if filtered_results:
                        print(f"✓ Lọc được {len(filtered_results)} trạm Bách Khoa")
                        for loc in filtered_results:
                            print(f"  - {loc.get('name')}")
                        return filtered_results
                    else:
                        print(" Không tìm thấy trạm Bách Khoa")
                        return []
                else:
                    print(" Không có dữ liệu từ API")
                    return []

        except requests.exceptions.RequestException as e:
            print(f"✗ Lỗi khi gọi OpenAQ API: {e}")
            return []
        except Exception as e:
            print(f"✗ Lỗi không xác định: {e}")
            return []

    def fetch_latest_measurements_for_sensor(self, sensor_id):
        """
        Lấy giá trị đo mới nhất cho một sensor cụ thể

        Args:
            sensor_id: ID của sensor

        Returns:
            dict: Dữ liệu đo mới nhất hoặc None
        """
        try:
            headers = {}
            if self.api_key:
                headers['X-API-Key'] = self.api_key

       
            url = f"https://api.openaq.org/v3/sensors/{sensor_id}/measurements"
            params = {
                'limit': 1,
                'order_by': 'datetime',
                'sort': 'desc'
            }

            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 200:
                data = response.json()
                if data.get('results') and len(data['results']) > 0:
                    return data['results'][0]

            return None

        except Exception as e:
            print(f"  ⚠ Không lấy được measurements cho sensor {sensor_id}: {e}")
            return None

    def process_location_data(self, location):
        """
        Xử lý dữ liệu từ một location thành format phù hợp

        Args:
            location: Dữ liệu location từ API

        Returns:
            dict: Dữ liệu đã được xử lý
        """
        processed_data = {
            'timestamp': datetime.now().isoformat(),
            'location_id': location.get('id'),
            'location_name': location.get('name'),
            'locality': location.get('locality'),
            'country': location.get('country', {}).get('name'),
            'country_code': location.get('country', {}).get('code'),
            'coordinates': {
                'latitude': location.get('coordinates', {}).get('latitude'),
                'longitude': location.get('coordinates', {}).get('longitude')
            },
            'is_mobile': location.get('isMobile', False),
            'is_monitor': location.get('isMonitor', False),
            'sensors': []
        }

       
        if 'sensors' in location:
            print(f"   Đang lấy measurements cho {len(location['sensors'])} sensors...")
            for sensor in location['sensors']:
               
                latest_measurement = self.fetch_latest_measurements_for_sensor(sensor.get('id'))

                sensor_data = {
                    'id': sensor.get('id'),
                    'name': sensor.get('name'),
                    'parameter': sensor.get('parameter', {}).get('name'),
                    'parameter_display': sensor.get('parameter', {}).get('displayName'),
                    'unit': sensor.get('parameter', {}).get('units'),
                    'latest_value': latest_measurement.get('value') if latest_measurement else None,
                    'latest_datetime': latest_measurement.get('date', {}).get('utc') if latest_measurement else None,
                    'latest_datetime_local': latest_measurement.get('date', {}).get('local') if latest_measurement else None
                }
                processed_data['sensors'].append(sensor_data)

        return processed_data

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
        print(f"BẮT ĐẦU STREAMING DỮ LIỆU TỪ OPENAQ API")
        print(f"{'='*80}")
        print(f"Interval: {interval} giây")
        print(f"Count: {'Vô hạn' if count is None else count}")
        print(f"{'='*80}\n")

        sent_count = 0

        try:
            while True:
                
                locations = self.fetch_air_quality_data()

                if locations:
                   
                    for location in locations:
                        processed_data = self.process_location_data(location)
                        key = str(processed_data['location_id'])
                        self.send_message(processed_data, key=key)

                    print(f"\n{'='*80}")
                    print(f"✓ Đã gửi {len(locations)} locations lên Kafka")
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
 
    producer = OpenAQProducer()

    # Bắt đầu streaming dữ liệu
    # interval=60 -> Gọi API mỗi 60 giây
    # count=None -> Chạy vô hạn cho đến khi nhấn Ctrl+C
    producer.start_streaming(interval=600, count=None)


if __name__ == '__main__':
    main()
