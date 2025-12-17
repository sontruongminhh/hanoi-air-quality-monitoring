"""
Kafka S3 Consumer - Đọc dữ liệu từ Kafka và đẩy lên S3 (BRONZE layer)
"""

import json
import os
import gzip
from datetime import datetime, timezone
from io import BytesIO
import time
from kafka import KafkaConsumer
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()


class S3Consumer:
    def __init__(self):
        """
        Khởi tạo Kafka Consumer và S3 client
        """
        # Kafka config
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.topic = os.getenv('KAFKA_TOPIC', 'air-quality-topic')
        group_id = 's3-consumer-group'  # Khác với alert consumer group

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        print(f"[OK] S3 Consumer da ket noi toi Kafka: {self.bootstrap_servers}")
        print(f"[OK] Topic: {self.topic}")
        print(f"[OK] Group ID: {group_id}")

        # S3 config
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_region = os.getenv('S3_REGION', 'us-east-1')
        self.s3_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.s3_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')  # Cho S3-compatible storage (MinIO, etc.)
        
        # Batch config
        self.batch_size = int(os.getenv('S3_BATCH_SIZE', '100'))  # Số messages mỗi batch
        self.batch_timeout = int(os.getenv('S3_BATCH_TIMEOUT', '300'))  # Timeout (giây) để flush batch
        
        # S3 path prefix
        self.s3_prefix = os.getenv('S3_PREFIX', 'bronze/openaq/raw')

        if not self.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is required")

        # Initialize S3 client
        s3_kwargs = {
            'region_name': self.s3_region,
        }
        
        if self.s3_access_key and self.s3_secret_key:
            s3_kwargs['aws_access_key_id'] = self.s3_access_key
            s3_kwargs['aws_secret_access_key'] = self.s3_secret_key
        
        if self.s3_endpoint_url:
            s3_kwargs['endpoint_url'] = self.s3_endpoint_url

        self.s3_client = boto3.client('s3', **s3_kwargs)
        
        # Test S3 connection
        try:
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            print(f"[OK] Ket noi S3 thanh cong: {self.s3_bucket}")
        except ClientError as e:
            print(f"[WARN] Canh bao: Khong the ket noi S3: {e}")
            print("  Tiep tuc chay nhung co the loi khi ghi file...")

        # Batch storage
        self.batch = []
        self.last_flush_time = time.time()

    def _get_s3_key(self, timestamp_str=None):
        """
        Tạo S3 key dựa trên timestamp (partition theo ngày/giờ)
        
        Format: bronze/openaq/raw/year=YYYY/month=MM/day=DD/hour=HH/data_YYYYMMDD_HHMMSS.json.gz
        """
        if timestamp_str:
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            except:
                dt = datetime.now(timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
        
        year = dt.strftime('%Y')
        month = dt.strftime('%m')
        day = dt.strftime('%d')
        hour = dt.strftime('%H')
        timestamp = dt.strftime('%Y%m%d_%H%M%S')
        
        key = f"{self.s3_prefix}/year={year}/month={month}/day={day}/hour={hour}/data_{timestamp}.json.gz"
        return key

    def _compress_data(self, data):
        """
        Nén dữ liệu JSON thành gzip
        """
        json_str = json.dumps(data, ensure_ascii=False)
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(json_str.encode('utf-8'))
        return buffer.getvalue()

    def _upload_batch_to_s3(self, batch_data, s3_key):
        """
        Upload batch data lên S3
        """
        try:
            # Compress data
            compressed_data = self._compress_data(batch_data)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/json',
                ContentEncoding='gzip',
                Metadata={
                    'record_count': str(len(batch_data)),
                    'uploaded_at': datetime.now(timezone.utc).isoformat()
                }
            )
            
            print(f"[OK] Da upload {len(batch_data)} records len S3: s3://{self.s3_bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            print(f"[ERROR] Loi khi upload len S3: {e}")
            return False
        except Exception as e:
            print(f"[ERROR] Loi khong xac dinh khi upload S3: {e}")
            return False

    def _should_flush_batch(self):
        """
        Kiểm tra xem có nên flush batch không
        """
        # Flush nếu đủ số lượng
        if len(self.batch) >= self.batch_size:
            return True
        
        # Flush nếu quá timeout
        current_time = time.time()
        if len(self.batch) > 0 and (current_time - self.last_flush_time) >= self.batch_timeout:
            return True
        
        return False

    def _flush_batch(self):
        """
        Flush batch hiện tại lên S3
        """
        if not self.batch:
            return
        
        # Lấy timestamp từ message đầu tiên hoặc dùng thời gian hiện tại
        first_timestamp = self.batch[0].get('timestamp')
        s3_key = self._get_s3_key(first_timestamp)
        
        # Upload batch
        success = self._upload_batch_to_s3(self.batch, s3_key)
        
        if success:
            self.batch = []
            self.last_flush_time = time.time()
        else:
            # Neu loi, giu lai batch de retry (co the implement retry logic sau)
            print(f"[WARN] Giu lai {len(self.batch)} messages de retry sau")

    def start_consuming(self):
        """
        Bắt đầu nhận dữ liệu từ Kafka và đẩy lên S3
        """
        print(f"\n{'='*80}")
        print(f"BẮT ĐẦU S3 CONSUMER - ĐẨY DỮ LIỆU LÊN S3 BRONZE")
        print(f"{'='*80}")
        print(f"S3 Bucket: {self.s3_bucket}")
        print(f"S3 Prefix: {self.s3_prefix}")
        print(f"Batch Size: {self.batch_size}")
        print(f"Batch Timeout: {self.batch_timeout}s")
        print(f"{'='*80}\n")

        try:
            for message in self.consumer:
                data = message.value
                
                # Thêm metadata Kafka vào data
                data['_kafka_metadata'] = {
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp,
                    'key': message.key
                }
                
                # Thêm vào batch
                self.batch.append(data)
                
                print(f"[INFO] Da nhan message: {data.get('location_name', 'Unknown')} "
                      f"(Batch: {len(self.batch)}/{self.batch_size})")
                
                # Kiểm tra xem có nên flush batch không
                if self._should_flush_batch():
                    self._flush_batch()
                    print(f"  -> Da flush batch len S3\n")

        except KeyboardInterrupt:
            print("\n[OK] Dung S3 consumer.")
            # Flush batch con lai truoc khi dung
            if self.batch:
                print(f"  -> Dang flush batch cuoi ({len(self.batch)} messages)...")
                self._flush_batch()

        finally:
            self.close()

    def close(self):
        """
        Dong ket noi Consumer
        """
        print("\nDang dong S3 Consumer...")
        # Flush batch cuoi cung
        if self.batch:
            print(f"  -> Dang flush batch cuoi ({len(self.batch)} messages)...")
            self._flush_batch()
        self.consumer.close()
        print("[OK] S3 Consumer da dong.")


def main():
    """Main function"""
    try:
        consumer = S3Consumer()
        consumer.start_consuming()
    except ValueError as e:
        print(f"[ERROR] Loi cau hinh: {e}")
        print("\nVui lòng cấu hình các biến môi trường sau trong file .env:")
        print("  - S3_BUCKET (bắt buộc)")
        print("  - AWS_ACCESS_KEY_ID")
        print("  - AWS_SECRET_ACCESS_KEY")
        print("  - S3_REGION (mặc định: us-east-1)")
        print("  - S3_ENDPOINT_URL (tùy chọn, cho S3-compatible storage)")
        print("  - S3_PREFIX (mặc định: bronze/openaq/raw)")
        print("  - S3_BATCH_SIZE (mặc định: 100)")
        print("  - S3_BATCH_TIMEOUT (mặc định: 300 giây)")


if __name__ == '__main__':
    main()
