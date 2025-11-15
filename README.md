# Kafka Producer/Consumer với OpenAQ API v3

Hệ thống streaming dữ liệu chất lượng không khí từ OpenAQ API v3 vào Apache Kafka.

## 📋 Yêu cầu

- Python 3.7+
- Apache Kafka đang chạy (localhost:9092)
- Kết nối Internet để gọi OpenAQ API

## 🚀 Cài đặt

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Cấu hình file .env

Chỉnh sửa file `.env` với các thông số phù hợp:

```env
# OpenAQ API v3 Configuration
OPENAQ_API_URL=https://api.openaq.org/v3/locations
OPENAQ_API_KEY=                    # Để trống nếu không có API key (optional)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=air-quality-topic

# OpenAQ Query Parameters
OPENAQ_LIMIT=100                   # Số lượng locations tối đa mỗi lần gọi
OPENAQ_COUNTRY=VN                  # Mã quốc gia (VN=Vietnam, US=USA, etc.)
OPENAQ_RADIUS=25000                # Bán kính tìm kiếm (meters)
OPENAQ_COORDINATES=                # Tọa độ (ví dụ: 21.0285,105.8542 cho Hà Nội)

# S3 Configuration (cho S3 Consumer)
S3_BUCKET=your-bucket-name         # Tên S3 bucket (bắt buộc)
S3_REGION=us-east-1                # AWS region (mặc định: us-east-1)
AWS_ACCESS_KEY_ID=your-access-key  # AWS Access Key ID
AWS_SECRET_ACCESS_KEY=your-secret  # AWS Secret Access Key
S3_ENDPOINT_URL=                   # Endpoint URL (tùy chọn, cho S3-compatible storage như MinIO)
S3_PREFIX=bronze/openaq/raw        # Prefix cho S3 path (mặc định: bronze/openaq/raw)
S3_BATCH_SIZE=100                  # Số messages mỗi batch (mặc định: 100)
S3_BATCH_TIMEOUT=300               # Timeout để flush batch (giây, mặc định: 300)
```

### 3. Khởi động Kafka

Đảm bảo Kafka đang chạy:

```bash
# Sử dụng docker-compose (nếu có)
docker-compose up -d

# Hoặc khởi động Kafka thủ công
```

## 📊 Sử dụng

### Chạy Producer (Đẩy dữ liệu từ OpenAQ API lên Kafka)

```bash
python producer.py
```

Producer sẽ:
- Gọi OpenAQ API v3 mỗi 60 giây
- Lấy dữ liệu chất lượng không khí theo country filter
- Xử lý và gửi từng location lên Kafka topic
- Chạy liên tục cho đến khi nhấn Ctrl+C

### Chạy Consumer (Nhận dữ liệu từ Kafka)

Mở terminal mới và chạy:

```bash
python consumer.py
```

Consumer sẽ:
- Kết nối tới Kafka topic
- Nhận và hiển thị dữ liệu chất lượng không khí
- Hiển thị thông tin location, tọa độ, và các thông số (PM2.5, PM10, etc.)
- Tính toán AQI và gửi cảnh báo nếu vượt ngưỡng
- Lưu dữ liệu vào Postgres

### Chạy S3 Consumer (Đẩy dữ liệu từ Kafka lên S3)

S3 Consumer sẽ tự động chạy nếu bạn sử dụng `docker-compose up`. Hoặc chạy thủ công:

```bash
python s3_consumer.py
```

S3 Consumer sẽ:
- Kết nối tới Kafka topic (sử dụng consumer group riêng)
- Nhận dữ liệu từ Kafka
- Batch messages theo cấu hình
- Upload lên S3 dưới dạng JSON nén (gzip)
- Tổ chức dữ liệu theo partition: `year=YYYY/month=MM/day=DD/hour=HH/`

## 🔧 Tùy chỉnh

### Thay đổi khoảng thời gian gọi API

Trong file `producer.py`, dòng cuối cùng của hàm `main()`:

```python
producer.start_streaming(interval=60, count=None)  # interval=thời gian (giây)
```

### Thay đổi quốc gia

Trong file `.env`:

```env
OPENAQ_COUNTRY=US  # USA
OPENAQ_COUNTRY=TH  # Thailand
OPENAQ_COUNTRY=JP  # Japan
```

### Lọc theo tọa độ

Trong file `.env`:

```env
OPENAQ_COORDINATES=21.0285,105.8542  # Hà Nội
OPENAQ_RADIUS=25000                   # 25km
```

## 📝 Cấu trúc dữ liệu

Dữ liệu được gửi lên Kafka có format:

```json
{
  "timestamp": "2025-10-18T10:30:00",
  "location_id": 12345,
  "location_name": "Hanoi Station",
  "locality": "Hanoi",
  "country": "Vietnam",
  "country_code": "VN",
  "coordinates": {
    "latitude": 21.0285,
    "longitude": 105.8542
  },
  "is_mobile": false,
  "is_monitor": true,
  "sensors": [
    {
      "id": 67890,
      "name": "PM2.5 Sensor",
      "parameter": "pm25",
      "parameter_display": "PM2.5",
      "unit": "µg/m³",
      "latest_value": 45.2,
      "latest_datetime": "2025-10-18T10:25:00"
    }
  ]
}
```

## 🔍 OpenAQ API v3

API Documentation: https://docs.openaq.org/

### Endpoints được sử dụng

- **GET /v3/locations** - Lấy danh sách locations và dữ liệu sensors mới nhất

### Parameters hỗ trợ

- `limit`: Số lượng kết quả (mặc định: 100)
- `countries`: Mã quốc gia (ISO 3166-1 alpha-2)
- `coordinates`: Tọa độ (latitude,longitude)
- `radius`: Bán kính tìm kiếm (meters)
- `order_by`: Sắp xếp theo (lastUpdated, name, etc.)

## ⚠️ Lưu ý

- OpenAQ API v3 không yêu cầu API key nhưng có rate limiting
- Nếu bạn có API key, thêm vào file `.env` để tăng rate limit
- Interval nên >= 60 giây để tránh vượt quá rate limit
- Dữ liệu phụ thuộc vào các trạm monitoring có sẵn ở quốc gia đó

## 📦 S3 Storage (BRONZE Layer)

Hệ thống hỗ trợ đẩy dữ liệu từ Kafka lên S3 để lưu trữ dài hạn (BRONZE layer).

### Cấu hình S3

1. **Tạo S3 bucket** trên AWS (hoặc S3-compatible storage như MinIO)

2. **Lấy AWS credentials:**
   - AWS Access Key ID
   - AWS Secret Access Key
   - Region của bucket

3. **Thêm vào file `.env`:**
   ```env
   S3_BUCKET=your-bucket-name
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   S3_REGION=us-east-1
   ```

### Cấu trúc dữ liệu trên S3

Dữ liệu được lưu theo partition theo thời gian:

```
s3://your-bucket/
└── bronze/
    └── openaq/
        └── raw/
            └── year=2025/
                └── month=01/
                    └── day=18/
                        └── hour=10/
                            ├── data_20250118_100000.json.gz
                            ├── data_20250118_100500.json.gz
                            └── ...
```

Mỗi file chứa một batch messages (mặc định 100 messages), được nén bằng gzip.

### Sử dụng với MinIO hoặc S3-compatible storage

Nếu bạn sử dụng MinIO hoặc storage tương thích S3 khác:

```env
S3_ENDPOINT_URL=http://localhost:9000  # MinIO endpoint
S3_BUCKET=openaq-bronze
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_REGION=us-east-1
```

## 🐛 Troubleshooting

### Lỗi kết nối Kafka

```
NoBrokersAvailable
```

→ Kiểm tra Kafka đang chạy tại `localhost:9092`

### Không có dữ liệu từ API

```
⚠ Không có dữ liệu từ API
```

→ Thử thay đổi `OPENAQ_COUNTRY` hoặc bỏ country filter (để trống)

### Rate limit exceeded

```
429 Too Many Requests
```

→ Tăng `interval` trong `producer.py` hoặc thêm API key vào `.env`

### Lỗi kết nối S3

```
⚠ Cảnh báo: Không thể kết nối S3
```

→ Kiểm tra:
- `S3_BUCKET` đã được cấu hình
- `AWS_ACCESS_KEY_ID` và `AWS_SECRET_ACCESS_KEY` đúng
- Bucket tồn tại và có quyền truy cập
- Nếu dùng S3-compatible storage, kiểm tra `S3_ENDPOINT_URL`

