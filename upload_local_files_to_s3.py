"""
Script upload các file CSV.gz từ local lên S3 bucket (nhánh backfill)
"""

import os
import re
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import sys
from pathlib import Path

# Load environment variables
load_dotenv()

# S3 bucket của bạn
TARGET_BUCKET = os.getenv('S3_BUCKET')
TARGET_PREFIX = 'bronze/openaq/backfill'  # Nhánh backfill

# S3 config
S3_REGION = os.getenv('S3_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')

# OpenAQ Location ID
LOCATION_ID = os.getenv('OPENAQ_LOCATION_ID', '4946813')


def get_s3_client():
    """Tạo S3 client cho bucket của bạn"""
    s3_kwargs = {
        'region_name': S3_REGION,
    }
    
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        s3_kwargs['aws_access_key_id'] = AWS_ACCESS_KEY_ID
        s3_kwargs['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
    
    if S3_ENDPOINT_URL:
        s3_kwargs['endpoint_url'] = S3_ENDPOINT_URL
    
    return boto3.client('s3', **s3_kwargs)


def parse_filename(filename):
    """
    Parse tên file để lấy thông tin ngày tháng
    Format: location-4946813-YYYYMMDD.csv.gz
    
    Returns:
        dict với keys: location_id, year, month, day, date_str
        None nếu không parse được
    """
    # Pattern: location-<location_id>-<YYYYMMDD>.csv.gz
    pattern = r'location-(\d+)-(\d{8})\.csv\.gz'
    match = re.match(pattern, filename)
    
    if match:
        location_id = match.group(1)
        date_str = match.group(2)  # YYYYMMDD
        
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        
        return {
            'location_id': location_id,
            'year': year,
            'month': month,
            'day': day,
            'date_str': date_str
        }
    
    return None


def get_s3_key(file_info, filename):
    """
    Tạo S3 key dựa trên thông tin file
    Format: bronze/openaq/backfill/year=YYYY/month=MM/day=DD/locationid=XXX/filename
    """
    return f"{TARGET_PREFIX}/year={file_info['year']}/month={file_info['month']}/day={file_info['day']}/locationid={file_info['location_id']}/{filename}"


def upload_file(s3_client, local_path, s3_key):
    """
    Upload file lên S3
    
    Args:
        s3_client: S3 client
        local_path: Đường dẫn file local
        s3_key: S3 key (path trong bucket)
    
    Returns:
        True nếu thành công, False nếu thất bại
    """
    try:
        # Kiểm tra file đã tồn tại chưa
        try:
            s3_client.head_object(Bucket=TARGET_BUCKET, Key=s3_key)
            return 'exists'  # File đã tồn tại
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
        
        # Upload file
        s3_client.upload_file(
            local_path,
            TARGET_BUCKET,
            s3_key,
            ExtraArgs={'ContentType': 'application/gzip'}
        )
        
        return True
    
    except ClientError as e:
        print(f"    [ERROR] Loi khi upload {local_path}: {e}")
        return False
    except Exception as e:
        print(f"    [ERROR] Loi khong xac dinh: {e}")
        return False


def upload_directory(local_dir):
    """
    Upload tất cả file CSV.gz trong thư mục lên S3
    
    Args:
        local_dir: Đường dẫn thư mục chứa các file CSV.gz
    """
    if not TARGET_BUCKET:
        print("[ERROR] Loi: S3_BUCKET chua duoc cau hinh trong .env")
        return
    
    print(f"\n{'='*80}")
    print(f"BẮT ĐẦU UPLOAD DỮ LIỆU LÊN S3")
    print(f"Thư mục local: {local_dir}")
    print(f"Target: s3://{TARGET_BUCKET}/{TARGET_PREFIX}")
    print(f"Location ID: {LOCATION_ID}")
    print(f"{'='*80}\n")
    
    # Kiểm tra thư mục tồn tại
    if not os.path.isdir(local_dir):
        print(f"[ERROR] Loi: Thu muc khong ton tai: {local_dir}")
        return
    
    s3_client = get_s3_client()
    
    # Test kết nối S3
    try:
        s3_client.head_bucket(Bucket=TARGET_BUCKET)
        print(f"[OK] Ket noi S3 thanh cong: {TARGET_BUCKET}\n")
    except ClientError as e:
        print(f"[ERROR] Khong the ket noi S3 bucket: {e}")
        return
    
    # Tìm tất cả file CSV.gz
    csv_files = []
    for root, dirs, files in os.walk(local_dir):
        for file in files:
            if file.endswith('.csv.gz'):
                csv_files.append(os.path.join(root, file))
    
    if not csv_files:
        print(f"[ERROR] Khong tim thay file CSV.gz nao trong thu muc: {local_dir}")
        return
    
    print(f"Tìm thấy {len(csv_files)} file(s) CSV.gz\n")
    
    # Upload từng file
    uploaded_count = 0
    exists_count = 0
    failed_count = 0
    skipped_count = 0
    
    for local_path in csv_files:
        filename = os.path.basename(local_path)
        
        # Parse tên file
        file_info = parse_filename(filename)
        if not file_info:
            print(f"  [WARN] Bo qua file khong dung format: {filename}")
            skipped_count += 1
            continue
        
        # Chỉ upload file có location_id khớp
        if file_info['location_id'] != LOCATION_ID:
            print(f"  [WARN] Bo qua file location ID khac ({file_info['location_id']}): {filename}")
            skipped_count += 1
            continue
        
        # Tạo S3 key
        s3_key = get_s3_key(file_info, filename)
        
        print(f"  -> Upload: {filename}")
        print(f"    S3 key: {s3_key}")
        
        # Upload file
        result = upload_file(s3_client, local_path, s3_key)
        
        if result == True:
            uploaded_count += 1
            print(f"    [OK] Upload thanh cong")
        elif result == 'exists':
            exists_count += 1
            print(f"    [SKIP] File da ton tai tren S3")
        else:
            failed_count += 1
            print(f"    [ERROR] Upload that bai")
        
        print()
    
    # Tổng kết
    print(f"{'='*80}")
    print(f"KẾT QUẢ UPLOAD")
    print(f"{'='*80}")
    print(f"Tổng số file: {len(csv_files)}")
    print(f"Upload thành công: {uploaded_count}")
    print(f"Đã tồn tại: {exists_count}")
    print(f"Thất bại: {failed_count}")
    print(f"Bỏ qua (format/location ID không đúng): {skipped_count}")
    print(f"{'='*80}\n")


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Cách sử dụng:")
        print("  python upload_local_files_to_s3.py <thư_mục_chứa_file_csv.gz>")
        print("\nVí dụ:")
        print("  python upload_local_files_to_s3.py \"C:\\Users\\HH\\Downloads\\openaq_4946813_2025_07_12\"")
        print("  python upload_local_files_to_s3.py \"./data\"")
        sys.exit(1)
    
    local_dir = sys.argv[1]
    
    # Chuyển đường dẫn tương đối thành tuyệt đối
    if not os.path.isabs(local_dir):
        local_dir = os.path.abspath(local_dir)
    
    upload_directory(local_dir)


if __name__ == '__main__':
    main()

