"""
Script copy dữ liệu từ OpenAQ S3 archive (public) sang S3 bucket của bạn (nhánh backfill)
"""

import os
import boto3
from botocore.exceptions import ClientError
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()

# OpenAQ S3 Archive (public, không cần credentials)
OPENAQ_ARCHIVE_BUCKET = 'openaq-fetches'
OPENAQ_ARCHIVE_PREFIX = 'realtime-gzipped'

# S3 bucket của bạn
TARGET_BUCKET = os.getenv('S3_BUCKET')
TARGET_PREFIX = 'bronze/openaq/backfill'  # Nhánh backfill

# OpenAQ Location ID
LOCATION_ID = os.getenv('OPENAQ_LOCATION_ID', '4946813')

# S3 config cho bucket của bạn
S3_REGION = os.getenv('S3_REGION', 'us-east-1')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')


def get_openaq_s3_client():
    """Tạo S3 client cho OpenAQ archive (public, không cần credentials)"""
    # OpenAQ archive là public bucket, cần dùng UNSIGNED config
    # Một số public bucket cần RequestPayer='requester'
    return boto3.client(
        's3',
        config=Config(
            signature_version=UNSIGNED,
            s3={'addressing_style': 'path'}
        ),
        region_name='us-east-1'
    )


def test_openaq_connection(openaq_client):
    """Test kết nối tới OpenAQ archive"""
    try:
        # Thử list một vài object ở root level
        response = openaq_client.list_objects_v2(
            Bucket=OPENAQ_ARCHIVE_BUCKET,
            Prefix=OPENAQ_ARCHIVE_PREFIX,
            MaxKeys=5
        )
        
        if 'Contents' in response:
            print(f"[OK] Ket noi OpenAQ archive thanh cong!")
            print(f"  Tim thay {len(response['Contents'])} file(s) mau:")
            for obj in response['Contents'][:3]:
                print(f"    - {obj['Key']}")
            return True
        else:
            print(f"[WARN] Ket noi duoc nhung khong co file nao trong prefix: {OPENAQ_ARCHIVE_PREFIX}")
            return False
    
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'AccessDenied':
            print(f"[ERROR] Access Denied - Bucket co the khong phai public hoan toan")
            print(f"  Thu su dung AWS CLI: aws s3 ls s3://{OPENAQ_ARCHIVE_BUCKET}/{OPENAQ_ARCHIVE_PREFIX}/ --no-sign-request")
        else:
            print(f"[ERROR] Loi khi test ket noi: {e}")
        return False


def get_target_s3_client():
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


def list_openaq_files(openaq_client, year, month=None, day=None, hour=None):
    """
    List các file trong OpenAQ archive cho location ID cụ thể
    
    Args:
        openaq_client: S3 client cho OpenAQ archive
        year: Năm (YYYY)
        month: Tháng (MM), optional
        day: Ngày (DD), optional
        hour: Giờ (HH), optional
    
    Returns:
        List các S3 keys
    """
    files = []
    
    # Thử nhiều cấu trúc path khác nhau
    prefix_variants = []
    
    # Cấu trúc 1: realtime-gzipped/year=YYYY/month=MM/day=DD/hour=HH/locationid=XXX/
    if month:
        prefix_variants.append(f'{OPENAQ_ARCHIVE_PREFIX}/year={year}/month={month:02d}/day={day:02d if day else "*"}/hour={hour:02d if hour is not None else "*"}/locationid={LOCATION_ID}/')
    else:
        # Nếu không có month, list tất cả trong năm
        prefix_variants.append(f'{OPENAQ_ARCHIVE_PREFIX}/year={year}/')
    
    # Cấu trúc 2: realtime-gzipped/YYYY/MM/DD/HH/locationid=XXX/
    if month and day and hour is not None:
        prefix_variants.append(f'{OPENAQ_ARCHIVE_PREFIX}/{year}/{month:02d}/{day:02d}/{hour:02d}/locationid={LOCATION_ID}/')
    
    # Cấu trúc 3: realtime-gzipped/locationid=XXX/year=YYYY/month=MM/
    if month:
        prefix_variants.append(f'{OPENAQ_ARCHIVE_PREFIX}/locationid={LOCATION_ID}/year={year}/month={month:02d}/')
    
    for prefix in prefix_variants:
        print(f"  Dang thu tim kiem: s3://{OPENAQ_ARCHIVE_BUCKET}/{prefix}")
        
        try:
            # Thử list với prefix
            paginator = openaq_client.get_paginator('list_objects_v2')
            # Thử không có RequestPayer trước
            try:
                pages = paginator.paginate(
                    Bucket=OPENAQ_ARCHIVE_BUCKET, 
                    Prefix=prefix.replace('*', '')  # Bỏ wildcard
                )
            except ClientError:
                # Nếu lỗi, thử với RequestPayer
                pages = paginator.paginate(
                    Bucket=OPENAQ_ARCHIVE_BUCKET, 
                    Prefix=prefix.replace('*', ''),
                    RequestPayer='requester'
                )
            
            page_count = 0
            for page in pages:
                page_count += 1
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Lọc theo location ID nếu prefix không chứa locationid
                        if f'locationid={LOCATION_ID}' in obj['Key'] or f'/locationid={LOCATION_ID}/' in obj['Key']:
                            # Lọc theo year/month nếu cần
                            if month and f'/month={month:02d}/' not in obj['Key'] and f'/{month:02d}/' not in obj['Key']:
                                continue
                            if year and f'year={year}' not in obj['Key'] and not obj['Key'].startswith(f'{OPENAQ_ARCHIVE_PREFIX}/{year}/'):
                                continue
                            files.append(obj['Key'])
            
            if files:
                print(f"  Tim thay {len(files)} file(s) voi prefix: {prefix}")
                return files  # Trả về ngay khi tìm thấy
            elif page_count > 0:
                print(f"  Khong tim thay file nao voi prefix: {prefix}")
        
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchBucket':
                print(f"  [ERROR] Bucket khong ton tai: {OPENAQ_ARCHIVE_BUCKET}")
                break
            elif error_code == 'AccessDenied':
                print(f"  [WARN] Access Denied voi prefix: {prefix} (co the prefix khong dung)")
                continue
            else:
                print(f"  [ERROR] Loi khi list files: {e}")
                continue
    
    if not files:
        print(f"  Khong tim thay file nao cho location ID {LOCATION_ID} trong nam {year}")
        print(f"  Goi y: Kiem tra lai location ID hoac thu list tat ca locations trong nam {year}")
    
    return files


def copy_file(openaq_client, target_client, source_key, target_key):
    """
    Copy file từ OpenAQ archive sang bucket của bạn
    
    Args:
        openaq_client: S3 client cho OpenAQ archive
        target_client: S3 client cho bucket của bạn
        source_key: S3 key trong OpenAQ archive
        target_key: S3 key trong bucket của bạn
    
    Returns:
        True nếu thành công, False nếu thất bại
    """
    try:
        # Download từ OpenAQ archive
        response = openaq_client.get_object(Bucket=OPENAQ_ARCHIVE_BUCKET, Key=source_key)
        data = response['Body'].read()
        
        # Upload lên bucket của bạn
        target_client.put_object(
            Bucket=TARGET_BUCKET,
            Key=target_key,
            Body=data,
            ContentType=response.get('ContentType', 'application/gzip')
        )
        
        return True
    
    except ClientError as e:
        print(f"    [ERROR] Loi khi copy {source_key}: {e}")
        return False


def copy_year_data(year, start_month=1, end_month=12):
    """
    Copy dữ liệu cho một năm
    
    Args:
        year: Năm (YYYY)
        start_month: Tháng bắt đầu (1-12)
        end_month: Tháng kết thúc (1-12)
    """
    if not TARGET_BUCKET:
        print("[ERROR] Loi: S3_BUCKET chua duoc cau hinh trong .env")
        return
    
    print(f"\n{'='*80}")
    print(f"BẮT ĐẦU COPY DỮ LIỆU NĂM {year}")
    print(f"Location ID: {LOCATION_ID}")
    print(f"Target: s3://{TARGET_BUCKET}/{TARGET_PREFIX}")
    print(f"{'='*80}\n")
    
    openaq_client = get_openaq_s3_client()
    target_client = get_target_s3_client()
    
    # Test kết nối OpenAQ archive
    print("Đang test kết nối tới OpenAQ archive...")
    if not test_openaq_connection(openaq_client):
        print("\n[WARN] Canh bao: Khong the ket noi toi OpenAQ archive")
        print("  Co the bucket khong phai public hoan toan")
        print("  Hoac cau truc path khac voi du kien")
        print("\n  Thu su dung AWS CLI de kiem tra:")
        print(f"     aws s3 ls s3://{OPENAQ_ARCHIVE_BUCKET}/{OPENAQ_ARCHIVE_PREFIX}/ --no-sign-request --recursive | grep locationid={LOCATION_ID}")
        print("\n  Hoac kiem tra cau truc bucket:")
        print(f"     aws s3 ls s3://{OPENAQ_ARCHIVE_BUCKET}/ --no-sign-request")
        print("\n  Neu AWS CLI khong hoat dong, co the can:")
        print("     1. Su dung OpenAQ API de lay du lieu thay vi S3 archive")
        print("     2. Hoac kiem tra lai location ID co ton tai trong archive khong")
        print("\n  Script se tiep tuc thu copy nhung co the khong tim thay file...")
        print()
    
    # Test kết nối target bucket
    try:
        target_client.head_bucket(Bucket=TARGET_BUCKET)
        print(f"[OK] Ket noi target bucket thanh cong: {TARGET_BUCKET}\n")
    except ClientError as e:
        print(f"[ERROR] Khong the ket noi target bucket: {e}")
        return
    
    total_files = 0
    copied_files = 0
    failed_files = 0
    
    # Duyệt qua từng tháng
    for month in range(start_month, end_month + 1):
        print(f"\n[INFO] Thang {month:02d}/{year}")
        
        # List tất cả files trong tháng (không filter theo day/hour để lấy hết)
        source_files = list_openaq_files(openaq_client, year, month=month)
        
        if not source_files:
            print(f"  Khong co du lieu cho thang {month:02d}/{year}")
            continue
        
        # Copy từng file
        for source_key in source_files:
            total_files += 1
            
            # Tạo target key: giữ nguyên cấu trúc nhưng đổi prefix
            # Source: realtime-gzipped/year=YYYY/month=MM/day=DD/hour=HH/locationid=XXX/file.json.gz
            # Target: bronze/openaq/backfill/year=YYYY/month=MM/day=DD/hour=HH/locationid=XXX/file.json.gz
            target_key = source_key.replace(OPENAQ_ARCHIVE_PREFIX, TARGET_PREFIX)
            
            # Kiểm tra file đã tồn tại chưa
            try:
                target_client.head_object(Bucket=TARGET_BUCKET, Key=target_key)
                print(f"  -> [SKIP] Da ton tai: {target_key.split('/')[-1]}")
                continue
            except ClientError:
                # File chưa tồn tại, tiếp tục copy
                pass
            
            if copy_file(openaq_client, target_client, source_key, target_key):
                copied_files += 1
                if copied_files % 10 == 0:
                    print(f"  Da copy {copied_files} file(s)...")
            else:
                failed_files += 1
        
        print(f"  [OK] Hoan thanh thang {month:02d}/{year}: {len(source_files)} file(s)")
    
    print(f"\n{'='*80}")
    print(f"KẾT QUẢ COPY DỮ LIỆU NĂM {year}")
    print(f"{'='*80}")
    print(f"Tổng số file tìm thấy: {total_files}")
    print(f"Đã copy thành công: {copied_files}")
    print(f"Thất bại: {failed_files}")
    print(f"Đã tồn tại: {total_files - copied_files - failed_files}")
    print(f"{'='*80}\n")


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Cách sử dụng:")
        print("  python copy_openaq_archive_to_s3.py <year> [start_month] [end_month]")
        print("\nVí dụ:")
        print("  python copy_openaq_archive_to_s3.py 2024          # Copy cả năm 2024")
        print("  python copy_openaq_archive_to_s3.py 2024 1 6      # Copy 6 tháng đầu 2024")
        print("  python copy_openaq_archive_to_s3.py 2023 7 12     # Copy 6 tháng cuối 2023")
        sys.exit(1)
    
    year = int(sys.argv[1])
    start_month = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    end_month = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    
    if not (1 <= start_month <= 12) or not (1 <= end_month <= 12):
        print("[ERROR] Loi: Thang phai tu 1 den 12")
        sys.exit(1)
    
    if start_month > end_month:
        print("[ERROR] Loi: start_month phai <= end_month")
        sys.exit(1)
    
    copy_year_data(year, start_month, end_month)


if __name__ == '__main__':
    main()

