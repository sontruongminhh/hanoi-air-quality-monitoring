"""
Airflow DAG cho ETL pipeline chất lượng không khí
Bronze (backfill) -> Silver (transform) -> Gold (marts)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os

# Default arguments
default_args = {
    'owner': 'aqi-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'aqi_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline cho dữ liệu chất lượng không khí từ Bronze -> Silver -> Gold',
    schedule_interval='@daily',  # Chạy hàng ngày
    start_date=days_ago(1),
    catchup=False,
    tags=['aqi', 'etl', 'openaq'],
)

# S3 Configuration
S3_BUCKET = os.getenv('S3_BUCKET', 'aqi-hanoi-bronze')
BRONZE_PREFIX = 'bronze/openaq/backfill'
SILVER_PREFIX = 'silver/openaq/transform'
GOLD_PREFIX = 'gold/openaq/marts'

# Location ID
LOCATION_ID = os.getenv('OPENAQ_LOCATION_ID', '4946813')


def bronze_to_silver_transform(**context):
    """
    Transform dữ liệu từ Bronze sang Silver
    - Đọc CSV.gz từ Bronze
    - Chuẩn hóa đơn vị (µg/m³)
    - Validate và clean data
    - Ghi lại dạng Parquet vào Silver
    """
    import pandas as pd
    import gzip
    import boto3
    from io import BytesIO
    from datetime import datetime
    
    # Lấy execution date từ context
    execution_date = context['execution_date']
    year = execution_date.strftime('%Y')
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    
    s3_client = boto3.client('s3')
    
    # Đường dẫn Bronze
    bronze_prefix = f"{BRONZE_PREFIX}/year={year}/month={month}/day={day}/locationid={LOCATION_ID}/"
    
    # Đường dẫn Silver
    silver_prefix = f"{SILVER_PREFIX}/year={year}/month={month}/day={day}/locationid={LOCATION_ID}/"
    
    print(f"Đang transform từ: s3://{S3_BUCKET}/{bronze_prefix}")
    print(f"Sang: s3://{S3_BUCKET}/{silver_prefix}")
    
    # List files trong Bronze
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=bronze_prefix
    )
    
    if 'Contents' not in response:
        print(f"Không có file nào trong {bronze_prefix}")
        return
    
    # Process từng file
    all_data = []
    for obj in response['Contents']:
        if not obj['Key'].endswith('.csv.gz'):
            continue
        
        print(f"  Dang xu ly: {obj['Key']}")
        
        # Download và đọc file
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
        with gzip.open(BytesIO(file_obj['Body'].read()), 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f)
        
        # Transform và chuẩn hóa
        # TODO: Thêm logic transform cụ thể
        # - Chuẩn hóa đơn vị
        # - Validate data
        # - Clean data
        
        all_data.append(df)
    
    if not all_data:
        print("Không có dữ liệu để transform")
        return
    
    # Combine tất cả data
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Ghi lại dạng Parquet vào Silver
    parquet_buffer = BytesIO()
    combined_df.to_parquet(parquet_buffer, index=False, compression='snappy')
    parquet_buffer.seek(0)
    
    silver_key = f"{silver_prefix}data_{year}{month}{day}.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    print(f"[OK] Da ghi Silver: {silver_key}")


def silver_to_gold_aggregate(**context):
    """
    Aggregate dữ liệu từ Silver sang Gold
    - Đọc Parquet từ Silver
    - Aggregate theo giờ (hourly) và ngày (daily)
    - Tính các metrics: min, max, avg, median
    - Ghi vào Gold marts
    """
    import pandas as pd
    import boto3
    from io import BytesIO
    from datetime import datetime
    
    execution_date = context['execution_date']
    year = execution_date.strftime('%Y')
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    
    s3_client = boto3.client('s3')
    
    # Đường dẫn Silver
    silver_prefix = f"{SILVER_PREFIX}/year={year}/month={month}/day={day}/locationid={LOCATION_ID}/"
    
    # Đường dẫn Gold
    gold_hourly_prefix = f"{GOLD_PREFIX}/fact_api_hourly/year={year}/month={month}/day={day}/locationid={LOCATION_ID}/"
    gold_daily_prefix = f"{GOLD_PREFIX}/fact_api_daily/year={year}/month={month}/locationid={LOCATION_ID}/"
    
    print(f"Đang aggregate từ: s3://{S3_BUCKET}/{silver_prefix}")
    
    # List và đọc file từ Silver
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=silver_prefix
    )
    
    if 'Contents' not in response:
        print(f"Không có file nào trong {silver_prefix}")
        return
    
    all_data = []
    for obj in response['Contents']:
        if not obj['Key'].endswith('.parquet'):
            continue
        
        print(f"  Dang doc: {obj['Key']}")
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
        df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
        all_data.append(df)
    
    if not all_data:
        print("Không có dữ liệu để aggregate")
        return
    
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Parse datetime nếu cần
    if 'datetime' in combined_df.columns:
        combined_df['datetime'] = pd.to_datetime(combined_df['datetime'])
        combined_df['hour'] = combined_df['datetime'].dt.hour
        combined_df['date'] = combined_df['datetime'].dt.date
    
    # Aggregate hourly
    hourly_agg = combined_df.groupby(['date', 'hour']).agg({
        'value': ['min', 'max', 'mean', 'median', 'count']
    }).reset_index()
    
    hourly_agg.columns = ['date', 'hour', 'value_min', 'value_max', 'value_avg', 'value_median', 'value_count']
    
    # Ghi hourly aggregation
    hourly_buffer = BytesIO()
    hourly_agg.to_parquet(hourly_buffer, index=False, compression='snappy')
    hourly_buffer.seek(0)
    
    hourly_key = f"{gold_hourly_prefix}data_{year}{month}{day}.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=hourly_key,
        Body=hourly_buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    print(f"[OK] Da ghi Gold hourly: {hourly_key}")
    
    # Aggregate daily
    daily_agg = combined_df.groupby('date').agg({
        'value': ['min', 'max', 'mean', 'median', 'count']
    }).reset_index()
    
    daily_agg.columns = ['date', 'value_min', 'value_max', 'value_avg', 'value_median', 'value_count']
    
    # Ghi daily aggregation
    daily_buffer = BytesIO()
    daily_agg.to_parquet(daily_buffer, index=False, compression='snappy')
    daily_buffer.seek(0)
    
    daily_key = f"{gold_daily_prefix}data_{year}{month}{day}.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=daily_key,
        Body=daily_buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    print(f"[OK] Da ghi Gold daily: {daily_key}")


# Tasks
task_bronze_to_silver = PythonOperator(
    task_id='bronze_to_silver_transform',
    python_callable=bronze_to_silver_transform,
    dag=dag,
)

task_silver_to_gold = PythonOperator(
    task_id='silver_to_gold_aggregate',
    python_callable=silver_to_gold_aggregate,
    dag=dag,
)

# Task dependencies
task_bronze_to_silver >> task_silver_to_gold

