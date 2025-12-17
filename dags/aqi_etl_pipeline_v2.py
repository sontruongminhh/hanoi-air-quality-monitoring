"""
Airflow DAG chuyên nghiệp cho ETL pipeline chất lượng không khí
Bronze (backfill) -> Silver (transform) -> Gold (marts)

Version 2.0 - Tách nhỏ tasks theo Single Responsibility Principle
- 15+ tasks riêng biệt
- Data quality checks
- Parallel execution
- XCom data sharing
- Error handling và notifications
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import os
import json



S3_BUCKET = os.getenv('S3_BUCKET', 'aqi-open-data-hust')
BRONZE_PREFIX = 'bronze/openaq/backfill'
SILVER_PREFIX = 'silver/openaq/cleaned'
GOLD_PREFIX = 'gold/openaq/marts'
LOCATION_ID = os.getenv('OPENAQ_LOCATION_ID', '4946813')

# Data Quality thresholds
QUALITY_THRESHOLDS = {
    'min_records': 10,           # Tối thiểu số records
    'max_null_percent': 20,      # Tối đa % null values
    'max_duplicate_percent': 5,  # Tối đa % duplicates
}

# Valid ranges cho từng parameter (µg/m³)
VALID_RANGES = {
    'pm25': (0, 1000),
    'pm10': (0, 2000),
    'no2': (0, 1000),
    'o3': (0, 1000),
    'co': (0, 50000),
    'so2': (0, 2000),
}



default_args = {
    'owner': 'aqi-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}



with DAG(
    dag_id='aqi_etl_pipeline_v2',
    default_args=default_args,
    description='ETL Pipeline chuyen nghiep: Bronze -> Silver -> Gold voi nhieu tasks',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['aqi', 'etl', 'openaq', 'production'],
    doc_md="""
    ## AQI ETL Pipeline v2.0
    
    Pipeline xử lý dữ liệu chất lượng không khí theo kiến trúc Data Lakehouse:
    
    ### Bronze Zone (Raw)
    - Đọc dữ liệu CSV.gz từ S3
    - Giữ nguyên format gốc
    
    ### Silver Zone (Cleaned)
    - Chuẩn hóa đơn vị
    - Loại bỏ duplicates
    - Validate ranges
    - Remove outliers
    - Tính AQI
    
    ### Gold Zone (Marts)
    - Aggregate hourly
    - Aggregate daily
    - Build dimension tables
    """,
) as dag:


    
    def get_s3_client():
        """Tạo S3 client"""
        import boto3
        return boto3.client('s3')
    
    def get_execution_params(context):
        """Lấy parameters từ execution context hoặc dag_run.conf"""
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        
        execution_date = context['execution_date']
        
        return {
            'year': conf.get('year', execution_date.strftime('%Y')),
            'month': conf.get('month', execution_date.strftime('%m')),
            'day': conf.get('day', execution_date.strftime('%d')),
            'location_id': conf.get('location_id', LOCATION_ID),
        }
    

    
    def check_bronze_exists(**context):
        """
        Task 1: Kiểm tra Bronze có dữ liệu không
        Returns: True nếu có data, False nếu không
        """
        import boto3
        
        params = get_execution_params(context)
        s3_client = boto3.client('s3')
        
        bronze_prefix = f"{BRONZE_PREFIX}/year={params['year']}/month={params['month']}/day={params['day']}/locationid={params['location_id']}/"
        
        print(f"[INFO] Checking Bronze: s3://{S3_BUCKET}/{bronze_prefix}")
        
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=bronze_prefix,
            MaxKeys=1
        )
        
        has_data = 'Contents' in response and len(response['Contents']) > 0
        
        if has_data:
            print(f"[OK] Bronze co du lieu")
            return 'ingest_bronze'
        else:
            print(f"[WARN] Bronze KHONG co du lieu - skip pipeline")
            return 'skip_no_data'
    
    def ingest_bronze(**context):
        """
        Task 2: Đọc tất cả CSV.gz từ Bronze
        Output: List of DataFrames via XCom
        """
        import pandas as pd
        import gzip
        import boto3
        from io import BytesIO
        
        params = get_execution_params(context)
        s3_client = boto3.client('s3')
        
        bronze_prefix = f"{BRONZE_PREFIX}/year={params['year']}/month={params['month']}/day={params['day']}/locationid={params['location_id']}/"
        
        print(f"[INFO] Ingesting from: s3://{S3_BUCKET}/{bronze_prefix}")
        
        response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=bronze_prefix)
        
        all_data = []
        files_processed = []
        
        for obj in response.get('Contents', []):
            if not obj['Key'].endswith('.csv.gz'):
                continue
            
            print(f"  Reading: {obj['Key']}")
            file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=obj['Key'])
            
            with gzip.open(BytesIO(file_obj['Body'].read()), 'rt', encoding='utf-8') as f:
                df = pd.read_csv(f)
            
            all_data.append(df.to_dict('records'))
            files_processed.append(obj['Key'])
        
        # Push to XCom
        context['ti'].xcom_push(key='raw_data', value=all_data)
        context['ti'].xcom_push(key='files_processed', value=files_processed)
        context['ti'].xcom_push(key='params', value=params)
        
        total_records = sum(len(d) for d in all_data)
        print(f"[OK] Ingested {len(files_processed)} files, {total_records} records")
        
        return {'files': len(files_processed), 'records': total_records}
    

    
    def deduplicate_records(**context):
        """
        Task 3: Loại bỏ duplicate records
        """
        import pandas as pd
        
        ti = context['ti']
        raw_data = ti.xcom_pull(task_ids='ingest_bronze', key='raw_data')
        
        if not raw_data:
            print("[WARN] No data to deduplicate")
            return {'removed': 0}
        
        # Combine all data
        all_records = []
        for data_list in raw_data:
            all_records.extend(data_list)
        
        df = pd.DataFrame(all_records)
        original_count = len(df)
        
        
        key_columns = ['location_id', 'datetime', 'parameter'] if all(c in df.columns for c in ['location_id', 'datetime', 'parameter']) else df.columns.tolist()[:3]
        
        df_deduped = df.drop_duplicates(subset=key_columns, keep='first')
        removed_count = original_count - len(df_deduped)
        
        print(f"[OK] Deduplicated: {original_count} -> {len(df_deduped)} (removed {removed_count})")
        
        ti.xcom_push(key='deduped_data', value=df_deduped.to_dict('records'))
        
        return {'original': original_count, 'deduped': len(df_deduped), 'removed': removed_count}
    
    def normalize_units(**context):
        """
        Task 4: Chuẩn hóa đơn vị về µg/m³
        """
        import pandas as pd
        
        ti = context['ti']
        deduped_data = ti.xcom_pull(task_ids='deduplicate_records', key='deduped_data')
        
        if not deduped_data:
            print("[WARN] No data to normalize")
            return {'converted': 0}
        
        df = pd.DataFrame(deduped_data)
        converted_count = 0
        
        # Unit conversion factors
        conversions = {
            ('co', 'ppm'): 1000,
            ('no2', 'ppb'): 1.88,
            ('o3', 'ppb'): 2.0,
            ('so2', 'ppb'): 2.62,
            ('*', 'mg/m³'): 1000,
        }
        
        if 'parameter' in df.columns and 'unit' in df.columns and 'value' in df.columns:
            for idx, row in df.iterrows():
                param = str(row.get('parameter', '')).lower()
                unit = str(row.get('unit', '')).lower()
                
                for (p, u), factor in conversions.items():
                    if (p == '*' or p == param) and u in unit:
                        df.at[idx, 'value'] = row['value'] * factor
                        df.at[idx, 'unit'] = 'µg/m³'
                        converted_count += 1
                        break
        
        print(f"[OK] Normalized {converted_count} values to ug/m3")
        
        ti.xcom_push(key='normalized_data', value=df.to_dict('records'))
        
        return {'converted': converted_count}
    
    def parse_datetime(**context):
        """
        Task 5: Parse và chuẩn hóa datetime columns
        """
        import pandas as pd
        
        ti = context['ti']
        normalized_data = ti.xcom_pull(task_ids='normalize_units', key='normalized_data')
        
        if not normalized_data:
            print("[WARN] No data to parse datetime")
            return {'parsed': 0}
        
        df = pd.DataFrame(normalized_data)
        parsed_count = 0
        
        
        datetime_cols = ['datetime', 'date_utc', 'timestamp', 'time']
        
        for col in datetime_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], utc=True)
                    parsed_count += 1
                    print(f"  -> Parsed column: {col}")
                except Exception as e:
                    print(f"   [WARN] Could not parse {col}: {e}")
        
        # Extract date parts
        if 'datetime' in df.columns:
            df['date'] = df['datetime'].dt.date.astype(str)
            df['hour'] = df['datetime'].dt.hour
            df['day_of_week'] = df['datetime'].dt.dayofweek
            df['datetime'] = df['datetime'].astype(str)  # Convert to string for XCom
        
        print(f"[OK] Parsed {parsed_count} datetime columns")
        
        ti.xcom_push(key='datetime_parsed_data', value=df.to_dict('records'))
        
        return {'parsed': parsed_count}
    
    def merge_cleaned_data(**context):
        """
        Task 6: Merge tất cả cleaned data từ các parallel tasks
        """
        import pandas as pd
        
        ti = context['ti']
        
        # Pull từ task cuối cùng của cleaning chain
        data = ti.xcom_pull(task_ids='parse_datetime', key='datetime_parsed_data')
        
        if not data:
            print("[WARN] No data to merge")
            return {'records': 0}
        
        df = pd.DataFrame(data)
        
        print(f"[OK] Merged data: {len(df)} records")
        
        ti.xcom_push(key='merged_data', value=df.to_dict('records'))
        
        return {'records': len(df)}
    
    def validate_ranges(**context):
        """
        Task 7: Validate giá trị trong khoảng hợp lệ
        """
        import pandas as pd
        
        ti = context['ti']
        merged_data = ti.xcom_pull(task_ids='merge_cleaned_data', key='merged_data')
        
        if not merged_data:
            print("[WARN] No data to validate")
            return {'invalid': 0}
        
        df = pd.DataFrame(merged_data)
        original_count = len(df)
        invalid_count = 0
        
        if 'parameter' in df.columns and 'value' in df.columns:
            valid_mask = pd.Series([True] * len(df))
            
            for idx, row in df.iterrows():
                param = str(row.get('parameter', '')).lower()
                value = row.get('value')
                
                if pd.isna(value):
                    valid_mask[idx] = False
                    invalid_count += 1
                    continue
                
                if param in VALID_RANGES:
                    min_val, max_val = VALID_RANGES[param]
                    if value < min_val or value > max_val:
                        valid_mask[idx] = False
                        invalid_count += 1
            
            df = df[valid_mask].reset_index(drop=True)
        
        print(f"[OK] Validated: {original_count} -> {len(df)} (invalid: {invalid_count})")
        
        ti.xcom_push(key='validated_data', value=df.to_dict('records'))
        
        return {'original': original_count, 'valid': len(df), 'invalid': invalid_count}
    
    def remove_outliers(**context):
        """
        Task 8: Loại bỏ outliers bằng IQR method
        """
        import pandas as pd
        import numpy as np
        
        ti = context['ti']
        validated_data = ti.xcom_pull(task_ids='validate_ranges', key='validated_data')
        
        if not validated_data:
            print("[WARN] No data to remove outliers")
            return {'removed': 0}
        
        df = pd.DataFrame(validated_data)
        original_count = len(df)
        
        if 'value' in df.columns and len(df) > 10:
            # IQR method
            Q1 = df['value'].quantile(0.25)
            Q3 = df['value'].quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            df = df[(df['value'] >= lower_bound) & (df['value'] <= upper_bound)]
        
        removed_count = original_count - len(df)
        print(f"[OK] Removed {removed_count} outliers (IQR method)")
        
        ti.xcom_push(key='outliers_removed_data', value=df.to_dict('records'))
        
        return {'original': original_count, 'cleaned': len(df), 'removed': removed_count}
    
    def calculate_aqi(**context):
        """
        Task 9: Tính AQI từ PM2.5
        """
        import pandas as pd
        
        ti = context['ti']
        data = ti.xcom_pull(task_ids='remove_outliers', key='outliers_removed_data')
        
        if not data:
            print("[WARN] No data to calculate AQI")
            return {'calculated': 0}
        
        df = pd.DataFrame(data)
        calculated_count = 0
        
        # AQI breakpoints cho PM2.5
        def calc_aqi(pm25):
            if pd.isna(pm25):
                return None, None
            
            breakpoints = [
                (0, 12.0, 0, 50, 'Good'),
                (12.1, 35.4, 51, 100, 'Moderate'),
                (35.5, 55.4, 101, 150, 'Unhealthy for Sensitive Groups'),
                (55.5, 150.4, 151, 200, 'Unhealthy'),
                (150.5, 250.4, 201, 300, 'Very Unhealthy'),
                (250.5, 500, 301, 500, 'Hazardous'),
            ]
            
            for bp_low, bp_high, aqi_low, aqi_high, level in breakpoints:
                if bp_low <= pm25 <= bp_high:
                    aqi = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (pm25 - bp_low) + aqi_low)
                    return aqi, level
            
            if pm25 > 500:
                return 500, 'Hazardous'
            return None, None
        
        # Tính AQI cho PM2.5 records
        if 'parameter' in df.columns and 'value' in df.columns:
            df['aqi'] = None
            df['aqi_level'] = None
            
            pm25_mask = df['parameter'].str.lower() == 'pm25'
            for idx in df[pm25_mask].index:
                aqi, level = calc_aqi(df.at[idx, 'value'])
                df.at[idx, 'aqi'] = aqi
                df.at[idx, 'aqi_level'] = level
                if aqi is not None:
                    calculated_count += 1
        
        print(f"[OK] Calculated AQI for {calculated_count} PM2.5 records")
        
        ti.xcom_push(key='aqi_calculated_data', value=df.to_dict('records'))
        
        return {'calculated': calculated_count}
    
    def quality_check(**context):
        """
        Task 10: Data Quality Check - Gate trước khi ghi Silver
        """
        import pandas as pd
        
        ti = context['ti']
        data = ti.xcom_pull(task_ids='calculate_aqi', key='aqi_calculated_data')
        
        if not data:
            raise ValueError("[ERROR] QUALITY CHECK FAILED: No data available")
        
        df = pd.DataFrame(data)
        
        # Quality metrics
        total_records = len(df)
        null_count = df.isnull().sum().sum()
        null_percent = (null_count / (total_records * len(df.columns))) * 100 if total_records > 0 else 0
        
        quality_report = {
            'total_records': total_records,
            'null_count': null_count,
            'null_percent': round(null_percent, 2),
            'columns': list(df.columns),
            'passed': True,
            'issues': []
        }
        
        # Check thresholds
        if total_records < QUALITY_THRESHOLDS['min_records']:
            quality_report['issues'].append(f"Too few records: {total_records} < {QUALITY_THRESHOLDS['min_records']}")
            quality_report['passed'] = False
        
        if null_percent > QUALITY_THRESHOLDS['max_null_percent']:
            quality_report['issues'].append(f"Too many nulls: {null_percent}% > {QUALITY_THRESHOLDS['max_null_percent']}%")
            quality_report['passed'] = False
        
        print(f"[INFO] QUALITY REPORT:")
        print(f"   Records: {total_records}")
        print(f"   Null %: {null_percent}%")
        print(f"   Status: {'[OK] PASSED' if quality_report['passed'] else '[ERROR] FAILED'}")
        
        if quality_report['issues']:
            print(f"   Issues: {quality_report['issues']}")
        
        ti.xcom_push(key='quality_report', value=quality_report)
        ti.xcom_push(key='silver_data', value=data)
        
        if not quality_report['passed']:
            raise ValueError(f"Quality check failed: {quality_report['issues']}")
        
        return quality_report
    
    def write_silver(**context):
        """
        Task 11: Ghi dữ liệu đã clean vào Silver (Parquet)
        """
        import pandas as pd
        import boto3
        from io import BytesIO
        
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        data = ti.xcom_pull(task_ids='quality_check', key='silver_data')
        
        if not data:
            print("[WARN] No data to write to Silver")
            return {'written': False}
        
        df = pd.DataFrame(data)
        s3_client = boto3.client('s3')
        
        # Silver path
        silver_key = f"{SILVER_PREFIX}/year={params['year']}/month={params['month']}/day={params['day']}/locationid={params['location_id']}/data.parquet"
        
        # Write Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, compression='snappy')
        parquet_buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=silver_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        print(f"[OK] Written Silver: s3://{S3_BUCKET}/{silver_key}")
        print(f"   Records: {len(df)}")
        
        ti.xcom_push(key='silver_path', value=silver_key)
        
        return {'written': True, 'path': silver_key, 'records': len(df)}
    

    
    def aggregate_hourly(**context):
        """
        Task 12: Aggregate dữ liệu theo giờ
        """
        import pandas as pd
        import boto3
        from io import BytesIO
        
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        silver_path = ti.xcom_pull(task_ids='write_silver', key='silver_path')
        
        s3_client = boto3.client('s3')
        
        # Read from Silver
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=silver_path)
        df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
        
        print(f"Aggregating hourly from {len(df)} records")
        
        # Parse datetime if needed
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.date.astype(str)
            df['hour'] = df['datetime'].dt.hour
        
        # Group by date, hour, parameter
        group_cols = ['date', 'hour']
        if 'parameter' in df.columns:
            group_cols.append('parameter')
        
        if 'value' in df.columns:
            hourly_agg = df.groupby(group_cols).agg({
                'value': ['min', 'max', 'mean', 'median', 'count', 'std']
            }).reset_index()
            
            # Flatten columns
            hourly_agg.columns = group_cols + ['value_min', 'value_max', 'value_avg', 'value_median', 'value_count', 'value_std']
        else:
            hourly_agg = df.groupby(group_cols).size().reset_index(name='count')
        
        # Write to Gold
        gold_key = f"{GOLD_PREFIX}/fact_aqi_hourly/year={params['year']}/month={params['month']}/day={params['day']}/locationid={params['location_id']}/data.parquet"
        
        parquet_buffer = BytesIO()
        hourly_agg.to_parquet(parquet_buffer, index=False, compression='snappy')
        parquet_buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=gold_key,
            Body=parquet_buffer.getvalue()
        )
        
        print(f"Written Gold hourly: {gold_key}")
        
        ti.xcom_push(key='hourly_path', value=gold_key)
        
        return {'path': gold_key, 'records': len(hourly_agg)}
    
    def aggregate_daily(**context):
        """
        Task 13: Aggregate dữ liệu theo ngày
        """
        import pandas as pd
        import boto3
        from io import BytesIO
        
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        silver_path = ti.xcom_pull(task_ids='write_silver', key='silver_path')
        
        s3_client = boto3.client('s3')
        
        # Read from Silver
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=silver_path)
        df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
        
        print(f"Aggregating daily from {len(df)} records")
        
        # Parse datetime if needed
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.date.astype(str)
        
        # Group by date, parameter
        group_cols = ['date']
        if 'parameter' in df.columns:
            group_cols.append('parameter')
        
        if 'value' in df.columns:
            daily_agg = df.groupby(group_cols).agg({
                'value': ['min', 'max', 'mean', 'median', 'count', 'std']
            }).reset_index()
            
            daily_agg.columns = group_cols + ['value_min', 'value_max', 'value_avg', 'value_median', 'value_count', 'value_std']
        else:
            daily_agg = df.groupby(group_cols).size().reset_index(name='count')
        
        # Write to Gold
        gold_key = f"{GOLD_PREFIX}/fact_aqi_daily/year={params['year']}/month={params['month']}/locationid={params['location_id']}/day_{params['day']}.parquet"
        
        parquet_buffer = BytesIO()
        daily_agg.to_parquet(parquet_buffer, index=False, compression='snappy')
        parquet_buffer.seek(0)
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=gold_key,
            Body=parquet_buffer.getvalue()
        )
        
        print(f"Written Gold daily: {gold_key}")
        
        ti.xcom_push(key='daily_path', value=gold_key)
        
        return {'path': gold_key, 'records': len(daily_agg)}
    
    def build_dimensions(**context):
        """
        Task 14: Tạo/cập nhật dimension tables
        """
        import pandas as pd
        import boto3
        from io import BytesIO
        from datetime import datetime
        
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        silver_path = ti.xcom_pull(task_ids='write_silver', key='silver_path')
        
        s3_client = boto3.client('s3')
        
        # Read from Silver
        file_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=silver_path)
        df = pd.read_parquet(BytesIO(file_obj['Body'].read()))
        
        dimensions_created = []
        
        # 1. Dim Location
        if 'location_id' in df.columns:
            dim_location = df[['location_id']].drop_duplicates()
            dim_location['location_name'] = f"Location {params['location_id']}"
            dim_location['city'] = 'Hanoi'
            dim_location['country'] = 'Vietnam'
            
            dim_key = f"{GOLD_PREFIX}/dim_location/location_{params['location_id']}.parquet"
            buffer = BytesIO()
            dim_location.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_client.put_object(Bucket=S3_BUCKET, Key=dim_key, Body=buffer.getvalue())
            dimensions_created.append('dim_location')
            print(f"  Created: {dim_key}")
        
        # 2. Dim Parameter
        if 'parameter' in df.columns:
            dim_param = df[['parameter']].drop_duplicates()
            dim_param['parameter_name'] = dim_param['parameter'].str.upper()
            dim_param['unit'] = 'µg/m³'
            
            dim_key = f"{GOLD_PREFIX}/dim_parameter/parameters.parquet"
            buffer = BytesIO()
            dim_param.to_parquet(buffer, index=False)
            buffer.seek(0)
            s3_client.put_object(Bucket=S3_BUCKET, Key=dim_key, Body=buffer.getvalue())
            dimensions_created.append('dim_parameter')
            print(f"  Created: {dim_key}")
        
        # 3. Dim Time
        dim_time = pd.DataFrame({
            'date': [params['day']],
            'month': [params['month']],
            'year': [params['year']],
            'day_of_week': [datetime.strptime(f"{params['year']}-{params['month']}-{params['day']}", '%Y-%m-%d').strftime('%A')],
            'is_weekend': [datetime.strptime(f"{params['year']}-{params['month']}-{params['day']}", '%Y-%m-%d').weekday() >= 5],
        })
        
        dim_key = f"{GOLD_PREFIX}/dim_time/date_{params['year']}{params['month']}{params['day']}.parquet"
        buffer = BytesIO()
        dim_time.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=S3_BUCKET, Key=dim_key, Body=buffer.getvalue())
        dimensions_created.append('dim_time')
        print(f"  Created: {dim_key}")
        
        print(f"Built {len(dimensions_created)} dimension tables")
        
        return {'dimensions': dimensions_created}
    
    def write_gold_summary(**context):
        """
        Task 15: Tổng hợp và ghi metadata cho Gold zone
        """
        import json
        import boto3
        from datetime import datetime
        
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        quality_report = ti.xcom_pull(task_ids='quality_check', key='quality_report')
        hourly_path = ti.xcom_pull(task_ids='aggregate_hourly', key='hourly_path')
        daily_path = ti.xcom_pull(task_ids='aggregate_daily', key='daily_path')
        
        s3_client = boto3.client('s3')
        
        # Create summary metadata
        summary = {
            'pipeline_run': datetime.utcnow().isoformat(),
            'execution_date': f"{params['year']}-{params['month']}-{params['day']}",
            'location_id': params['location_id'],
            'quality_report': quality_report,
            'outputs': {
                'hourly': hourly_path,
                'daily': daily_path,
            },
            'status': 'SUCCESS'
        }
        
        # Write summary
        summary_key = f"{GOLD_PREFIX}/metadata/run_{params['year']}{params['month']}{params['day']}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=summary_key,
            Body=json.dumps(summary, indent=2, default=str),
            ContentType='application/json'
        )
        
        print(f"Written Gold summary: {summary_key}")
        
        return summary
    
    def notify_pipeline_complete(**context):
        """
        Task 16: Thông báo pipeline hoàn thành
        """
        ti = context['ti']
        params = ti.xcom_pull(task_ids='ingest_bronze', key='params')
        quality_report = ti.xcom_pull(task_ids='quality_check', key='quality_report')
        
        print("=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 60)
        print(f"Date: {params['year']}-{params['month']}-{params['day']}")
        print(f"Location: {params['location_id']}")
        print(f"Records processed: {quality_report.get('total_records', 'N/A')}")
        print(f"Quality check: PASSED")
        print("=" * 60)
        
        # TODO: 
        
        return {'status': 'completed', 'date': f"{params['year']}-{params['month']}-{params['day']}"}
    

    # Start
    start = EmptyOperator(task_id='start')
    
    # Bronze Zone
    check_bronze = BranchPythonOperator(
        task_id='check_bronze_exists',
        python_callable=check_bronze_exists,
    )
    
    skip_no_data = EmptyOperator(task_id='skip_no_data')
    
    ingest = PythonOperator(
        task_id='ingest_bronze',
        python_callable=ingest_bronze,
    )
    
    # Silver Zone - Cleaning (sequential chain)
    dedupe = PythonOperator(
        task_id='deduplicate_records',
        python_callable=deduplicate_records,
    )
    
    normalize = PythonOperator(
        task_id='normalize_units',
        python_callable=normalize_units,
    )
    
    parse_dt = PythonOperator(
        task_id='parse_datetime',
        python_callable=parse_datetime,
    )
    
    merge = PythonOperator(
        task_id='merge_cleaned_data',
        python_callable=merge_cleaned_data,
    )
    
    validate = PythonOperator(
        task_id='validate_ranges',
        python_callable=validate_ranges,
    )
    
    outliers = PythonOperator(
        task_id='remove_outliers',
        python_callable=remove_outliers,
    )
    
    calc_aqi = PythonOperator(
        task_id='calculate_aqi',
        python_callable=calculate_aqi,
    )
    
    quality = PythonOperator(
        task_id='quality_check',
        python_callable=quality_check,
    )
    
    write_slv = PythonOperator(
        task_id='write_silver',
        python_callable=write_silver,
    )
    
    # Gold Zone - Aggregation (parallel)
    agg_hourly = PythonOperator(
        task_id='aggregate_hourly',
        python_callable=aggregate_hourly,
    )
    
    agg_daily = PythonOperator(
        task_id='aggregate_daily',
        python_callable=aggregate_daily,
    )
    
    build_dims = PythonOperator(
        task_id='build_dimensions',
        python_callable=build_dimensions,
    )
    
    # Gold Summary
    gold_summary = PythonOperator(
        task_id='write_gold_summary',
        python_callable=write_gold_summary,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    # Notification
    notify = PythonOperator(
        task_id='notify_pipeline_complete',
        python_callable=notify_pipeline_complete,
    )
    
    # End
    end = EmptyOperator(
        task_id='end',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    

    
    # Bronze
    start >> check_bronze
    check_bronze >> [ingest, skip_no_data]
    skip_no_data >> end
    
    # Silver - Cleaning chain
    ingest >> dedupe >> normalize >> parse_dt >> merge >> validate >> outliers >> calc_aqi >> quality >> write_slv
    
    # Gold - Parallel aggregation
    write_slv >> [agg_hourly, agg_daily, build_dims]
    
    # Gold Summary
    [agg_hourly, agg_daily, build_dims] >> gold_summary >> notify >> end

