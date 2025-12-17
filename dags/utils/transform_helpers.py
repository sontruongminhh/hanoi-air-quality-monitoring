"""
Helper functions cho ETL pipeline
Air Quality Data Processing Utilities

Version: 2.0
Author: AQI Team
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Tuple, Optional, Dict, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

# Valid ranges cho từng parameter (µg/m³)
VALID_RANGES: Dict[str, Tuple[float, float]] = {
    'pm25': (0, 1000),
    'pm10': (0, 2000),
    'no2': (0, 1000),
    'o3': (0, 1000),
    'co': (0, 50000),
    'so2': (0, 2000),
}

# Unit conversion factors to µg/m³
UNIT_CONVERSIONS: Dict[Tuple[str, str], float] = {
    ('co', 'ppm'): 1145,      # CO: 1 ppm = 1145 µg/m³ at 25°C
    ('no2', 'ppb'): 1.88,     # NO2: 1 ppb = 1.88 µg/m³
    ('o3', 'ppb'): 2.0,       # O3: 1 ppb = 2.0 µg/m³
    ('so2', 'ppb'): 2.62,     # SO2: 1 ppb = 2.62 µg/m³
    ('*', 'mg/m³'): 1000,     # Generic mg to µg
}

# AQI Breakpoints cho PM2.5 (US EPA)
AQI_BREAKPOINTS_PM25 = [
    (0, 12.0, 0, 50, 'Good', '#00E400'),
    (12.1, 35.4, 51, 100, 'Moderate', '#FFFF00'),
    (35.5, 55.4, 101, 150, 'Unhealthy for Sensitive Groups', '#FF7E00'),
    (55.5, 150.4, 151, 200, 'Unhealthy', '#FF0000'),
    (150.5, 250.4, 201, 300, 'Very Unhealthy', '#8F3F97'),
    (250.5, 500.4, 301, 500, 'Hazardous', '#7E0023'),
]

# AQI Breakpoints cho PM10 (US EPA)
AQI_BREAKPOINTS_PM10 = [
    (0, 54, 0, 50, 'Good', '#00E400'),
    (55, 154, 51, 100, 'Moderate', '#FFFF00'),
    (155, 254, 101, 150, 'Unhealthy for Sensitive Groups', '#FF7E00'),
    (255, 354, 151, 200, 'Unhealthy', '#FF0000'),
    (355, 424, 201, 300, 'Very Unhealthy', '#8F3F97'),
    (425, 604, 301, 500, 'Hazardous', '#7E0023'),
]


# ============================================================================
# UNIT NORMALIZATION
# ============================================================================

def normalize_units(df, parameter_column='parameter', value_column='value', unit_column='unit'):
    """
    Chuẩn hóa đơn vị về µg/m³ cho các thông số
    
    Args:
        df: DataFrame chứa dữ liệu
        parameter_column: Tên cột chứa parameter (pm25, pm10, no2, etc.)
        value_column: Tên cột chứa giá trị
        unit_column: Tên cột chứa đơn vị
    
    Returns:
        DataFrame đã được chuẩn hóa
    """
    df = df.copy()
    
    # Mapping đơn vị cần convert
    unit_conversions = {
        'mg/m³': 1000,  # mg/m³ -> µg/m³
        'ppm': None,  # ppm cần convert theo từng parameter
        'ppb': None,  # ppb cần convert theo từng parameter
    }
    
    # Convert ppm/ppb sang µg/m³ (tùy theo parameter)
    # PM2.5, PM10: đã là µg/m³
    # NO2: 1 ppb = 1.88 µg/m³
    # O3: 1 ppb = 2.0 µg/m³
    # CO: 1 ppm = 1000 µg/m³
    # SO2: 1 ppb = 2.62 µg/m³
    
    for idx, row in df.iterrows():
        param = str(row.get(parameter_column, '')).lower()
        unit = str(row.get(unit_column, '')).lower()
        value = row.get(value_column)
        
        if pd.isna(value) or value is None:
            continue
        
        # Convert ppm/ppb sang µg/m³
        if 'ppm' in unit:
            if param in ['co']:
                df.at[idx, value_column] = value * 1000  # CO: ppm -> µg/m³
                df.at[idx, unit_column] = 'µg/m³'
            else:
                # Các parameter khác ít dùng ppm
                df.at[idx, value_column] = value * 1000
                df.at[idx, unit_column] = 'µg/m³'
        
        elif 'ppb' in unit:
            if param in ['no2']:
                df.at[idx, value_column] = value * 1.88  # NO2: ppb -> µg/m³
                df.at[idx, unit_column] = 'µg/m³'
            elif param in ['o3']:
                df.at[idx, value_column] = value * 2.0  # O3: ppb -> µg/m³
                df.at[idx, unit_column] = 'µg/m³'
            elif param in ['so2']:
                df.at[idx, value_column] = value * 2.62  # SO2: ppb -> µg/m³
                df.at[idx, unit_column] = 'µg/m³'
            else:
                # Giữ nguyên nếu không có conversion factor
                pass
        
        elif 'mg/m³' in unit:
            df.at[idx, value_column] = value * 1000  # mg/m³ -> µg/m³
            df.at[idx, unit_column] = 'µg/m³'
    
    return df


def validate_data(df, parameter_column='parameter', value_column='value'):
    """
    Validate dữ liệu: loại bỏ outliers và giá trị không hợp lệ
    
    Args:
        df: DataFrame chứa dữ liệu
        parameter_column: Tên cột chứa parameter
        value_column: Tên cột chứa giá trị
    
    Returns:
        DataFrame đã được validate
    """
    df = df.copy()
    
    # Ngưỡng hợp lệ cho từng parameter (µg/m³)
    valid_ranges = {
        'pm25': (0, 1000),
        'pm10': (0, 2000),
        'no2': (0, 1000),
        'o3': (0, 1000),
        'co': (0, 50000),
        'so2': (0, 2000),
    }
    
    # Loại bỏ giá trị âm hoặc quá lớn
    mask = pd.Series([True] * len(df))
    
    for idx, row in df.iterrows():
        param = str(row.get(parameter_column, '')).lower()
        value = row.get(value_column)
        
        if pd.isna(value) or value is None:
            mask[idx] = False
            continue
        
        # Kiểm tra range hợp lệ
        if param in valid_ranges:
            min_val, max_val = valid_ranges[param]
            if value < min_val or value > max_val:
                mask[idx] = False
                continue
        
        # Loại bỏ giá trị = 0 (có thể là missing data)
        if value == 0:
            mask[idx] = False
    
    return df[mask].reset_index(drop=True)


def calculate_aqi_from_pm25(pm25_value):
    """
    Tính AQI từ giá trị PM2.5 (µg/m³) theo tiêu chuẩn US EPA
    
    Args:
        pm25_value: Giá trị PM2.5 (µg/m³)
    
    Returns:
        AQI value và level
    """
    if pd.isna(pm25_value) or pm25_value is None:
        return None, None
    
    # Breakpoints cho PM2.5 (µg/m³) -> AQI
    breakpoints = [
        (0, 12.0, 0, 50, 'Good'),
        (12.1, 35.4, 51, 100, 'Moderate'),
        (35.5, 55.4, 101, 150, 'Unhealthy for Sensitive Groups'),
        (55.5, 150.4, 151, 200, 'Unhealthy'),
        (150.5, 250.4, 201, 300, 'Very Unhealthy'),
        (250.5, float('inf'), 301, 500, 'Hazardous'),
    ]
    
    for bp_low, bp_high, aqi_low, aqi_high, level in breakpoints:
        if bp_low <= pm25_value <= bp_high:
            if bp_high == float('inf'):
                aqi = aqi_high
            else:
                aqi = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (pm25_value - bp_low) + aqi_low)
            return aqi, level
    
    return None, None


def aggregate_hourly(df, datetime_column='datetime', value_column='value', parameter_column='parameter'):
    """
    Aggregate dữ liệu theo giờ
    
    Args:
        df: DataFrame chứa dữ liệu
        datetime_column: Tên cột chứa datetime
        value_column: Tên cột chứa giá trị
        parameter_column: Tên cột chứa parameter
    
    Returns:
        DataFrame đã được aggregate theo giờ
    """
    df = df.copy()
    
    # Đảm bảo datetime column là datetime type
    if datetime_column in df.columns:
        df[datetime_column] = pd.to_datetime(df[datetime_column])
        df['date'] = df[datetime_column].dt.date
        df['hour'] = df[datetime_column].dt.hour
    else:
        raise ValueError(f"Column '{datetime_column}' not found in DataFrame")
    
    # Group by date, hour, parameter
    agg_df = df.groupby(['date', 'hour', parameter_column]).agg({
        value_column: ['min', 'max', 'mean', 'median', 'count', 'std']
    }).reset_index()
    
    # Flatten column names
    agg_df.columns = [
        'date', 'hour', parameter_column,
        'value_min', 'value_max', 'value_avg', 'value_median', 'value_count', 'value_std'
    ]
    
    return agg_df


def aggregate_daily(df, datetime_column='datetime', value_column='value', parameter_column='parameter'):
    """
    Aggregate dữ liệu theo ngày
    
    Args:
        df: DataFrame chứa dữ liệu
        datetime_column: Tên cột chứa datetime
        value_column: Tên cột chứa giá trị
        parameter_column: Tên cột chứa parameter
    
    Returns:
        DataFrame đã được aggregate theo ngày
    """
    df = df.copy()
    
    # Đảm bảo datetime column là datetime type
    if datetime_column in df.columns:
        df[datetime_column] = pd.to_datetime(df[datetime_column])
        df['date'] = df[datetime_column].dt.date
    else:
        raise ValueError(f"Column '{datetime_column}' not found in DataFrame")
    
    # Group by date, parameter
    agg_df = df.groupby(['date', parameter_column]).agg({
        value_column: ['min', 'max', 'mean', 'median', 'count', 'std']
    }).reset_index()
    
    # Flatten column names
    agg_df.columns = [
        'date', parameter_column,
        'value_min', 'value_max', 'value_avg', 'value_median', 'value_count', 'value_std'
    ]
    
    return agg_df


# ============================================================================
# DATA QUALITY FUNCTIONS
# ============================================================================

def check_data_quality(
    df: pd.DataFrame,
    min_records: int = 10,
    max_null_percent: float = 20.0,
    max_duplicate_percent: float = 5.0
) -> Dict:
    """
    Kiểm tra data quality và trả về báo cáo chi tiết
    
    Args:
        df: DataFrame cần kiểm tra
        min_records: Số records tối thiểu
        max_null_percent: Phần trăm null tối đa cho phép
        max_duplicate_percent: Phần trăm duplicate tối đa cho phép
    
    Returns:
        Dict chứa quality report
    """
    total_records = len(df)
    
    # Check nulls
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    null_percent = (total_nulls / (total_records * len(df.columns))) * 100 if total_records > 0 else 0
    
    # Check duplicates
    duplicate_count = df.duplicated().sum()
    duplicate_percent = (duplicate_count / total_records) * 100 if total_records > 0 else 0
    
    # Build report
    issues = []
    passed = True
    
    if total_records < min_records:
        issues.append(f"Too few records: {total_records} < {min_records}")
        passed = False
    
    if null_percent > max_null_percent:
        issues.append(f"Too many nulls: {null_percent:.1f}% > {max_null_percent}%")
        passed = False
    
    if duplicate_percent > max_duplicate_percent:
        issues.append(f"Too many duplicates: {duplicate_percent:.1f}% > {max_duplicate_percent}%")
        passed = False
    
    report = {
        'total_records': total_records,
        'null_count': int(total_nulls),
        'null_percent': round(null_percent, 2),
        'duplicate_count': int(duplicate_count),
        'duplicate_percent': round(duplicate_percent, 2),
        'columns': list(df.columns),
        'column_null_counts': null_counts.to_dict(),
        'passed': passed,
        'issues': issues,
    }
    
    logger.info(f"Quality check: {'PASSED' if passed else 'FAILED'} - {total_records} records, {null_percent:.1f}% nulls")
    
    return report


def remove_outliers_iqr(
    df: pd.DataFrame,
    value_column: str = 'value',
    multiplier: float = 1.5
) -> Tuple[pd.DataFrame, int]:
    """
    Loại bỏ outliers bằng phương pháp IQR
    
    Args:
        df: DataFrame chứa dữ liệu
        value_column: Tên cột chứa giá trị
        multiplier: Hệ số IQR (default 1.5)
    
    Returns:
        Tuple (DataFrame đã clean, số outliers đã loại bỏ)
    """
    if value_column not in df.columns:
        logger.warning(f"Column '{value_column}' not found")
        return df, 0
    
    original_count = len(df)
    
    Q1 = df[value_column].quantile(0.25)
    Q3 = df[value_column].quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    df_clean = df[(df[value_column] >= lower_bound) & (df[value_column] <= upper_bound)].copy()
    
    removed_count = original_count - len(df_clean)
    logger.info(f"Removed {removed_count} outliers (IQR method, multiplier={multiplier})")
    
    return df_clean, removed_count


def remove_outliers_zscore(
    df: pd.DataFrame,
    value_column: str = 'value',
    threshold: float = 3.0
) -> Tuple[pd.DataFrame, int]:
    """
    Loại bỏ outliers bằng phương pháp Z-score
    
    Args:
        df: DataFrame chứa dữ liệu
        value_column: Tên cột chứa giá trị
        threshold: Ngưỡng Z-score (default 3.0)
    
    Returns:
        Tuple (DataFrame đã clean, số outliers đã loại bỏ)
    """
    if value_column not in df.columns:
        logger.warning(f"Column '{value_column}' not found")
        return df, 0
    
    original_count = len(df)
    
    mean_val = df[value_column].mean()
    std_val = df[value_column].std()
    
    if std_val == 0:
        return df, 0
    
    z_scores = np.abs((df[value_column] - mean_val) / std_val)
    df_clean = df[z_scores < threshold].copy()
    
    removed_count = original_count - len(df_clean)
    logger.info(f"Removed {removed_count} outliers (Z-score method, threshold={threshold})")
    
    return df_clean, removed_count


# ============================================================================
# AQI CALCULATION FUNCTIONS
# ============================================================================

def calculate_aqi_pm25(pm25_value: float) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Tính AQI từ giá trị PM2.5 (µg/m³) theo tiêu chuẩn US EPA
    
    Args:
        pm25_value: Giá trị PM2.5 (µg/m³)
    
    Returns:
        Tuple (AQI value, level name, color code)
    """
    if pd.isna(pm25_value) or pm25_value is None or pm25_value < 0:
        return None, None, None
    
    for bp_low, bp_high, aqi_low, aqi_high, level, color in AQI_BREAKPOINTS_PM25:
        if bp_low <= pm25_value <= bp_high:
            aqi = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (pm25_value - bp_low) + aqi_low)
            return aqi, level, color
    
    # Nếu vượt quá 500.4
    if pm25_value > 500.4:
        return 500, 'Hazardous', '#7E0023'
    
    return None, None, None


def calculate_aqi_pm10(pm10_value: float) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Tính AQI từ giá trị PM10 (µg/m³) theo tiêu chuẩn US EPA
    
    Args:
        pm10_value: Giá trị PM10 (µg/m³)
    
    Returns:
        Tuple (AQI value, level name, color code)
    """
    if pd.isna(pm10_value) or pm10_value is None or pm10_value < 0:
        return None, None, None
    
    for bp_low, bp_high, aqi_low, aqi_high, level, color in AQI_BREAKPOINTS_PM10:
        if bp_low <= pm10_value <= bp_high:
            aqi = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (pm10_value - bp_low) + aqi_low)
            return aqi, level, color
    
    if pm10_value > 604:
        return 500, 'Hazardous', '#7E0023'
    
    return None, None, None


def add_aqi_columns(df: pd.DataFrame, parameter_column: str = 'parameter', value_column: str = 'value') -> pd.DataFrame:
    """
    Thêm các cột AQI vào DataFrame
    
    Args:
        df: DataFrame chứa dữ liệu
        parameter_column: Tên cột chứa parameter
        value_column: Tên cột chứa giá trị
    
    Returns:
        DataFrame với các cột AQI mới
    """
    df = df.copy()
    df['aqi'] = None
    df['aqi_level'] = None
    df['aqi_color'] = None
    
    for idx, row in df.iterrows():
        param = str(row.get(parameter_column, '')).lower()
        value = row.get(value_column)
        
        if param == 'pm25':
            aqi, level, color = calculate_aqi_pm25(value)
        elif param == 'pm10':
            aqi, level, color = calculate_aqi_pm10(value)
        else:
            continue
        
        df.at[idx, 'aqi'] = aqi
        df.at[idx, 'aqi_level'] = level
        df.at[idx, 'aqi_color'] = color
    
    aqi_count = df['aqi'].notna().sum()
    logger.info(f"Calculated AQI for {aqi_count} records")
    
    return df


# ============================================================================
# DATETIME UTILITIES
# ============================================================================

def parse_datetime_column(
    df: pd.DataFrame,
    datetime_column: str = 'datetime',
    timezone: str = 'UTC'
) -> pd.DataFrame:
    """
    Parse và chuẩn hóa datetime column
    
    Args:
        df: DataFrame chứa dữ liệu
        datetime_column: Tên cột datetime
        timezone: Timezone để convert
    
    Returns:
        DataFrame với datetime đã parse
    """
    df = df.copy()
    
    if datetime_column not in df.columns:
        logger.warning(f"Column '{datetime_column}' not found")
        return df
    
    try:
        df[datetime_column] = pd.to_datetime(df[datetime_column], utc=True)
        
        # Extract useful date parts
        df['date'] = df[datetime_column].dt.date
        df['hour'] = df[datetime_column].dt.hour
        df['day_of_week'] = df[datetime_column].dt.dayofweek
        df['day_name'] = df[datetime_column].dt.day_name()
        df['is_weekend'] = df['day_of_week'] >= 5
        
        logger.info(f"Parsed datetime column: {datetime_column}")
        
    except Exception as e:
        logger.error(f"Error parsing datetime: {e}")
    
    return df


# ============================================================================
# DEDUPLICATION
# ============================================================================

def deduplicate_records(
    df: pd.DataFrame,
    key_columns: Optional[List[str]] = None,
    keep: str = 'first'
) -> Tuple[pd.DataFrame, int]:
    """
    Loại bỏ duplicate records
    
    Args:
        df: DataFrame chứa dữ liệu
        key_columns: Các cột dùng để xác định duplicate
        keep: 'first', 'last', hoặc False
    
    Returns:
        Tuple (DataFrame đã dedupe, số duplicates đã loại bỏ)
    """
    original_count = len(df)
    
    if key_columns is None:
        # Auto-detect key columns
        potential_keys = ['location_id', 'datetime', 'parameter', 'timestamp']
        key_columns = [c for c in potential_keys if c in df.columns]
        
        if not key_columns:
            key_columns = df.columns.tolist()[:3]
    
    df_deduped = df.drop_duplicates(subset=key_columns, keep=keep).reset_index(drop=True)
    
    removed_count = original_count - len(df_deduped)
    logger.info(f"Deduplicated: {original_count} -> {len(df_deduped)} (removed {removed_count})")
    
    return df_deduped, removed_count


# ============================================================================
# FULL TRANSFORM PIPELINE
# ============================================================================

def transform_bronze_to_silver(
    df: pd.DataFrame,
    parameter_column: str = 'parameter',
    value_column: str = 'value',
    unit_column: str = 'unit',
    datetime_column: str = 'datetime'
) -> Tuple[pd.DataFrame, Dict]:
    """
    Full transform pipeline từ Bronze sang Silver
    
    Args:
        df: DataFrame raw từ Bronze
        parameter_column: Tên cột parameter
        value_column: Tên cột value
        unit_column: Tên cột unit
        datetime_column: Tên cột datetime
    
    Returns:
        Tuple (DataFrame đã transform, transform report)
    """
    report = {
        'original_records': len(df),
        'steps': [],
    }
    
    # Step 1: Deduplicate
    df, removed = deduplicate_records(df)
    report['steps'].append({'step': 'deduplicate', 'removed': removed})
    
    # Step 2: Normalize units
    df = normalize_units(df, parameter_column, value_column, unit_column)
    report['steps'].append({'step': 'normalize_units', 'status': 'completed'})
    
    # Step 3: Validate ranges
    df = validate_data(df, parameter_column, value_column)
    report['steps'].append({'step': 'validate_ranges', 'records_after': len(df)})
    
    # Step 4: Parse datetime
    df = parse_datetime_column(df, datetime_column)
    report['steps'].append({'step': 'parse_datetime', 'status': 'completed'})
    
    # Step 5: Remove outliers
    df, removed = remove_outliers_iqr(df, value_column)
    report['steps'].append({'step': 'remove_outliers', 'removed': removed})
    
    # Step 6: Add AQI
    df = add_aqi_columns(df, parameter_column, value_column)
    report['steps'].append({'step': 'calculate_aqi', 'status': 'completed'})
    
    # Step 7: Quality check
    quality = check_data_quality(df)
    report['quality_check'] = quality
    
    report['final_records'] = len(df)
    report['success'] = quality['passed']
    
    logger.info(f"Transform complete: {report['original_records']} -> {report['final_records']} records")
    
    return df, report
