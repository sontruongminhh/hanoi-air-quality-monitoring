"""
Data Quality Rules Configuration
Định nghĩa các quy tắc kiểm tra chất lượng dữ liệu

Sử dụng trong các DAG tasks để validate data trước khi ghi vào Silver/Gold
"""

from typing import Dict, List, Tuple, Any
from dataclasses import dataclass
from enum import Enum


class QualityLevel(Enum):
    """Mức độ nghiêm trọng của quality issue"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityRule:
    """Định nghĩa một quality rule"""
    name: str
    description: str
    level: QualityLevel
    threshold: Any
    check_type: str  # 'null_check', 'range_check', 'duplicate_check', 'count_check'


# ============================================================================
# THRESHOLDS
# ============================================================================

# Số records tối thiểu
MIN_RECORDS = 10

# Phần trăm null tối đa
MAX_NULL_PERCENT = 20.0

# Phần trăm duplicate tối đa
MAX_DUPLICATE_PERCENT = 5.0

# Phần trăm outliers tối đa
MAX_OUTLIER_PERCENT = 10.0


# ============================================================================
# VALID RANGES CHO TỪNG PARAMETER (µg/m³)
# ============================================================================

VALID_RANGES: Dict[str, Tuple[float, float]] = {
    # Particulate Matter
    'pm25': (0, 1000),      # PM2.5: 0-1000 µg/m³
    'pm10': (0, 2000),      # PM10: 0-2000 µg/m³
    'pm1': (0, 500),        # PM1: 0-500 µg/m³
    
    # Gases
    'no2': (0, 1000),       # NO2: 0-1000 µg/m³
    'no': (0, 500),         # NO: 0-500 µg/m³
    'nox': (0, 1500),       # NOx: 0-1500 µg/m³
    'o3': (0, 1000),        # O3: 0-1000 µg/m³
    'co': (0, 50000),       # CO: 0-50000 µg/m³
    'so2': (0, 2000),       # SO2: 0-2000 µg/m³
    
    # Other
    'bc': (0, 100),         # Black Carbon: 0-100 µg/m³
    'temperature': (-50, 60),  # Temperature: -50 to 60 °C
    'humidity': (0, 100),   # Humidity: 0-100 %
    'pressure': (800, 1200),  # Pressure: 800-1200 hPa
}


# ============================================================================
# AQI BREAKPOINTS (US EPA)
# ============================================================================

AQI_CATEGORIES = [
    {'min': 0, 'max': 50, 'level': 'Good', 'color': '#00E400', 'health_message': 'Chất lượng không khí tốt'},
    {'min': 51, 'max': 100, 'level': 'Moderate', 'color': '#FFFF00', 'health_message': 'Chất lượng không khí chấp nhận được'},
    {'min': 101, 'max': 150, 'level': 'Unhealthy for Sensitive Groups', 'color': '#FF7E00', 'health_message': 'Không tốt cho nhóm nhạy cảm'},
    {'min': 151, 'max': 200, 'level': 'Unhealthy', 'color': '#FF0000', 'health_message': 'Không tốt cho sức khỏe'},
    {'min': 201, 'max': 300, 'level': 'Very Unhealthy', 'color': '#8F3F97', 'health_message': 'Rất không tốt cho sức khỏe'},
    {'min': 301, 'max': 500, 'level': 'Hazardous', 'color': '#7E0023', 'health_message': 'Nguy hiểm'},
]

# PM2.5 breakpoints (24-hour average, µg/m³)
PM25_BREAKPOINTS = [
    (0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 500.4, 301, 500),
]

# PM10 breakpoints (24-hour average, µg/m³)
PM10_BREAKPOINTS = [
    (0, 54, 0, 50),
    (55, 154, 51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 604, 301, 500),
]

# O3 breakpoints (8-hour average, ppb)
O3_BREAKPOINTS = [
    (0, 54, 0, 50),
    (55, 70, 51, 100),
    (71, 85, 101, 150),
    (86, 105, 151, 200),
    (106, 200, 201, 300),
]

# NO2 breakpoints (1-hour average, ppb)
NO2_BREAKPOINTS = [
    (0, 53, 0, 50),
    (54, 100, 51, 100),
    (101, 360, 101, 150),
    (361, 649, 151, 200),
    (650, 1249, 201, 300),
    (1250, 2049, 301, 500),
]

# CO breakpoints (8-hour average, ppm)
CO_BREAKPOINTS = [
    (0, 4.4, 0, 50),
    (4.5, 9.4, 51, 100),
    (9.5, 12.4, 101, 150),
    (12.5, 15.4, 151, 200),
    (15.5, 30.4, 201, 300),
    (30.5, 50.4, 301, 500),
]

# SO2 breakpoints (1-hour average, ppb)
SO2_BREAKPOINTS = [
    (0, 35, 0, 50),
    (36, 75, 51, 100),
    (76, 185, 101, 150),
    (186, 304, 151, 200),
    (305, 604, 201, 300),
    (605, 1004, 301, 500),
]


# ============================================================================
# QUALITY RULES DEFINITIONS
# ============================================================================

QUALITY_RULES: List[QualityRule] = [
    QualityRule(
        name="min_records",
        description="Kiểm tra số lượng records tối thiểu",
        level=QualityLevel.ERROR,
        threshold=MIN_RECORDS,
        check_type="count_check"
    ),
    QualityRule(
        name="max_null_percent",
        description="Kiểm tra phần trăm giá trị null",
        level=QualityLevel.WARNING,
        threshold=MAX_NULL_PERCENT,
        check_type="null_check"
    ),
    QualityRule(
        name="max_duplicate_percent",
        description="Kiểm tra phần trăm records trùng lặp",
        level=QualityLevel.WARNING,
        threshold=MAX_DUPLICATE_PERCENT,
        check_type="duplicate_check"
    ),
    QualityRule(
        name="value_range",
        description="Kiểm tra giá trị trong khoảng hợp lệ",
        level=QualityLevel.ERROR,
        threshold=VALID_RANGES,
        check_type="range_check"
    ),
]


# ============================================================================
# UNIT CONVERSION FACTORS
# ============================================================================

# Convert sang µg/m³
UNIT_CONVERSIONS = {
    # Từ ppm sang µg/m³ (tại 25°C, 1 atm)
    ('co', 'ppm'): 1145,      # CO: 1 ppm = 1145 µg/m³
    ('no2', 'ppm'): 1880,     # NO2: 1 ppm = 1880 µg/m³
    ('o3', 'ppm'): 1960,      # O3: 1 ppm = 1960 µg/m³
    ('so2', 'ppm'): 2620,     # SO2: 1 ppm = 2620 µg/m³
    
    # Từ ppb sang µg/m³
    ('co', 'ppb'): 1.145,     # CO: 1 ppb = 1.145 µg/m³
    ('no2', 'ppb'): 1.88,     # NO2: 1 ppb = 1.88 µg/m³
    ('o3', 'ppb'): 1.96,      # O3: 1 ppb = 1.96 µg/m³
    ('so2', 'ppb'): 2.62,     # SO2: 1 ppb = 2.62 µg/m³
    
    # Từ mg/m³ sang µg/m³
    ('*', 'mg/m³'): 1000,
    ('*', 'mg/m3'): 1000,
}


# ============================================================================
# ALERT THRESHOLDS
# ============================================================================

ALERT_THRESHOLDS = {
    'aqi_unhealthy': 150,           # AQI > 150: Unhealthy
    'aqi_very_unhealthy': 200,      # AQI > 200: Very Unhealthy
    'aqi_hazardous': 300,           # AQI > 300: Hazardous
    
    'pm25_who_guideline': 15,       # WHO guideline (24h average)
    'pm25_vn_standard': 50,         # Vietnam QCVN 05:2013
    
    'pm10_who_guideline': 45,       # WHO guideline (24h average)
    'pm10_vn_standard': 100,        # Vietnam QCVN 05:2013
}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_aqi_category(aqi_value: int) -> Dict:
    """Lấy category từ AQI value"""
    for cat in AQI_CATEGORIES:
        if cat['min'] <= aqi_value <= cat['max']:
            return cat
    return AQI_CATEGORIES[-1]  # Hazardous


def get_valid_range(parameter: str) -> Tuple[float, float]:
    """Lấy valid range cho parameter"""
    param_lower = parameter.lower().replace('.', '').replace('-', '')
    return VALID_RANGES.get(param_lower, (0, float('inf')))


def get_conversion_factor(parameter: str, unit: str) -> float:
    """Lấy conversion factor để convert sang µg/m³"""
    param_lower = parameter.lower()
    unit_lower = unit.lower()
    
    # Tìm exact match
    key = (param_lower, unit_lower)
    if key in UNIT_CONVERSIONS:
        return UNIT_CONVERSIONS[key]
    
    # Tìm wildcard match
    key = ('*', unit_lower)
    if key in UNIT_CONVERSIONS:
        return UNIT_CONVERSIONS[key]
    
    return 1.0  # No conversion needed


def is_value_valid(parameter: str, value: float) -> bool:
    """Kiểm tra giá trị có hợp lệ không"""
    if value is None or (isinstance(value, float) and (value != value)):  # Check NaN
        return False
    
    min_val, max_val = get_valid_range(parameter)
    return min_val <= value <= max_val

