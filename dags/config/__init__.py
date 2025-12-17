"""
Configuration package for AQI ETL Pipeline
"""

from .quality_rules import (
    VALID_RANGES,
    AQI_CATEGORIES,
    PM25_BREAKPOINTS,
    PM10_BREAKPOINTS,
    QUALITY_RULES,
    UNIT_CONVERSIONS,
    ALERT_THRESHOLDS,
    get_aqi_category,
    get_valid_range,
    get_conversion_factor,
    is_value_valid,
)

__all__ = [
    'VALID_RANGES',
    'AQI_CATEGORIES',
    'PM25_BREAKPOINTS',
    'PM10_BREAKPOINTS',
    'QUALITY_RULES',
    'UNIT_CONVERSIONS',
    'ALERT_THRESHOLDS',
    'get_aqi_category',
    'get_valid_range',
    'get_conversion_factor',
    'is_value_valid',
]

