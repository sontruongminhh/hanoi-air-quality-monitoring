#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script để kiểm tra OpenAQ API và xem dữ liệu trả về
"""

import json
import requests
from dotenv import load_dotenv
import os


load_dotenv()


def test_api_with_location_id():
    """Test API với LOCATION_ID cụ thể"""
    print("="*80)
    print("TEST 1: Lấy dữ liệu từ trạm theo LOCATION_ID")
    print("="*80)

    api_url = os.getenv('OPENAQ_API_URL', 'https://api.openaq.org/v3/locations')
    api_key = os.getenv('OPENAQ_API_KEY', '')
    location_id = os.getenv('OPENAQ_LOCATION_ID', '4946813')

    headers = {}
    if api_key:
        headers['X-API-Key'] = api_key

    url = f"{api_url}/{location_id}"
    print(f"\n URL: {url}")
    print(f" API Key: {'***' + api_key[-10:] if api_key else 'None'}")

    try:
        response = requests.get(url, headers=headers, timeout=30)

        print(f"\n Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()

            print(f"Success!")
            print(f"\n Cấu trúc dữ liệu:")
            print(f"  - Meta keys: {list(data.get('meta', {}).keys())}")
            print(f"  - Results count: {len(data.get('results', []))}")

            if data.get('results'):
                location = data['results'][0]
                print(f"\n Thông tin trạm:")
                print(f"  - ID: {location.get('id')}")
                print(f"  - Name: {location.get('name')}")
                print(f"  - Locality: {location.get('locality')}")
                print(f"  - Country: {location.get('country', {}).get('name')} ({location.get('country', {}).get('code')})")
                print(f"  - Coordinates: {location.get('coordinates', {}).get('latitude')}, {location.get('coordinates', {}).get('longitude')}")
                print(f"  - Is Mobile: {location.get('isMobile')}")
                print(f"  - Is Monitor: {location.get('isMonitor')}")

                print(f"\n Sensors ({len(location.get('sensors', []))}):")
                for i, sensor in enumerate(location.get('sensors', []), 1):
                    param = sensor.get('parameter', {})
                    latest = sensor.get('latest', {})
                    print(f"  {i}. {param.get('displayName', 'N/A')} ({param.get('name', 'N/A')})")
                    print(f"     - Unit: {param.get('units', 'N/A')}")
                    print(f"     - Latest Value: {latest.get('value', 'N/A')}")
                    print(f"     - Latest Time: {latest.get('datetime', 'N/A')}")

                print(f"\n Full JSON response:")
                print(json.dumps(data, indent=2, ensure_ascii=False))

              
                with open('test_api_response.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"\n Đã lưu response vào file: test_api_response.json")

        else:
            print(f" Error: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f" Exception: {e}")


def test_api_with_search():
    """Test API với tìm kiếm theo vùng địa lý"""
    print("\n" + "="*80)
    print("TEST 2: Tìm kiếm trạm theo vùng địa lý")
    print("="*80)

    api_url = os.getenv('OPENAQ_API_URL', 'https://api.openaq.org/v3/locations')
    api_key = os.getenv('OPENAQ_API_KEY', '')
    country = os.getenv('OPENAQ_COUNTRY', 'VN')
    coordinates = os.getenv('OPENAQ_COORDINATES', '21.0285,105.8542')
    radius = os.getenv('OPENAQ_RADIUS', '25000')

    headers = {}
    if api_key:
        headers['X-API-Key'] = api_key

    params = {
        'limit': 10,
        'countries': country,
        'coordinates': coordinates,
        'radius': radius
    }

    print(f"\n URL: {api_url}")
    print(f" Parameters:")
    for key, value in params.items():
        print(f"  - {key}: {value}")

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=30)

        print(f"\n Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()

            print(f" Success!")
            print(f"  - Tìm thấy {len(data.get('results', []))} trạm")

            print(f"\n Danh sách trạm:")
            for i, location in enumerate(data.get('results', [])[:10], 1):
                sensors_count = len(location.get('sensors', []))
                print(f"  {i}. {location.get('name')} (ID: {location.get('id')}) - {sensors_count} sensors")

           
            bach_khoa_stations = [
                loc for loc in data.get('results', [])
                if 'bách khoa' in loc.get('name', '').lower() or
                   'bach khoa' in loc.get('name', '').lower()
            ]

            if bach_khoa_stations:
                print(f"\n Tìm thấy {len(bach_khoa_stations)} trạm Bách Khoa:")
                for station in bach_khoa_stations:
                    print(f"  - {station.get('name')} (ID: {station.get('id')})")

        else:
            print(f" Error: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f" Exception: {e}")


def test_measurements_endpoint():
    """Test endpoint measurements để lấy dữ liệu đo chi tiết"""
    print("\n" + "="*80)
    print("TEST 3: Lấy dữ liệu measurements (dữ liệu đo chi tiết)")
    print("="*80)

    api_key = os.getenv('OPENAQ_API_KEY', '')
    location_id = os.getenv('OPENAQ_LOCATION_ID', '4946813')

    headers = {}
    if api_key:
        headers['X-API-Key'] = api_key

   
    url = f"https://api.openaq.org/v3/locations/{location_id}/measurements"

    params = {
        'limit': 100,
        'date_from': '2025-01-17T00:00:00Z',  # 24h trước
        'date_to': '2025-01-18T23:59:59Z'
    }

    print(f"\n URL: {url}")
    print(f" Parameters:")
    for key, value in params.items():
        print(f"  - {key}: {value}")

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)

        print(f"\n Status Code: {response.status_code}")

        if response.status_code == 200:
            data = response.json()

            print(f" Success!")
            print(f"  - Tìm thấy {len(data.get('results', []))} measurements")

            if data.get('results'):
                print(f"\n Một số measurements mẫu:")
                for i, measurement in enumerate(data.get('results', [])[:5], 1):
                    param = measurement.get('parameter', {})
                    print(f"  {i}. {param.get('displayName')} = {measurement.get('value')} {param.get('units')}")
                    print(f"     Time: {measurement.get('date', {}).get('utc')}")

               
                with open('test_measurements_response.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                print(f"\n Đã lưu measurements vào file: test_measurements_response.json")

        else:
            print(f" Error: {response.status_code}")
            print(f"Response: {response.text}")

    except Exception as e:
        print(f" Exception: {e}")


def main():
    """Main function"""
    print("\n" + " OPENAQ API TEST SCRIPT" + "\n")

    # Test 1: Lấy dữ liệu từ location cụ thể
    test_api_with_location_id()

    # Test 2: Tìm kiếm theo vùng địa lý
    test_api_with_search()

    # Test 3: Lấy measurements
    test_measurements_endpoint()

    print("\n" + "="*80)
    print(" Hoàn thành tất cả tests!")
    print("="*80 + "\n")


if __name__ == '__main__':
    main()

