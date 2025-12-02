# Thêm 2 dòng này lên ĐẦU TIÊN để tải file .env
from dotenv import load_dotenv
load_dotenv()

import os
import smtplib, ssl  # Thư viện gửi mail tích hợp sẵn
from email.message import EmailMessage  # Thư viện soạn mail
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone


def calculate_aqi_from_value(value: float, parameter: str) -> tuple[int, str]:
    """
    Tính toán AQI và mức độ từ giá trị đo được.
    
    Args:
        value: Giá trị đo được
        parameter: Tên thông số (pm25, pm10, no2, o3, co, so2)
    
    Returns:
        tuple: (aqi_value, aqi_level)
    """
    # Bảng ngưỡng AQI cho PM2.5 (µg/m³) - theo tiêu chuẩn US EPA
    # Có thể mở rộng cho các thông số khác
    aqi_breakpoints = {
        'pm25': [
            (0, 12.0, 0, 50, 'Tốt'),
            (12.1, 35.4, 51, 100, 'Trung bình'),
            (35.5, 55.4, 101, 150, 'Kém (Unhealthy for Sensitive Groups)'),
            (55.5, 150.4, 151, 200, 'Xấu (Unhealthy)'),
            (150.5, 250.4, 201, 300, 'Rất xấu (Very Unhealthy)'),
            (250.5, float('inf'), 301, 500, 'Nguy hiểm (Hazardous)')
        ],
        'pm10': [
            (0, 54, 0, 50, 'Tốt'),
            (55, 154, 51, 100, 'Trung bình'),
            (155, 254, 101, 150, 'Kém (Unhealthy for Sensitive Groups)'),
            (255, 354, 151, 200, 'Xấu (Unhealthy)'),
            (355, 424, 201, 300, 'Rất xấu (Very Unhealthy)'),
            (425, float('inf'), 301, 500, 'Nguy hiểm (Hazardous)')
        ]
    }
    
    # Lấy bảng breakpoints cho parameter, nếu không có thì dùng pm25
    breakpoints = aqi_breakpoints.get(parameter.lower(), aqi_breakpoints['pm25'])
    
    # Tìm mức AQI phù hợp
    for bp_low, bp_high, aqi_low, aqi_high, level in breakpoints:
        if bp_low <= value <= bp_high:
            # Tính AQI theo công thức: AQI = ((AQI_high - AQI_low) / (BP_high - BP_low)) * (value - BP_low) + AQI_low
            if bp_high == float('inf'):
                aqi_value = aqi_high
            else:
                aqi_value = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (value - bp_low) + aqi_low)
            return aqi_value, level
    
    # Fallback: nếu không tìm thấy, tính đơn giản dựa trên tỷ lệ
    # Giả sử threshold tương ứng với AQI 100
    if value <= 0:
        return 0, 'Tốt'
    
    # Tính AQI đơn giản: AQI ≈ (value / threshold) * 100
    estimated_aqi = int((value / 50.0) * 100) if parameter.lower() == 'pm25' else int((value / 100.0) * 100)
    estimated_aqi = min(500, max(0, estimated_aqi))  # Giới hạn trong khoảng 0-500
    
    if estimated_aqi <= 50:
        level = 'Tốt'
    elif estimated_aqi <= 100:
        level = 'Trung bình'
    elif estimated_aqi <= 150:
        level = 'Kém (Unhealthy for Sensitive Groups)'
    elif estimated_aqi <= 200:
        level = 'Xấu (Unhealthy)'
    elif estimated_aqi <= 300:
        level = 'Rất xấu (Very Unhealthy)'
    else:
        level = 'Nguy hiểm (Hazardous)'
    
    return estimated_aqi, level


def send_aqi_alert_email(
    smtp_host=None,
    smtp_port=None,
    smtp_user=None,
    smtp_password=None,
    alert_email_from=None,
    alert_email_to=None,
    subject=None,
    param_name: str = None,
    value: float = None,
    unit: str = None,
    threshold: float = None,
    location_name: str = None,
    locality: str = None,
    country: str = None,
    latitude: float = None,
    longitude: float = None,
    measurement_time: datetime = None,
):
    """
    Gửi email cảnh báo khi chất lượng không khí vượt ngưỡng.
    Hàm này được gọi từ consumer.py khi phát hiện dữ liệu vượt ngưỡng.

    Args:
        smtp_host, smtp_port, smtp_user, smtp_password: SMTP config (không dùng, dùng Gmail)
        alert_email_from: Email người gửi (không dùng, dùng GMAIL_USER)
        alert_email_to: Email người nhận
        subject: Chủ đề email
        param_name: Tên thông số hiển thị (vd: "PM2.5")
        value: Giá trị đo được
        unit: Đơn vị (vd: "µg/m³")
        threshold: Ngưỡng cảnh báo
        location_name: Tên trạm/khu vực
        locality: Địa phương
        country: Quốc gia
        latitude, longitude: Tọa độ
        measurement_time: Thời điểm đo (datetime object)
    """
    
    # Validate dữ liệu bắt buộc
    if not alert_email_to or value is None or not location_name or not param_name:
        print("Lỗi: Thiếu thông tin bắt buộc để gửi email.")
        return

    # 1. Lấy thông tin đăng nhập Gmail từ biến môi trường (.env)
    sender_email = os.environ.get("GMAIL_USER")
    app_password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender_email or not app_password:
        print("Lỗi: GMAIL_USER hoặc GMAIL_APP_PASSWORD chưa được thiết lập.")
        return
    
    # 2. Tính toán AQI và mức độ từ giá trị thực tế
    # Chuẩn hóa tên parameter để tính AQI (vd: "PM2.5" -> "pm25")
    param_normalized = param_name.lower().replace('.', '').replace(' ', '').replace('_', '')
    if 'pm25' in param_normalized or 'pm2.5' in param_normalized:
        param_for_aqi = 'pm25'
    elif 'pm10' in param_normalized:
        param_for_aqi = 'pm10'
    else:
        param_for_aqi = 'pm25'  # Fallback
    
    aqi, aqi_level = calculate_aqi_from_value(float(value), param_for_aqi)
    
    # 3. Format thời gian đo
    if measurement_time:
        if isinstance(measurement_time, datetime):
            if measurement_time.tzinfo is None:
                # Nếu không có timezone, giả định là UTC
                measurement_time = measurement_time.replace(tzinfo=timezone.utc)
            measured_at_str = measurement_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            measured_at_str = str(measurement_time)
    else:
        measured_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # 4. Tạo chủ đề email nếu chưa có
    if not subject:
        subject = f"[CẢNH BÁO AQI] {location_name} vượt ngưỡng an toàn (AQI={aqi})"

    # 5. Tạo nội dung HTML email cảnh báo AQI
    html_content = f"""<html>
<head>
    <style>
        .container {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            width: 90%;
            max-width: 500px;
            margin: 20px auto;
            background-color: #f9f9f9;
        }}
        .header {{
            font-size: 24px;
            font-weight: bold;
            color: #c0392b;
            text-align: center;
        }}
        .aqi-value {{
            font-size: 32px;
            font-weight: bold;
            color: #e74c3c;
            text-align: center;
            margin: 20px 0;
            letter-spacing: 2px;
        }}
        .sub-header {{
            font-size: 16px;
            font-weight: bold;
            text-align: center;
            margin-bottom: 10px;
        }}
        .detail {{
            font-size: 14px;
            color: #333;
        }}
        .badge-level {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
            background-color: #f39c12;
            color: #fff;
        }}
        .footer {{
            font-size: 12px;
            color: #888;
            text-align: center;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">CẢNH BÁO CHẤT LƯỢNG KHÔNG KHÍ</div>
        <p class="detail">
            Hệ thống giám sát phát hiện <b>chỉ số chất lượng không khí (AQI)</b> tại khu vực
            <b>{location_name}</b> đã <b>vượt ngưỡng an toàn</b>.
        </p>

        <div class="sub-header">Chỉ số AQI hiện tại</div>
        <div class="aqi-value">{aqi}</div>
        <p style="text-align: center;">
            Mức độ: <span class="badge-level">{aqi_level}</span>
        </p>

        <p class="detail">
            <b>Thông tin chi tiết:</b><br>
            - Thông số chính: <b>{param_name}</b><br>
            - Giá trị đo được: <b>{value} {unit or ''}</b><br>
            - Ngưỡng cảnh báo: <b>{threshold} {unit or ''}</b><br>
            - Thời điểm đo: <b>{measured_at_str}</b>
        </p>

        <p class="detail">
            <b>Khuyến nghị:</b><br>
            - Hạn chế các hoạt động ngoài trời kéo dài, đặc biệt với người già, trẻ nhỏ và người có bệnh hô hấp.<br>
            - Đeo khẩu trang phù hợp khi ra ngoài.<br>
            - Đóng cửa sổ, sử dụng máy lọc không khí nếu có điều kiện.
        </p>

        <p class="detail">
            Hệ thống sẽ tiếp tục theo dõi và gửi email khi chất lượng không khí cải thiện
            hoặc có biến động bất thường.
        </p>

        <div class="footer">
            © 2025 Hệ thống giám sát &amp; cảnh báo chất lượng không khí Hà Nội
        </div>
    </div>
</body>
</html>"""

    # 6. Tạo đối tượng MIMEMultipart với alternative subtype
    msg = MIMEMultipart('alternative')
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = alert_email_to

    # Nội dung text fallback
    location_info = f"{location_name}"
    if locality:
        location_info += f", {locality}"
    if country:
        location_info += f", {country}"
    
    text_content = (
        f"CẢNH BÁO AQI tại {location_info}: AQI={aqi}, mức {aqi_level}. "
        f"Giá trị {param_name} = {value} {unit or ''}, vượt ngưỡng {threshold} {unit or ''}. "
        f"Thời điểm đo: {measured_at_str}"
    )
    
    # Tạo text part
    text_part = MIMEText(text_content, 'plain', 'utf-8')
    msg.attach(text_part)
    
    # Tạo HTML part (quan trọng: attach sau text để email client ưu tiên HTML)
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    # 7. Gửi email qua Gmail SMTP
    context = ssl.create_default_context()

    try:
        print("Đang kết nối đến máy chủ Gmail...")

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, app_password)
            print("Đăng nhập thành công!")

            server.sendmail(sender_email, alert_email_to, msg.as_string())
            print(f"Email cảnh báo AQI đã được gửi thành công tới {alert_email_to}")
            print(f"  → Location: {location_name}, Parameter: {param_name}, Value: {value} {unit}, AQI: {aqi}")

    except Exception as e:
        print(f"Lỗi khi gửi email: {e}")


def send_aqi_alert_email_summary(
    alert_email_to=None,
    location_name: str = None,
    locality: str = None,
    country: str = None,
    latitude: float = None,
    longitude: float = None,
    measurement_time: datetime = None,
    alert_sensors: list = None,
):
    """
    Gửi email cảnh báo tổng hợp cho nhiều thông số vượt ngưỡng.
    
    Args:
        alert_email_to: Email người nhận
        location_name: Tên trạm/khu vực
        locality: Địa phương
        country: Quốc gia
        latitude, longitude: Tọa độ
        measurement_time: Thời điểm đo (datetime object)
        alert_sensors: Danh sách các sensor vượt ngưỡng, mỗi item có:
            - param_display: Tên thông số hiển thị
            - value: Giá trị đo được
            - unit: Đơn vị
            - threshold: Ngưỡng cảnh báo
            - aqi: Chỉ số AQI
            - aqi_level: Mức độ AQI
    """
    
    # Validate dữ liệu bắt buộc
    if not alert_email_to or not location_name or not alert_sensors or len(alert_sensors) == 0:
        print("Lỗi: Thiếu thông tin bắt buộc để gửi email tổng hợp.")
        return

    # 1. Lấy thông tin đăng nhập Gmail từ biến môi trường (.env)
    sender_email = os.environ.get("GMAIL_USER")
    app_password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender_email or not app_password:
        print("Lỗi: GMAIL_USER hoặc GMAIL_APP_PASSWORD chưa được thiết lập.")
        return
    
    # 2. Tìm AQI cao nhất
    max_aqi = max(s['aqi'] for s in alert_sensors)
    max_aqi_level = next(s['aqi_level'] for s in alert_sensors if s['aqi'] == max_aqi)
    
    # 3. Format thời gian đo
    if measurement_time:
        if isinstance(measurement_time, datetime):
            if measurement_time.tzinfo is None:
                measurement_time = measurement_time.replace(tzinfo=timezone.utc)
            measured_at_str = measurement_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            measured_at_str = str(measurement_time)
    else:
        measured_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # 4. Tạo chủ đề email
    num_exceeded = len(alert_sensors)
    if num_exceeded > 1:
        subject = f"[CẢNH BÁO AQI] {num_exceeded} thông số vượt ngưỡng tại {location_name} (AQI cao nhất: {max_aqi})"
    else:
        subject = f"[CẢNH BÁO AQI] {location_name} vượt ngưỡng an toàn (AQI={max_aqi})"
    
    # 5. Tạo bảng HTML cho các thông số vượt ngưỡng
    sensors_table_rows = ""
    for sensor in alert_sensors:
        # Format giá trị và ngưỡng với số thập phân phù hợp
        value_str = f"{sensor['value']:.2f}" if isinstance(sensor['value'], (int, float)) else str(sensor['value'])
        threshold_str = f"{sensor['threshold']:.1f}" if isinstance(sensor['threshold'], (int, float)) else str(sensor['threshold'])
        unit_str = sensor.get('unit', 'µg/m³')
        
        sensors_table_rows += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;"><b>{sensor['param_display']}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center; color: #e74c3c;"><b>{value_str} {unit_str}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">{threshold_str} {unit_str}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;"><b>{sensor['aqi']}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">{sensor['aqi_level']}</td>
            </tr>
        """
    
    # 6. Tạo nội dung HTML email tổng hợp
    html_content = f"""<html>
<head>
    <style>
        .container {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            padding: 20px;
            border: 1px solid #ddd;
            border-radius: 5px;
            width: 90%;
            max-width: 600px;
            margin: 20px auto;
            background-color: #f9f9f9;
        }}
        .header {{
            font-size: 24px;
            font-weight: bold;
            color: #c0392b;
            text-align: center;
        }}
        .aqi-value {{
            font-size: 32px;
            font-weight: bold;
            color: #e74c3c;
            text-align: center;
            margin: 20px 0;
            letter-spacing: 2px;
        }}
        .sub-header {{
            font-size: 16px;
            font-weight: bold;
            text-align: center;
            margin-bottom: 10px;
        }}
        .detail {{
            font-size: 14px;
            color: #333;
        }}
        .badge-level {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: bold;
            background-color: #f39c12;
            color: #fff;
        }}
        .sensors-table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
            background-color: #fff;
        }}
        .sensors-table th {{
            background-color: #34495e;
            color: #fff;
            padding: 10px;
            text-align: left;
            font-weight: bold;
        }}
        .footer {{
            font-size: 12px;
            color: #888;
            text-align: center;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">CẢNH BÁO CHẤT LƯỢNG KHÔNG KHÍ</div>
        <p class="detail">
            Hệ thống giám sát phát hiện <b>{num_exceeded} thông số</b> tại khu vực
            <b>{location_name}</b> đã <b>vượt ngưỡng an toàn</b>.
        </p>

        <div class="sub-header">Chỉ số AQI cao nhất</div>
        <div class="aqi-value">{max_aqi}</div>
        <p style="text-align: center;">
            Mức độ: <span class="badge-level">{max_aqi_level}</span>
        </p>

        <p class="detail">
            <b>Danh sách các thông số vượt ngưỡng:</b>
        </p>
        <table class="sensors-table">
            <thead>
                <tr>
                    <th>Thông số</th>
                    <th style="text-align: center;">Giá trị đo được</th>
                    <th style="text-align: center;">Ngưỡng</th>
                    <th style="text-align: center;">AQI</th>
                    <th style="text-align: center;">Mức độ</th>
                </tr>
            </thead>
            <tbody>
                {sensors_table_rows}
            </tbody>
        </table>

        <p class="detail">
            <b>Thông tin địa điểm:</b><br>
            - Địa điểm: <b>{location_name}</b><br>
            {f"- Địa phương: <b>{locality}</b><br>" if locality else ""}
            {f"- Quốc gia: <b>{country}</b><br>" if country else ""}
            - Thời điểm đo: <b>{measured_at_str}</b>
        </p>

        <p class="detail">
            <b>Khuyến nghị:</b><br>
            - Hạn chế các hoạt động ngoài trời kéo dài, đặc biệt với người già, trẻ nhỏ và người có bệnh hô hấp.<br>
            - Đeo khẩu trang phù hợp khi ra ngoài.<br>
            - Đóng cửa sổ, sử dụng máy lọc không khí nếu có điều kiện.
        </p>

        <p class="detail">
            Hệ thống sẽ tiếp tục theo dõi và gửi email khi chất lượng không khí cải thiện
            hoặc có biến động bất thường.
        </p>

        <div class="footer">
            © 2025 Hệ thống giám sát &amp; cảnh báo chất lượng không khí Hà Nội
        </div>
    </div>
</body>
</html>"""

    # 7. Tạo đối tượng MIMEMultipart với alternative subtype
    msg = MIMEMultipart('alternative')
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = alert_email_to

    # Nội dung text fallback
    location_info = f"{location_name}"
    if locality:
        location_info += f", {locality}"
    if country:
        location_info += f", {country}"
    
    sensors_text = "\n".join([
        f"  - {s['param_display']}: {s['value']} {s['unit']} (AQI={s['aqi']}, Ngưỡng={s['threshold']} {s['unit']})"
        for s in alert_sensors
    ])
    
    text_content = (
        f"CẢNH BÁO AQI tại {location_info}: {num_exceeded} thông số vượt ngưỡng, AQI cao nhất={max_aqi}.\n\n"
        f"Các thông số vượt ngưỡng:\n{sensors_text}\n\n"
        f"Thời điểm đo: {measured_at_str}"
    )
    
    # Tạo text part
    text_part = MIMEText(text_content, 'plain', 'utf-8')
    msg.attach(text_part)
    
    # Tạo HTML part (quan trọng: attach sau text để email client ưu tiên HTML)
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    # 8. Gửi email qua Gmail SMTP
    context = ssl.create_default_context()

    try:
        print("Đang kết nối đến máy chủ Gmail...")

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, app_password)
            print("Đăng nhập thành công!")

            server.sendmail(sender_email, alert_email_to, msg.as_string())
            print(f"Email cảnh báo AQI tổng hợp đã được gửi thành công tới {alert_email_to}")
            print(f"  → Location: {location_name}, {num_exceeded} thông số vượt ngưỡng, AQI cao nhất: {max_aqi}")

    except Exception as e:
        print(f"Lỗi khi gửi email: {e}")
