
from dotenv import load_dotenv
load_dotenv()

import os
import smtplib, ssl  
from email.message import EmailMessage  
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
    
    
    breakpoints = aqi_breakpoints.get(parameter.lower(), aqi_breakpoints['pm25'])
    
 
    for bp_low, bp_high, aqi_low, aqi_high, level in breakpoints:
        if bp_low <= value <= bp_high:
           
            if bp_high == float('inf'):
                aqi_value = aqi_high
            else:
                aqi_value = int(((aqi_high - aqi_low) / (bp_high - bp_low)) * (value - bp_low) + aqi_low)
            return aqi_value, level
    

    if value <= 0:
        return 0, 'Tốt'
    
  
    estimated_aqi = int((value / 50.0) * 100) if parameter.lower() == 'pm25' else int((value / 100.0) * 100)
    estimated_aqi = min(500, max(0, estimated_aqi)) 
    
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
    main_pollutant: str = None,      
):
    """
    Gửi email cảnh báo AQI (chỉ số AQI tổng, không dùng các thông số phụ).
    """
    
    if not alert_email_to or value is None or not location_name:
        print("Lỗi: Thiếu thông tin bắt buộc để gửi email.")
        return

   
    sender_email = alert_email_from or smtp_user or os.environ.get("GMAIL_USER")
    app_password = smtp_password or os.environ.get("GMAIL_APP_PASSWORD")

    host = smtp_host or os.environ.get("SMTP_HOST") or "smtp.gmail.com"
    port = int(smtp_port or os.environ.get("SMTP_PORT") or 465)

    if not sender_email or not app_password:
        print("Lỗi: Thiếu SMTP user/app password (GMAIL_USER / GMAIL_APP_PASSWORD hoặc SMTP_USER / SMTP_PASSWORD).")
        return
    
   
    aqi = float(value)
    if aqi <= 50:
        aqi_level = 'Tốt'
    elif aqi <= 100:
        aqi_level = 'Trung bình'
    elif aqi <= 150:
        aqi_level = 'Kém (Unhealthy for Sensitive Groups)'
    elif aqi <= 200:
        aqi_level = 'Xấu (Unhealthy)'
    elif aqi <= 300:
        aqi_level = 'Rất xấu (Very Unhealthy)'
    else:
        aqi_level = 'Nguy hiểm (Hazardous)'

  
    if measurement_time:
        if isinstance(measurement_time, datetime):
            if measurement_time.tzinfo is None:
                measurement_time = measurement_time.replace(tzinfo=timezone.utc)
            measured_at_str = measurement_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            measured_at_str = str(measurement_time)
    else:
        measured_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
   
    main_text = f" - Chất ô nhiễm chính: {main_pollutant}" if main_pollutant else ""
    if not subject:
        subject = f"[CẢNH BÁO AQI] {location_name} vượt ngưỡng (AQI={aqi}{main_text})"

   
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
            Hệ thống giám sát phát hiện <b>chỉ số ô nhiễm chính: {main_pollutant if main_pollutant else 'N/A'}</b> tại khu vực
            <b>{location_name}</b> đã <b>vượt ngưỡng an toàn</b>.
        </p>

        <div class="sub-header">Chỉ số AQI hiện tại</div>
        <div class="aqi-value">{aqi}</div>
        <p style="text-align: center;">
            Mức độ: <span class="badge-level">{aqi_level}</span>
        </p>

        <p class="detail">
            <b>Thông tin chi tiết:</b><br>
            - AQI: <b>{aqi}</b> (ngưỡng: {threshold or 'N/A'})<br>
            {f"- Chất ô nhiễm chính: <b>{main_pollutant}</b><br>" if main_pollutant else ""}
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

   
    msg = MIMEMultipart('alternative')
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = alert_email_to

 
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
    
    
    text_part = MIMEText(text_content, 'plain', 'utf-8')
    msg.attach(text_part)
    
   
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

    
    context = ssl.create_default_context()

    try:
        print(f"Đang kết nối SMTP tới {host}:{port} bằng user {sender_email} ...")

        with smtplib.SMTP_SSL(host, port, context=context) as server:
            server.login(sender_email, app_password)
            print("Đăng nhập SMTP thành công!")

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
    Gửi email cảnh báo tổng hợp cho AQI (có thể nhiều bản ghi AQI nếu chạy nhiều location).
    """
   
    if not alert_email_to or not location_name or not alert_sensors or len(alert_sensors) == 0:
        print("Lỗi: Thiếu thông tin bắt buộc để gửi email tổng hợp.")
        return

   
    sender_email = os.environ.get("GMAIL_USER")
    app_password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender_email or not app_password:
        print("Lỗi: GMAIL_USER hoặc GMAIL_APP_PASSWORD chưa được thiết lập.")
        return
    
   
    max_aqi = max(s['aqi'] for s in alert_sensors)
    max_aqi_level = next(s['aqi_level'] for s in alert_sensors if s['aqi'] == max_aqi)
    main_pollutant = alert_sensors[0].get('main_pollutant') if alert_sensors else None
    
   
    if measurement_time:
        if isinstance(measurement_time, datetime):
            if measurement_time.tzinfo is None:
                measurement_time = measurement_time.replace(tzinfo=timezone.utc)
            measured_at_str = measurement_time.strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            measured_at_str = str(measurement_time)
    else:
        measured_at_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    
   
    num_exceeded = len(alert_sensors)
    mp_text = f", Main: {main_pollutant}" if main_pollutant else ""
    if num_exceeded > 1:
        subject = f"[CẢNH BÁO AQI] {num_exceeded} bản ghi vượt ngưỡng tại {location_name} (AQI cao nhất: {max_aqi}{mp_text})"
    else:
        subject = f"[CẢNH BÁO AQI] {location_name} vượt ngưỡng (AQI={max_aqi}{mp_text})"
    
   
    sensors_table_rows = ""
    for sensor in alert_sensors:
        value_str = f"{sensor['value']:.0f}" if isinstance(sensor['value'], (int, float)) else str(sensor['value'])
        threshold_str = f"{sensor['threshold']:.0f}" if isinstance(sensor['threshold'], (int, float)) else str(sensor['threshold'])
        unit_str = sensor.get('unit', 'AQI')
        mp = sensor.get('main_pollutant')
        mp_str = f"(Main: {mp})" if mp else ""
        
        sensors_table_rows += f"""
            <tr>
                <td style="padding: 8px; border-bottom: 1px solid #ddd;"><b>{sensor['param_display']} {mp_str}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center; color: #e74c3c;"><b>{value_str} {unit_str}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">{threshold_str} {unit_str}</td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;"><b>{sensor['aqi']}</b></td>
                <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: center;">{sensor['aqi_level']}</td>
            </tr>
        """
    
    
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
            Hệ thống giám sát phát hiện <b>chỉ số ô nhiễm chính: {main_pollutant if main_pollutant else 'N/A'}</b> tại khu vực
            <b>{location_name}</b> đã <b>vượt ngưỡng an toàn</b>.
        </p>

        <div class="sub-header">Chỉ số AQI cao nhất</div>
        <div class="aqi-value">{max_aqi}</div>
        <p style="text-align: center;">
            Mức độ: <span class="badge-level">{max_aqi_level}</span>
        </p>

        <p class="detail">
            <b>Danh sách AQI vượt ngưỡng:</b>
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

   
    msg = MIMEMultipart('alternative')
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = alert_email_to

 
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
    
   
    html_part = MIMEText(html_content, 'html', 'utf-8')
    msg.attach(html_part)

   
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
