# Thêm 2 dòng này lên ĐẦU TIÊN để tải file .env
from dotenv import load_dotenv
load_dotenv()

import os
import random
import smtplib, ssl # Thư viện gửi mail tích hợp sẵn
from email.message import EmailMessage # Thư viện soạn mail

def generate_otp():
    """Tạo một mã OTP ngẫu nhiên 6 chữ số."""
    return str(random.randint(100000, 999999))

def send_otp_email(to_email):
    """
    Tạo OTP, tạo nội dung email HTML và gửi email bằng Gmail SMTP.
    """

    # 1. Lấy thông tin đăng nhập Gmail từ biến môi trường
    # (Từ file .env của bạn)
    sender_email = os.environ.get("GMAIL_USER")
    app_password = os.environ.get("GMAIL_APP_PASSWORD")

    if not sender_email or not app_password:
        print("Lỗi: GMAIL_USER hoặc GMAIL_APP_PASSWORD chưa được thiết lập.")
        return

    # 2. Tạo mã OTP
    otp = generate_otp()
    print(f"Mã OTP đã tạo: {otp}")

    # 3. Thông tin người gửi, chủ đề
    subject = f'Mã OTP của bạn là: {otp}'

    # 4. Tạo nội dung HTML (Giữ nguyên mẫu của bạn)
    html_content = f"""
    <html>
    <head>
        <style>
            .container {{ font-family: Arial, sans-serif; line-height: 1.6; padding: 20px; border: 1px solid #ddd; border-radius: 5px; width: 90%; max-width: 500px; margin: 20px auto; background-color: #f9f9f9; }}
            .header {{ font-size: 24px; font-weight: bold; color: #333; text-align: center; }}
            .otp-code {{ font-size: 32px; font-weight: bold; color: #007BFF; text-align: center; margin: 20px 0; letter-spacing: 2px; }}
            .footer {{ font-size: 12px; color: #888; text-align: center; margin-top: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">Xác thực tài khoản của bạn</div>
            <p>Vui lòng sử dụng mã OTP dưới đây để hoàn tất việc xác thực. Mã này sẽ hết hạn sau 5 phút.</p>
            <div class="otp-code">{otp}</div>
            <p>Nếu bạn không yêu cầu mã này, vui lòng bỏ qua email này.</p>
            <div class="footer">© 2025 [Tên Công Ty Của Bạn]</div>
        </div>
    </body>
    </html>
    """

    # 5. Tạo đối tượng EmailMessage
    # (Đây là cách soạn mail của smtplib)
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = to_email

    # Thêm nội dung. add_alternative giúp mail hiển thị HTML
    msg.set_content("Vui lòng kích hoạt HTML để xem nội dung.")
    msg.add_alternative(html_content, subtype='html')

    # 6. Gửi email qua Gmail
    # Tạo ngữ cảnh SSL an toàn
    context = ssl.create_default_context()

    try:
        print("Đang kết nối đến máy chủ Gmail...")

        # Kết nối đến server SMTP của Gmail qua cổng 465 (SSL)
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, app_password) # Đăng nhập
            print("Đăng nhập thành công!")

            server.send_message(msg) # Gửi mail

            print(f"Email đã được gửi thành công tới {to_email}")

    except Exception as e:
        print(f"Lỗi khi gửi email: {e}")

# --- SỬ DỤNG THỬ ---
if __name__ == "__main__":
    # Thay thế bằng email của người nhận
    recipient_email = "thonglangtu09@gmail.com"

    # Kiểm tra xem biến môi trường Gmail đã được đặt chưa
    if not os.environ.get('GMAIL_USER') or not os.environ.get('GMAIL_APP_PASSWORD'):
        print("Vui lòng đặt GMAIL_USER và GMAIL_APP_PASSWORD trong file .env")
    else:
        send_otp_email(recipient_email)