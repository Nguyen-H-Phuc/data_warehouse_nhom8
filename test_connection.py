import mysql.connector
from config import DB_CONFIG

try:
    conn = mysql.connector.connect(**DB_CONFIG)
    print("✅ Kết nối MySQL thành công!")
    conn.close()
except Exception as e:
    print("❌ Kết nối thất bại:", e)
