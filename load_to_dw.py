import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime
from config import DB_CONFIG

def load_to_dw(df):
    #1.1 Ghi lại thời gian bắt đầu quá trình load
    start_time = datetime.now()
     # 1.2 Tạo engine SQLAlchemy để ghi DataFrame vào MySQL
    engine = create_engine(f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")

    # 2️ NẠP DỮ LIỆU VÀO STAGING (stg_stock_price)

    # 2.1 Ghi toàn bộ dữ liệu DataFrame `df` vào bảng staging
    df.to_sql("stg_stock_price", con=engine, if_exists="append", index=False)
    print("✅ Đã nạp dữ liệu vào STAGING.")
 # 2.2 Kết nối MySQL native để chạy các câu lệnh SQL (INSERT,...)
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 3️ NẠP DỮ LIỆU VÀO BẢNG DIMENSION

    # 3.1 Nạp dữ liệu vào bảng dim_date
    #     - Lấy ngày duy nhất từ bảng staging
    #     - Tạo cột: year, month, day, weekday, is_business_day
    cursor.execute("""
        INSERT IGNORE INTO dim_date (date_key, year, month, day, weekday, is_business_day)
        SELECT DISTINCT date, YEAR(date), MONTH(date), DAY(date), DAYNAME(date), 
        CASE WHEN DAYOFWEEK(date) BETWEEN 2 AND 6 THEN TRUE ELSE FALSE END
        FROM stg_stock_price;
    """)
 # 3.2 Nạp dữ liệu vào bảng dim_stock
    #     - Lấy danh sách symbol duy nhất từ staging
    #     - Thêm thông tin công ty từ bảng stg_company_info
    #     - Dùng INSERT IGNORE để tránh trùng lặp
    cursor.execute("""
        INSERT IGNORE INTO dim_stock (symbol, company_name, exchange, sector, listing_date)
        SELECT DISTINCT s.symbol, c.company_name, c.exchange, c.industry, c.listing_date
        FROM stg_stock_price s
        LEFT JOIN stg_company_info c ON s.symbol = c.symbol;
    """)

    # 4️ NẠP DỮ LIỆU VÀO BẢNG FACT

    # 4.1 Nạp dữ liệu vào bảng fact_daily_price
    #     - JOIN dim_stock để lấy stock_id (khóa ngoại)
    #     - Lưu các chỉ số giá, khối lượng, giá trị,...
    #     - Ghi thời điểm hiện tại vào cột created_at
    cursor.execute("""
        INSERT INTO fact_daily_price (
            date_key, stock_id, source_id, adjusted_price, close_price, change_value, change_percent,
            trading_volume, trading_value, deal_volume, deal_value, open_price, high_price, low_price, created_at
        )
        SELECT s.date, ds.stock_id, 1, s.adjusted_price, s.close_price, s.change_value, s.change_percent,
               s.volume, s.trading_value, s.deal_volume, s.deal_value, s.open_price, s.high_price, s.low_price, NOW()
        FROM stg_stock_price s
        JOIN dim_stock ds ON ds.symbol = s.symbol;
    """)
 # 4.2 Commit toàn bộ dữ liệu fact và dim
    conn.commit()
     # 5️ GHI LOG ETL

    # 5.1 Ghi nhật ký vào bảng etl_log để theo dõi tiến trình
    cursor.execute(
        "INSERT INTO etl_log (process_name, start_time, end_time, records_inserted, status) VALUES ('ETL_CAFEF', %s, NOW(), %s, 'SUCCESS')",
        (start_time, len(df))
    )
    conn.commit()
     #6️ DỌN DẸP & KẾT THÚC

    # 6.1 Xóa sạch dữ liệu staging để chuẩn bị cho lần ETL kế tiếp
    cursor.execute("TRUNCATE TABLE stg_stock_price;")
    conn.commit()
     # 6.2 Đóng kết nối DB và con trỏ
    cursor.close()
    conn.close()
      # 6.3 In thông báo hoàn tất
    print("✅ Load hoàn tất vào Data Warehouse.")
