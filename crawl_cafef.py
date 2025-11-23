import mariadb
import os
import sys
import requests
import pandas as pd
from datetime import datetime

# 1.1 Đọc thông tin khởi tạo connection được lưu trong file "D:\ETL_STockDW\connection.txt" trả về dictonary 
def read_file():
    # 1.1.1 Tạo dictionary rỗng để lưu các cặp key=value trong file "D:\ETL_STockDW\connection.txt"
    connection_paramaters = {}
    # 1.1.2 try except để bắt lỗi trong quá trình đọc file
    try:
        # 1.1.2.1 Mở file "D:\ETL_STockDW\connection.txt" theo chế độ chỉ đọc
        with open(r'D:\ETL_STockDW\connection.txt', 'r') as file:
            #  1.1.2.2 Duyệt từng dòng trong file cho đến khi hết file
            for line in file:
                #  1.1.2.3 nếu dòng có chứa dấu '='?
                if '=' in line:
                    # 1.1.2.4 Tách dòng thành 2 phần: key và value, chỉ tách 1 lần đầu tiên
                    key, value = line.strip().split('=', 1)
                    #  1.1.2.5 Loại bỏ khoảng trắng hai bên key, value và lưu vào dictionary
                    connection_paramaters[key.strip()] = value.strip()
        # 1.1.2.6 Trả về dictionary chứa các key:value đã đọc được
        return connection_paramaters
    except Exception as e:
        # 1.1.2.7 in ra terminal thông báo Không thể đọc file D:\\ETL_STockDW\\connection.txt nếu đoạn script chạy thủ công
        print(f"Không thể đọc file D:\\ETL_STockDW\\connection.txt")
        # 1.1.2.8 thoát chương trình với mã lỗi 1
        sys.exit(1)                            

# 1.2 khởi tạo kết nối tới và trả về connection
def get_connection_mariadb(connection_parameters):
    # 1.2.1 Tạo kết nối với MariaDB dựa trên các thông số trong dictionary gồm tên đăng nhập, mật khẩu, địa chỉ host hoặc ip, tên database cần kết nối 
    connection = mariadb.connect(
        user=connection_parameters.get('user'),        # Tên đăng nhập
        password=connection_parameters.get('password'),# Mật khẩu
        host=connection_parameters.get('host'),        # Địa chỉ host hoặc IP
        database=connection_parameters.get('database') # Tên cơ sở dữ liệu cần kết nối
    )
    # 1.2.2 Trả về đối tượng connection để dùng cho các thao tác tiếp theo
    return connection

# 1.3 Tải cấu hình từ database data_config
def load_config(connection):
    # 1.3.1 Tạo con trỏ để thực thi câu lệnh SQL
    cursor = connection.cursor()
    
    # 1.3.2 Thực thi truy vấn lấy thông tin cấu hình từ bảng config_feed
    cursor.execute("SELECT id, stock_symbol, source_url, dest_file FROM config_src WHERE active = 1")

    # 1.3.3 Lấy một hàng từ kết quả câu truy vấn trả về
    row = cursor.fetchone()
    
    # 1.3.4 Đóng con trỏ
    cursor.close()

    #  1.3.5 Nếu biến row có dữ liệu
    if row:
        """ 
        1.3.5.1 Giải nén các giá trị từ tuple thành biến mã config, 
        stock_symbol (danh sách mã cổ phiếu), source_url (đường dẫn trang web lấy thông tin), 
        destination_file (nơi file được lưu).
        """
        id, stock_symbol, source_url, destination_file = row
        
        # 1.3.5.2 Trả về các giá trị, id stock_symbol, source_url, destiation_file
        return id, stock_symbol, source_url, destination_file
    else:
        # 1.3.5.3 ghi log "FAILED", "Không tìm thấy cấu hình" vào bảng src_log
        log_to_db(connection, 0, "FAILED", 'Không tìm thấy cấu hình')
        # 1.3.5.4 Thoát chương trình với mã lỗi 1
        sys.exit(1)

# 1.4 Crawl dữ liệu từ trang web CafeF
def fetch_stock_data(db_connection, id, stock_symbol, source_url, start_date=datetime.now().date(), end_date=datetime.now().date()):
    # 1.4.1 Tạo header để gửi kèm với request HTTP
    headers = {"User-Agent": "Mozilla/5.0"}

    # 1.4.2 Tạo danh sách rỗng để lưu dataframe của từng mã cổ phiếu
    all_data = []

    #  1.4.3 Tách chuỗi stock_symbol theo dấu phẩy để lấy từng mã riêng lẻ
    symbols = stock_symbol.split(",")

    #  1.4.4 Duyệt từng mã cổ phiếu trong symbols cho đến hết
    for symbol in symbols:
        # 1.4.4.1 Với mỗi mã cổ phiếu tạo dictionary params gồm mã cỗ phiếu, 
        # ngày bắt đầu lấy, ngày kết thúc, lấy trang đầu tiên, lấy tối đa 1 
        # bản ghi gửi kèm query string cho request
        params = {
        "Symbol": symbol.strip(),                       # Mã cổ phiếu đã loại bỏ khoảng trắng thừa
        "StartDate": start_date.strftime("%d/%m/%Y"),   # Ngày bắt đầu lấy dữ liệu
        "EndDate": end_date.strftime("%d/%m/%Y"),       # Ngày kết thúc lấy dữ liệu
        "PageIndex": 1,                                 # Số trang (ở đây chỉ lấy trang đầu tiên)
        "PageSize": 1                                   # Số bản ghi trên một trang
    }

        # 1.4.4.2 Gửi request GET tới source_url với params và headers
        res = requests.get(source_url, params=params, headers=headers, timeout=10)

        # 1.4.4.3 Chuyển kết quả JSON thành dictionary
        data_json = res.json()
        
        # 1.4.4.4 Nếu tồn tại "Data" ở cấp ngoài và cấp trong, dữ liệu hợp lệ
        if "Data" in data_json and "Data" in data_json["Data"]:
            # 1.4.4.4.1 Chuyển dữ liệu thành DataFrame
            df = pd.DataFrame(data_json["Data"]["Data"])

            # 1.4.4.4.2 Thêm cột "symbol" để biết dữ liệu thuộc mã nào
            df["symbol"] = symbol.strip()

            # 1.4.4.4.3 Thêm DataFrame này vào danh sách all_data
            all_data.append(df)
        else:
            # 1.4.4.4.4 goi hàm log_to_db ghi lỗi "Không có dữ liệu hợp lệ cho mã cổ phiếu {symbol}" vào database
            log_to_db(db_connection, id, "WARNING", f"Không có dữ liệu hợp lệ cho mã {symbol}")

    # 1.4.5 ghép tất cả DataFrame lại thành một DataFrame lớn, bỏ qua index và trả về
    return pd.concat(all_data, ignore_index=True)

#  1.5 Ghi dữ liệu vào file "destination_fileyyyymmdd.csv"
def write_data(connection, id, dest_file, dataframe):
    # 1.5.1 Tạo đường dẫn file "destination_fileyyyymmdd.csv" theo ngày hiện tại
    file_path = f"{dest_file}/stocks-{datetime.now().strftime('%Y%m%d')}.csv"
    # 1.5.2 try - except block
    try:
        # 1.5.2.1 Xuất DataFrame ra file CSV, không ghi chỉ số, sử dụng encoding UTF-8 có BOM
        dataframe.to_csv(file_path, index=False, encoding="utf-8-sig")

        # 1.5.2.2 Ghi vào bảng log "Crawl dữ liệu thành công, dữ liệu được lưu tại "destination_fileyyyymmdd.csv"
        log_to_db(connection, id, "SUCCESS", f"Crawl thành công, dữ liệu được lưu tại {file_path}")
    except Exception as e:
        # 1.5.2.3 Ghi vào bảng log khi xảy ra lỗi trong quá trình lưu file: "Đã xãy ra lỗi trong quá trình lưu dữ liệu vào file "destination_fileyyyymmdd.csv"
        log_to_db(connection, id, f"FAILED", "Đã xãy ra lỗi trong quá trình lưu dữ liệu vào file {file_path}  ")

# 1.6 Ghi log vào database data_cofig
def log_to_db(connection, config_id, status, message):
    # 1.6.1 Tạo con trỏ để thao tác với database
    cursor = connection.cursor()

    #  1.6.2 Thêm một bản ghi log vào bảng src_log
    cursor.execute("""
        INSERT INTO src_log (config_id, status, message, process_id)
        VALUES (?, ?, ?, ?)
    """, (config_id, status, message, os.getpid()))

    #  1.6.3 Commit thay đổi để lưu log vào DB data_config
    connection.commit()

    #  1.6.4 Đóng con trỏ
    cursor.close()

if __name__ == "__main__":
    result = read_file()
    conn = get_connection_mariadb(result)
    id, stock_symbol, source_url, destination_file = load_config(conn)
    all_data = fetch_stock_data(conn, id, stock_symbol, source_url)
    write_data(conn, id, destination_file, all_data)