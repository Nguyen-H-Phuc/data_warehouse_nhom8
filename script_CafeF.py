import requests
import pandas as pd
from datetime import datetime, timedelta

#1 URL và danh sách mã
url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx"
symbols = ["VCB", "VIC", "VHM", "BID", "CTG", "TCB", "GAS", "HPG", "FPT", "VPB"]
headers = {"User-Agent": "Mozilla/5.0"}
all_data = []

# Xác định tuần hiện tại (thứ 2 → thứ 6)
today = datetime.today()
# Tính ngày thứ 2 của tuần hiện tại
start_of_week = today - timedelta(days=today.weekday())  
end_of_week = start_of_week + timedelta(days=4)  # thứ 6

# format mm/dd/yyyy để request
start_str = start_of_week.strftime("%m/%d/%Y")
end_str = end_of_week.strftime("%m/%d/%Y")

for symbol in symbols:
    params = {
        "Symbol": symbol,
        "StartDate": start_str,
        "EndDate": end_str,
        "PageIndex": 1,
        "PageSize": 1000
    }
    res = requests.get(url, params=params, headers=headers)
    data_json = res.json()
    if "Data" in data_json and "Data" in data_json["Data"]:
        data = data_json["Data"]["Data"]
        df = pd.DataFrame(data)
        df["symbol"] = symbol
        all_data.append(df)

# Ghép tất cả lại
final_df = pd.concat(all_data, ignore_index=True)

# Chuẩn hóa ngày
final_df['Ngay'] = pd.to_datetime(final_df['Ngay'], errors='coerce', dayfirst=True)

# Lọc chỉ thứ 2 → thứ 6
final_df = final_df[final_df['Ngay'].dt.weekday < 5]

# Tách cột thay đổi
def split_change(value):
    try:
        abs_change, pct_change = value.split('(')
        abs_change = abs_change.strip()
        pct_change = pct_change.replace(')', '').replace('%', '').strip()
        return pd.Series([float(abs_change), float(pct_change)])
    except:
        return pd.Series([None, None])

final_df[['change_value', 'change_percent']] = final_df['ThayDoi'].apply(split_change)

# Đổi tên cột
final_df = final_df.rename(columns={
    "Ngay": "trade_date",
    "GiaDieuChinh": "adjusted_price",
    "GiaDongCua": "close_price",
    "KhoiLuongKhopLenh": "trading_volume",
    "GiaTriKhopLenh": "trading_value",
    "KLThoaThuan": "deal_volume",
    "GtThoaThuan": "deal_value",
    "GiaMoCua": "open_price",
    "GiaCaoNhat": "high_price",
    "GiaThapNhat": "low_price",
})

# Chia giá trị cho 1 tỷ
final_df['trading_value'] = final_df['trading_value'].astype(float) / 1_000_000_000
final_df['deal_value'] = final_df['deal_value'].astype(float) / 1_000_000_000

# Xóa cột gốc ThayDoi
final_df = final_df.drop(columns=['ThayDoi'])

# Lưu CSV với tên theo tuần
file_name = f"stocks-{start_of_week.strftime('%Y%m%d')}-{end_of_week.strftime('%Y%m%d')}.csv"
final_df.to_csv(f"C:\\Users\\phuc1\\Downloads\\{file_name}", index=False, encoding="utf-8-sig")

print(f"Hoàn tất! Dữ liệu tuần đã lưu: {file_name}")
