import requests
import pandas as pd
from datetime import datetime, timedelta
import os
from config import STOCK_SYMBOLS, DATA_PATH

def extract_cafef_data():
    url = "https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx"
    headers = {"User-Agent": "Mozilla/5.0"}
    all_data = []

    today = datetime.today()
    start_of_week = today - timedelta(days=today.weekday())
    end_of_week = start_of_week + timedelta(days=4)

    start_str = start_of_week.strftime("%m/%d/%Y")
    end_str = end_of_week.strftime("%m/%d/%Y")

    for symbol in STOCK_SYMBOLS:
        params = {"Symbol": symbol, "StartDate": start_str, "EndDate": end_str, "PageIndex": 1, "PageSize": 1000}
        res = requests.get(url, params=params, headers=headers)
        data_json = res.json()
        if "Data" in data_json and "Data" in data_json["Data"]:
            df = pd.DataFrame(data_json["Data"]["Data"])
            df["symbol"] = symbol
            all_data.append(df)

    df_final = pd.concat(all_data, ignore_index=True)
    df_final['Ngay'] = pd.to_datetime(df_final['Ngay'], errors='coerce', dayfirst=True)
    df_final = df_final[df_final['Ngay'].dt.weekday < 5]

    os.makedirs(DATA_PATH, exist_ok=True)
    file_name = f"cafef_{start_of_week.strftime('%Y%m%d')}_{end_of_week.strftime('%Y%m%d')}.csv"
    file_path = os.path.join(DATA_PATH, file_name)
    df_final.to_csv(file_path, index=False, encoding="utf-8-sig")

    print(f"✅ Đã tải dữ liệu: {file_path}")
    return file_path
