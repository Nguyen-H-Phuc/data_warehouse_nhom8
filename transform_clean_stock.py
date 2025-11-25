import pandas as pd
from datetime import datetime
from config import STOCK_INFO

def split_change(value):
    try:
        abs_change, pct_change = value.split('(')
        abs_change = abs_change.strip()
        pct_change = pct_change.replace(')', '').replace('%', '').strip()
        return pd.Series([float(abs_change), float(pct_change)])
    except:
        return pd.Series([None, None])

def transform_data(file_path):
    df = pd.read_csv(file_path)
    df[['change_value', 'change_percent']] = df['ThayDoi'].apply(split_change)

    df = df.rename(columns={
        "Ngay": "date",
        "GiaMoCua": "open_price",
        "GiaCaoNhat": "high_price",
        "GiaThapNhat": "low_price",
        "GiaDongCua": "close_price",
        "GiaDieuChinh": "adjusted_price",
        "KhoiLuongKhopLenh": "volume",
        "GiaTriKhopLenh": "trading_value",
        "KLThoaThuan": "deal_volume",
        "GtThoaThuan": "deal_value"
    })

    for col in ['trading_value', 'deal_value']:
        df[col] = pd.to_numeric(df[col], errors='coerce') / 1_000_000_000

    df['source'] = 'CafeF'
    df['load_time'] = datetime.now()

    df = df[['symbol', 'date', 'open_price', 'high_price', 'low_price', 'close_price',
             'adjusted_price', 'change_value', 'change_percent', 'volume', 'trading_value',
             'deal_volume', 'deal_value', 'source', 'load_time']]

    print("✅ Dữ liệu transform hoàn tất.")
    return df
