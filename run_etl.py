from extract_cafef import extract_cafef_data
from transform_clean_stock import transform_data
from load_to_dw import load_to_dw

def main():
    print("🚀 BẮT ĐẦU ETL PIPELINE")
    csv_file = extract_cafef_data()
    df = transform_data(csv_file)
    load_to_dw(df)
    print("✅ ETL HOÀN TẤT!")

if __name__ == "__main__":
    main()
