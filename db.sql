
CREATE DATABASE IF NOT EXISTS stock_data_warehouse;
USE stock_data_warehouse;

-- ============================================================
-- BẢNG CONFIG
-- ============================================================
DROP TABLE IF EXISTS config_feed;
CREATE TABLE config_feed (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_symbol VARCHAR(100) NOT NULL,
    source_url VARCHAR(255),
    schedule VARCHAR(50),
    last_run DATETIME,
    active BOOLEAN DEFAULT TRUE
);

-- ============================================================
-- 2️⃣ BẢNG LOG ETL (GHI NHẬT KÝ CHẠY)
-- ============================================================
DROP TABLE IF EXISTS etl_log;
CREATE TABLE etl_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100),
    stock_symbol VARCHAR(100),
    source_url VARCHAR(255),
    start_time DATETIME,
    end_time DATETIME,
    records_inserted INT,
    status ENUM('SUCCESS','FAIL','WARNING'),
    message TEXT
);

-- ============================================================
-- 3️⃣ BẢNG STAGING: DỮ LIỆU GIÁ CHỨNG KHOÁN HÀNG NGÀY
-- ============================================================
DROP TABLE IF EXISTS stg_stock_price;
CREATE TABLE stg_stock_price (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    date DATE,
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    adjusted_price DECIMAL(12,2),
    change_value DECIMAL(12,2),
    change_percent DECIMAL(6,2),
    volume BIGINT,
    trading_value DECIMAL(18,2),
    deal_volume BIGINT,
    deal_value DECIMAL(18,2),
    source VARCHAR(50),
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 4️⃣ BẢNG STAGING: THÔNG TIN CÔNG TY
-- ============================================================
DROP TABLE IF EXISTS stg_company_info;
CREATE TABLE stg_company_info (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    company_name VARCHAR(255),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    listing_date DATE,
    market_cap DECIMAL(18,2),
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 5️⃣ BẢNG STAGING: BÁO CÁO TÀI CHÍNH
-- ============================================================
DROP TABLE IF EXISTS stg_financial_report;
CREATE TABLE stg_financial_report (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    period VARCHAR(10),
    revenue DECIMAL(18,2),
    profit DECIMAL(18,2),
    eps DECIMAL(10,2),
    roa DECIMAL(10,2),
    roe DECIMAL(10,2),
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- 6️⃣ DIM_DATE: BẢNG THỜI GIAN (LỊCH NGÀY)
-- ============================================================
DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(10),
    is_business_day BOOLEAN
);

-- ============================================================
-- 7️⃣ DIM_STOCK: THÔNG TIN CỔ PHIẾU
-- ============================================================
DROP TABLE IF EXISTS dim_stock;
CREATE TABLE dim_stock (
    stock_id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE,
    company_name VARCHAR(255),
    exchange VARCHAR(50),
    sector VARCHAR(100),
    listing_date DATE
);

-- ============================================================
-- 8️⃣ DIM_SOURCE: NGUỒN DỮ LIỆU (CAFEF)
-- ============================================================
DROP TABLE IF EXISTS dim_source;
CREATE TABLE dim_source (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(50),
    base_url VARCHAR(255),
    description TEXT
);

INSERT INTO dim_source (source_name, base_url, description)
VALUES ('CafeF', 'https://cafef.vn', 'Nguồn dữ liệu chứng khoán CafeF');

-- ============================================================
-- 9️⃣ FACT_DAILY_PRICE: BẢNG FACT LƯU GIÁ CHỨNG KHOÁN
-- ============================================================
DROP TABLE IF EXISTS fact_daily_price;
CREATE TABLE fact_daily_price (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key DATE NOT NULL,
    stock_id INT NOT NULL,
    source_id INT,
    adjusted_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    change_value DECIMAL(12,2),
    change_percent DECIMAL(6,2),
    trading_volume BIGINT,
    trading_value DECIMAL(18,2),
    deal_volume BIGINT,
    deal_value DECIMAL(18,2),
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (source_id) REFERENCES dim_source(source_id)
);
