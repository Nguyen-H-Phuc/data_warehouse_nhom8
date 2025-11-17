CREATE DATABASE IF NOT EXISTS data_warehouse;
USE data_warehouse;

CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(10),
    is_business_day BOOLEAN
);

CREATE TABLE dim_source (
    source_id INT PRIMARY KEY,
    source_name VARCHAR(50),
    base_url VARCHAR(255),
    description TEXT
);

CREATE TABLE dim_stock (
    stock_id INT PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    company_name VARCHAR(100),
    exchange VARCHAR(20),
    sector VARCHAR(50),
    listing_date DATE
);

CREATE TABLE fact_daily_price (
    fact_id BIGINT PRIMARY KEY,
    date_key DATE NOT NULL,
    stock_id INT NOT NULL,
    source_id INT NOT NULL,
    adjusted_price DECIMAL(18,2),
    close_price DECIMAL(18,2),
    change_value DECIMAL(18,2),
    change_percent DECIMAL(5,2),
    trading_volume BIGINT,
    trading_value BIGINT,
    deal_volume BIGINT,
    deal_value BIGINT,
    open_price DECIMAL(18,2),
    high_price DECIMAL(18,2),
    low_price DECIMAL(18,2),
    created_at DATETIME,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (stock_id) REFERENCES dim_stock(stock_id),
    FOREIGN KEY (source_id) REFERENCES dim_source(source_id)
);
