CREATE DATABASE IF NOT EXISTS data_staging;
USE data_staging;

CREATE TABLE stg_company_info (
    id BIGINT PRIMARY KEY,
    symbol VARCHAR(10) UNIQUE,
    company_name VARCHAR(255),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    listing_date DATE,
    market_cap DECIMAL(18,2),
    load_time TIMESTAMP
);

CREATE TABLE stg_financial_report (
    id BIGINT PRIMARY KEY,
    symbol VARCHAR(10),
    period VARCHAR(10),
    revenue DECIMAL(18,2),
    profit DECIMAL(18,2),
    eps DECIMAL(10,2),
    pe_ratio DECIMAL(10,2),
    load_time TIMESTAMP,
    FOREIGN KEY (symbol) REFERENCES stg_company_info(symbol)
);

CREATE TABLE stg_stock_price (
    id BIGINT PRIMARY KEY,
    symbol VARCHAR(10),
    date DATE,
    open_price DECIMAL(12,2),
    high_price DECIMAL(12,2),
    low_price DECIMAL(12,2),
    close_price DECIMAL(12,2),
    volume BIGINT,
    source VARCHAR(50),
    load_time TIMESTAMP,
    FOREIGN KEY (symbol) REFERENCES stg_company_info(symbol)
);
