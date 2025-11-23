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

INSERT INTO dim_date (date_key, year, month, day, weekday, is_business_day) VALUES
('2025-01-20', 2025, 1, 20, 'Mon', TRUE),
('2025-01-21', 2025, 1, 21, 'Tue', TRUE),
('2025-01-22', 2025, 1, 22, 'Wed', TRUE),
('2025-01-23', 2025, 1, 23, 'Thu', TRUE),
('2025-01-24', 2025, 1, 24, 'Fri', TRUE);

INSERT INTO dim_source (source_id, source_name, base_url, description) VALUES
(1, 'CafeF', 'https://cafef.vn/', 'Dữ liệu từ trang web CafeF'),

INSERT INTO dim_stock (stock_id, symbol, company_name, exchange, sector, listing_date) VALUES
(1, 'VCB', 'Vietcombank', 'HOSE', 'Banking', '2009-06-30'),
(2, 'VIC', 'Vingroup', 'HOSE', 'Real Estate', '2007-09-19'),
(3, 'VHM', 'Vinhomes', 'HOSE', 'Real Estate', '2018-05-17'),
(4, 'BID', 'BIDV', 'HOSE', 'Banking', '2014-01-24'),
(5, 'CTG', 'VietinBank', 'HOSE', 'Banking', '2009-07-16'),
(6, 'TCB', 'Techcombank', 'HOSE', 'Banking', '2018-06-04'),
(7, 'GAS', 'PV Gas', 'HOSE', 'Energy', '2010-11-21'),
(8, 'HPG', 'Hoa Phat Group', 'HOSE', 'Materials', '2007-11-15'),
(9, 'FPT', 'FPT Corporation', 'HOSE', 'Technology', '2006-12-13'),
(10, 'VPB', 'VPBank', 'HOSE', 'Banking', '2017-08-17');

INSERT INTO fact_daily_price (
    fact_id, date_key, stock_id, source_id,
    adjusted_price, close_price, change_value, change_percent,
    trading_volume, trading_value, deal_volume, deal_value,
    open_price, high_price, low_price, created_at
) VALUES
-- ==== 2025-01-20 ====
(1, '2025-01-20', 1, 1, 88, 88, 1.2, 1.4, 5200000, 457600000000, 12000, 1056000000, 87, 89, 86, NOW()),
(2, '2025-01-20', 2, 1, 48, 48, -0.5, -1.0, 3000000, 144000000000, 8000, 384000000, 49, 49, 47, NOW()),
(3, '2025-01-20', 3, 1, 62, 62, 0.3, 0.5, 2500000, 155000000000, 7000, 434000000, 62, 63, 61, NOW()),
(4, '2025-01-20', 4, 1, 41, 41, 0.4, 1.0, 4000000, 164000000000, 9000, 369000000, 40, 42, 39, NOW()),
(5, '2025-01-20', 5, 1, 34, 34, -0.2, -0.6, 4500000, 153000000000, 11000, 374000000, 34, 35, 33, NOW()),
(6, '2025-01-20', 6, 1, 32, 32, 0.1, 0.3, 6000000, 192000000000, 12000, 384000000, 32, 33, 31, NOW()),
(7, '2025-01-20', 7, 1, 86, 86, -1.4, -1.6, 2000000, 172000000000, 6000, 516000000, 87, 87, 85, NOW()),
(8, '2025-01-20', 8, 1, 29, 29, 0.3, 1.0, 9000000, 261000000000, 14000, 406000000, 29, 30, 28, NOW()),
(9, '2025-01-20', 9, 1, 122, 122, 2.0, 1.7, 1500000, 183000000000, 5000, 610000000, 121, 123, 120, NOW()),
(10, '2025-01-20', 10, 1, 21, 21, 0.1, 0.5, 8000000, 168000000000, 15000, 315000000, 21, 22, 20, NOW()),

-- ==== 2025-01-21 ====
(11, '2025-01-21', 1, 1, 89, 89, 1.1, 1.2, 5300000, 471700000000, 12000, 1080000000, 88, 90, 88, NOW()),
(12, '2025-01-21', 2, 1, 49, 49, 1.0, 2.0, 3100000, 151900000000, 8200, 402000000, 48, 50, 48, NOW()),
(13, '2025-01-21', 3, 1, 63, 63, 1.0, 1.6, 2600000, 163800000000, 7200, 453000000, 62, 64, 62, NOW()),
(14, '2025-01-21', 4, 1, 42, 42, 1.0, 2.4, 4200000, 176400000000, 9000, 378000000, 41, 43, 41, NOW()),
(15, '2025-01-21', 5, 1, 34.5, 34.5, 0.5, 1.4, 4700000, 162150000000, 11000, 379500000, 34, 35, 34, NOW()),
(16, '2025-01-21', 6, 1, 32.3, 32.3, 0.3, 0.9, 6100000, 196030000000, 12000, 387000000, 32, 33, 32, NOW()),
(17, '2025-01-21', 7, 1, 86.5, 86.5, 0.5, 0.6, 2100000, 181650000000, 6000, 522000000, 86, 87, 85, NOW()),
(18, '2025-01-21', 8, 1, 29.4, 29.4, 0.4, 1.3, 9200000, 270480000000, 14000, 412000000, 29, 30, 28, NOW()),
(19, '2025-01-21', 9, 1, 123, 123, 1.0, 0.8, 1600000, 196800000000, 5000, 615000000, 122, 124, 121, NOW()),
(20, '2025-01-21', 10, 1, 21.2, 21.2, 0.2, 0.9, 8200000, 174040000000, 15000, 318000000, 21, 22, 20, NOW());
