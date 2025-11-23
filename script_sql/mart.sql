CREATE DATABASE IF NOT EXISTS data_mart;
USE data_mart;

CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(10),
    is_business_day BOOLEAN
);

CREATE TABLE dm_stock_price_analysis (
    analysis_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    date_key DATE,
    symbol VARCHAR(10),
    company_name VARCHAR(100),
    sector VARCHAR(50),

    -- Dữ liệu gốc
    close_price DECIMAL(18,2),
    open_price DECIMAL(18,2),
    high_price DECIMAL(18,2),
    low_price DECIMAL(18,2),
    trading_volume BIGINT,
    trading_value BIGINT,

    -- Metrics tính trước
    price_change_1d DECIMAL(5,2),
    price_change_7d DECIMAL(5,2),
    price_change_30d DECIMAL(5,2),

    -- Moving Averages
    ma_20 DECIMAL(18,2),
    ma_50 DECIMAL(18,2),
    ma_200 DECIMAL(18,2),

    -- Signals
    volume_vs_avg_20d DECIMAL(5,2),
    is_breakout BOOLEAN,

    updated_at DATETIME,

    -- Foreign key
    CONSTRAINT fk_price_date FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key)
);

CREATE TABLE dm_sector_performance (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    date_key DATE,
    sector VARCHAR(50),

    avg_price_change DECIMAL(5,2),
    total_trading_volume BIGINT,
    total_trading_value BIGINT,

    advancing_stocks INT,
    declining_stocks INT,
    unchanged_stocks INT,

    top_stock_symbol VARCHAR(10),
    top_stock_change DECIMAL(5,2)
);

CREATE TABLE dm_stock_fundamental (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,

    symbol VARCHAR(10),
    report_period VARCHAR(20),
    date_key DATE,

    -- Financial data
    revenue DECIMAL(18,2),
    profit_after_tax DECIMAL(18,2),
    eps DECIMAL(10,2),

    -- Valuation
    close_price_at_period DECIMAL(18,2),
    pe_ratio DECIMAL(10,2),
    pb_ratio DECIMAL(10,2),
    market_cap DECIMAL(18,2),

    -- Growth
    revenue_growth_yoy DECIMAL(5,2),
    profit_growth_yoy DECIMAL(5,2)
);

INSERT INTO dim_date (date_key, year, month, day, weekday, is_business_day) VALUES
('2025-11-20', 2025, 11, 20, 'Thu', TRUE),
('2025-11-21', 2025, 11, 21, 'Fri', TRUE);

INSERT INTO dm_stock_price_analysis (
    date_key, symbol, company_name, sector,
    close_price, open_price, high_price, low_price, trading_volume, trading_value,
    price_change_1d, price_change_7d, price_change_30d,
    ma_20, ma_50, ma_200,
    volume_vs_avg_20d, is_breakout, updated_at
) VALUES
-- ==== NGÀY 20/11/2025 ====
('2025-11-20','VCB','Vietcombank','Banking',96,95.5,96.5,95,4200000,403200000000,0.5,1.5,4.0,94.5,92,89,0.9,FALSE,NOW()),
('2025-11-20','VIC','Vingroup','Real Estate',42,42.5,42.8,41.8,2500000,105000000000,-1.2,-2.0,-5.0,43,44,46,0.8,FALSE,NOW()),
('2025-11-20','VHM','Vinhomes','Real Estate',58,58,58.5,57.5,2000000,116000000000,0.0,-0.5,-1.5,58.5,59,60,0.7,FALSE,NOW()),
('2025-11-20','BID','BIDV','Banking',46,45.5,46.2,45.2,3500000,161000000000,1.1,2.0,5.5,45,44,41,1.1,TRUE,NOW()),
('2025-11-20','CTG','VietinBank','Banking',38,37.8,38.2,37.5,4000000,152000000000,0.8,1.2,4.0,37,36,34,1.0,FALSE,NOW()),
('2025-11-20','TCB','Techcombank','Banking',36.5,36,36.8,35.8,5500000,200750000000,1.4,3.0,6.0,35.5,34,32,1.2,TRUE,NOW()),
('2025-11-20','GAS','PV Gas','Energy',91,90.5,91.5,90,1200000,109200000000,0.6,1.0,2.5,90,89,86,0.9,FALSE,NOW()),
('2025-11-20','HPG','Hoa Phat','Materials',34.5,34,35,33.8,12000000,414000000000,1.5,4.0,10.0,32,31,30,1.3,TRUE,NOW()),
('2025-11-20','FPT','FPT','Technology',152,150,153,149,2500000,380000000000,2.0,5.0,12.0,145,140,130,1.5,TRUE,NOW()),
('2025-11-20','VPB','VPBank','Banking',23,22.8,23.2,22.7,6000000,138000000000,0.9,1.5,3.0,22.5,22,21,1.0,FALSE,NOW()),

-- ==== NGÀY 21/11/2025 (Điều chỉnh) ====
('2025-11-21','VCB','Vietcombank','Banking',95.5,96,96.2,95.2,3800000,362900000000,-0.5,1.0,3.5,94.8,92.2,89.1,0.85,FALSE,NOW()),
('2025-11-21','VIC','Vingroup','Real Estate',41.8,42,42.2,41.5,2800000,117040000000,-0.4,-2.4,-5.2,42.8,43.8,45.8,0.9,FALSE,NOW()),
('2025-11-21','VHM','Vinhomes','Real Estate',57.5,58,58.2,57.2,2200000,126500000000,-0.8,-1.3,-2.0,58.3,58.8,59.8,0.8,FALSE,NOW()),
('2025-11-21','BID','BIDV','Banking',45.8,46,46.1,45.5,3000000,137400000000,-0.4,1.6,5.1,45.2,44.2,41.2,0.8,FALSE,NOW()),
('2025-11-21','CTG','VietinBank','Banking',37.9,38,38.1,37.6,3500000,132650000000,-0.2,1.0,3.8,37.2,36.1,34.1,0.9,FALSE,NOW()),
('2025-11-21','TCB','Techcombank','Banking',36.8,36.5,37,36.2,5000000,184000000000,0.8,3.8,6.8,35.8,34.2,32.2,1.1,TRUE,NOW()), -- TCB vẫn khỏe
('2025-11-21','GAS','PV Gas','Energy',90.5,91,91.2,90.2,1000000,90500000000,-0.5,0.5,2.0,90.2,89.1,86.1,0.7,FALSE,NOW()),
('2025-11-21','HPG','Hoa Phat','Materials',35,34.5,35.5,34.2,15000000,525000000000,1.4,5.4,11.5,32.5,31.2,30.2,1.6,TRUE,NOW()),
('2025-11-21','FPT','FPT','Technology',151,152,152.5,150.5,1800000,271800000000,-0.6,4.4,11.4,146,141,131,0.8,FALSE,NOW()),
('2025-11-21','VPB','VPBank','Banking',22.9,23,23.1,22.8,5500000,125950000000,-0.4,1.1,2.6,22.6,22.1,21.1,0.9,FALSE,NOW());


INSERT INTO dm_sector_performance (
    date_key, sector, avg_price_change, total_trading_volume, total_trading_value,
    advancing_stocks, declining_stocks, unchanged_stocks, top_stock_symbol, top_stock_change
) VALUES
-- ==== NGÀY 20/11/2025 ====
-- Banking: 5 mã đều tăng (VCB, BID, CTG, TCB, VPB)
('2025-11-20','Banking',0.94,23200000,1054950000000,5,0,0,'TCB',1.4),
-- Real Estate: VIC giảm, VHM đứng giá
('2025-11-20','Real Estate',-0.60,4500000,221000000000,0,1,1,'VHM',0.0),
-- Technology: FPT tăng mạnh
('2025-11-20','Technology',2.00,2500000,380000000000,1,0,0,'FPT',2.0),
-- Materials: HPG tăng mạnh
('2025-11-20','Materials',1.50,12000000,414000000000,1,0,0,'HPG',1.5),
-- Energy: GAS tăng nhẹ
('2025-11-20','Energy',0.60,1200000,109200000000,1,0,0,'GAS',0.6),

-- ==== NGÀY 21/11/2025 ====
-- Banking: Hầu hết giảm nhẹ, chỉ TCB còn tăng
('2025-11-21','Banking',-0.14,20800000,942900000000,1,4,0,'TCB',0.8),
-- Real Estate: Cả 2 đều đỏ
('2025-11-21','Real Estate',-0.60,5000000,243540000000,0,2,0,'VIC',-0.4),
-- Technology: Điều chỉnh
('2025-11-21','Technology',-0.60,1800000,271800000000,0,1,0,'FPT',-0.6),
-- Materials: Tiếp tục hút tiền
('2025-11-21','Materials',1.40,15000000,525000000000,1,0,0,'HPG',1.4),
-- Energy: Điều chỉnh
('2025-11-21','Energy',-0.50,1000000,90500000000,0,1,0,'GAS',-0.5);

INSERT INTO dm_stock_fundamental (
    symbol, report_period, date_key,
    revenue, profit_after_tax, eps,
    close_price_at_period, pe_ratio, pb_ratio, market_cap,
    revenue_growth_yoy, profit_growth_yoy
) VALUES
-- ==== QUÝ 2/2025 (Công bố tháng 7) ====
('BID', 'Q2 2025', '2025-07-30', 200000000000, 68000000000, 2.7, 43, 15.9, 1.6, 430000000000, 10.0, 12.0),
('CTG', 'Q2 2025', '2025-07-30', 175000000000, 58000000000, 2.4, 35, 14.5, 1.3, 360000000000, 9.0, 10.5),
('TCB', 'Q2 2025', '2025-07-30', 150000000000, 51000000000, 2.3, 33, 14.3, 1.5, 320000000000, 6.0, 7.0),
('VPB', 'Q2 2025', '2025-07-30', 120000000000, 39000000000, 1.9, 21.5, 11.3, 1.2, 215000000000, 8.0, 8.5),
('GAS', 'Q2 2025', '2025-07-30', 140000000000, 52000000000, 4.1, 88, 21.4, 3.1, 880000000000, 4.0, 3.0),

-- ==== QUÝ 3/2025 (Công bố tháng 10 - Tác động trực tiếp đến giá tháng 11) ====
-- BID & CTG: Tăng trưởng tín dụng cuối năm mạnh
('BID', 'Q3 2025', '2025-10-30', 220000000000, 75000000000, 3.0, 45, 15.0, 1.7, 450000000000, 12.0, 15.0),
('CTG', 'Q3 2025', '2025-10-30', 190000000000, 64000000000, 2.6, 37, 14.2, 1.4, 380000000000, 10.0, 12.0),
-- TCB: Casa phục hồi tốt
('TCB', 'Q3 2025', '2025-10-30', 165000000000, 58000000000, 2.5, 35, 14.0, 1.6, 340000000000, 8.5, 11.0),
-- VPB: Bán vốn công ty con, ghi nhận lợi nhuận đột biến nhẹ
('VPB', 'Q3 2025', '2025-10-30', 135000000000, 45000000000, 2.1, 22.5, 10.7, 1.2, 225000000000, 10.0, 14.0),
-- GAS: Nhu cầu khí đốt mùa đông tăng
('GAS', 'Q3 2025', '2025-10-30', 155000000000, 56000000000, 4.3, 90, 20.9, 3.2, 900000000000, 6.0, 5.0),
-- FPT & HPG & VIC/VHM (đã có ở prompt trước, nhưng điền lại Q3 cho đủ bộ 10 mã nếu cần)
('FPT', 'Q3 2025', '2025-10-30', 210000000000, 90000000000, 7.5, 148, 19.7, 5.0, 900000000000, 25.0, 30.0),
('HPG', 'Q3 2025', '2025-10-30', 260000000000, 100000000000, 4.5, 34, 7.5, 1.5, 340000000000, 18.0, 22.0),
('VCB', 'Q3 2025', '2025-10-30', 375000000000, 135000000000, 5.6, 95, 16.9, 2.4, 2050000000000, 9.0, 10.0),
('VIC', 'Q3 2025', '2025-10-30', 230000000000, 60000000000, 3.5, 43, 12.2, 1.5, 690000000000, -5.0, -15.0),
('VHM', 'Q3 2025', '2025-10-30', 130000000000, 48000000000, 4.1, 59, 14.4, 1.8, 710000000000, 4.0, 5.0);