CREATE DATABASE IF NOT EXISTS data_config;
USE data_config;

CREATE TABLE config_feed (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    source_url VARCHAR(255) NOT NULL,
    schedule VARCHAR(50) DEFAULT 'daily',
    last_run DATETIME,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE etl_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100),
    stock_symbol VARCHAR(10),
    source_url VARCHAR(255),
    start_time DATETIME,
    end_time DATETIME,
    records_inserted INT,
    status ENUM('SUCCESS','FAIL','WARNING') DEFAULT 'SUCCESS',
    message TEXT
);

CREATE TABLE config_src (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stock_symbol VARCHAR(255) NOT NULL,
    source_url VARCHAR(255) NOT NULL,
    dest_file VARCHAR(255) NOT NULL,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE log_config_src (
    id INT AUTO_INCREMENT PRIMARY KEY,
    config_id INT,
    process_id VARCHAR(100),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status ENUM('SUCCESS','FAIL','WARNING') DEFAULT 'SUCCESS',
    message TEXT
);

INSERT INTO config_src(stock_symbol, source_url, dest_file)
VALUES ("VCB, VIC, VHM, BID, CTG, TCB, GAS, HPG, FPT, VPB", 
"https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx",
"D:\data_warehouse")

