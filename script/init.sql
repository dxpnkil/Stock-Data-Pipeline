create database if not EXISTS stock;
use stock;

CREATE TABLE IF NOT EXISTS daily_dim(
    `ticker` STRING,
    `date` DATE,
    `open` FLOAT,
    `high` FLOAT,
    `low` FLOAT,
    `close` FLOAT,
    `volume` BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/data/stock/stock_data.csv' OVERWRITE INTO TABLE daily_dim;

CREATE TABLE IF NOT EXISTS stock_returns (
    `simple_return` FLOAT,
    `log_return` FLOAT,
    `cumulative_return` FLOAT
) PARTITIONED BY (`date` DATE, `ticker` STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET;
