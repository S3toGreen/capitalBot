import logging
logging.basicConfig(level=logging.DEBUG)
import clickhouse_connect
from Config import passwd
import asyncio
import pandas as pd
"""
MATERIALIZED view

CREATE TABLE fp_data
(
    time        DateTime CODEC(DoubleDelta,ZSTD(3)),    -- Aggregated time window
    symbol      LowCardinality(String) CODEC(LZ4),  -- Instrument name (e.g., ES, BTCUSDT)
    open        Float32 CODEC(Delta, ZSTD(3)),          -- First price in the footprint
    high        Float32 CODEC(Delta, ZSTD(3)),          -- Highest price in the footprint
    low         Float32 CODEC(Delta, ZSTD(3)),          -- Lowest price in the footprint
    close       Float32 CODEC(Delta, ZSTD(3)),          -- Last price in the footprint
    volume      UInt32 CODEC(T64, LZ4),                 -- Total traded volume
    aggBuy      UInt32,                                 --not sure if needed
    aggSell     UInt32,
    price_map   Map(Float32, Tuple(UInt32, Int32))       -- Pice level and (volume,delta)
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
SETTINGS index_granularity = 8192, compress_primary_key=1;

CREATE TABLE orderflowOS
(
    time        DateTime('America/Chicago') CODEC(DoubleDelta,ZSTD(3)),    -- Aggregated time window
    symbol      LowCardinality(String) CODEC(LZ4),  -- Instrument name (e.g., ES, BTCUSDT)
    open        Float32 CODEC(Delta, ZSTD(3)),          -- First price in the footprint
    high        Float32 CODEC(Delta, ZSTD(3)),          -- Highest price in the footprint
    low         Float32 CODEC(Delta, ZSTD(3)),          -- Lowest price in the footprint
    close       Float32 CODEC(Delta, ZSTD(3)),          -- Last price in the footprint
    volume      UInt32 CODEC(T64, LZ4),                 -- Total traded volume
    delta       Tuple(Int32, Int32, Int32) CODEC(Delta, ZSTD(3)),
    trades_delta Int32 CODEC(T64, LZ4),
    price_map   Array(Tuple(Float32,UInt32, Int32, Int32)) CODEC(Delta,ZSTD(3)), -- Pice level and (volume,delta,tradesdelta)
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
TTL time + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, compress_primary_key=1;

CREATE TABLE orderflowDM
(
    time        DateTime('Asia/Taipei') CODEC(DoubleDelta,ZSTD(3)),    -- Aggregated time window
    symbol      LowCardinality(String) CODEC(LZ4),  -- Instrument name (e.g., ES, BTCUSDT)
    open        Float32 CODEC(Delta, ZSTD(3)),          -- First price in the footprint
    high        Float32 CODEC(Delta, ZSTD(3)),          -- Highest price in the footprint
    low         Float32 CODEC(Delta, ZSTD(3)),          -- Lowest price in the footprint
    close       Float32 CODEC(Delta, ZSTD(3)),          -- Last price in the footprint
    volume      UInt32 CODEC(T64, LZ4),                 -- Total traded volume
    delta       Tuple(Int32, Int32, Int32) CODEC(Delta, ZSTD(3)), --(high, low, close)
    trades_delta Int32 CODEC(T64, LZ4),                 
    price_map   Array(Tuple(Float32, UInt32, Int32, Int32)) CODEC(Delta, ZSTD(3)), --(Pice level, volume, delta, tradesdelta)
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
TTL time + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, compress_primary_key=1;

CREATE TABLE daily_info
(
    dt             Date,                              -- Date for the daily summary
    symbol         LowCardinality(String) CODEC(LZ4),
    circulating    Float32 CODEC(Delta, ZSTD(3)),                           -- Example: circulating supply
    margin_rate    Float32 CODEC(Delta, ZSTD(3)),                           -- Example: margin rate
    -- Add additional daily fields as needed
    additional_metrics Nested(
        metric_name  String,
        metric_value Float32
    )
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (symbol, dt)
SETTINGS index_granularity = 8192, compress_primary_key = 1;

derivable data:
    imbalance_ratio Float32,       -- Imbalance between bid/ask at POC
    delta sum(delta_at_price)
"""

async def main():
    # df = pd.read_parquet('footprint.pq')
    client = await clickhouse_connect.get_async_client(host='localhost',user='admin',password=passwd,compression=True)
    res = await client.query('show create fp_data')
    # res = client.insert("fp_data",)
    print(res.result_rows)

    # print(df.reset_index().to_numpy())
    await client.close()
    
    
if __name__=='__main__':
    asyncio.run(main())