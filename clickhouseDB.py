import clickhouse_connect
import logging
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
    price_levels      Array(Float32) CODEC(DoubleDelta, ZSTD(3)),  -- Array of distinct price levels traded in the minute
    volume_at_price   Array(UInt64) CODEC(T64, ZSTD(3)),           -- Array of volumes corresponding to each price level
    delta_at_price    Array(Int32) CODEC(T64, ZSTD(3)),             -- Array of delta values at each price level (could be computed as ask volume - bid volume)
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
SETTINGS index_granularity = 8192, compress_primary_key=1;

CREATE TABLE fp
(
    time              DateTime CODEC(DoubleDelta,ZSTD(3)),         -- Minute timestamp
    symbol            LowCardinality(String) CODEC(LZ4), -- Asset symbol
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (symbol, time)
SETTINGS index_granularity = 8192, compress_primary_key = 1;

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
logging.basicConfig(level=logging.DEBUG)

async def main():
    df = pd.read_parquet('footprint.pq')
    client = await clickhouse_connect.get_async_client(host='localhost',user='admin',password=passwd,compression=True)
    res = await client.query('show create fp_data')
    # res = client.insert("fp_data",)
    print(res.result_rows)

    print(df.reset_index().to_numpy())
    await client.close()
    
    
if __name__=='__main__':
    asyncio.run(main())