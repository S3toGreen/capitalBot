import logging
logging.basicConfig(level=logging.DEBUG)
import clickhouse_connect
from redisworker.Config import passwd
import asyncio
import pandas as pd
"""
CREATE TABLE ticks
(
    ptr     UInt32 CODEC(DoubleDelta, ZSTD),
    time    DateTime CODEC(DoubleDelta, ZSTD),
    symbol  LowCardinality(String) CODEC(LZ4),
    price   Decimal64(4) CODEC(Delta, ZSTD),
    side    Int8 CODEC(LZ4),
    qty     UInt32 CODEC(Delta, ZSTD),
    type    Enum('overseas' = 1, 'domestic' = 2) CODEC(LZ4)
)ENGINE = ReplacingMergeTree
PARTITION BY toDate(time)
PRIMARY KEY (symbol,time,ptr)
ORDER BY (symbol,time,ptr)
TTL time + INTERVAL 3 DAY
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE default.bar_pipe
(    
    `type` Enum('overseas' = 1, 'domestic' = 2),
    `time` DateTime,
    `symbol` LowCardinality(String),
    `open` Decimal64(4),
    `high` Decimal64(4),
    `low` Decimal64(4),
    `close` Decimal64(4),
    `vol` UInt32,
    `delta_high`   Int32,
    `delta_low`    Int32,
    `delta_close`  Int32, 
    `trades_delta` Int32,
    `price_map` Array(Tuple(price Decimal64(4), neutral UInt32, agg_buy UInt32, agg_sell UInt32)) -- Send the map as an Array of Tuples
)
ENGINE = Null;

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
    price_map   Array(Tuple(Float32,UInt32, Int32, Int32)) CODEC(Delta,ZSTD(3)), -- Pice level and (volume,aggBuy,aggSell)
    PRIMARY KEY (symbol, time)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(time)
ORDER BY (symbol, time)
TTL time + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192, compress_primary_key=1;

CREATE TABLE default.ohlcvOS                              
(                                                              
    `time` DateTime('America/Chicago') CODEC(DoubleDelta, ZSTD(3)),
    `symbol` LowCardinality(String) CODEC(LZ4),                
    `open` Decimal64(4) CODEC(Delta, ZSTD(3)),                   
    `high` Decimal64(4) CODEC(Delta, ZSTD(3)),                   
    `low` Decimal64(4) CODEC(Delta, ZSTD(3)),                    
    `close` Decimal64(4) CODEC(Delta, ZSTD(3)),                  
    `vol` UInt32 CODEC(T64, ZSTD(3)),
    `delta_high`   Int32 CODEC(T64, ZSTD(3)),
    `delta_low`    Int32 CODEC(T64, ZSTD(3)),
    `delta_close`  Int32 CODEC(T64, ZSTD(3)),                                      
    `trades_delta` Int32 CODEC(T64, ZSTD(3)),      -- Total trade count delta                                     
)                                                              
ENGINE = ReplacingMergeTree(vol)                                           
PARTITION BY toYYYYMMDD(time)                                 
PRIMARY KEY (symbol, time)                                    
ORDER BY (symbol, time)                                       
SETTINGS index_granularity = 8192, compress_primary_key = 1, min_age_to_force_merge_seconds = 3600;

CREATE TABLE default.fpOS                              
(                                                              
    `time` DateTime('America/Chicago') CODEC(DoubleDelta, ZSTD(3)),
    `symbol` LowCardinality(String) CODEC(LZ4),
    `price` Decimal64(4) CODEC(Delta, ZSTD(3)),
    `neutral` UInt32 CODEC(T64, ZSTD(3)),
    `agg_buy` UInt32 CODEC(T64, ZSTD(3)),   -- agg sell
    `agg_sell` UInt32 CODEC(T64, ZSTD(3)),   -- agg buy
    `vol`    UInt32 MATERIALIZED  (neutral + agg_buy + agg_sell) CODEC(T64, ZSTD(3))
    --`trades_delta` Int32 CODEC(T64, ZSTD(3)),
)
ENGINE = ReplacingMergeTree(vol)                                            
PARTITION BY toYYYYMMDD(time)                                 
PRIMARY KEY (symbol, time, price)                                    
ORDER BY (symbol, time, price)                                       
SETTINGS index_granularity = 8192, compress_primary_key = 1, min_age_to_force_merge_seconds = 3600;

CREATE MATERIALIZED VIEW default.mv_to_ohlcvOS TO default.ohlcvOS
AS SELECT
    time, symbol, open, high, low, close, vol, delta_high, delta_low, delta_close, trades_delta
FROM default.bar_pipe
WHERE type = 'overseas'; -- ROUTING LOGIC HERE

CREATE MATERIALIZED VIEW default.mv_to_fpOS TO default.fpOS
AS SELECT
    time, symbol, price_map.price AS price, price_map.neutral AS neutral, price_map.agg_buy AS agg_buy, price_map.agg_sell AS agg_sell
FROM default.bar_pipe
ARRAY JOIN price_map
WHERE type = 'overseas'; -- ROUTING LOGIC HERE

--------------------------------------------------DM------------------------------------------------------------------
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

CREATE TABLE default.ohlcvDM                              
(                                                              
    `time` DateTime('Asia/Taipei') CODEC(DoubleDelta, ZSTD(3)),
    `symbol` LowCardinality(String) CODEC(LZ4),
    `open` Decimal64(4) CODEC(Delta, ZSTD(3)),
    `high` Decimal64(4) CODEC(Delta, ZSTD(3)),
    `low` Decimal64(4) CODEC(Delta, ZSTD(3)),                    
    `close` Decimal64(4) CODEC(Delta, ZSTD(3)),                  
    `vol` UInt32 CODEC(T64, ZSTD(3)),                           
    `delta_high`   Int32 CODEC(T64, ZSTD(3)),
    `delta_low`    Int32 CODEC(T64, ZSTD(3)),
    `delta_close`  Int32 CODEC(T64, ZSTD(3)),
    `trades_delta` Int32 CODEC(T64, ZSTD(3)),      -- Total trade count delta                                     
)                                                              
ENGINE = ReplacingMergeTree(vol)                                            
PARTITION BY toYYYYMMDD(time)                                 
PRIMARY KEY (symbol, time)                                    
ORDER BY (symbol, time)                                       
SETTINGS index_granularity = 8192, compress_primary_key = 1, min_age_to_force_merge_seconds = 3600;

CREATE TABLE default.fpDM                              
(                                                              
    `time`      DateTime('Asia/Taipei') CODEC(DoubleDelta, ZSTD(3)),
    `symbol`    LowCardinality(String) CODEC(LZ4),
    `price`     Decimal64(4) CODEC(Delta, ZSTD(3)),
    `neutral` UInt32 CODEC(T64, ZSTD(3)),
    `agg_buy` UInt32 CODEC(T64, ZSTD(3)),   
    `agg_sell` UInt32 CODEC(T64, ZSTD(3)),   
    `vol`    UInt32 MATERIALIZED (neutral + agg_buy + agg_sell) CODEC(T64, ZSTD(3))
    -- `trades_delta` Int32 CODEC(T64, ZSTD(3)),
)
ENGINE = ReplacingMergeTree(vol)                                            
PARTITION BY toYYYYMMDD(time)                                 
PRIMARY KEY (symbol, time, price)                                    
ORDER BY (symbol, time, price)                                       
SETTINGS index_granularity = 8192, compress_primary_key = 1, min_age_to_force_merge_seconds = 3600;

CREATE MATERIALIZED VIEW default.mv_to_ohlcvDM TO default.ohlcvDM
AS SELECT
    time, symbol, open, high, low, close, vol, delta_high, delta_low, delta_close, trades_delta
FROM default.bar_pipe
WHERE type = 'domestic'; 

CREATE MATERIALIZED VIEW default.mv_to_fpDM TO default.fpDM
AS SELECT
    time, symbol, price_map.price AS price, price_map.neutral AS neutral, price_map.agg_buy AS agg_buy, price_map.agg_sell AS agg_sell
FROM default.bar_pipe
ARRAY JOIN price_map
WHERE type = 'domestic'; 


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