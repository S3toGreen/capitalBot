import clickhouse_connect.driver
import clickhouse_connect.driver.exceptions
import redis.asyncio as redis
import asyncio, json
import pandas as pd
# import datetime
from collections import defaultdict
import clickhouse_connect
from quote.tools import Bar
from Config import passwd

class TickProducer:
    def __init__(self, market, db=None):
        self.redis = redis.Redis(connection_pool=redis.ConnectionPool(), decode_responses=True)
        self.market = market
        self.lastest_ptr = defaultdict(int)
        self.lastest_ts = defaultdict(str)
        self.db:clickhouse_connect.driver.AsyncClient = db
    @classmethod
    async def create(cls, market):
        db = await clickhouse_connect.get_async_client(host='localhost',user='admin',password=passwd,compression=True)
        return cls(market, db)
    
    async def push_tick(self, symbol, price, qty, ts, side, ptr=None):
        tick_data = {
            'ts': ts, 'p': price, 'q': qty, 's': side
        }
        stream_key = f"Tick:{self.market}:{symbol}"
        pub_key = f"Pub:{self.market}"
        await asyncio.gather(
            self.redis.xadd(stream_key, tick_data, maxlen=6000, approximate=True),
            self.redis.publish(pub_key, json.dumps(tick_data))
        )
        # await self.redis.xadd(stream_key,tick_data,maxlen=6000,approximate=True)
        # self.lastest_ptr[symbol]=ptr

    async def push_bars(self, symbol, datas:list):
        insert_rows = []
        trig=False
        if self.lastest_ts[symbol]=='':
            ts = (await self.db.query(f"select time from orderflow{self.market} where symbol='{symbol}' order by time desc limit 1")).result_rows
            self.lastest_ts[symbol] = pd.Timestamp(ts[0][0]) if ts else pd.Timestamp(0,tz=datas[0].time.tz)

        for bar in datas:
            if trig or bar.time>self.lastest_ts[symbol]:
                trig=True
                insert_rows.append((
                    bar.time,symbol,bar.open,bar.high,bar.low,bar.close,bar.vol,tuple(bar.delta_hlc),bar.trades_delta,
                    [(price,v[0],v[1],v[2]) for price,v in reversed(bar.price_map.items()) if bar.price_map]
                    ))
                print(bar.time)
        # print(insert_rows)
        try:
            if insert_rows:
                await self.db.insert(f'orderflow{self.market}',insert_rows)
                self.lastest_ts[symbol] = insert_rows[-1][0]
                print(f"push to orderflow{self.market} succeed!")
            else:
                print('nothing to push.', self.lastest_ts[symbol])
            await self.update_lastest_ptr(symbol)
        except clickhouse_connect.driver.exceptions.ClickHouseError as e:
            print('clickhouseError:', e)
            raise

    async def pub_sym(self, symbol, unclosed=None):
        pub_key=f'channel:{self.market}'
        data={'id':symbol}
        if unclosed:
            # unclosed[0]=unclosed[0].strftime('%Y-%m-%d %H:%M:%S')
            data.update({'unclosed':unclosed})
        await self.redis.publish(pub_key, json.dumps(data,default=str))

    async def get_ptr(self, symbols: list[str]) -> dict[str,int]:
        key = f"last_ptr:{self.market}"
        t = await self.redis.hmget(key, symbols)
        res = {symbol: int(ptr) if ptr else 0 for symbol, ptr in zip(symbols, t)}
        print(f'Fetched {self.market}',res)
        return res 
    
    def expireTS(self):
        now = pd.Timestamp.utcnow()
        if self.market=='DM': # utc+8 
            exTS = pd.Timestamp.utcnow().replace(hour=6,minute=45)
        else: # utc-5
            exTS = pd.Timestamp.utcnow().replace(hour=21,minute=45)
        if now> exTS:
            exTS += pd.Timedelta(days=1) 
        # print(exTS)
        return int(exTS.timestamp())
    async def update_lastest_ptr(self,symbol):
        if self.lastest_ptr[symbol]:
            key = f'last_ptr:{self.market}'
            p=self.redis.pipeline()
            p.hmset(key,{symbol: self.lastest_ptr[symbol]})
            p.expireat(key,self.expireTS())
            await p.execute()
            # await self.redis.aclose()
            print(f'Saved {self.market}', {symbol:self.lastest_ptr[symbol]})
