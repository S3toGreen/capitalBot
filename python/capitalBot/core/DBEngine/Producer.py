import json, asyncio
import pandas as pd
from collections import defaultdict
from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ClickHouseError
import os
from redis.asyncio import Redis
from itertools import dropwhile
import msgspec

class DataProducer:
    def __init__(self, market, worker, db, redis:Redis):
        self.redis = redis
        self.async_worker = worker
        self.market = market # OS,DM
        self.lastest_ptr = defaultdict(int)
        self.ticks_buf = defaultdict(list)
        self.lastest_bar = {}
        self.db = db

    @classmethod
    def create(cls, market, worker):
        return asyncio.run_coroutine_threadsafe(cls.create_async(market,worker), worker.loop).result(3)
    @classmethod
    async def create_async(cls, market, worker):
        db = await get_async_client(host='localhost',user='admin',password=os.getenv('BROKER_PASS'),compression=True)
        redis = Redis(decode_responses=True,
                socket_connect_timeout=3,
                socket_timeout=6,
                socket_keepalive=True,
                health_check_interval=30,
                max_connections=30
            )
        return cls(market,worker,db,redis)

    def pub_snap(self, symbol,bars:list):
        self.async_worker.submit(self.pub_snap_async(symbol, bars))    
    async def pub_snap_async(self, symbol, bars:list):
        channel = f'snap:{self.market}:{symbol}'
        payload = [ bar.to_dict() for bar in bars] 
        await self.redis.publish(channel, msgspec.msgpack.encode(payload))
    
    def pub_quote(self, data):
        self.async_worker.submit(self.pub_quote_async(data))
    async def pub_quote_async(self, data:dict):
        pub_key = f'quote:{self.market}'
        # print(data)
        await self.redis.publish(pub_key, json.dumps(data))

    def push_bars(self, symbol, bars:list):
        self.async_worker.submit(self.push_bars_async(symbol, bars))

    async def push_bars_async(self, symbol, bars:list):
        # push to ohlcv and fp separately
        m_type = 1 if self.market=='OS' else 2
        symbol = symbol[:2]+'0000' if self.market=='OS' else symbol
        if symbol not in self.lastest_bar:
            ts = (await self.db.query(f"SELECT time FROM ohlcv{self.market} WHERE symbol='{symbol}' ORDER BY time DESC LIMIT 1")).result_rows
            self.lastest_bar[symbol] = pd.Timestamp(ts[0][0]) if ts else pd.Timestamp(0,tz=bars[0].time.tz)

        new_bars = dropwhile(lambda b: b.time <= self.lastest_bar[symbol], bars)

        rows = [(
            m_type, bar.time.tz_localize(), symbol, bar.open/100, bar.high/100, bar.low/100, bar.close/100, bar.vol,
            bar.delta_hlc[0], bar.delta_hlc[1], bar.delta_hlc[2], bar.trades_delta,
            [(p/100, v[0], v[1], v[2]) for p,v in bar.price_map.items()]
            ) for bar in new_bars
        ]
        try:
            if rows:
                await self.db.insert(f'bar_pipe', rows)
                print(f"push to orderflow{self.market} succeed!")
                self.lastest_bar[symbol] = bars[-1].time
            else:
                print('nothing to push.', self.lastest_bar[symbol])
            await self.update_lastest_ptr(symbol)
            
        except ClickHouseError as e:
            print('clickhouseError:', e)
            raise

    def pub_ticks(self,symbol, tick):
        self.async_worker.submit(self.pub_ticks_async(symbol, tick))
    async def pub_ticks_async(self, symbol, tick):
        pub_key = f'tick:{self.market}:{symbol}'
        await self.redis.publish(pub_key, json.dumps(tick.to_dict()))

    def insert_ticks(self):
        return self.ticks_buf.clear()
        asyncio.run_coroutine_threadsafe(self.insert_ticks_async(), self.async_worker.loop).result()
    async def insert_ticks_async(self):
        rows=[]
        m_type = 1 if self.market=='OS' else 2
        for symbol, ticks in self.ticks_buf.items():
            if not ticks: continue
            for tick in ticks:
                rows.append((tick.ptr, tick.time, symbol, tick.price, tick.side, tick.qty, m_type))
            ticks.clear()
        if rows:
            try:
                await self.db.insert('ticks', rows)
                print(f"{self.market}: push to ticks succeed!")
            except ClickHouseError as e:
                print('clickhouseError:', e)
                raise

    def get_ptr(self, symbols:list)-> dict:
        # return self.async_worker.submit(self.get_ptr_async(symbols)).result()
        return asyncio.run_coroutine_threadsafe(self.get_ptr_async(symbols), self.async_worker.loop).result()
    async def get_ptr_async(self, symbols: list[str]) -> dict[str,int]:
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
        
        return int(exTS.timestamp())
    async def update_lastest_ptr(self,symbol):
        if self.lastest_ptr[symbol]:
            key = f'last_ptr:{self.market}'
            p=self.redis.pipeline()
            p.hmset(key,{symbol: self.lastest_ptr[symbol]})
            p.expireat(key,self.expireTS())
            await p.execute()
            print(f'Saved {self.market}', {symbol:self.lastest_ptr[symbol]})
    # async def pub_sym_async(self, symbol, unclosed=None):
    #     pub_key=f'channel:{self.market}'
    #     data={'id':symbol}
    #     if unclosed:
    #         # unclosed[0]=unclosed[0].strftime('%Y-%m-%d %H:%M:%S')
    #         data.update({'unclosed':unclosed})
    #     await self.redis.publish(pub_key, json.dumps(data,default=str))

    # async def push_bars(self, symbol, datas:list):
    #     insert_rows = []
    #     trig=False
    #     if self.lastest_ts[symbol]=='':
    #         ts = (await self.db.query(f"select time from orderflow{self.market} where symbol='{symbol}' order by time desc limit 1")).result_rows
    #         self.lastest_ts[symbol] = pd.Timestamp(ts[0][0]) if ts else pd.Timestamp(0,tz=datas[0].time.tz)

    #     for bar in datas:
    #         if trig or bar.time>self.lastest_ts[symbol]:
    #             trig=True
    #             insert_rows.append((
    #                 bar.time,symbol,bar.open,bar.high,bar.low,bar.close,bar.vol,tuple(bar.delta_hlc),bar.trades_delta,
    #                 [(price,v[0],v[1],v[2]) for price,v in reversed(bar.price_map.items()) if bar.price_map]
    #                 ))
    #             print(bar.time)
    #     # print(insert_rows)
    #     try:
    #         if insert_rows:
    #             await self.db.insert(f'orderflow{self.market}',insert_rows)
    #             self.lastest_ts[symbol] = insert_rows[-1][0]
    #             print(f"push to orderflow{self.market} succeed!")
    #         else:
    #             print('nothing to push.', self.lastest_ts[symbol])
    #         await self.update_lastest_ptr(symbol)
    #     except clickhouse_connect.driver.exceptions.ClickHouseError as e:
    #         print('clickhouseError:', e)
    #         raise