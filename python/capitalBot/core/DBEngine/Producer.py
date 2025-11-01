import json, asyncio
# import pandas as pd
from datetime import datetime, timedelta
# from collections import defaultdict
# from clickhouse_connect import get_async_client
# from clickhouse_connect.driver.asyncclient import AsyncClient
# from clickhouse_connect.driver.exceptions import ClickHouseError
import os
from redis.asyncio import Redis
# from itertools import dropwhile
from msgspec import msgpack
from zoneinfo import ZoneInfo
from core.tools import Tick
import numpy as np
import logging
logger = logging.getLogger("TickerApp.Producer")

class DataProducer:
    def __init__(self, redis:Redis, db:AsyncClient=None):
        self.redis = redis
        self.db = db

        # self.async_worker = worker
        self.lastest_ptr = {} #defaultdict(int) {market:{symbol:ptr,...}}
        # self.big_trade_buf = defaultdict(list)
        # self._buf_lock = Lock()
        # using queue for non-blocking
        # self.lastest_bar = {}
        self._tz = {
            "DM":[ZoneInfo("Asia/Taipei"),(14, 45)],
            "OS":[ZoneInfo("America/Chicago"),(16, 45)]
        } # tz & expire time

        self.expiry = {}

    # @classmethod
    # def create(cls, market, worker):
    #     return asyncio.run_coroutine_threadsafe(cls.create_async(market,worker), worker.loop).result(3)
    @classmethod
    async def create_async(cls):
        try:
            r = Redis(decode_responses=True,
                    socket_connect_timeout=3,
                    socket_timeout=6,
                    socket_keepalive=True,
                    health_check_interval=30,
                    max_connections=30
                )
            await r.ping()

            # ch = await get_async_client(host='localhost',user='admin',password=os.getenv('BROKER_PASS'),compression=True)
            # if not await ch.ping():
            #     raise
        except Exception as e:
            logger.exception(f"Error connecting to databases: {e}")
            raise
        return cls(r)

    # def set_snap(self, symbol, bars:list):
    #     self.async_worker.submit(self.set_snap_async(symbol, bars))    
    # async def set_snap_async(self, symbol, bars:list):
    #     pub_key = f'snap:{self.market}:{symbol}'
    #     payload = [ bar.to_dict() for bar in bars]
    #     await self.redis.publish(pub_key, msgspec.msgpack.encode(payload))
    # def xadd_tick(self, symbol, tick):
    #     self.async_worker.submit(self.xadd_tick_async(symbol, tick))
    async def xadd_tick(self, market:str, symbol:str, tick:Tick):
        key = f"tick:{market}:{symbol}"
        try:
            await self.redis.xadd(key, tick.pack(), id=str(tick.ptr))
        except Exception as e:
            if "top-most ID" in str(e):
                pass # Ignore duplicate
            else:
                logger.error(f"XADD Error for {key}: {e}")
    # def pub_quote(self, symbol, data):
    #     self.async_worker.submit(self.pub_quote_async(symbol, data))
    async def pub_quote(self, market:str, symbol:str, data:dict):
        pub_key = f'quote:{market}:{symbol}'
        await self.redis.publish(pub_key, msgpack.encode(data))

    # def push_bars(self, symbol, bars:list):
    #     self.async_worker.submit(self.push_bars_async(symbol, bars))
    # async def push_bars_async(self, symbol, bars:list):
    #     # push to ohlcv and fp separately
    #     m_type = 1 if self.market=='OS' else 2
    #     symbol = symbol[:2]+'0000' if self.market=='OS' else symbol
    #     if symbol not in self.lastest_bar:
    #         ts = (await self.db.query(f"SELECT time FROM ohlcv{self.market} WHERE symbol='{symbol}' ORDER BY time DESC LIMIT 1")).result_rows
    #         self.lastest_bar[symbol] = pd.Timestamp(ts[0][0]) if ts else pd.Timestamp(0,tz=bars[0].time.tz)

    #     new_bars = dropwhile(lambda b: b.time <= self.lastest_bar[symbol], bars)

    #     rows = [(
    #         m_type, bar.time, symbol, bar.open/100, bar.high/100, bar.low/100, bar.close/100, bar.vol,
    #         bar.delta_hlc[0], bar.delta_hlc[1], bar.delta_hlc[2], bar.trades_delta,
    #         [(p/100, v[0], v[1], v[2]) for p,v in bar.price_map.items()]
    #         ) for bar in new_bars
    #     ]

    #     try:
    #         if rows:
    #             await self.db.insert(f'bar_pipe', rows)
    #             print(f"Push to orderflow{self.market} succeed!")
    #             self.lastest_bar[symbol] = rows[-1][1]
    #             await self.update_lastest_ptr(symbol)
    #         else:
    #             print(f'Nothing to push. {symbol}', self.lastest_bar[symbol])
            
    #     except ClickHouseError as e:
    #         print('clickhouseError:', e)
    #         raise

    # def pub_depth(self, symbol, data):
    #     self.async_worker.submit(self.pub_depth_async(symbol,data))
    async def pub_depth(self, market:str, symbol:str, data:np.ndarray):
        pub_key = f'dom:{market}:{symbol}'
        data_dict = {"shape": data.shape, "dtype":str(data.dtype), "payload":data.tobytes()}
        await self.redis.publish(pub_key, msgpack.encode(data_dict))

    # def pub_tick(self,symbol, tick):
    #     self.async_worker.submit(self.pub_tick_async(symbol, tick))
    # async def pub_tick_async(self, symbol, tick):
    #     pub_key = 'trade:{self.market}:{symbol}'
    #     await self.redis.publish(pub_key, msgpack.encode(tick.to_dict()))

    # def insert_ticks(self):
    #     tmp = self.big_trade_buf
    #     self.big_trade_buf = defaultdict(list)
    #     self.async_worker.submit(self.insert_ticks_async(tmp))
    # async def insert_ticks_async(self, trade_buf):
    #     m_type = 1 if self.market=='OS' else 2
    #     rows=[]
    #     # with self._buf_lock:
    #     for symbol, ticks in trade_buf.items():
    #         if not ticks: continue
    #         rows.extend((tick.ptr, tick.time, symbol, tick.price/100, tick.side, tick.qty, m_type)for tick in ticks)

    #     if rows:
    #         try:
    #             await self.db.insert('ticks', rows)
    #             # print(f"{self.market}: push to ticks succeed!")
    #         except ClickHouseError as e:
    #             print('clickhouseError:', e)
    #             raise

    # def get_ptr(self, symbols:list)-> dict:
    #     # return self.async_worker.submit(self.get_ptr_async(symbols)).result()
    #     return asyncio.run_coroutine_threadsafe(self.get_ptr_async(symbols), self.async_worker.loop).result()
    async def get_ptr_async(self) -> dict[str,int]:
        res = {}
        for k in self._tz.keys():
            key = f"last_ptr:{k}"
            data = await self.redis.hgetall(key)
            res[k] = {key:int(v) for key,v in data.items()}
        logger.info(f"Fetched PTR: {res}")
        return res
    
    def update_expiry(self, now:datetime):
        if not self.expiry:
            for k,v in self._tz.items():
                tmp = datetime.now(tz=v[0])
                self.expiry[k] = tmp.replace(hour = v[1][0], minute = v[1][1]).astimezone(ZoneInfo("UTC"))
        
        for k in self.expiry.keys():
            if now >= self.expiry[k]:
                self.expiry[k] += timedelta(days=1)

    async def update_lastest_ptr(self,symbol):
        now = datetime.now(tz=self._tz)
        if self.expiry is None or now>=self.expiry:
            self.update_expiry(now)
        if self.lastest_ptr[symbol]:
            key = f'last_ptr:{self.market}'
            p=self.redis.pipeline()
            p.hset(key, symbol, self.lastest_ptr[symbol])
            p.expireat(key, int(self.expiry.timestamp()))
            await p.execute()
            print(f'Saved {self.market}', {symbol:self.lastest_ptr[symbol]})

    async def update_ptr(self):
        if not self.lastest_ptr:
            return 
        ptr_to_save = self.lastest_ptr
        self.lastest_ptr = {}

        now = datetime.now(tz=ZoneInfo("UTC"))
        self.update_expiry(now)

        p = self.redis.pipeline()
        #separate ptr_to_save into DM/OS
        for k,v in ptr_to_save.items():
            key = f'last_ptr:{k}'
            p.hset(key, mapping=v)
            p.expireat(key, int(self.expiry[k]))
        await p.execute()
        print(f"Saved {ptr_to_save}")

    async def close(self):
        """Graceful shutdown for async clients."""
        try:
            if self.lastest_ptr:
                self.update_ptr()
            if self.redis:
                await self.redis.aclose()
            if self.db:
                await self.db.close()
            logger.info("Producer connections closed.")
        except Exception:
            logger.exception("Error during producer close.")

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