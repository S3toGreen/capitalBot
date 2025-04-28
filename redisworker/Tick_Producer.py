import clickhouse_connect.driver
import clickhouse_connect.driver.asyncclient
import clickhouse_connect.driver.exceptions
import redis.asyncio as redis
import asyncio, json
import pandas as pd
from collections import defaultdict
import clickhouse_connect
from Config import passwd

class TickProducer:
    def __init__(self, market, db=None):
        self.redis = redis.Redis(connection_pool=redis.ConnectionPool(), decode_responses=True)
        self.market = market
        self.lastest_ptr=defaultdict(int)
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
    async def push_bars(self, symbol, datas):
        # push minute bars or range bars
        # stream_key = f'orderflow:{symbol}'
        insert_rows = []
        print(datas)
        # return
        for bar in datas:
            insert_rows.append((
                symbol,
                bar[0],bar[1],bar[2],bar[3],bar[4],bar[5],bar[6],bar[7],
                {price:(v[0],v[1],v[2]) for price,v in bar[8].items()}
                ))
        # print(len(insert_rows))

        try:
            await self.db.insert('orderflowDM',insert_rows,column_names=['symbol','time','open','high','low','close','volume','delta','trades_delta','price_map'])
            print("push to orderflowDM succeed!")
        except clickhouse_connect.driver.exceptions.ClickHouseError as e:
            print('clickhouseError:', e)
            raise
        # await self.db.insert(table='fp_new',data=data,column_names=[])
        # with open('fp_tmp.txt','a+') as fp:
        #     fp.writelines(str(data)+'\n')
        # await self.redis.xadd(stream_key, data)

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
        return {symbol: int(ptr) if ptr else 0 for symbol, ptr in zip(symbols, t)}
    
    def expireTS(self):
        # utc+8 
        exTS = pd.Timestamp.utcnow().replace(hour=6,minute=45)
        now = pd.Timestamp.utcnow()
        if now> exTS:
            exTS += pd.Timedelta(days=1) 
        # print(exTS)
        return int(exTS.timestamp())
    async def update_lastest_ptr(self):
        if self.lastest_ptr:
            key = f'last_ptr:{self.market}'
            p=self.redis.pipeline()
            p.hmset(key,self.lastest_ptr)
            p.expireat(key,self.expireTS())
            await p.execute()
            # await self.redis.aclose()
            print('saved', self.lastest_ptr)
