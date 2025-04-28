import redis
from collections import defaultdict
from sortedcontainers import SortedDict
from PySide6.QtCore import QObject
import pandas as pd
import numpy as np

footprint = np.empty((0, 2),dtype=object)  #{Timestamp:{price:[aggbuy, aggsell]}} or [[timestamp,{price:aggbuy, aggsell}]

class DefaultSortedDict(SortedDict):
    def __init__(self, default_factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_factory = default_factory

    def __getitem__(self, key):
        if key not in self:
            self[key] = self.default_factory()
        return super().__getitem__(key)

class TickConsumer(QObject):
    def __init__(self, market='DM'):
        super().__init__()
        self.redis = redis.Redis(host='localhost',decode_responses=True)
        self.market = market
        self.running = True
        # self.redis.flushall()

    def run(self):
        key = f'Tick:{self.market}:TX00'
        last_id = '$'
        while self.running:
            messages = self.redis.xread({key: last_id}, block=100)
            for stream_key, entries in messages:
                for msg_id, tick in entries:
                    last_id = msg_id
                    self.process_tick(tick)

    def process_tick(self, tick):
        # Store to ClickHouse or signal to UI
        print("Tick:", tick)

    def expireTS(self):
        exTS = pd.Timestamp.today().replace(hour=14,minute=45)
        now = pd.Timestamp.today()
        if now> exTS:
            exTS += pd.Timedelta(days=1) 
        return int(exTS.timestamp())
    async def update_lastest_ptr(self,lastest_ptr):
        key = f'last_ptr:{self.market}'
        p = self.redis.pipeline()
        p.hmset(key,lastest_ptr)
        p.expireat(key,self.expireTS())
        await p.execute()
        
if __name__=='__main__':
    con = TickConsumer()
    con.run()


    # if self.SKQuoteEvent.tmpK.size:
    #     print("Saving min data...")
    #     t1=pd.read_parquet("TXfp.pq")
    #     t=np.flip(footprint,axis=0)
    #     _, j = np.unique(t[:,0],return_index=True)
    #     footprint=t[j]
    #     t = []
    #     filter = []
    #     for ts, price in footprint:
    #         for p,c in reversed(price.items()):
    #             t.append({'Time':ts,'Price':int(p),'volume':c[0],'delta':c[1]})
    #             filter.append(ts)
    #     pd.concat([t1[~t1.index.get_level_values('Time').isin(filter)],pd.DataFrame(t).set_index(['Time','Price'])]).to_parquet('TXfp.pq')
        
    #     t1 = pd.read_csv("TX_Tick.csv",parse_dates=['Time'], dtype=np.int32)
    #     t = np.flip(self.SKQuoteEvent.tmpK, axis=0) 
    #     _, j = np.unique(t[:,0],return_index=True)
    #     self.SKQuoteEvent.tmpK = t[j]
    #     pd.concat([t1[~t1['Time'].isin(self.SKQuoteEvent.tmpK[:,0])], pd.DataFrame(self.SKQuoteEvent.tmpK[:,:-1],columns=t1.columns)]).set_index('Time').to_csv("TX_Tick.csv")
