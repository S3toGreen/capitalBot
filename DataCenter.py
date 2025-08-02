import asyncio
from redisworker.Receiver import DataReceiver
from clickhouse_connect import get_async_client
from PySide6.QtCore import QObject, Slot, Signal
from redisworker.AsyncWorker import AsyncWorker


class DataCenter(QObject):
    quote_sig = Signal(str,dict)

    def __init__(self, worker:AsyncWorker, recvr:DataReceiver, db):
        super().__init__()
        self.worker = worker
        self.db = db
        self.recvr = recvr
        self._signals= {'quote:*':self.quote_sig}
        
    @classmethod
    def create(cls):
        # customize your channels
        worker = AsyncWorker()
        channels = ['quote:*'] 
        data_center = asyncio.run_coroutine_threadsafe(cls.create_async(worker,channels), worker.loop).result()
        data_center.recvr.message_received.connect(data_center._msg_handler)
        return data_center
    @classmethod
    async def create_async(cls, worker, channels):
        db = get_async_client(host='localhost',user='client',password='Seto898998',compression=True)
        recvr = DataReceiver.create_async(worker, channels)
        db, recvr = await asyncio.gather(db, recvr)
        return cls(worker,recvr,db)
    @Slot(str,str,dict)
    def _msg_handler(self, pattern, channel, data):
        if pattern:
            sig = self._signals.get(pattern)
            if sig:
                sig.emit(channel,data)
        else: # extract the symbol
            market, symbol = channel.split(':')[-2:]
            print(data)
            sig = self._signals.get(symbol)
            if sig:
                sig.emit(data)
    
    def sub_ticker(self,market:str, symbol:str, slot_func):
        df=asyncio.run_coroutine_threadsafe(self.sub_ticker_async(market, symbol, slot_func), self.worker.loop)
        return df

    async def sub_ticker_async(self, market:str, symbol:str, slot_func):
        # pyqtgraph visualize
        query = f"""
            SELECT *
            FROM orderflow{market}
            WHERE symbol='{symbol}'
            ORDER BY time
        """
        await self.recvr.add_channels(f'snap:{market}:{symbol}',f'bars:{symbol}')

        # if symbol not in self._signals:
        #     self._signals[symbol] = Signal(dict)
        #     self._signals[symbol].connect(slot_func)

        df = await self.db.query_np(query)
        # print(df)
        return df

    def stop(self):
        self.recvr.stop()
        self.worker.stop()

        
