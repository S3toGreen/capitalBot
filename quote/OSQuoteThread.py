import numpy as np
import pandas as pd
from SignalManager import SignalManager
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal, Slot
import asyncio, datetime
from redisworker.Tick_Producer import TickProducer
from redisworker.Async_Worker import AsyncRedisWorker
from collections import defaultdict
from sortedcontainers import SortedDict
from numba import njit
import pythoncom
from quote.tools import Bar

@njit(cache=True, fastmath=True)
def update_depth(dep, args):
    for i in range(10):
        dep[0, i][0] = args[i * 2]/100        # bid price
        dep[0, i][1] = args[i * 2 + 1]        # bid qty
        dep[1, i][0] = args[20 + i * 2]/100   # ask price
        dep[1, i][1] = args[20 + i * 2 + 1] # ask qty

class SKOSQuoteLibEvent(QObject):
    reconn = Signal()
    def __init__(self, skC, skOSQ, redis_worker, market='OS'):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skOSQ = skOSQ
        self.stockid = {}
        self.ptr = defaultdict(int)
        self.last_ptr = {}
        self.status = -1 
        self.redis_worker = redis_worker
        self.producer = asyncio.run_coroutine_threadsafe(TickProducer.create(market),self.redis_worker.loop).result()
        self.orderflow = defaultdict(SortedDict)
        self.market_dep = defaultdict(lambda: np.zeros((2,10),dtype=[('p', 'f4'), ('q', 'u2')]))
        self.signals.OS_store_sig.connect(self.store)

        # --- PERFORMANCE OPTIMIZATIONS ---
        # Cache for date objects to avoid expensive parsing on every tick.
        self._cached_date = 0
        self._cached_today_dt = None
        self._tz = 'America/Chicago'
        # --- END PERFORMANCE OPTIMIZATIONS ---

    def _get_bar_time(self, nDate, nTime):
        """
        PERFORMANCE: Calculates bar timestamp using integer math and caching.
        This avoids expensive string and datetime parsing in the hot path.
        """
        if nDate != self._cached_date:
            # Date has changed (e.g., midnight rollover), update the cached date object.
            self._cached_date = nDate
            self._cached_today_dt = pd.to_datetime(str(nDate), format='%Y%m%d').tz_localize(self._tz)
        # Use integer arithmetic which is significantly faster than string conversion.
        hour = nTime // 10000
        minute = (nTime // 100) % 100
        bar_time = self._cached_today_dt.replace(hour=hour, minute=minute) + pd.Timedelta(minutes=1)
        return bar_time, hour, minute
    
    @Slot(int,int)
    def store(self, sHour, sMin):
        if sHour==5 and sMin: #Reset ptr at market close
            for i in self.ptr.keys():
                self.ptr[i]=0

        for i in list(self.orderflow.keys()):
            if not self.orderflow[i]:
                continue
            last_key, last_value = self.orderflow[i].peekitem(-1)

            if sMin+1==last_value.time.minute:
                self.orderflow[i].popitem()
                bars_to_push = list(self.orderflow[i].values())
                if bars_to_push:
                    print(self.orderflow[i])
                    self.redis_worker.submit(self.producer.push_bars(i, bars_to_push))
                self.orderflow[i] = SortedDict({last_key: last_value})
            else:
                self.producer.lastest_ptr[i]=self.last_ptr[i]+1
                self.redis_worker.submit(self.producer.push_bars(i,list(self.orderflow[i].values())))
                del self.orderflow[i]
            
    def fetch_ptr(self, stocklist:list[str]):
        keys = [i.split(',')[1] for i in stocklist]
        self.ptr = asyncio.run_coroutine_threadsafe(self.producer.get_ptr(keys),self.redis_worker.loop).result()
    # def sync_ptr(self):
    #     asyncio.run_coroutine_threadsafe(self.producer.update_lastest_ptr(),self.redis_worker.loop).result()

    def OnConnect(self, nKind, nCode): 
        status = nKind-3000
        msg = "【OSConnection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status in [2,33] and self.status==1:
            self.orderflow.clear()
            self.reconn.emit()
        self.status = status

    def OnNotifyTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        # store orderflow of NY session only (open at 8:31~15:00) (TODO store also huge volume)
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nPtr<self.ptr[symbol]:
            return
        
        self.last_ptr[symbol]=nPtr
        nClose/=100

        bar_time, hour, minute = self._get_bar_time(nDate, nTime)
        # time_as_int = hour * 100 + minute
        # session = 830 <= time_as_int <= 1500

        current_bar:Bar = self.orderflow[symbol].get(bar_time)#None
        if current_bar is None:
            self.producer.lastest_ptr[symbol] = nPtr
            if self.orderflow[symbol]:
                last_bar:Bar = self.orderflow[symbol].peekitem(-1)[1]
                delta_close = last_bar.delta_hlc[-1]
                total_volume = last_bar.vol
                if total_volume > 0:
                    denominator = total_volume - delta_close
                    t = (total_volume + delta_close) / denominator if denominator != 0 else float('inf')
                    if abs(delta_close) > 150 and (t > 2 or t < 0.5):
                        msg = f'【{symbol}】{last_bar.time.time()} {last_bar.delta_hlc}'
                        self.signals.alert.emit('Delta Imbalanced', msg)    
                print(f"【{symbol}】", last_bar)
                
            tmp = Bar(bar_time,nClose,nClose,nClose,nClose,nQty)#, [0]*3, 0, DefaultSortedDict(lambda: [0,0,0])]
            self.orderflow[symbol][bar_time] = tmp
            current_bar = tmp
        else:
            current_bar = self.orderflow[symbol][bar_time]
            if nClose > current_bar.high:
                current_bar.high = nClose
            elif nClose < current_bar.low:
                current_bar.low = nClose
            current_bar.close = nClose
            current_bar.vol += nQty

        current_bar.price_map[nClose][0]+=nQty
        bid = self.market_dep[symbol][0,0][0] 
        ask = self.market_dep[symbol][1,0][0]
        tmp = (ask+bid)/2
        # Optimized
        if nClose > tmp:
            side = 1
        elif nClose < tmp:
            side = -1
        else:
            side = 0
        # orderflow data

        # if session:
        if side>0:
            current_bar.delta_hlc[-1]+=nQty #close delta
            current_bar.trades_delta+=1
            current_bar.price_map[nClose][1]+=nQty
            current_bar.price_map[nClose][2]+=1
        elif side<0:
            current_bar.delta_hlc[-1]-=nQty
            current_bar.trades_delta-=1
            current_bar.price_map[nClose][1]-=nQty
            current_bar.price_map[nClose][2]-=1
        # else:
        #     if side>0:
        #         current_bar.delta_hlc[-1]+=nQty #close delta
        #         current_bar.trades_delta+=1
        #     elif side<0:
        #         current_bar.delta_hlc[-1]-=nQty
        #         current_bar.trades_delta-=1
        if current_bar.delta_hlc[-1]>current_bar.delta_hlc[0]:
            current_bar.delta_hlc[0]=current_bar.delta_hlc[-1]
        elif current_bar.delta_hlc[-1]<current_bar.delta_hlc[1]:
            current_bar.delta_hlc[1]=current_bar.delta_hlc[-1]

        if nQty>60:
            time_str = f"{hour:02}:{minute:02}:{nTime % 100:02}"
            msg=f"【{symbol}】{time_str} {'SELL' if side<0 else 'BUY'} {nQty} at ${nClose}"
            self.signals.alert.emit('BigTrade',msg)

    def OnNotifyHistoryTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        # cant form orderflow data
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nPtr<self.ptr[symbol]:
            return
        
        # nTimehms=str(f'{nTime:06}')
        # idx = int(nTimehms[2:-2])
        # bar_time = pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=int(nTimehms[:2]),minute=idx).tz_localize('America/Chicago')+pd.Timedelta(minutes=1)
        price = nClose / 100
        bar_time, _, _ = self._get_bar_time(nDate, nTime)

        current_bar:Bar = self.orderflow[symbol].get(bar_time)
        if not current_bar:
            self.producer.lastest_ptr[symbol] = nPtr
            tmp = Bar(bar_time,price, price, price, price, nQty)#, [0]*3, 0, DefaultSortedDict(lambda: [0, 0, 0])]
            self.orderflow[symbol][bar_time] = tmp
        else:
            current_bar = self.orderflow[symbol][bar_time]
            if nClose > current_bar.high:
                current_bar.high = nClose
            elif nClose < current_bar.low:
                current_bar.low = nClose
            current_bar.close = nClose
            current_bar.vol += nQty
        return
    
    def OnNotifyBest10NineDigitLONG(self, nStockidx, *args):
        if not (symbol:=self.stockid.get(nStockidx)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nStockidx, pSKStock)
            self.stockid[nStockidx] = symbol = pSKStock.bstrStockNo
        update_depth(self.market_dep[symbol],args)

    def OnNotifyQuoteLONG(self, nIndex):
        pSKStock = sk.SKFOREIGNLONG()
        pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
        data={'Symbol':pSKStock.bstrStockName,
            'Price':pSKStock.nClose/(10**pSKStock.sDecimal),
            'Open':pSKStock.nOpen/(10**pSKStock.sDecimal),
            'High':pSKStock.nHigh/(10**pSKStock.sDecimal),
            'Low':pSKStock.nLow/(10**pSKStock.sDecimal),
            'Vol':pSKStock.nTQty,'Ref':pSKStock.nRef/(10**pSKStock.sDecimal)}
        self.signals.quote_update.emit(pSKStock.bstrStockNo, data, 'OS')

    def OnOverseaProducts(self, bstrValue):
        if bstrValue.split(',')[0]!='CME':
            return
        msg = "【OnOverseaProducts】" + str(bstrValue)
        print(msg)

class OverseaQuote(QObject):
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()

    def run(self):
        pythoncom.CoInitialize()
        try:
            self.redis_worker = AsyncRedisWorker()
            self.skOSQ = cc.CreateObject(sk.SKOSQuoteLib,interface=sk.ISKOSQuoteLib)
            self.SKOSQuoteEvent = SKOSQuoteLibEvent(self.skC, self.skOSQ, self.redis_worker)
            self.SKOSQuoteLibEventHandler = cc.GetEvents(self.skOSQ, self.SKOSQuoteEvent)
            self.SKOSQuoteEvent.reconn.connect(self.conn_wrap)

        finally:
            pythoncom.CoUninitialize()
            self.timer = QTimer()
            self.timer.setInterval(1500)  # check every 1.5s
            self.timer.timeout.connect(self.check_connection_status)
            self.conn_wrap()

    def init(self):
        #CBOT,YM0000, CME,NQ0000, CME,ES0000
        stocklist=['CME,NQ0000', 'CME,ES0000']
        self.SKOSQuoteEvent.fetch_ptr(stocklist)
        self.subtick(stocklist)
        self.subquote(stocklist)
        # self.skOSQ.SKOSQuoteLib_RequestOverseaProducts()
    @Slot()
    def conn_wrap(self):
        self.SKOSQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.quoteConnect()

    def quoteConnect(self):
        nCode = self.skOSQ.SKOSQuoteLib_EnterMonitorLONG()
        msg = "【OS_Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        self.retry_count = 0
        self.timer.start()
    @Slot()
    def check_connection_status(self):
        self.retry_count += 1
        if self.SKOSQuoteEvent.status == 1:
            self.timer.stop()
            self.SKOSQuoteEvent.reconn.connect(self.conn_wrap)
            self.init()
        elif self.retry_count >= 30:
            self.timer.stop()
            self.quoteDC()  
            msg = "Time Out! Failed to connect. Retry after 30s"
            self.signals.log_sig.emit(msg)
            QTimer.singleShot(30000, self.quoteConnect)  # retry after 30s      

    def quoteDC(self):
        nCode = self.skOSQ.SKOSQuoteLib_LeaveMonitor()
        msg = "【OSQuote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    def subtick(self, stocklist: list[str]):
        for stockNo in stocklist:
            psPageNo, nCode = self.skOSQ.SKOSQuoteLib_RequestTicks(-1, stockNo)
            msg = "【OSSubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def subquote(self, stocklist: list[str]):
        for i in range(0,len(stocklist),100):
            tmp = '#'.join(stocklist[i:i+100])
            _, nCode = self.skOSQ.SKOSQuoteLib_RequestStocks(-1, tmp)
            msg = "【OSSubQuote】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def cleanup(self):
        if hasattr(self,'SKOSQuoteEvent'):
            self.redis_worker.stop()
        print("OverSea exit.")
