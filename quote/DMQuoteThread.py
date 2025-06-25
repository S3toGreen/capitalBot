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
from numba import njit
import pythoncom
import ctypes
from quote.tools import Bar

@njit(cache=True, fastmath=True)
def update_depth(dep, args):
    for i in range(5):
        dep[0, i][0] = args[i * 2]/100        # bid price
        dep[0, i][1] = args[i * 2 + 1]    # bid qty
        dep[1, i][0] = args[12 + i * 2]/100   # ask price
        dep[1, i][1] = args[12 + i * 2 + 1] # ask qty

class SKQuoteLibEvent(QObject):
    reconn = Signal()
    def __init__(self, skC, skQ, redis_worker, market='DM'):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skQ = skQ
        self.stockid = {}
        self.ptr = defaultdict(int)
        self.last_ptr = {}
        self.status = -1 
        self.redis_worker = redis_worker
        self.producer= asyncio.run_coroutine_threadsafe(TickProducer.create(market),self.redis_worker.loop).result()
        self.orderflow = defaultdict(list)
        self.market_dep = defaultdict(lambda: np.zeros((2,5),dtype=[('p', 'f4'), ('q', 'u2')]))
        # --- PERFORMANCE OPTIMIZATIONS ---
        # Cache for date objects to avoid expensive parsing on every tick.
        self._cached_date = 0
        self._cached_today_dt = None
        self._tz = 'Asia/Taipei'
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
    
    def fetch_ptr(self, stocklist:list[str]):
        self.ptr = asyncio.run_coroutine_threadsafe(self.producer.get_ptr(stocklist), self.redis_worker.loop).result()
    # def sync_ptr(self):
    #     asyncio.run_coroutine_threadsafe(self.producer.update_lastest_ptr(),self.redis_worker.loop).result()

    def OnConnection(self, nKind, nCode): 
        status = nKind-3000
        msg = "【Connection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status in [2,33] and self.status==3:
            self.orderflow.clear()
            self.reconn.emit()
        self.status = status

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        if sSecond or sMinute%15:
            return
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)
        self.signals.OS_store_sig.emit(sHour, sMinute)
        if sHour==14:
            for i in self.ptr.keys():
                self.ptr[i]=0

        for i in list(self.orderflow.keys()):
            if not self.orderflow[i]:
                continue
            if sMinute+1==self.orderflow[i][-1].time.minute:
                self.redis_worker.submit(self.producer.push_bars(i,self.orderflow[i][:-1].copy()))
                self.orderflow[i]=self.orderflow[i][-1:]
            else:
                self.producer.lastest_ptr[i]=self.last_ptr[i]+1
                self.redis_worker.submit(self.producer.push_bars(i,self.orderflow[i].copy()))
                del self.orderflow[i]
        
    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr[symbol]:
            return
        self.last_ptr[symbol]=nPtr
        nClose/=100
        nBid/=100
        nAsk/=100
        bar_time, hour, minute = self._get_bar_time(nDate, nTimehms)
        time_str = f"{hour:02}:{minute:02}:{nTimehms % 100:02}"

        # self.redis_worker.submit(self.producer.push_tick(symbol,nClose,nQty,time,side,nPtr))
        # idx = int(nTimehms[2:-2])
        minute_changed = not self.orderflow[symbol] or self.orderflow[symbol][-1].time.minute!=(minute+1)%60
        last:Bar = None
        if minute_changed:
            self.producer.lastest_ptr[symbol] = nPtr
            if self.orderflow[symbol]:
                tmp:Bar = self.orderflow[symbol][-1]
                dnom = tmp.vol - tmp.delta_hlc[-1]
                t=(tmp.vol+tmp.delta_hlc[-1])/dnom if dnom!=0 else float('inf')
                if abs(tmp.delta_hlc[-1])>150 and (t>2 or t<0.5):
                    msg=f'【{symbol}】{tmp.time.time()} {tmp.delta_hlc}'
                    self.signals.alert.emit('Delta Imbalanced',msg)
                print(f"【{symbol}】",tmp)

            tmp = Bar(bar_time,nClose,nClose,nClose,nClose,nQty)#, [0]*3, 0, DefaultSortedDict(lambda: [0,0,0])
            self.orderflow[symbol].append(tmp)
            last = self.orderflow[symbol][-1]
        else:
            last = self.orderflow[symbol][-1]
            if nClose>last.high:
                last.high=nClose
            elif nClose<last.low:
                last.low=nClose
            last.close=nClose
            last.vol+=nQty

        last.price_map[nClose][0] += nQty
        tmp = (nAsk+nBid)/2
        if nClose > tmp:
            side = 1
        elif nClose < tmp:
            side = -1
        else:
            side = 0

        if side>0:
            last.delta_hlc[-1]+=nQty #close delta
            last.trades_delta+=1
            last.price_map[nClose][1]+=nQty
            last.price_map[nClose][2]+=1
        elif side<0:
            last.delta_hlc[-1]-=nQty
            last.trades_delta-=1
            last.price_map[nClose][1]-=nQty
            last.price_map[nClose][2]-=1
            
        if last.delta_hlc[-1]>last.delta_hlc[0]:
            last.delta_hlc[0]=last.delta_hlc[-1]
        elif last.delta_hlc[-1]<last.delta_hlc[1]:
            last.delta_hlc[1]=last.delta_hlc[-1]

        self.signals.data_sig.emit(f"【{symbol}】Time:{time_str} Bid:{nBid} Ask:{nAsk} Strike:{(nClose)} Qty:{nQty}", side)
        if nQty>30:
            msg=f"【{symbol}】{time_str} {'SELL' if side<0 else 'BUY'} {nQty} at ${nClose}"
            self.signals.alert.emit('BigTrade',msg)

    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        # TODO: batch process history data, minute chart & range chart
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex]= symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr[symbol]:
            return
        # Not sure if needed sometimes last tick never push
        self.last_ptr[symbol]=nPtr 
        nClose/=100
        nBid/=100
        nAsk/=100
        bar_time, hour, minute = self._get_bar_time(nDate, nTimehms)
        time_str = f"{hour:02}:{minute:02}:{nTimehms % 100:02}"

        # self.redis_worker.submit(self.producer.push_tick(symbol,nClose,nQty,time,side,nPtr))
        minute_changed = not self.orderflow[symbol] or self.orderflow[symbol][-1].time.minute!=(minute+1)%60
        last:Bar = None
        if minute_changed:
            self.producer.lastest_ptr[symbol] = nPtr
            tmp = Bar(bar_time,nClose,nClose,nClose,nClose,nQty)#, [0]*3, 0, DefaultSortedDict(lambda: [0,0,0]))
            self.orderflow[symbol].append(tmp)
            last = self.orderflow[symbol][-1]
        else: 
            last = self.orderflow[symbol][-1]
            if nClose>last.high:
                last.high=nClose
            elif nClose<last.low:
                last.low=nClose
            last.close=nClose
            last.vol+=nQty

        last.price_map[nClose][0] += nQty
        tmp = (nAsk+nBid)/2
        if nClose > tmp:
            side = 1
        elif nClose < tmp:
            side = -1
        else:
            side = 0

        if side>0:
            last.delta_hlc[-1]+=nQty #close delta
            last.trades_delta+=1
            last.price_map[nClose][1]+=nQty
            last.price_map[nClose][2]+=1
        elif side<0:
            last.delta_hlc[-1]-=nQty
            last.trades_delta-=1
            last.price_map[nClose][1]-=nQty
            last.price_map[nClose][2]-=1
            
        if last.delta_hlc[-1]>last.delta_hlc[0]:
            last.delta_hlc[0]=last.delta_hlc[-1]
        elif last.delta_hlc[-1]<last.delta_hlc[1]:
            last.delta_hlc[1]=last.delta_hlc[-1]

    def OnNotifyBest5LONG(self, sMarketNo, nStockidx, *args):
        # we focus on the change of order book
        if not (symbol:=self.stockid.get(nStockidx)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skQ.SKQuoteLib_GetStockByIndexLONG(nStockidx, pSKStock)
            self.stockid[nStockidx] = symbol = pSKStock.bstrStockNo
        if args[-1]:
            return
        update_depth(self.market_dep[symbol], args[:-1])
        
    def OnNotifyCommodityListWithTypeNo(self, sMarketNo, bstrCommodityData):
        if bstrCommodityData[:2]=='##':
            return
        msg = "【OnNotifyCommodityListWithTypeNo】" + str(sMarketNo) + "_" + bstrCommodityData
        ls = bstrCommodityData.split('%203%')[0]
        print(ls)

    def OnNotifyStrikePrices(self,bstrOptionData):
        data = bstrOptionData.split(',')
        if "AM" in data[2] or data[0]!='TXO':
            return
        print(data)
    
    def OnNotifyFutureTradeInfoLONG(self, bstrStockNo, sMarketNo, nStockidx, nBuyTotalCount, nSellTotalCount, nBuyTotalQty, nSellTotalQty, nBuyDealTotalCount, nSellDealTotalCount):
        # we focus on the change of order book
        
        # msg = ("【FutureInfo】"+ bstrStockNo + 
        # " 委託買進筆數" + str(nBuyTotalCount)+ 
        # " 委託賣出筆數" + str(nSellTotalCount)+ 
        # " 委託買進口數" + str(nBuyTotalQty)+ 
        # " 委託賣出口數" + str(nSellTotalQty)+ 
        # " 成交買進筆數" + str(nBuyDealTotalCount)+
        # " 成交賣出筆數" + str(nSellDealTotalCount))
        data={
            'avg_order_b':nBuyTotalQty/nBuyTotalCount,
            "avg_order_s": nSellTotalQty/nSellTotalCount,
            "order_sqty":nSellTotalQty,
            "deal_bc": nBuyDealTotalCount,
            "deal_sc": nSellDealTotalCount
        }
        # if bstrStockNo=='MTX00':
            # print(msg)
    
    def OnNotifyQuoteLONG(self, sMarketNo, nIndex):
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
        # msg= ("【OnNotifyQuoteLONG】" + "商品代碼" + str(pSKStock.bstrStockNo) + " 名稱" + str(pSKStock.bstrStockName) + " 開盤價" + str(pSKStock.nOpen / 100) + " 成交價" + str(pSKStock.nClose / 100) + " 最高" + str(pSKStock.nHigh / 100) + " 最低" + str(pSKStock.nLow / 100) + " 買盤量" + str(pSKStock.nTBc) + " 賣盤量" + str(pSKStock.nTAc) + " 總量" + str(pSKStock.nTQty) + " 昨收" + str(pSKStock.nRef / 100) + " 昨量" + str(pSKStock.nYQty) + " 買價" + str(pSKStock.nBid/100) + " 買量" + str(pSKStock.nBc) + " 賣價" + str(pSKStock.nAsk/100) + " 賣量" + str(pSKStock.nAc) +" OI"+str(pSKStock.nFutureOI))
        data={'Symbol':pSKStock.bstrStockName,
            'Price':pSKStock.nClose/(10**pSKStock.sDecimal),
            'Open':pSKStock.nOpen/(10**pSKStock.sDecimal),
            'High':pSKStock.nHigh/(10**pSKStock.sDecimal),
            'Low':pSKStock.nLow/(10**pSKStock.sDecimal),
            'Vol':pSKStock.nTQty,'YVol':pSKStock.nYQty,'Ref':pSKStock.nRef/(10**pSKStock.sDecimal),'OI':pSKStock.nFutureOI}
        self.signals.quote_update.emit(pSKStock.bstrStockNo, data, 'DM')
        # print(data)

class DomesticQuote(QObject):
    ready_sig=Signal()
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()

    def run(self):
        pythoncom.CoInitialize()
        try:
            self.redis_worker = AsyncRedisWorker()
            self.skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
            self.SKQuoteEvent = SKQuoteLibEvent(self.skC, self.skQ, self.redis_worker)
            self.SKQuoteLibEventHandler = cc.GetEvents(self.skQ, self.SKQuoteEvent)
            self.SKQuoteEvent.reconn.connect(self.conn_wrap)
        
        finally:
            pythoncom.CoUninitialize()
            self.timer = QTimer()
            self.timer.setInterval(1500)  # check every 1.5s
            self.timer.timeout.connect(self.check_connection_status)
            self.conn_wrap()

    def init(self):
        # self.fetch_options()
        stocklist = ['TX00', 'MTX00']
        self.SKQuoteEvent.fetch_ptr(stocklist)
        self.subtick(stocklist)
        self.subquote(stocklist)

        self.ready_sig.emit()
        pass
    @Slot()
    def conn_wrap(self):
        self.SKQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.quoteConnect()

    def quoteConnect(self):
        # print('quoteCalled')
        nCode = self.skQ.SKQuoteLib_EnterMonitorLONG()
        msg = "【Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        self.retry_count = 0
        self.timer.start()
    @Slot()
    def check_connection_status(self):
        self.retry_count += 1
        if self.SKQuoteEvent.status == 3:
            self.timer.stop()
            self.SKQuoteEvent.reconn.connect(self.conn_wrap)
            self.init()
        elif self.retry_count >= 30:
            self.timer.stop()
            self.quoteDC()  
            msg = "Time Out! Failed to connect. Retry after 30s"
            self.signals.log_sig.emit(msg)
            QTimer.singleShot(30000, self.quoteConnect)  # retry after 90s   

    def quoteDC(self):
        nCode = self.skQ.SKQuoteLib_LeaveMonitor()
        msg = "【Quote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    def subtick(self, stocklist: list[str]):
        pg=0
        for stockNo in stocklist:
            psPageNo, nCode = self.skQ.SKQuoteLib_RequestTicks(pg, stockNo)
            msg = "【SubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            nCode = self.skQ.SKQuoteLib_RequestFutureTradeInfo(ctypes.c_short(pg), stockNo)
            msg = "【FutureInfo】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            pg+=1

    def subquote(self, stocklist: list[str]):
        # could only subscribe 100 stock 
        stocklist.extend(['2408','TSEA','OTCA','2330'])#'CYF00'
        for i in range(0,len(stocklist),100):
            tmp = ','.join(stocklist[i:i+100])
            _, nCode = self.skQ.SKQuoteLib_RequestStocks(-1, tmp)
            msg = "【SubQuote】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def suboptions(self, optionId):
        #TX1,TX2,TXO,TX3,TX4 <= one of these
        # 5 digits for price
        # A-L call option for 12 month, M-X is put option for 12 month
        # last character is a last digit of the year 202'5'
        
        list = [] #get strike price of  
        _, nCode = self.skQ.SKQuoteLib_RequestStocks(list)

    def fetch_options(self):
        nCode = self.skQ.SKQuoteLib_RequestStockList(3)

    def requestKlines(self, stockNo: str):
        global Klines
        Klines = np.empty((0, 6))

        # min(volume)
        now = datetime.datetime.now()
        since = now - pd.Timedelta(days=75)
        nCode = self.skQ.SKQuoteLib_RequestKLineAMByDate(stockNo,0,1,0,since.strftime("%Y%m%d"),now.strftime("%Y%m%d"),15)
        msg = "【RequestKLine】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)        

        df = pd.DataFrame(Klines,columns=["Time","Open","High","Low","Close","Volume"]).set_index("Time").astype(float).astype(int)
        df.index = pd.to_datetime(df.index)

        return df.tail(900)
    
    def cleanup(self):
        if hasattr(self,'SKQuoteEvent'):
            self.redis_worker.stop()
        print("Domestic exit.")

