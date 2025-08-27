import numpy as np
import pandas as pd
from SignalManager import SignalManager
import comtypes
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal, Slot
import asyncio, datetime
from redisworker.Producer import DataProducer
from redisworker.AsyncWorker import AsyncWorker
from collections import defaultdict
from numba import njit
import ctypes
from quote.tools import Tick, Bar
from rust_engine import register_sink

@njit(cache=True, fastmath=True)
def update_depth(dep, args):
    for i in range(5):
        dep[0, i][0] = args[i * 2]     # bid price
        dep[0, i][1] = args[i * 2 + 1]      # bid qty
        dep[1, i][0] = args[12 + i * 2] # ask price
        dep[1, i][1] = args[12 + i * 2 + 1] # ask qty

class SKQuoteLibEvent(QObject):
    reconn = Signal()
    def __init__(self, skC, skQ, worker, market='DM'):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skQ = skQ
        self.producer= DataProducer.create(market, worker)

        self.stockid = {}
        self.ptr = {}
        self.last_ptr = {}
        self.status = -1 
        self.tick_buffer = defaultdict(list)

        self.orderflow = defaultdict(list)
        self.market_dep = defaultdict(lambda: np.zeros((2,5), dtype=[('p', 'f4'), ('q', 'u2')]))

        # Debounce timer
        self.backfill_timer = QTimer(self)
        self.backfill_timer.setSingleShot(True)
        self.backfill_timer.setInterval(3000)
        self.backfill_timer.timeout.connect(self._finalize_backfill)
        # self.backfill_symbols = set()
        self.live_timer = QTimer(self)
        self.live_timer.setInterval(300)
        self.live_timer.timeout.connect(self._live_tick)

        #EOD detect
        self.idle_timer = QTimer(singleShot=True)
        self.idle_timer.setInterval(600000)
        self.idle_timer.timeout.connect(self._EOD)
        self.idle_timer.start()

        # Cache for date objects to avoid expensive parsing on every tick.
        self._cached_date = 0
        self._cached_today_dt = None
        self._tz = 'Asia/Taipei'

    def _to_timestamp(self, nDate, nTime):
        if nDate != self._cached_date:
            self._cached_date = nDate
            self._cached_today_dt = pd.to_datetime(str(nDate), format='%Y%m%d').tz_localize(self._tz)
        hour = nTime // 10000
        minute = (nTime // 100) % 100
        sec = nTime%100
        return self._cached_today_dt.replace(hour=hour, minute=minute, second=sec)

    def _process_tick_buffer(self):
        if not self.tick_buffer:
            return

        # tmp_buf = {
        #     symbol: ticks.copy()
        #     for symbol, ticks in self.tick_buffer.items() if ticks
        # }
        # self.producer.insert_ticks(tmp_buf)

        for symbol, ticks in self.tick_buffer.items():
            if not ticks: continue
            self.producer.ticks_buf[symbol].extend(ticks)
            self._agg_tick(symbol, ticks)
            ticks.clear()

    def _finalize_backfill(self):
        self._process_tick_buffer()
        self.live_timer.start()
    
    def _live_tick(self):
        self._process_tick_buffer()

    def _agg_tick(self, symbol, ticks:list):
        # TODO push the last bar EOD
        ticks.sort(key=lambda t:t.ptr)
        bar_time_end = (self.orderflow[symbol][-1].time + pd.Timedelta(minutes=1)) if self.orderflow[symbol] else None
        for t in ticks:
            minute_changed = not bar_time_end or t.time >= bar_time_end
            last:Bar = None
            if minute_changed:
                if bar_time_end:
                    self.producer.pub_snap(symbol, self.orderflow[symbol][-1])
                bar_time = t.time.replace(second=0)
                bar_time_end = bar_time + pd.Timedelta(minutes=1)
                self.producer.lastest_ptr[symbol] = t.ptr
                tmp = Bar(bar_time, t.price, t.price, t.price, t.price, t.qty)
                self.orderflow[symbol].append(tmp)
                last = self.orderflow[symbol][-1]
            else: 
                last = self.orderflow[symbol][-1]
                if t.price>last.high:
                    last.high=t.price
                elif t.price<last.low:
                    last.low=t.price
                last.close=t.price
                last.vol+=t.qty

            # last.price_map[t.price][0] += t.qty

            last.delta_hlc[-1] += t.qty*t.side #close delta
            last.trades_delta += t.side
            # if t.side: # 1 aggb, -1 aggs, 0 neutral
            last.price_map[t.price][t.side] += t.qty 
                
            if last.delta_hlc[-1]>last.delta_hlc[0]:
                last.delta_hlc[0]=last.delta_hlc[-1]
            elif last.delta_hlc[-1]<last.delta_hlc[1]:
                last.delta_hlc[1]=last.delta_hlc[-1]

        self.last_ptr[symbol] = ticks[-1].ptr+1
        if len(self.orderflow[symbol])>1:
            self.producer.push_bars(symbol, self.orderflow[symbol][:-1].copy())
            self.orderflow[symbol]= [last]
        # snapshot?
        self.producer.pub_snap(symbol, last)

    def _EOD(self):
        for symbol, bars in self.orderflow.items():
            if bars:
                self.producer.lastest_ptr[symbol] = self.last_ptr[symbol]
                self.producer.push_bars(symbol, bars)
        self.orderflow.clear()
        self.live_timer.stop()
        self.producer.insert_ticks()
        print("------DM MARKET CLOSED------")

    def fetch_ptr(self, stocklist:list[str]):
        # if not self.ptr:
        self.ptr = self.producer.get_ptr(stocklist)

    def OnConnection(self, nKind, nCode): 
        status = nKind-3000
        msg = "【Connection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        # print(msg) 
        # closed of the market, accident disconnect
        if status in [2,33] and self.status==3:
            if self.last_ptr:
                self.ptr = self.last_ptr.copy()
            self.reconn.emit()
        self.status = status

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        if sSecond or sMinute%30:
            return
        if (sHour, sMinute)==(5, 30):
            self.signals.OS_reset.emit()
        elif (sHour, sMinute)==(8, 30):
            for symbol in self.ptr.keys():
                if symbol.isdigit():
                    self.ptr[symbol] = 0
                    self.last_ptr[symbol] = 0
        elif (sHour, sMinute)==(14, 30):
            if self.ptr:
                self.ptr.clear()
                self.last_ptr.clear()
                self.producer.insert_ticks()
                if self.orderflow:
                    print(f'ERROR orderflow data not pushed.\n{self.orderflow}')
                else:
                    print('Reset DM data')

        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)
        
    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        # backfill data 8:00~14:45
        pass

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTime, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        #TODO rust based
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, _= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr.get(symbol,0):
            return
        if not self.live_timer.isActive() and not self.backfill_timer.isActive():
            self.live_timer.start()
        self.idle_timer.start()
        mid_price = (nAsk+nBid)/2
        side = 0
        if mid_price:
            if nClose > mid_price:
                side = 1
            elif nClose < mid_price:
                side = -1
        tick = Tick(ptr=nPtr, time=self._to_timestamp(nDate,nTime), side=side, price=nClose/100, qty=nQty)
        self.producer.pub_ticks(symbol, tick)
        self.tick_buffer[symbol].append(tick)
        
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTime, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        #TODO rust based
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, _= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex]= symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr.get(symbol,0):
            return
        self.live_timer.stop()
        mid_price = (nAsk+nBid)/2
        side = 0
        if mid_price:
            if nClose > mid_price:
                side = 1
            elif nClose < mid_price:
                side = -1
        tick = Tick(ptr=nPtr, time=self._to_timestamp(nDate,nTime), side=side, price=nClose/100, qty=nQty)
        self.tick_buffer[symbol].append(tick)

        self.backfill_timer.start()

    def OnNotifyBest5LONG(self, sMarketNo, nStockidx, *args):
        #TODO rust based
        # we focus on the change of order book
        if not (symbol:=self.stockid.get(nStockidx)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, nCode = self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nStockidx, pSKStock)
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
        #TODO rust based
        # we focus on the change of order book
        data={
            'avg_order_b':nBuyTotalQty/nBuyTotalCount,
            "avg_order_s": nSellTotalQty/nSellTotalCount,
            # "order_sqty":nSellTotalQty,
            "deal_bc": nBuyDealTotalCount,
            "deal_sc": nSellDealTotalCount
        }
        return
    
    def OnNotifyQuoteLONG(self, sMarketNo, nIndex):
        #TODO rust based
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
        self.stockid[nIndex] = pSKStock.bstrStockNo
        data={'Symbol':pSKStock.bstrStockName,
            'C':pSKStock.nClose/(10**pSKStock.sDecimal),
            'O':pSKStock.nOpen/(10**pSKStock.sDecimal),
            'H':pSKStock.nHigh/(10**pSKStock.sDecimal),
            'L':pSKStock.nLow/(10**pSKStock.sDecimal),
            'Vol':pSKStock.nTQty,'YVol':pSKStock.nYQty,'Ref':pSKStock.nRef/(10**pSKStock.sDecimal),'OI':pSKStock.nFutureOI, 'ID':pSKStock.bstrStockNo, "aggBuy":pSKStock.nTBc,"aggSell":pSKStock.nTAc}
        # self.signals.quote_update.emit(pSKStock.bstrStockNo, data, 'DM')
        self.producer.pub_quote(data)

    def cleanup(self):
        if self.backfill_timer.isActive():
            self.backfill_timer.stop()
        if self.live_timer.isActive():
            self.live_timer.stop()
        if self.idle_timer.isActive():
            self.idle_timer.stop()
        self.producer.insert_ticks()

class DomesticQuote(QObject):
    # acc=''
    # passwd=''
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()
        self.symlist = ['TX00', 'MTX00', '2330', '2454','2317','2308'] # this is for tick

    def run(self):
        comtypes.CoInitialize()
        try:
            # self.skC = cc.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
            # self.skC.SKCenterLib_LoginSetQuote(self.acc,self.passwd,'Y')
            self.async_worker = AsyncWorker()
            self.skQ = cc.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
            self.SKQuoteEvent = SKQuoteLibEvent(self.skC, self.skQ, self.async_worker)
            ptr = int.from_bytes(self.skQ.value, byteorder="little", signed=False)
            # register_sink(ptr)
            self.SKQuoteLibEventHandler = cc.GetEvents(self.skQ, self.SKQuoteEvent)
        finally:
            comtypes.CoUninitialize()
            self.SKQuoteEvent.reconn.connect(self.conn_wrap)
            self.timer = QTimer()
            self.timer.setInterval(1500)  # check every 1.5s
            self.timer.timeout.connect(self.check_connection_status)

            self.signals.restart_dm.connect(self.quoteDC)
            self.SKQuoteEvent.fetch_ptr(self.symlist)
            self.conn_wrap()

    def init(self):
        # self.fetch_options()
        try:
            self.subquote()
        except Exception as e:
            print(e)
            self.signals.restart_dm.emit()
            return
        self.SKQuoteEvent.backfill_timer.start()
        self.subtick()

    @Slot()
    def conn_wrap(self):
        self.SKQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.signals.restart_dm.disconnect(self.quoteDC)
        self.quoteConnect()

    def quoteConnect(self):
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
            self.signals.restart_dm.connect(self.quoteDC)
            self.init()
        elif self.retry_count >= 30:
            self.timer.stop()
            self.quoteDC()  
            msg = "Time Out! Failed to connect. Retry after 30s"
            self.signals.log_sig.emit(msg)
            QTimer.singleShot(30000, self.quoteConnect)  # retry after 30s   

    def quoteDC(self):
        nCode = self.skQ.SKQuoteLib_LeaveMonitor()
        msg = "【Quote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    def subtick(self):
        pg=-1
        for stockNo in self.symlist:
            psPageNo, nCode = self.skQ.SKQuoteLib_RequestTicks(pg, stockNo)
            msg = "【SubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)+'\t'+stockNo
            self.signals.log_sig.emit(msg)
            # nCode = self.skQ.SKQuoteLib_RequestFutureTradeInfo(ctypes.c_short(pg), stockNo)
            # msg = "【FutureInfo】" +stockNo+ self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            # self.signals.log_sig.emit(msg)
            pg = psPageNo+1

    def subquote(self):
        # could only subscribe 100 stock 
        pg=-1
        ls = self.symlist.copy()
        ls.extend(['TSEA','OTCA'])#'CYF00' # Additional quote
        for i in range(0,len(ls),100):
            tmp = ','.join(ls[i:i+100])
            psPageNo, nCode = self.skQ.SKQuoteLib_RequestStocks(pg, tmp)
            if nCode:
                raise RuntimeError("DM failed to subscribe. Restart!") 
            msg = "【SubQuote】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)+'\t'+tmp
            self.signals.log_sig.emit(msg)
            pg = psPageNo+1

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
        pass
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
    
    def stop(self):
        if hasattr(self,'SKQuoteEvent'):
            self.SKQuoteEvent.cleanup()
            self.async_worker.stop()
            print("Domestic exit.")

