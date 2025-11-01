import numpy as np
from core.SignalManager import SignalManager
import comtypes
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal, Slot, QThread
import asyncio
from core.DBEngine.Producer import DataProducer
from core.DBEngine.Receiver import DataReceiver
from core.DBEngine.AsyncWorker import AsyncWorker
from collections import defaultdict
from numba import njit
import ctypes
from core.tools import Tick, Bar
from zoneinfo import ZoneInfo
from datetime import datetime
# from rust_engine import register_sink

QTY_THRESH=30
Klines = np.empty((0, 6))

@njit(cache=True, fastmath=True)
def update_depth(dep:np.ndarray, args):
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
        self.signals.DM_reset.connect(self.reset_ptr)

        self.stockid = {}
        self.ptr = {}
        self.last_ptr = {}
        self.status = -1 
        self.tick_buffer = defaultdict(list)
        self.orderflow = defaultdict(list)
        self.market_dep = defaultdict(lambda: np.zeros((2,5), dtype=[('p', 'f4'), ('q', 'u2')]))

        # Debounce timer
        # self.backfill_timer = QTimer(self,singleShot=True)
        # self.backfill_timer.setInterval(3000)
        # self.backfill_timer.timeout.connect(self._finalize_backfill)
        # self.backfill_symbols = set()
        self.live_timer = QTimer(self)
        self.live_timer.setInterval(30000)
        self.live_timer.timeout.connect(self.producer.update_ptr)

        #EOD detect
        self.idle_timer = QTimer(self,singleShot=True)
        self.idle_timer.setInterval(540000)
        self.idle_timer.timeout.connect(self._EOD)

        # Cache for date objects to avoid expensive parsing on every tick.
        self._epoch_cache_us = {}
        self._tz = ZoneInfo('Asia/Taipei')

    # def _process_tick_buffer(self):
    #     if not self.tick_buffer:
    #         return
    #     self.producer.insert_ticks()

    #     for symbol, ticks in self.tick_buffer.items():
    #         if not ticks: continue
    #         self._agg_ticks(symbol, ticks)
    #         ticks.clear()

    # def _finalize_backfill(self):
    #     self._process_tick_buffer()
    #     self.live_timer.start()
    
    # def _live_tick(self):
    #     self._process_tick_buffer()

    # def _agg_ticks(self, symbol, ticks:list):
    #     ticks.sort(key=lambda t:t.ptr)
    #     bar_time_end = (self.orderflow[symbol][-1].time + pd.Timedelta(minutes=1)) if self.orderflow[symbol] else None
    #     for t in ticks:
    #         minute_changed = not bar_time_end or t.time >= bar_time_end
    #         last:Bar = None
    #         if minute_changed:
    #             # if bar_time_end:
    #             #     self.producer.pub_snap(symbol, self.orderflow[symbol][-1])
    #             bar_time = t.time.replace(second=0)
    #             bar_time_end = bar_time + pd.Timedelta(minutes=1)
    #             self.producer.lastest_ptr[symbol] = t.ptr
    #             tmp = Bar(bar_time, t.price, t.price, t.price, t.price, t.qty)
    #             self.orderflow[symbol].append(tmp)
    #             last = self.orderflow[symbol][-1]
    #         else: 
    #             last = self.orderflow[symbol][-1]
    #             if t.price>last.high:
    #                 last.high=t.price
    #             elif t.price<last.low:
    #                 last.low=t.price
    #             last.close=t.price
    #             last.vol+=t.qty

    #         last.delta_hlc[-1] += t.qty*t.side #close delta
    #         last.trades_delta += t.side
    #         # if t.side: # 1 aggb, -1 aggs, 0 neutral
    #         last.price_map[t.price][t.side] += t.qty 
                
    #         if last.delta_hlc[-1]>last.delta_hlc[0]:
    #             last.delta_hlc[0]=last.delta_hlc[-1]
    #         elif last.delta_hlc[-1]<last.delta_hlc[1]:
    #             last.delta_hlc[1]=last.delta_hlc[-1]

    #     self.last_ptr[symbol] = ticks[-1].ptr+1

    #     self.producer.set_snap(symbol, self.orderflow[symbol])
    #     if len(self.orderflow[symbol])>1: #store only needed no intraday
    #         self.producer.push_bars(symbol, self.orderflow[symbol][:-1]) #shallowcopy
    #     self.orderflow[symbol]= [last]
    #     # snapshot?

    def _EOD(self):
        print("------DM MARKET CLOSED------")
        self.live_timer.stop()
        self.producer.sync_ptr()
        # for symbol, bars in self.orderflow.items():
        #     if bars:
        #         self.producer.lastest_ptr[symbol] = self.last_ptr[symbol]
        #         self.producer.push_bars(symbol, bars)
        # self.orderflow.clear()

    def fetch_ptr(self, stocklist:list[str]):
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
        if sMinute%15:
            return
        if sSecond==0:
            msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
            print(msg)
        elif (sHour, sMinute)==(8, 30):
            for symbol in self.ptr.keys():
                # if self.orderflow[symbol]:
                #     print(f'ERROR orderflow data not pushed. {symbol}')
                #     self._EOD()
                if symbol.isdigit():
                    self.ptr[symbol] = 0
                    self.producer.lastest_ptr[symbol] = 0
                    self.last_ptr[symbol] = 0

    @Slot()
    def reset_ptr(self):
        if self.orderflow:
            print(f'ERROR orderflow data not pushed. {self.orderflow}')
            self._EOD()
        if self.ptr:
            self.ptr.clear()
            self.last_ptr.clear()
            print('Reset DM data')

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        d,t,o,h,l,c,v = bstrData.split(',')[2:]
        ts = pd.Timestamp(f"{d} {t}", tzinfo=self._tz)
    def OnKLineComplete(self, bstrEndString):
        pass

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTime, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        #TODO rust based
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, _ = self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr.get(symbol,0):
            return
        # if not self.live_timer.isActive() and not self.backfill_timer.isActive():
        self.live_timer.start()
        self.idle_timer.start()
        mid_price = (nAsk+nBid)/2
        side = 0
        if mid_price:
            if nClose > mid_price:
                side = 1
            elif nClose < mid_price:
                side = -1

        # --- FAST EPOCH CREATION ---
        base_epoch = self._epoch_cache_us.get(nDate)
        if base_epoch is None:
            y = nDate//10000
            m = (nDate%10000)//100
            d = nDate%100
            local_midnight = datetime(y, m, d, tzinfo=self._tz)
            base_epoch = int(local_midnight.timestamp()) * 1_000_000
            self._epoch_cache_us[nDate] = base_epoch
        hour = nTime // 10000
        minute = (nTime % 10000) // 100
        second = nTime % 100
        micros_offset = (hour * 3600 + minute * 60 + second) * 1_000_000 + (nTimemillismicros)
        utc_epoch_micros = base_epoch + micros_offset

        tick = Tick(ptr=nPtr, time=utc_epoch_micros, side=side, price=nClose, qty=nQty)
        self.producer.xadd_tick(symbol, tick)

        self.producer.lastest_ptr[symbol]=nPtr
        # self.producer.pub_ticks(symbol, tick)
        # self.tick_buffer[symbol].append(tick)

        
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTime, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        #TODO rust based
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, _ = self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex]= symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr.get(symbol,0):
            return
        # self.live_timer.stop()
        mid_price = (nAsk+nBid)/2
        side = 0
        if mid_price:
            if nClose > mid_price:
                side = 1
            elif nClose < mid_price:
                side = -1

        # --- FAST EPOCH CREATION ---
        base_epoch = self._epoch_cache_us.get(nDate)
        if base_epoch is None:
            y = nDate//10000
            m = (nDate%10000)//100
            d = nDate%100
            local_midnight = datetime(y, m, d, tzinfo=self._tz)
            base_epoch = int(local_midnight.timestamp()) * 1_000_000
            self._epoch_cache_us[nDate] = base_epoch
        hour = nTime // 10000
        minute = (nTime % 10000) // 100
        second = nTime % 100
        micros_offset = (hour * 3600 + minute * 60 + second) * 1_000_000 + (nTimemillismicros)
        utc_epoch_micros = base_epoch + micros_offset

        tick = Tick(ptr=nPtr, time=utc_epoch_micros, side=side, price=nClose, qty=nQty)
        self.producer.xadd_tick(symbol, tick)

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
        self.producer.pub_depth(symbol, self.market_dep[symbol])
        
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
        scale = pSKStock.sDecimal
        data={'C':pSKStock.nClose,
            'O':pSKStock.nOpen,
            'H':pSKStock.nHigh,
            'L':pSKStock.nLow,
            'Vol':pSKStock.nTQty,'YVol':pSKStock.nYQty,'Ref':pSKStock.nRef,'OI':pSKStock.nFutureOI, 'ID':pSKStock.bstrStockNo, "aggBuy":pSKStock.nTBc,"aggSell":pSKStock.nTAc}
        # self.signals.quote_update.emit(pSKStock.bstrStockNo, data, 'DM')
        self.producer.pub_quote(pSKStock.bstrStockNo, data)

    def cleanup(self):
        self.backfill_timer.stop()
        self.live_timer.stop()
        self.idle_timer.stop()

class DomesticQuote(QThread):
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()
        self.symlist = {'TX00', 'MTX00', '2330', '2454', '2317', '2308'}
        self.quotelist = self.symlist.union(['TSEA','OTCA'])

    @Slot()
    def run(self):
        comtypes.CoInitializeEx(comtypes.COINIT_APARTMENTTHREADED)
        try:
            self.skQ = cc.CreateObject(sk.SKQuoteLib, interface=sk.ISKQuoteLib)
            self.SKQuoteEvent = SKQuoteLibEvent(self.skC, self.skQ, self.async_worker)
            
            # ptr = ctypes.cast(self.skQ, ctypes.c_void_p).value
            # print("python addr:", self.skQ, "rs:",ptr)
            # register_sink(ptr)
            self.SKQuoteEvent.reconn.connect(self.conn_wrap)
            self.SKQuoteLibEventHandler = cc.GetEvents(self.skQ, self.SKQuoteEvent)
        finally:
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
        pg=-1

        # for i in range(0,len(symbols),100):
        tmp = ','.join(self.quotelist)
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
        
        lst = [] #get strike price of  
        _, nCode = self.skQ.SKQuoteLib_RequestStocks(lst)

    def fetch_options(self):
        nCode = self.skQ.SKQuoteLib_RequestStockList(3)

    def requestKlines(self, stockNo: str):
        Klines = np.empty((0, 6))
        # min(volume)
        now = datetime.now()
        since = now - pd.Timedelta(days=30)
        nCode = self.skQ.SKQuoteLib_RequestKLineAMByDate(stockNo,0,1,0,since.strftime("%Y%m%d"),now.strftime("%Y%m%d"),3)
        msg = "【RequestKLine】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)        

    def _pubkline(self):
        global Klines

        return
        
    @Slot(str,str,bytes)
    def request_ticker(self, pattern, channel, data):
        #fetch candlestick, subscribe quote, tick
        try:
            symbol = data.decode()
        except UnicodeDecodeError:
            pass
        if symbol not in self.quotelist:
            self.symlist.add(symbol)
            self.quotelist.add(symbol)
            self.requestKlines(symbol)
            self.subquote(symbol)

    @Slot()
    def stop(self):
        comtypes.CoUninitialize()
        if hasattr(self,'SKQuoteEvent'):
            self.SKQuoteEvent.cleanup()
            print("Domestic exit.")

