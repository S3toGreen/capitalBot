import numpy as np
import pandas as pd
from SignalManager import SignalManager
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal, Slot
import asyncio, datetime
from redisworker.Producer import DataProducer
from redisworker.AsyncWorker import AsyncWorker
from collections import defaultdict
from sortedcontainers import SortedDict
from numba import njit
import pythoncom
from quote.tools import Bar, Tick

@njit(cache=True, fastmath=True)
def update_depth(dep, args):
    for i in range(10):
        dep[0, i][0] = args[i * 2]/100        # bid price
        dep[0, i][1] = args[i * 2 + 1]        # bid qty
        dep[1, i][0] = args[20 + i * 2]/100   # ask price
        dep[1, i][1] = args[20 + i * 2 + 1]   # ask qty

class SKOSQuoteLibEvent(QObject):
    reconn = Signal()
    def __init__(self, skC, skOSQ, worker, market='OS'):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skOSQ = skOSQ
        self.producer = DataProducer.create(market, worker)
        self.signals.OS_reset.connect(self.reset_ptr)

        self.stockid = {}
        self.ptr = {}
        self.last_ptr = {}
        self.status = -1 
        self.tick_buffer = defaultdict(list)
        self.orderflow = defaultdict(list)
        self.market_dep = defaultdict(lambda: np.zeros((2,10),dtype=[('p', 'f4'), ('q', 'u2')]))

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
        self._tz = 'America/Chicago'

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
        for t in ticks:
            bar_time = t.time.floor('min') #+ pd.Timedelta(minutes=1) # start of minute
            minute_changed = not self.orderflow[symbol] or self.orderflow[symbol][-1].time != bar_time
            last:Bar = None
            if minute_changed:
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
        print("------OS MARKET CLOSED------")

    def fetch_ptr(self, stocklist:list[str]):
        if not self.ptr:
            keys = [i.split(',')[1] for i in stocklist]
            self.ptr = self.producer.get_ptr(keys)

    def OnConnect(self, nKind, nCode): 
        status = nKind-3000
        msg = "【OSConnection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        # print(msg)
        if status in [2,33] and self.status==1:
            if self.last_ptr:
                self.ptr = self.last_ptr.copy()
            self.reconn.emit()
        self.status = status

    @Slot()
    def reset_ptr(self):
        if self.ptr:
            self.ptr=defaultdict(int)
            self.last_ptr.clear()
            self.producer.insert_ticks()
            if self.orderflow:
                print(f'ERROR orderflow data not pushed.\n{self.orderflow}')
            else:
                print('Reset OS data')

    def OnNotifyTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        # store orderflow of NY session only (open at 8:31~15:00) (TODO store also huge volume)
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, _ = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nPtr<self.ptr[symbol]:
            return

        if not self.live_timer.isActive() and not self.backfill_timer.isActive():
            self.live_timer.start()
        self.idle_timer.start()

        bid = self.market_dep[symbol][0,0][0] 
        ask = self.market_dep[symbol][1,0][0]

        mid_price = (ask+bid)/2
        side = 0
        if mid_price:
            if nClose > mid_price:
                side = 1
            elif nClose < mid_price:
                side = -1
        tick = Tick(ptr=nPtr, time=self._to_timestamp(nDate,nTime), side=side, price=nClose/100, qty=nQty)
        self.producer.pub_ticks(symbol, tick)# print(symbol,tick)
        self.tick_buffer[symbol].append(tick)

    def OnNotifyHistoryTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        # cant form orderflow data
        if not (symbol:=self.stockid.get(nIndex)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
            self.stockid[nIndex] = symbol = pSKStock.bstrStockNo
        if nPtr<self.ptr[symbol]:
            return
        
        self.live_timer.stop()
        # self.backfill_symbols.add(symbol)
        tick = Tick(ptr=nPtr, time=self._to_timestamp(nDate,nTime), side=0, price=nClose/100, qty=nQty)
        self.tick_buffer[symbol].append(tick)

        self.backfill_timer.start()
    
    def OnNotifyBest10NineDigitLONG(self, nStockidx, *args):
        if not (symbol:=self.stockid.get(nStockidx)):
            pSKStock = sk.SKFOREIGNLONG()
            pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nStockidx, pSKStock)
            self.stockid[nStockidx] = symbol = pSKStock.bstrStockNo
        update_depth(self.market_dep[symbol],args)

    def OnNotifyQuoteLONG(self, nIndex):
        pSKStock = sk.SKFOREIGNLONG()
        pSKStock, nCode = self.skOSQ.SKOSQuoteLib_GetStockByIndexLONG(nIndex, pSKStock)
        self.stockid[nIndex] = pSKStock.bstrStockNo
        data={'Symbol':pSKStock.bstrStockName,
            'C':pSKStock.nClose/(10**pSKStock.sDecimal),
            'O':pSKStock.nOpen/(10**pSKStock.sDecimal),
            'H':pSKStock.nHigh/(10**pSKStock.sDecimal),
            'L':pSKStock.nLow/(10**pSKStock.sDecimal),
            'Vol':pSKStock.nTQty,'Ref':pSKStock.nRef/(10**pSKStock.sDecimal),'ID':pSKStock.bstrStockNo}
        self.producer.pub_quote(data)

    def OnOverseaProducts(self, bstrValue):
        if bstrValue.split(',')[0]!='CME':
            return
        msg = "【OnOverseaProducts】" + str(bstrValue)
        print(msg)
    def cleanup(self):
        if self.backfill_timer.isActive():
            self.backfill_timer.stop()
        if self.live_timer.isActive():
            self.live_timer.stop()
        if self.idle_timer.isActive():
            self.idle_timer.stop()
        self.producer.insert_ticks()

class OverseaQuote(QObject):
    def __init__(self,skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()
        self.stocklist=['CME,NQ0000', 'CME,ES0000']

    def run(self):
        pythoncom.CoInitialize()
        try:
            # self.skC = cc.CreateObject(sk.SKCenterLib, interface=sk.ISKCenterLib)
            # self.skC.SKCenterLib_LoginSetQuote(self.acc,self.passwd,'Y')
            self.redis_worker = AsyncWorker()
            self.skOSQ = cc.CreateObject(sk.SKOSQuoteLib,interface=sk.ISKOSQuoteLib)
            self.SKOSQuoteEvent = SKOSQuoteLibEvent(self.skC, self.skOSQ, self.redis_worker)
            self.SKOSQuoteLibEventHandler = cc.GetEvents(self.skOSQ, self.SKOSQuoteEvent)
            self.SKOSQuoteEvent.reconn.connect(self.conn_wrap)
        finally:
            pythoncom.CoUninitialize()
            self.timer = QTimer()
            self.timer.setInterval(1500)  # check every 1.5s
            self.timer.timeout.connect(self.check_connection_status)

            self.signals.restart_os.connect(self.quoteDC)
            self.SKOSQuoteEvent.fetch_ptr(self.stocklist)
            self.conn_wrap()

    def init(self):
        #CBOT,YM0000, CME,NQ0000, CME,ES0000
        try:
            self.subquote()
        except Exception as e:
            print(e)
            self.signals.restart_os.emit()
            return
        self.SKOSQuoteEvent.backfill_timer.start()
        self.subtick()

    @Slot()
    def conn_wrap(self):
        self.SKOSQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.signals.restart_os.disconnect(self.quoteDC)
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
            self.signals.restart_os.connect(self.quoteDC)
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
    
    def subtick(self):
        pg=-1
        for stockNo in self.stocklist:
            psPageNo, nCode = self.skOSQ.SKOSQuoteLib_RequestTicks(pg, stockNo)
            msg = "【OSSubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)+'\t'+stockNo
            self.signals.log_sig.emit(msg)
            pg=psPageNo+1

    def subquote(self):
        pg=-1
        for i in range(0,len(self.stocklist),100):
            tmp = '#'.join(self.stocklist[i:i+100])
            psPageNo, nCode = self.skOSQ.SKOSQuoteLib_RequestStocks(pg, tmp)
            if nCode:
                raise RuntimeError('OS failed to subscribe. Restart!')
            msg = "【OSSubQuote】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)+'\t'+tmp
            self.signals.log_sig.emit(msg)
            pg=psPageNo+1

    def stop(self):
        if hasattr(self,'SKOSQuoteEvent'):
            self.SKOSQuoteEvent.cleanup()
            self.redis_worker.stop()
            print("OverSea exit.")
