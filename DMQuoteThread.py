import numpy as np
import pandas as pd
from SignalManager import SignalManager
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal
import asyncio, datetime
from redisworker.Tick_Producer import TickProducer
from redisworker.Async_Worker import AsyncRedisWorker
from collections import defaultdict
from sortedcontainers import SortedDict
from listDM import quotelist

class DefaultSortedDict(SortedDict):
    def __init__(self, default_factory, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default_factory = default_factory

    def __getitem__(self, key):
        if key not in self:
            self[key] = self.default_factory()
        return super().__getitem__(key)

class SKQuoteLibEvent(QObject):
    reconn = Signal()
    def __init__(self, skC, skQ, redis_worker, market='DM'):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skQ = skQ
        self.stockid = defaultdict(str)
        self.ptr = defaultdict(int)
        self.status = -1 # download is 1, dc is 2, ready is 3
        self.redis_worker = redis_worker
        self.producer= asyncio.run_coroutine_threadsafe(TickProducer.create(market),self.redis_worker.loop).result()
        self.orderflow = defaultdict(lambda: np.empty((0,9),dtype=object))
        self.range = defaultdict(lambda: np.empty((0,8),dtype=object))
        # TODO batch process history tick
        # self.ticks_buffer = defaultdict(lambda: np.empty((0,4),dtype=np.int32))

    def fetch_ptr(self, stocklist:list[str]):
        self.ptr = asyncio.run_coroutine_threadsafe(self.producer.get_ptr(stocklist), self.redis_worker.loop).result()
        print(self.ptr)

    def sync_ptr(self):
        asyncio.run_coroutine_threadsafe(self.producer.update_lastest_ptr(),self.redis_worker.loop).result()

    def OnConnection(self, nKind, nCode): 
        status = nKind-3000
        msg = "【Connection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status in [2,33]:
            # remove unclosed
            for i in self.orderflow.keys():
                self.orderflow[i]=self.orderflow[i][:-1]
            self.reconn.emit()
            self.sync_ptr()
        self.status = status
        #     print("done")

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        # print(self.orderflow)
        if sSecond:
            return

        for i in list(self.orderflow.keys()):
            if self.orderflow[i].size>1 or self.orderflow[i][-1][0].minute==sMinute:
                self.redis_worker.submit(self.producer.push_bars(i, self.orderflow[i].copy()))
                del self.orderflow[i]
                self.producer.lastest_ptr[i]=self.ptr[i]+1
                print("push from servertime")

        if sMinute%15:
            if sHour==14:
                # reset at the end of trading day
                for i in self.ptr.keys():
                    self.ptr[i]=0
                    self.producer.lastest_ptr[i]=0
            return

        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        # msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        if not (symbol:=self.stockid[nIndex])=='':
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex] = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr[symbol]:
            return
        self.ptr[symbol] = nPtr

        #TODO last minute
        nClose=nClose//100
        nBid=nBid//100
        nAsk=nAsk//100
        nTimehms=str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        side = 1 if nClose>=nAsk else (-1 if nClose<=nBid else 0) 

        self.redis_worker.submit(self.producer.push_tick(symbol,nClose,nQty,time,side,nPtr))
        self.signals.data_sig.emit(f"【Tick】Time:{time} Bid:{nBid} Ask:{nAsk} Strike:{(nClose)} Qty:{nQty}", side)
        # sync unclosed
        idx = int(nTimehms[2:-2])
        if not self.orderflow[symbol].size or self.orderflow[symbol][-1][0].minute!=(idx+1)%60:
            # TODO unclosed bar propagate to consumer process 
            if self.orderflow[symbol].size:
                self.redis_worker.submit(self.producer.push_bars(symbol,self.orderflow[symbol].copy()))
                self.producer.lastest_ptr[symbol] = nPtr
            self.orderflow[symbol]=np.array([[pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=int(nTimehms[:2]),minute=idx)+pd.Timedelta(hours=-8,minutes=1),
                   nClose,nClose,nClose,nClose,nQty, 0, 0, DefaultSortedDict(lambda: [0,0,0])]],dtype=object)
        else:
            #closed and push
            self.orderflow[symbol][-1][2]=max(self.orderflow[symbol][-1][2], nClose)
            self.orderflow[symbol][-1][3]=min(self.orderflow[symbol][-1][3], nClose)
            self.orderflow[symbol][-1][4]=nClose
            self.orderflow[symbol][-1][5]+=nQty
            # self.redis_worker.submit(self.producer.push_bars(symbol,self.orderflow[symbol]))
        # print('close:',self.orderflow[symbol])
        # self.redis_worker.submit(self.producer.pub_sym(symbol, self.orderflow[symbol][-1]))

        self.orderflow[symbol][-1][-1][nClose][0]+=nQty
        if side>0:
            self.orderflow[symbol][-1][-3]+=nQty
            self.orderflow[symbol][-1][-2]+=1
            self.orderflow[symbol][-1][-1][nClose][1]+=nQty
            self.orderflow[symbol][-1][-1][nClose][2]+=1
        elif side<0:
            self.orderflow[symbol][-1][-3]-=nQty
            self.orderflow[symbol][-1][-2]-=1
            self.orderflow[symbol][-1][-1][nClose][1]-=nQty
            self.orderflow[symbol][-1][-1][nClose][2]-=1

    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        # TODO: batch process history data, minute chart & range chart
        if not (symbol:=self.stockid[nIndex]):
            pSKStock = sk.SKSTOCKLONG()
            pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
            self.stockid[nIndex]= symbol = pSKStock.bstrStockNo
        if nSimulate or nPtr<self.ptr[symbol]:
            return

        nClose=nClose//100
        nBid=nBid//100
        nAsk=nAsk//100
        nTimehms=str(f'{nTimehms:06}')
        # time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        side = 1 if nClose>=nAsk else (-1 if nClose<=nBid else 0)
        # self.redis_worker.submit(self.producer.push_tick(self.stockid[nIndex],nClose,nQty,time,side,nPtr))
#2359 0000
        idx = int(nTimehms[2:-2])
        if not self.orderflow[symbol].size or self.orderflow[symbol][-1][0].minute!=(idx+1)%60:
            self.producer.lastest_ptr[symbol] = nPtr
            tmp = [pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=int(nTimehms[:2]),minute=idx)+pd.Timedelta(hours=-8 ,minutes=1),
                   nClose,nClose,nClose,nClose,nQty, 0, 0, DefaultSortedDict(lambda: [0,0,0])]
            self.orderflow[symbol]=np.vstack([self.orderflow[symbol], tmp])
        else: 
            self.orderflow[symbol][-1][2]=max(self.orderflow[symbol][-1][2], nClose)
            self.orderflow[symbol][-1][3]=min(self.orderflow[symbol][-1][3], nClose)
            self.orderflow[symbol][-1][4]=nClose
            self.orderflow[symbol][-1][5]+=nQty

        self.orderflow[symbol][-1][-1][nClose][0]+=nQty
        if side>0:
            self.orderflow[symbol][-1][-3]+=nQty
            self.orderflow[symbol][-1][-2]+=1
            self.orderflow[symbol][-1][-1][nClose][1]+=nQty
            self.orderflow[symbol][-1][-1][nClose][2]+=1
        elif side<0:
            self.orderflow[symbol][-1][-3]-=nQty
            self.orderflow[symbol][-1][-2]-=1
            self.orderflow[symbol][-1][-1][nClose][1]-=nQty
            self.orderflow[symbol][-1][-1][nClose][2]-=1

    def OnNotifyBest5Long(self, sMarketNo, nStockidx, nBestBid1, nBestBidQty1, nBestBid2, nBestBidQty2, nBestBid3, nBestBidQty3, nBestBid4, nBestBidQty4, nBestBid5, nBestBidQty5, nExtendBid, nExtendBidQty, nBestAsk1, nBestAskQty1, nBestAsk2, nBestAskQty2, nBestAsk3, nBestAskQty3, nBestAsk4, nBestAskQty4, nBestAsk5, nBestAskQty5, nExtendAsk, nExtendAskQty, nSimulate):
        if nSimulate:
            return
        
    def OnNotifyCommodityListWithTypeNo(self, sMarketNo, bstrCommodityData):
        msg = "【OnNotifyCommodityListWithTypeNo】" + str(sMarketNo) + "_" + bstrCommodityData
        print(msg)

    def OnNotifyStrikePrices(self,bstrOptionData):
        data = bstrOptionData.split(',')
        if "AM" in data[2] or data[0]!='TXO':
            return
        print(data)
    
    def OnNotifyFutureTradeInfoLONG(self, bstrStockNo, sMarketNo, nStockidx, nBuyTotalCount, nSellTotalCount, nBuyTotalQty, nSellTotalQty, nBuyDealTotalCount, nSellDealTotalCount):
        msg = ("【OnNotifyFutureTradeInfoLONG】" + " 總委託買進筆數" + str(nBuyTotalCount)+ 
        " 總委託賣出筆數" + str(nSellTotalCount)+ 
        " 總委託買進口數" + str(nBuyTotalQty)+ 
        " 總委託賣出口數" + str(nSellTotalQty)+ 
        " 總成交買進筆數" + str(nBuyDealTotalCount) +
        " 總成交賣出筆數" + str(nSellDealTotalCount))
        # print(msg)

    def OnNotifyQuoteLONG(self, sMarketNo, nIndex):
        global i
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
        msg= ("【OnNotifyQuoteLONG】" + "商品代碼" + str(pSKStock.bstrStockNo) + " 名稱" + str(pSKStock.bstrStockName) + " 開盤價" + str(pSKStock.nOpen / 100) + " 成交價" + str(pSKStock.nClose / 100) + " 最高" + str(pSKStock.nHigh / 100) + " 最低" + str(pSKStock.nLow / 100) + " 買盤量" + str(pSKStock.nTBc) + " 賣盤量" + str(pSKStock.nTAc) + " 總量" + str(pSKStock.nTQty) + " 昨收" + str(pSKStock.nRef / 100) + " 昨量" + str(pSKStock.nYQty) + " 買價" + str(pSKStock.nBid/100) + " 買量" + str(pSKStock.nBc) + " 賣價" + str(pSKStock.nAsk/100) + " 賣量" + str(pSKStock.nAc) +" OI"+str(pSKStock.nFutureOI))
        print(msg)
#         if pSKStock.nClose<10000 and pSKStock.nClose>1500:
#             if pSKStock.bstrStockNo not in i:
#                 i.append(str(pSKStock.bstrStockNo))
#                 print(i)
# i=[]
class DomesticQuote(QObject):
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()

    def run(self):
        self.redis_worker = AsyncRedisWorker()
        self.skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
        self.SKQuoteEvent = SKQuoteLibEvent(self.skC, self.skQ, self.redis_worker)
        self.SKQuoteLibEventHandler = cc.GetEvents(self.skQ, self.SKQuoteEvent)
        self.SKQuoteEvent.reconn.connect(self.conn_wrap)
    
        self.timer = QTimer()
        self.timer.setInterval(1500)  # check every 1.5s
        self.timer.timeout.connect(self.check_connection_status)

        self.conn_wrap()

    def conn_wrap(self):
        self.SKQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.quoteConnect()

    def quoteConnect(self):
        nCode = self.skQ.SKQuoteLib_EnterMonitorLONG()
        msg = "【Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        self.retry_count = 0
        self.timer.start()
            # self.signals.log_sig.emit("Downloading Data...")

    def check_connection_status(self):
        self.retry_count += 1
        if self.SKQuoteEvent.status == 3:
            self.timer.stop()
            self.SKQuoteEvent.reconn.connect(self.conn_wrap)
            self.init()
        elif self.retry_count >= 10:
            self.timer.stop()
            self.quoteDC()  
            msg = "Time Out! Failed to connect. Retry after 30s"
            self.signals.log_sig.emit(msg)
            QTimer.singleShot(30000, self.quoteConnect)  # retry after 30s   

    def init(self):
        ticklist = ['TX00']
        self.SKQuoteEvent.fetch_ptr(ticklist)
        self.subtick(ticklist)
        self.subquote(quotelist)

        pass

    def quoteDC(self):
        nCode = self.skQ.SKQuoteLib_LeaveMonitor()
        msg = "【Quote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    def subtick(self, stocklist: list[str]):
        for stockNo in stocklist:
            psPageNo, nCode = self.skQ.SKQuoteLib_RequestTicks(-1, stockNo)
            msg = "【SubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def subquote(self, stocklist: list[str]):
        pg = -1
        # from listDM import unfilter
        # _, nCode = self.skQ.SKQuoteLib_RequestStocks(pg, ','.join(unfilter[200:]))

        for i in range(0,len(stocklist),100):
            _, nCode = self.skQ.SKQuoteLib_RequestStocks(pg, ','.join(stocklist[i:i+100]))

            msg = "【SubQuote】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)


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

        df.to_csv(f"{stockNo}.csv")
        return df.tail(900)
    
    def cleanup(self):
        # asyncio.run_coroutine_threadsafe(self.SKQuoteEvent.producer.update_lastest_ptr(),self.SKQuoteEvent.redis_worker.loop).result()
        if hasattr(self,'SKQuoteEvent'):
            self.SKQuoteEvent.sync_ptr()
            self.redis_worker.stop()
        print("Domestic exit.")

