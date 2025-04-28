import numpy as np
import pandas as pd
from SignalManager import SignalManager
from sortedcontainers import SortedDict
import comtypes.client as cc
import comtypes.gen.SKCOMLib as sk
from PySide6.QtCore import QObject, QTimer, Signal
import redis

class TickProducer:
    def __init__(self, market):
        self.redis = redis.Redis(host='localhost', decode_responses=True)
        self.market = market
    def push_tick(self, symbol, price, volume, ts, side):
        tick_data = {
            'sym': symbol,
            'ts': ts,
            'p': price,
            'Qty': volume,
            's': side
        }
        key = f"Tick:{self.market}"
        self.redis.xadd(key, tick_data, maxlen=15000, approximate=True)
    def expireTS(self):
        exTS = pd.Timestamp.today().replace(hour=14,minute=45)
        now = pd.Timestamp.today()
        exTS += pd.Timedelta(days=1) if now> exTS else 0
        return int(exTS.timestamp())
    def update_lastest_ptr(self,lastest_ptr):
        key = f'last_ptr:{self.market}'
        self.redis.hmset(key,lastest_ptr)
        self.redis.expireat(key,self.expireTS())
    def get_ptr(self, symbols:list[str]):
        key=f"last_ptr:{self.market}"
        t = self.redis.hmget(key, symbols)
        return {symbol:int(ptr) if ptr is not None else -1 for symbol, ptr in zip(symbols,t)}
    
class SKOSQuoteLibEvent(QObject):
    status_changed = Signal(int)
    reconn = Signal()
    def __init__(self, skC, skOSQ):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skOSQ = skOSQ
        self.producer = TickProducer('OS')

    def OnConnect(self, nKind, nCode): 
        status = nKind-3000
        msg = "【OS_Connection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status!=1:
            self.reconn.emit()
        self.status_changed.emit(status)
        #     print("done")

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        # msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnNotifyTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        b5 = sk.SKBEST5_9()
        self.skOSQ.SKOSQuoteLib_GetBest5NineDigitLONG(nIndex, b5)
        nTimehms=str(f'{nTime:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        pSKTick = sk.SKFOREIGNTICK_9()
        pSKTick, nCode = self.skOSQ.SKOSQuoteLib_GetTickNineDigitLONG(nIndex, nPtr, pSKTick)
        msg = "【OnNotifyTicksNineDigitLONG】" + " 成交時間" + str(pSKTick.nTime) + " 成交價" + str(pSKTick.nClose) + " 成交量" + str(pSKTick.nQty) + " 成交日期YYYYMMDD" + str(pSKTick.nDate) 

        msg = '【Ticks】'+'Time:'+time+" Bid:"+str(b5.nBid1)+" Ask:"+str(b5.nAsk1)+' Strike:'+ str(nClose)+' Qty:'+ str(nQty)
        print(msg)
        return

        nClose=np.int32(nClose//100)
        nBid=nBid//100
        nAsk=nAsk//100
        nTimehms=str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        idx = [int(nTimehms[2:])//100, int(nTimehms[:2])]
        sep=False

        if tmpK.size==0 or tmpK[-1][-1]!=idx[0]:
            tmp = [pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=idx[1],minute=idx[0]),
                   nClose,nClose,nClose,nClose,np.int32(nQty), np.int32(0), np.int32(0), idx[0]]
            tmp[0] += pd.Timedelta(minutes=1)
            # print(tmpK[-1][0].time(), tmpK[-1][1:-1].tolist())
            print(tmpK[-1,0].time(), *tmpK[-1,1:-1],'\n',footprint[-1,1],'\n')
            tmpK = np.vstack([tmpK, tmp]) 
            footprint = np.vstack([footprint,[tmp[0],SortedDict({})]])
            sep=True
            self.signals.order_sig.emit()
        else: 
            tmpK[-1][2]=max(tmpK[-1][2], nClose)
            tmpK[-1][3]=min(tmpK[-1][3], nClose)
            tmpK[-1][4]=nClose
            tmpK[-1][5]+=nQty

        side = 0
        p = int(nClose)
        if nClose>=nAsk: #aggressive buy
            side = 1
            tmpK[-1][-3]+=nQty 
            self.signals.vp_update_sig.emit(nClose,0,nQty,sep,1)
            if p in footprint[-1][-1]:
                footprint[-1][-1][p][0]+=nQty
            else:
                footprint[-1][-1][p]=[nQty,0]
            # print("\x1b[1;92m",end="")
        elif nClose<=nBid: #aggressive sell
            side = -1
            tmpK[-1][-2]+=nQty
            self.signals.vp_update_sig.emit(nClose,1,nQty,sep,1)
            if p in footprint[-1][-1]:
                footprint[-1][-1][p][1]+=nQty
            else:
                footprint[-1][-1][p]=[0,nQty]
            # print("\x1b[1;91m",end="")
        self.signals.data_sig.emit(f"【Tick】Time:{time} Bid:{nBid} Ask:{nAsk} Strike:{(nClose)} Qty:{nQty}", side)

    def OnNotifyHistoryTicksNineDigitLONG(self, nIndex, nPtr, nDate, nTime, nClose, nQty):
        # ToDo: change to batch update for better performance
        # pSKTick = sk.SKFOREIGNTICK_9()
        # pSKTick, nCode = self.skOSQ.SKOSQuoteLib_GetTickNineDigitLONG(nIndex, nPtr, pSKTick)
        # msg = "【OnNotifyTicksNineDigitLONG】" + " 成交時間" + str(pSKTick.nTime) + " 成交價" + str(pSKTick.nClose) + " 成交量" + str(pSKTick.nQty) + " 成交日期:" + str(pSKTick.nDate) 
        # msg = '【HistoryTicks】', nDate, nTime, nClose, nQty,nPtr
        # print(msg)
        return
        global tmpK, footprint
        if nSimulate:
            return
        nClose=np.int32(nClose//100)
        nBid=nBid//100
        nAsk=nAsk//100
        nTimehms=str(f'{nTimehms:06}')
        idx = [int(nTimehms[2:])//100, int(nTimehms[:2])]
        sep=False

        if tmpK.size==0 or tmpK[-1][-1]!=idx[0]:
            tmp = [pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=idx[1],minute=idx[0]),
                   nClose,nClose,nClose,nClose,np.int32(nQty), np.int32(0), np.int32(0), idx[0]]
            tmp[0] += pd.Timedelta(minutes=1)
            tmpK = np.vstack([tmpK, tmp])
            footprint = np.vstack([footprint,[tmp[0], SortedDict({})]])
            sep=True
        else:
            tmpK[-1][2]=max(tmpK[-1][2], nClose)
            tmpK[-1][3]=min(tmpK[-1][3], nClose)
            tmpK[-1][4]=nClose
            tmpK[-1][5]+=nQty

        p=int(nClose)
        if nClose>=nAsk: #aggressive buy
            tmpK[-1][-3]+=nQty
            self.signals.vp_update_sig.emit(nClose,0,nQty,sep,0)
            if p in footprint[-1][-1]:
                footprint[-1][-1][p][0]+=nQty
            else:
                footprint[-1][-1][p]=[nQty,0]
            # print("\x1b[1;92m",end="")
        elif nClose<=nBid: #aggressive sell
            tmpK[-1][-2]+=nQty
            self.signals.vp_update_sig.emit(nClose,1,nQty,sep,0)
            if p in footprint[-1][-1]:
                footprint[-1][-1][p][1]+=nQty
            else:
                footprint[-1][-1][p]=[0,nQty]
            # print("\x1b[1;91m",end="")

    # def OnNotifyBest5NineDigitLONG(self, nStockidx, nBestBid1, nBestBidQty1, nBestBid2, nBestBidQty2, nBestBid3, nBestBidQty3, nBestBid4, nBestBidQty4, nBestBid5, nBestBidQty5, nBestAsk1, nBestAskQty1, nBestAsk2, nBestAskQty2, nBestAsk3, nBestAskQty3, nBestAsk4, nBestAskQty4, nBestAsk5, nBestAskQty5):
    #     pSKBest5 = sk.SKBEST5_9()
    #     pSKTick, nCode = self.skOSQ.SKOSQuoteLib_GetBest5NineDigitLONG(nStockidx, pSKBest5)
    #     msg = "【OnNotifyBest5NineDigitLONG】" + " 1買量" + str(pSKBest5.nBidQty1) + " 1買價" + str(pSKBest5.nBid1 / 100) + " 1賣價" + str(pSKBest5.nAsk1 / 100) + " 1賣量" + str(pSKBest5.nAskQty1) + " 2買量" + str(pSKBest5.nBidQty2) + " 2買價" + str(pSKBest5.nBid2 / 100) + " 2賣價" + str(pSKBest5.nAsk2 / 100) + " 2賣量" + str(pSKBest5.nAskQty2) + " 3買量" + str(pSKBest5.nBidQty3) + " 3買價" + str(pSKBest5.nBid3 / 100) + " 3賣價" + str(pSKBest5.nAsk3 / 100) + " 3賣量" + str(pSKBest5.nAskQty3) + " 4買量" + str(pSKBest5.nBidQty4) + " 4買價" + str(pSKBest5.nBid4 / 100) + " 4賣價" + str(pSKBest5.nAsk4 / 100) + " 4賣量" + str(pSKBest5.nAskQty4) + " 5買量" + str(pSKBest5.nBidQty5) + " 5買價" + str(pSKBest5.nBid5 / 100) + " 5賣價" + str(pSKBest5.nAsk5 / 100) + " 5賣量" + str(pSKBest5.nAskQty5)
    #     print(msg)

    def OnNotifyBest10NineDigitLONG(self, nStockidx, nBestBid1, nBestBidQty1, nBestBid2, nBestBidQty2, nBestBid3, nBestBidQty3, nBestBid4, nBestBidQty4, nBestBid5, nBestBidQty5, nBestBid6, nBestBidQty6, nBestBid7, nBestBidQty7, nBestBid8, nBestBidQty8, nBestBid9, nBestBidQty9, nBestBid10, nBestBidQty10, nBestAsk1, nBestAskQty1, nBestAsk2, nBestAskQty2, nBestAsk3, nBestAskQty3, nBestAsk4, nBestAskQty4, nBestAsk5, nBestAskQty5, nBestAsk6, nBestAskQty6, nBestAsk7, nBestAskQty7, nBestAsk8, nBestAskQty8, nBestAsk9, nBestAskQty9, nBestAsk10, nBestAskQty10):
        msg = "【OnNotifyBest10NineDigitLONG】" + " 1買量" + str(nBestBidQty1) + " 1買價" + str(nBestBid1 / 100) + " 1賣價" + str(nBestAsk1 / 100) + " 1賣量" + str(nBestAskQty1) + " 2買量" + str(nBestBidQty2) + " 2買價" + str(nBestBid2 / 100) + " 2賣價" + str(nBestAsk2 / 100) + " 2賣量" + str(nBestAskQty2) + " 3買量" + str(nBestBidQty3) + " 3買價" + str(nBestBid3 / 100) + " 3賣價" + str(nBestAsk3 / 100) + " 3賣量" + str(nBestAskQty3) + " 4買量" + str(nBestBidQty4) + " 4買價" + str(nBestBid4 / 100) + " 4賣價" + str(nBestAsk4 / 100) + " 4賣量" + str(nBestAskQty4) + " 5買量" + str(nBestBidQty5) + " 5買價" + str(nBestBid5 / 100) + " 5賣價" + str(nBestAsk5 / 100) + " 5賣量" + str(nBestAskQty5) + " 6買量" + str(nBestBidQty6) + " 6買價" + str(nBestBid6 / 100) + " 6賣價" + str(nBestAsk6 / 100) + " 6賣量" + str(nBestAskQty6) + " 7買量" + str(nBestBidQty7) + " 7買價" + str(nBestBid7 / 100) + " 7賣價" + str(nBestAsk7 / 100) + " 7賣量" + str(nBestAskQty7) + " 8買量" + str(nBestBidQty8) + " 8買價" + str(nBestBid8 / 100) + " 8賣價" + str(nBestAsk8 / 100) + " 8賣量" + str(nBestAskQty8) + " 9買量" + str(nBestBidQty9) + " 9買價" + str(nBestBid9 / 100) + " 9賣價" + str(nBestAsk9 / 100) + " 9賣量" + str(nBestAskQty9) + " 10買量" + str(nBestBidQty10) + " 10買價" + str(nBestBid10 / 100) + " 10賣價" + str(nBestAsk10 / 100) + " 10賣量" + str(nBestAskQty10)
        # print(msg)
        
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
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
        msg= ("【OnNotifyQuoteLONG】" + "商品代碼" + str(pSKStock.bstrStockNo) + " 名稱" + str(pSKStock.bstrStockName) + " 開盤價" + str(pSKStock.nOpen / 100) + " 成交價" + str(pSKStock.nClose / 100) + " 最高" + str(pSKStock.nHigh / 100) + " 最低" + str(pSKStock.nLow / 100) + " 買盤量" + str(pSKStock.nTBc) + " 賣盤量" + str(pSKStock.nTAc) + " 總量" + str(pSKStock.nTQty) + " 昨收" + str(pSKStock.nRef / 100) + " 昨量" + str(pSKStock.nYQty) + " 買價" + str(pSKStock.nBid/100) + " 買量" + str(pSKStock.nBc) + " 賣價" + str(pSKStock.nAsk/100) + " 賣量" + str(pSKStock.nAc) +" OI"+str(pSKStock.nFutureOI))
        # print(msg)

class OverseaQuote(QObject):
    status = -1 # download is 1, ready is 3
    def __init__(self, skC):
        super().__init__()
        self.skC = skC
        self.retry_count = 0
        self.signals = SignalManager.get_instance()

    def run(self):
        self.skOSQ = cc.CreateObject(sk.SKOSQuoteLib,interface=sk.ISKOSQuoteLib)
        self.SKOSQuoteEvent = SKOSQuoteLibEvent(self.skC, self.skOSQ)
        self.SKOSQuoteLibEventHandler = cc.GetEvents(self.skOSQ, self.SKOSQuoteEvent)

        self.SKOSQuoteEvent.status_changed.connect(self.update_status)
        self.SKOSQuoteEvent.reconn.connect(self.conn_wrap)

        self.timer = QTimer()
        self.timer.setInterval(30000)  # check every 1.5s
        self.timer.timeout.connect(self.check_connection_status)

        self.conn_wrap()
    def conn_wrap(self):
        self.SKOSQuoteEvent.reconn.disconnect(self.conn_wrap)
        self.quoteConnect()

    def init(self):
        self.subtick('CBOT,YM0000')

    def quoteConnect(self):
        nCode = self.skOSQ.SKOSQuoteLib_EnterMonitorLONG()
        msg = "【OS_Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        self.retry_count = 0
        self.timer.start()
            # self.signals.log_sig.emit("Downloading Data...")

    def update_status(self, status):
        self.status = status

    def check_connection_status(self):
        self.retry_count += 1
        if self.status == 1:
            self.timer.stop()
            self.SKOSQuoteEvent.reconn.connect(self.conn_wrap)
            self.init()
        elif self.retry_count >= 10:
            self.timer.stop()
            self.quoteDC()  # you must define this
            msg = "Time Out! Failed to connect. Retry after 30s"
            self.signals.log_sig.emit(msg)
            QTimer.singleShot(30000, self.quoteConnect)  # retry after 45s   

    def quoteDC(self):
        nCode = self.skOSQ.SKOSQuoteLib_LeaveMonitor()
        msg = "【OSQuote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    #CBOT,YM0000, CME,NQ0000, CME,ES0000
    def subtick(self, stockNo: str):
        pn,nCode = self.skOSQ.SKOSQuoteLib_RequestLiveTick(-1, stockNo)
        msg = "【OS_SubTick】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        print(msg)
        pn, nCode = self.skOSQ.SKOSQuoteLib_RequestMarketDepth(-1, stockNo)
        msg = "【MarketDepth】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        print(msg)

    def cleanup(self):
        print("OverSea exit.")
