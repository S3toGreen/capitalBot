import pandas as pd
# import pandas_ta as ta
import datetime, time
from PySide6.QtCore import *
import comtypes.client as cc
# cc.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import numpy as np
from sortedcontainers import SortedDict



class SignalManager(QObject):
    log_sig = Signal(str)
    data_sig = Signal(str, int)
    dc_sig = Signal()
    order_sig = Signal()
    close_all_sig = Signal()
    vp_update_sig = Signal(int,int,int,bool,bool)

    _instance = None
    
    @staticmethod
    def get_instance():
        if SignalManager._instance is None:
            SignalManager._instance = SignalManager()
        return SignalManager._instance
    
    def __init__(self):
        if SignalManager._instance is not None:
            raise Exception("This class is a singleton!")
        super().__init__()

Klines = np.empty((0, 6)) 
acclist = {}
accinfo = [] # future right info
position = []
footprint = np.empty((0, 2),dtype=object)  #{Timestamp:{price:[aggbuy, aggsell]}} or [[timestamp,{price:aggbuy, aggsell}]
# sl=0
tmpK = np.empty((0,9),dtype=object)#np.pad(np.loadtxt("TX_Ticktmp.csv",delimiter=',', dtype=object),((0,0),(0,1)),constant_values=-1)  # daily 1 min klines
status=[-1,-1]   # download is 1, dc is 2, ready is 3
ID = ""

class SKReplyLibEvent():
    def __init__(self):
        super().__init__()
        self.singals = SignalManager.get_instance()

    def OnReplyMessage(self, bstrUserID, bstrMessages):
        # time.sleep(15)
        nConfirmCode = -1
        msg = "【Announcement】" + bstrMessages
        self.singals.log_sig.emit(msg)
        return nConfirmCode 

class SKCenterLibEvent():
    def OnShowAgreement(self, bstrData):
        msg = "【OnShowAgreement】" + bstrData

class SKQuoteLibEvent():
    def __init__(self, skC, skQ):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC
        self.skQ = skQ

    def OnConnection(self, nKind, nCode): 
        global status
        status[0] = nKind-3000
        msg = "【OnConnection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status[0]==2 or status[0]==33:
            self.signals.dc_sig.emit()
        #     print("done")

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        global tmpK
        tf=15
        if not sSecond:
            if not sMinute:
                np.savetxt("TX_Ticktmp.csv", tmpK[:, :-1], delimiter=',', fmt='%s', header='Time,Open,High,Low,Close,Volume,aggBuy,aggSell')
        if sSecond:
            return
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        # msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        global tmpK, footprint
        if nSimulate:
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

        # StopLoss
        if position and sl:
            if position[-1][1]=='S' and nClose>=sl:
                self.signals.close_all_sig.emit()
            elif position[-1][1]=='B' and nClose<=sl:
                self.signals.close_all_sig.emit()

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
    
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        # ToDo: change to batch update for better performance
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
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= self.skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)
        msg= ("【OnNotifyQuoteLONG】" + "商品代碼" + str(pSKStock.bstrStockNo) + " 名稱" + str(pSKStock.bstrStockName) + " 開盤價" + str(pSKStock.nOpen / 100) + " 成交價" + str(pSKStock.nClose / 100) + " 最高" + str(pSKStock.nHigh / 100) + " 最低" + str(pSKStock.nLow / 100) + " 買盤量" + str(pSKStock.nTBc) + " 賣盤量" + str(pSKStock.nTAc) + " 總量" + str(pSKStock.nTQty) + " 昨收" + str(pSKStock.nRef / 100) + " 昨量" + str(pSKStock.nYQty) + " 買價" + str(pSKStock.nBid/100) + " 買量" + str(pSKStock.nBc) + " 賣價" + str(pSKStock.nAsk/100) + " 賣量" + str(pSKStock.nAc) +" OI"+str(pSKStock.nFutureOI))
        # print(msg)

class SKOrderLibEvent():
    def __init__(self, skC) -> None:
        super().__init__()
        self.singals = SignalManager.get_instance()
        self.skC = skC
    def OnAccount(self, bstrLogInID, bstrAccountData):
        global acclist, ID
        msg = "【OnAccount】"
        values = bstrAccountData.split(',')
        ID = bstrLogInID
        acclist.setdefault(values[0], values[1]+values[3])
    def OnProxyStatus(self, bstrUserId, nCode):
        msg = "【OnProxyStatus】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.singals.log_sig.emit(msg)
    # 新版期貨智慧單(包含停損單、移動停損、二擇一、觸價單)被動回報查詢。透過呼叫GetStopLossReport後，資訊由該事件回傳。
    def OnStopLossReport(self, bstrData):
        if bstrData[0]=='#':
            return
        msg = "【StrategyReport】" + bstrData
        print(msg)
    # 非同步委託結果。
    def OnAsyncOrder(self, nThreadID, nCode, bstrMessage):
        msg = "【OnAsyncOrder】" + str(nThreadID)+ ", "+ str(nCode) +", "+ bstrMessage
        print(msg)
    def OnOpenInterest(self, bstrData):
        global position
        if bstrData[0]=='#':
            return
        data = bstrData.split(',')[2:7]
        if not data:
            return
        position.append(data)
        print(position)

    # 國內期貨權益數。透過呼叫 GetFutureRights 後，資訊由該事件回傳
    def OnFutureRights(self, bstrData):
        global accinfo
        if bstrData[0]=='#':
            return
        info = bstrData.split(',')
        msg = f"【FutureRights】Value: ${info[0]} PnL: ${info[1]} Available: ${info[31]}"
        print(msg)
        accinfo = [info[0], info[31]]
    def OnStrategyData(self, bstrUserID, bstrData):
        print("Strategy Data: ",bstrData.split(','))

class TickReceiver(QObject): 
    def __init__(self, *args,**kwargs):
        super().__init__()
        
        self.skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)
        SKReplyEvent = SKReplyLibEvent()
        self.SKReplyLibEventHandler = cc.GetEvents(self.skR, SKReplyEvent)
        
        self.skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
        SKCenterEvent = SKCenterLibEvent()
        self.SKCenterEventHandler = cc.GetEvents(self.skC, SKCenterEvent)

        self.skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
        SKOrderEvent = SKOrderLibEvent(self.skC)
        self.SKOrderLibEventHandler = cc.GetEvents(self.skO, SKOrderEvent)

        self.signals = SignalManager.get_instance()
        nCode = self.skC.SKCenterLib_SetLogPath("CapitalLog")

        if nCode:
            msg = "【SetLogPath】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            
        self.debug_state = False
        self.update_debug()

    def run(self):
        global ID

        self.skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
        SKQuoteEvent = SKQuoteLibEvent(self.skC, self.skQ)
        self.SKQuoteEventHandler = cc.GetEvents(self.skQ, SKQuoteEvent)

        # self.skOSQ = cc.CreateObject(SKOSQuoteLibEvent,interface=sk.ISKOSQuoteLib)
        # SKOSQuoteEvent = SKOSQuoteLibEvent(self.skC,self.skOSQ)
        # self.SKQuoteEventHandler = cc.GetEvents(self.skOSQ, SKOSQuoteEvent)

        self.signals.dc_sig.connect(self.init)
        self.signals.order_sig.connect(self.placeOrder)
        self.signals.close_all_sig.connect(self.close_all)
        self.init()

    def login(self, acc, passwd):
        global ID
        nCode = self.skC.SKCenterLib_Login(acc, passwd)
        msg = "【Login】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        if nCode==0:
            ID=acc
        self.signals.log_sig.emit(msg)
        return nCode
    
    def init(self):
        global df, acclist, tmpK, footprint
        self.quoteConnect()
        self.order_init()  

        df = self.requestKlines("TX00")
        self.subtick("TX00")
        # ToDo separate strategy environment 3m, 15m 

        # self.skO.GetStopLossReport(ID,acclist["TF"],0)

        # ncode = self.skQ.SKQuoteLib_RequestStockList(3)
        # print(self.skC.SKCenterLib_GetReturnCodeMessage(ncode))

        # call option:A~L, put option:M~X
        # e.g: 202501  call:TXO{strikePrice}A5
        #      202603  put:TXO{strikePrice}O6

        # self.skQ.SKQuoteLib_GetStrikePrices()
        # ncode = self.skQ.SKQuoteLib_RequestFutureTradeInfo(-1,"TXO22000O5")
        # ncode = self.skQ.SKQuoteLib_RequestStocks(-1,"TX00,TXO22000O5")
        
        if tmpK.size:
            t = np.flip(tmpK, axis=0) 
            _, j = np.unique(t[:,0],return_index=True)
            tmpK = t[j]

            t = np.flip(footprint, axis=0)
            _, j = np.unique(t[:,0],return_index=True)
            footprint = t[j]
            
        print("Data initialization finished!")

    def quoteConnect(self):
        global status
        self.signals.dc_sig.disconnect(self.init)
        while status[0]!=3:
            nCode = self.skQ.SKQuoteLib_EnterMonitorLONG()
            msg = "【Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            # self.signals.log_sig.emit("Downloading Data...")
            cnt=0
            while status[0]<=2:
                cnt+=1
                time.sleep(0.1)
                if cnt>150:
                    self.quoteDC()
                    self.signals.log_sig.emit("Time Out! Failed to connect. Retry after 45s")
                    time.sleep(45)
                    break
        self.signals.dc_sig.connect(self.init)            
        
            
    def subtick(self, stockNo: str):
        # 8 o'clock
        if status!=3:
            self.quoteConnect()
        psPageNo, nCode = self.skQ.SKQuoteLib_RequestTicks(-1, stockNo)
        msg = "【SubscribeTicks】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        # try:
        #     while True:
        #         time.sleep(0.3)
        #         if nCode==0:
        #             break
        # except KeyboardInterrupt:
        #     raise

    def requestKlines(self, stockNo: str):
        global Klines
        Klines = np.empty((0, 6))

        # 4k data per call
        if status!=3:
            self.quoteConnect()
        now = datetime.datetime.now()
        since = now - pd.Timedelta(days=75)
        nCode = self.skQ.SKQuoteLib_RequestKLineAMByDate(stockNo,0,1,0,since.strftime("%Y%m%d"),now.strftime("%Y%m%d"),15)
        msg = "【RequestKLine】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)        
        # try:
        #     while True:
        #         time.sleep(0.3)
        #         if nCode==0:
        #             break
        # except KeyboardInterrupt:            
        #     raise
        df = pd.DataFrame(Klines,columns=["Time","Open","High","Low","Close","Volume"]).set_index("Time").astype(float).astype(int)
        df.index = pd.to_datetime(df.index)

        df.to_csv(f"{stockNo}.csv")
        return df.tail(900)
        
    def order_init(self):
        global ID
        nCode = self.skO.SKOrderLib_Initialize()
        msg = "【OrderLib_Init】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        
        nCode = self.skO.ReadCertByID(ID)
        msg = "【ReadCertByID】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        # nCode = self.skO.SKOrderLib_InitialProxyByID(ID)
        # msg = "【Proxy_init】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)

        nCode = self.skO.GetUserAccount()
        msg = "【GetUserAccount】"+ self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        nCode = self.skO.SKOrderLib_GetSpeedyType(ID)
        if nCode == 0:
            msg = "一般線路"
        else:
            msg = "Speedy線路"
        msg = "【OrderLib_SpeedyType】" + msg
        self.signals.log_sig.emit(msg)  

        self.getInfo()
        # nCode = self.skO.SKOrderLib_TelnetTest()
        # msg = "【OrderLib_TelnetTest】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
    
    def getInfo(self):
        global ID, acclist, position
        position=[]
        nCode = self.skO.GetOpenInterestGW(ID,acclist["TF"],1)
        # msg="【GetOpenInterest】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
        
        nCode = self.skO.GetFutureRights(ID,acclist["TF"],1)
        # msg="【GetFutureRight】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # print(msg)

    def placeOrder(self):
        global df, accinfo, sl, position, tmpK
        tmp = pd.DataFrame(tmpK[:-1, :-1],columns=["Time","Open","High","Low","Close","Volume","aggBuy","aggSell"]).set_index("Time").resample('15min',closed='right',label='right').agg({
            'Open':'first',
            'High':'max',
            'Low':'min',
            'Close':'last',
            'Volume':'sum',
            'aggBuy': 'sum',
            'aggSell': 'sum',
        }).dropna()
        df = pd.concat([df[~df.index.isin(tmp.index)], tmp],join='inner').sort_index()

        print(df.tail(3))
        typical_price = (tmpK[:,2]+tmpK[:,3]+tmpK[:,4])/3
        vwap = np.average(typical_price,weights=tmpK[:,5])

        print("VWAP:",vwap)
        self.getInfo()

        print(f'{"-"*60}')

        if int(accinfo[0])<20000:
            self.signals.log_sig.emit("Insufficient funds!")
            return
        
        return
        vola = False        # side way filter

        side=qty=-1
        if ma1[0]>=ma2[0] and ma1[1]<ma2[1]:
            # close and short
            qty=0
            if position[-1][1]=='B': #close
                qty+=1   
                sl=0
                side = 1
            if vola and macd_hist[1]<0:
                qty+=1
                sl = ma1[-1]
                side = 1
        elif ma1[0]<=ma2[0] and ma1[1]>ma2[1]:
            # close and long
            qty=0
            if position[-1][1]=='S':
                qty+=1  #close
                sl=0
                side = 0
            if vola and macd_hist[1]>0:
                qty+=1
                sl = ma1[-1]
                side = 0            
        elif macd_hist[0]<0.15 and macd_hist[1]>0.15:
            if (sl and ma1[1]<ma2[1]):
                # close short
                qty = 1
                side = 0
                sl=0
            elif not sl and ma1[1]>ma2[1] and macd[-1]<low_percentile:
                # open long
                qty=1
                side = 0
                sl=ma1[-1]
        elif macd_hist[0]>-0.3 and macd_hist[1]<-0.3:
            if (sl and ma1[1]>ma2[1]):
                # close long
                qty = 1
                side = 1
                sl=0
            elif not sl and ma1[1]<ma2[1] and macd[-1]>high_percentile:
                # open short
                qty = 1
                side = 1
                sl=ma1[-1]

        if side<0 or qty<0:
            print(f"Debug: {side}, {qty}")
            self.getInfo()
            return
        
        order = sk.FUTUREORDER()
        order.bstrFullAccount = acclist["TF"]
        # TM0000
        order.bstrStockNo = "TM0000" #"MTX00"
        # 0:ROD  1:IOC  2:FOK
        order.sTradeType = 1
        # 0: buy 1: sell
        order.sBuySell = side
        order.sDayTrade = 0
        # 0:open 1:close 2:auto
        order.sNewClose = 2
        order.bstrPrice = "P"
        order.nQty = qty
        order.sReserved = 0

        self.skO.SendFutureOrderCLR(ID, 1, order)
        self.signals.log_sig.emit(f"Order Sent!")
        self.getInfo()


    def close_all(self):
        if not position:
            print("No postition to close!(Bug)")
            return
        global sl
        if position[-1][1]=='S':
            side = 0
        else:
            side = 1
        sl = 0
        order = sk.FUTUREORDER()
        order.bstrFullAccount = acclist["TF"]
        # TM0000
        order.bstrStockNo = "TM0000" #"MTX00"
        # 0:ROD  1:IOC  2:FOK
        order.sTradeType = 1
        # 0: buy 1: sell
        order.sBuySell = side
        order.sDayTrade = 0
        # 0:open 1:close 2:auto
        order.sNewClose = 2
        order.bstrPrice = "P"
        order.nQty = 1
        order.sReserved = 0

        self.skO.SendFutureOrderCLR(ID, 1, order)

        self.signals.log_sig.emit(f"Stop Loss Order Sent!")

    def update_debug(self):
        nCode = self.skC.SKCenterLib_Debug(self.debug_state)
        if nCode:
            msg = "【SetDebug】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

    def quoteDC(self):
        global status
        nCode = self.skQ.SKQuoteLib_LeaveMonitor()
        msg = "【Quote_DC】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

    def saveData(self):
        global tmpK,footprint
        if tmpK.size:
            print("Saving min data...")
            
            t1=pd.read_parquet("TXfp.pq")
            t=np.flip(footprint,axis=0)
            _, j = np.unique(t[:,0],return_index=True)
            footprint=t[j]
            t = []
            filter = []
            for ts, price in footprint:
                for p,c in reversed(price.items()):
                    t.append({'Time':ts,'Price':int(p),'aggBuy':c[0],'aggSell':c[1]})
                    filter.append(ts)
            pd.concat([t1[~t1.index.get_level_values('Time').isin(filter)],pd.DataFrame(t).set_index(['Time','Price'])]).to_parquet('TXfp.pq')
            
            t1 = pd.read_csv("TX_Tick.csv",parse_dates=['Time'], dtype=np.int32)
            t = np.flip(tmpK, axis=0) 
            _, j = np.unique(t[:,0],return_index=True)
            tmpK = t[j]
            pd.concat([t1[~t1['Time'].isin(tmpK[:,0])], pd.DataFrame(tmpK[:,:-1],columns=t1.columns)]).set_index('Time').to_csv("TX_Tick.csv")
