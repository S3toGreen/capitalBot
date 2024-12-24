import pandas as pd
# import pandas_ta as ta
import datetime
import time
from PyQt6.QtCore import *
import comtypes.client as cc
# comtypes.client.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import talib
import numpy as np

class SignalManager(QObject):
    log_sig = pyqtSignal(str)
    data_sig = pyqtSignal(str, int)
    dc_sig = pyqtSignal()
    order_sig = pyqtSignal()

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
accinfo = []
tmpK = np.empty((0,8))  # "Open","High","Low","Close","Volume"
status=-1               # download is 1, dc is 2, ready is 3
ID = ""

class SKReplyLibEvent(QObject):
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

class SKQuoteLibEvent(QObject):
    def __init__(self, skC):
        super().__init__()
        self.signals = SignalManager.get_instance()
        self.skC = skC

    def OnConnection(self, nKind, nCode): 
        global status
        status = nKind-3000
        msg = "【OnConnection】" + self.skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        if status==2 or status==33:
            time.sleep(300)
            self.signals.dc_sig.emit()
        #     print("done")

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        global tmpK,df
        if sSecond or sMinute%5!=0:
            return
        if len(tmpK)==1 and (tmpK[0][-2:]!=[sMinute//15,sHour]).any():
            # update K and TA
            tmp = pd.DataFrame(np.delete(tmpK,[-2,-1],1),columns=df.reset_index().columns).set_index("Time")
            tmpK = np.empty((0,8))
            df = pd.concat([df, tmp[~tmp.index.isin(df.index)]]).sort_index()
            self.signals.order_sig.emit()
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        global tmpK, df
        if nSimulate:
            return
        nClose=nClose/100
        nTimehms=str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        idx = [int(nTimehms[2:])//1500, int(nTimehms[:2])]  #0~1459 idx:1   idx:2 1500~2959   idx:3 3000~4459   idx:0 4500~5959  
        
        # ToDo put this in other place
        if len(tmpK)>1:
            # update K and TA
            tmp = pd.DataFrame(np.delete(tmpK[:-1],[-2,-1],1),columns=df.reset_index().columns).set_index("Time")
            tmpK = tmpK[-1:]
            df = pd.concat([df, tmp[~tmp.index.isin(df.index)]]).sort_index()
            self.signals.order_sig.emit()

        if not tmpK.size or (tmpK[-1][-2:]!=idx).any():
            m = (idx[0]+1)%4*15
            h = idx[1] if m else (idx[1]+1)%24
            tmp = [datetime.datetime.strptime(f"{nDate}",'%Y%m%d').replace(hour=h,minute=m),
                   nClose,nClose,nClose,nClose,nQty, idx[0], idx[1]]
            if idx[1]==23 and not m:
                tmp[0] += datetime.timedelta(days=1)
            tmpK = np.vstack([tmpK, tmp]) # Date and time
        else: 
            tmpK[-1][2]=max(tmpK[-1][2], nClose)
            tmpK[-1][3]=min(tmpK[-1][3], nClose)
            tmpK[-1][4]=nClose
            tmpK[-1][5]+=nQty

        side = 0
        if nClose>=nAsk:
            side = 1
            # print("\x1b[1;92m",end="")
        elif nClose<=nBid:
            side = -1
            # print("\x1b[1;91m",end="")
        self.signals.data_sig.emit(f"【Tick】Time:{time} Bid:{(nBid/100)} Ask:{(nAsk/100)} Strike:{(nClose)} Qty:{nQty}", side)
    
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        global tmpK
        if nSimulate:
            return
        # Recreate 15 mins Klines
        nClose = nClose/100
        nTimehms = str(f'{nTimehms:06}')
        idx = [int(nTimehms[2:])//1500, int(nTimehms[:2])]  #0~1459 idx:1   idx:2 1500~2959   idx:3 3000~4459   idx:0 4500~5959 
        
        if not tmpK.size or (tmpK[-1][-2:]!=idx).any():
            m = (idx[0]+1)%4*15 
            h = idx[1] if m else (idx[1]+1)%24
            tmp = [datetime.datetime.strptime(f"{nDate}",'%Y%m%d').replace(hour=h,minute=m),
                   nClose,nClose,nClose,nClose,nQty, idx[0], idx[1]]
            if idx[1]==23 and not m:
                tmp[0] += datetime.timedelta(days=1)
            tmpK = np.vstack([tmpK, tmp]) # Date and time
        else: 
            tmpK[-1][2]=max(tmpK[-1][2], nClose)
            tmpK[-1][3]=min(tmpK[-1][3], nClose)
            tmpK[-1][4]=nClose
            tmpK[-1][5]+=nQty

class SKOrderLibEvent(QObject):
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
        if bstrData[0]=='#':
            return
        msg = "【OnOpenInterest】" + bstrData
        print(msg) 
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



class TradingBot(QObject): 
    # skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
    # SKOrderEvent = SKOrderLibEvent(cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib))
    # SKOrderLibEventHandler = cc.GetEvents(skO, SKOrderEvent)
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
        # self.skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
        # SKOrderEvent = SKOrderLibEvent(self.skC)
        # self.SKOrderLibEventHandler = cc.GetEvents(self.skO, SKOrderEvent)

        self.skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
        SKQuoteEvent = SKQuoteLibEvent(self.skC)
        self.SKQuoteEventHandler = cc.GetEvents(self.skQ, SKQuoteEvent)

        self.signals.dc_sig.connect(self.init)
        self.signals.order_sig.connect(self.placeOrder)
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
        # This function should recreate Klines and basic TA such as EMA and MACD
        # ToDo vwap refresh at 9 AM less than 9 means yesterday greater means today
        # if current time<9 we need history K 9~14 and history T 15~now
        # else if (9~23) two cases <15 histoty K 9~14 and 
        # EMA 78,176 MACD(75,111,9) 
        global df, acclist
        self.order_init()  
        self.quoteConnect()

        df = self.requestKlines("TX00")
        # ToDo separate strategy environment 3m, 15m 
        self.subtick("TX00")

        # self.skO.GetStopLossReport(ID,acclist["TF"],0)
        # ma1 = print(talib.EMA(df["Close"], 78))
        # ma2 = print(talib.EMA(df["Close"], 176))

        # print(talib.MACD(df["Close"],))
        # self.placeOrder(0,0)

        print("Data initialization finished!")

    def quoteConnect(self):
        global status
        self.signals.dc_sig.disconnect(self.init)

        while status!=3:
            nCode = self.skQ.SKQuoteLib_EnterMonitorLONG()
            msg = "【Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            self.signals.log_sig.emit("Downloading Data...")
            cnt=0
            while status<=2:
                cnt+=1
                time.sleep(0.1)
                if cnt>300:
                    self.signals.log_sig.emit("Time Out! Failed to connect.")
                    self.quoteDC()
                    time.sleep(6)
                    break
        self.signals.dc_sig.connect(self.init)            
        
            
    def subtick(self, stockNo: str):
        # 8 o'clock
        if status!=3:
            self.quoteConnect()
        psPageNo, nCode = self.skQ.SKQuoteLib_RequestTicks(-1, stockNo)
        msg = "【SubscribeTicks】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        try:
            while True:
                time.sleep(0.3)
                if nCode==0:
                    break
        except KeyboardInterrupt:            
            raise
        # tmp=pd.DataFrame(tickdata,columns=["Time","Bid","Ask","Strike","Qty"])
        # tmp.to_csv(f"{stockNo}_Tick.csv")
        # if subscribe multiple product data process needs separate from index 

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
        try:
            while True:
                time.sleep(0.3)
                if nCode==0:
                    break
        except KeyboardInterrupt:            
            raise
        df = pd.DataFrame(Klines,columns=["Time","Open","High","Low","Close","Volume"]).set_index("Time").astype({"Open":float,"High":float,"Low":float,"Close":float})
        df.index = pd.to_datetime(df.index)
        # df['EMA_78'] = df['Close'].ewm(span=78,adjust=False).mean().round().astype(int)
        # print(df)
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
        global ID, acclist
        nCode = self.skO.GetOpenInterestGW(ID,acclist["TF"],1)
        msg="【GetOpenInterest】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
        
        nCode = self.skO.GetFutureRights(ID,acclist["TF"],1)
        msg="【GetFutureRight】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        print(msg)

    def placeOrder(self):
        global df, accinfo
        print(df)
        ema1 = talib.EMA(df["Close"], 35)[-2:].values 
        ema2 = talib.EMA(df['Close'], 225)[-2:].values 
        macd, macd_sig, macd_hist = talib.MACD(df["Close"], 75, 90, 9)#75,90 80,95
        macd_hist = macd_hist[-2:].values
        print(ema1)
        print(ema2)
        print(macd_hist)

        side=qty=-1
        if ema1[0]>ema2[0] and ema1[1]<ema2[1]:
            # close and short
            qty= 2 if accinfo[0]!=accinfo[1] else 1
            side = 1
        elif ema1[0]<ema2[0] and ema1[1]>ema2[1]:
            # close and long
            qty= 2 if accinfo[0]!=accinfo[1] else 1
            side = 0            
        elif macd_hist[0]<0 and macd_hist[1]>0:
            if (accinfo[0]!=accinfo[1] and ema1[1]<ema2[1]):
                # close short
                qty = 1
                side = 0
            elif (accinfo[0]==accinfo[1] and ema1[1]>ema2[1]):
                # open long
                qty=1
                side = 0
        elif macd_hist[0]>0 and macd_hist[1]<0:
            if (accinfo[0]!=accinfo[1] and ema1[1]>ema2[1]):
                # close long
                qty = 1
                side = 1
            elif (accinfo[0]==accinfo[1] and ema1[1]<ema2[1]):
                # open short
                qty = 1
                side = 1

        self.getInfo()

        if side<0 or qty<0:
            return
        
        self.signals.log_sig.emit(f"order triggered!")
        if int(accinfo[0])<20000:
            self.signals.log_sig.emit("Insufficient funds.")
            return
        
        order = sk.FUTUREORDER()
        order.bstrFullAccount = acclist["TF"]
        # TM0000
        order.bstrStockNo = "MTX00"
        # 0:ROD  1:IOC  2:FOK
        order.sTradeType = 1
        # 0: buy 1: sell
        order.sBuySell = side
        order.sDayTrade = 0
        # 0: open position 1: close position
        order.sNewClose = 2
        order.bstrPrice = "P"
        order.nQty = qty
        order.sReserved = 0

        self.skO.SendFutureOrderCLR(ID, 1, order)

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
