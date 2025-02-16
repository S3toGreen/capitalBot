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
    close_all_sig = pyqtSignal()

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
vwap=[0, 0] # normal vwap & potential big order vwap (minute wise or order wise), anchor with 8:45, 13:45
priceVol = {}
position = []
sl=0
tmpK = np.empty((0,9),dtype=object)#np.pad(np.loadtxt("TX_Ticktmp.csv",delimiter=',', dtype=object),((0,0),(0,1)),constant_values=-1)  # daily 1 min klines
status=-1   # download is 1, dc is 2, ready is 3
ID = ""

#多空差額 大單買賣力(Accumulate) 筆數, volum profile 
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
            self.signals.dc_sig.emit()
        #     print("done")

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        global tmpK,df
        tf=15
        if sSecond or sMinute%5!=0:
            return
        elif sMinute%tf==0:
            # if build 15 min from here sMinute=15 0~14, 30 15~29, 45 30~44, 0 45~59 
            if tmpK.size and tmpK[-1][-1]//tf==(sMinute-1)%60//tf:
                tmpK[-1][-1]=(tmpK[-1][-1]+15)%60
                np.savetxt("TX_Ticktmp.csv", tmpK[:, :-1], delimiter=',', fmt='%s', header='Time,Open,High,Low,Close,Volume,LongQty,ShortQty')
                tmp = pd.DataFrame(tmpK[:, :-1],columns=["Time","Open","High","Low","Close","Volume","LongQty","ShortQty"]).set_index("Time").resample('15min',closed='right',label='right').agg({
                    'Open':'first',
                    'High':'max',
                    'Low' :'min',
                    'Close':'last',
                    'Volume':'sum',
                    'LongQty': 'sum',
                    'ShortQty': 'sum',
                }).dropna()
                df = pd.concat([df[~df.index.isin(tmp.index)], tmp],join="inner").sort_index()
                print(tmpK[-1])
                self.signals.order_sig.emit()

        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)

    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        # msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines = np.vstack([Klines, data])

    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        global tmpK, df
        if nSimulate:
            return
        # 主動買賣多空比(大戶,散戶)
        nClose=np.int32(nClose//100)
        nBid=nBid//100
        nAsk=nAsk//100
        nTimehms=str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        idx = [int(nTimehms[2:])//100, int(nTimehms[:2])]

        if tmpK.size==0 or tmpK[-1][-1]!=idx[0]:
            m = (idx[0]+1)%60 
            h = idx[1] if m else (idx[1]+1)%24
            tmp = [pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=h,minute=m),
                   nClose,nClose,nClose,nClose,np.int32(nQty), np.int32(0), np.int32(0), idx[0]]
            if idx[1]==23 and not m:
                tmp[0] += pd.Timedelta(days=1)
            tmpK = np.vstack([tmpK, tmp]) # Date and time
            print(tmpK[-2][0].time(), tmpK[-2][1:-1])

            if tmpK[-2][-1]//15!=idx[0]//15:
                np.savetxt("TX_Ticktmp.csv", tmpK[:-1, :-1], delimiter=',', fmt='%s', header='Time,Open,High,Low,Close,Volume,LongQty,ShortQty')
                tmp = pd.DataFrame(tmpK[:-1, :-1],columns=["Time","Open","High","Low","Close","Volume","LongQty","ShortQty"]).set_index("Time").resample('15min',closed='right',label='right').agg({
                    'Open':'first',
                    'High':'max',
                    'Low':'min',
                    'Close':'last',
                    'Volume':'sum',
                    'LongQty': 'sum',
                    'ShortQty': 'sum',
                }).dropna()
                df = pd.concat([df[~df.index.isin(tmp.index)], tmp],join='inner').sort_index()
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
        if nClose>=nAsk:
            side = 1
            tmpK[-1][-3]+=nQty #long
            # print("\x1b[1;92m",end="")
        elif nClose<=nBid:
            side = -1
            tmpK[-1][-2]+=nQty #short
            # print("\x1b[1;91m",end="")
        self.signals.data_sig.emit(f"【Tick】Time:{time} Bid:{nBid} Ask:{nAsk} Strike:{(nClose)} Qty:{nQty}", side)
    
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        global tmpK
        if nSimulate:
            return
        # 主動買賣多空比(大戶,散戶)
        nClose = np.int32(nClose//100)
        nBid = nBid//100
        nAsk = nAsk//100
        nTimehms = str(f'{nTimehms:06}')
        idx = [int(nTimehms[2:])//100, int(nTimehms[:2])] 
        
        if tmpK.size==0 or tmpK[-1][-1]!=idx[0]:
            m = (idx[0]+1)%60
            h = idx[1] if m else (idx[1]+1)%24
            tmp = [pd.to_datetime(f"{nDate}",format='%Y%m%d').replace(hour=h,minute=m),
                   nClose,nClose,nClose,nClose,np.int32(nQty), np.int32(0), np.int32(0), idx[0]]
            if idx[1]==23 and not m:
                tmp[0] += datetime.timedelta(days=1)
            tmpK = np.vstack([tmpK, tmp]) # Date and time
        else: 
            tmpK[-1][2]=max(tmpK[-1][2], nClose)
            tmpK[-1][3]=min(tmpK[-1][3], nClose)
            tmpK[-1][4]=nClose
            tmpK[-1][5]+=nQty

        side=0
        if nClose>=nAsk:
            tmpK[-1][-3]+=nQty
            # print("\x1b[1;92m",end="")
        elif nClose<=nBid:
            tmpK[-1][-2]+=nQty
            # print("\x1b[1;91m",end="")
        # self.signals.data_sig.emit(f"【Tick】Time:{time} Bid:{(nBid/100)} Ask:{(nAsk/100)} Strike:{(nClose)} Qty:{nQty}", side)


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
        global df, acclist, tmpK
        self.quoteConnect()
        self.order_init()  

        df = self.requestKlines("TX00")
        self.subtick("TX00")
        # ToDo separate strategy environment 3m, 15m 

        # self.skO.GetStopLossReport(ID,acclist["TF"],0)

        # print(talib.MACD(df["Close"],))
        if tmpK.size:
            t = np.flip(tmpK, axis=0) 
            _, j = np.unique(t[:,0],return_index=True)
            tmpK = t[j]
        print("Data initialization finished!")

    def quoteConnect(self):
        global status
        self.signals.dc_sig.disconnect(self.init)
        while status!=3:
            nCode = self.skQ.SKQuoteLib_EnterMonitorLONG()
            msg = "【Quote_Connect】" + self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            # self.signals.log_sig.emit("Downloading Data...")
            cnt=0
            while status<=2:
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
        global tmpK
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
        msg="【GetOpenInterest】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)
        
        nCode = self.skO.GetFutureRights(ID,acclist["TF"],1)
        msg="【GetFutureRight】"+self.skC.SKCenterLib_GetReturnCodeMessage(nCode)
        print(msg)

    def placeOrder(self):
        global df, accinfo, sl, position
        print(df.tail(6))
        ma1 = talib.KAMA(df["Close"], 70)[-2:].values 
        ma2 = talib.EMA(df['Close'], 195)[-2:].values 
        macd, macd_sig, macd_hist = talib.MACD(df["Close"], 55, 150, 6)
        macd_hist = macd_hist[-2:].values
        macd = macd[-2:].values
        high_percentile = pd.Series(macd[-150:]).quantile(.7)
        low_percentile = pd.Series(macd[-150:]).quantile(.3)
        print(ma1)
        print(ma2)
        print(macd_hist)
        if position:
            atr = talib.ATR(df['High'],df['Low'],df['Close'],30).iloc[-1]
            if position[-1][1]=='S':
                sl = min(sl or 999999, df.iloc[-1]['Close']+6*atr)
            else:
                sl = max(sl or 0, df.iloc[-1]['Close']-6*atr)
            sl = int(sl)
            print("StopLoss:", sl)


        if int(accinfo[0])<20000:
            self.signals.log_sig.emit("Insufficient funds.")
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
        global tmpK
        if tmpK.size:
            print("Saving min data...")
            t1 = pd.read_csv("TX_Tick.csv",parse_dates=['Time'], dtype=np.int32)

            t = np.flip(tmpK, axis=0) 
            _, j = np.unique(t[:,0],return_index=True)
            tmpK = t[j]
            pd.concat([t1[~t1['Time'].isin(tmpK[:,0])], pd.DataFrame(tmpK[:,:-1],columns=t1.columns)]).set_index('Time').to_csv("TX_Tick.csv")
