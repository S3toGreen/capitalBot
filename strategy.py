import comtypes.client as cc
# comtypes.client.GetModule(r'./x64/SKCOM.dll')
import comtypes.gen.SKCOMLib as sk
import pandas as pd
# import pandas_ta as ta
import datetime
import time, pythoncom
from PyQt6.QtCore import *
from PyQt6.QtGui import QTextCharFormat

class SignalManager(QObject):
    log_sig = pyqtSignal(str)
    data_sig = pyqtSignal(str, int)
    dc_sig = pyqtSignal()

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

skC = cc.CreateObject(sk.SKCenterLib,interface=sk.ISKCenterLib)
skO = cc.CreateObject(sk.SKOrderLib,interface=sk.ISKOrderLib)
skQ = cc.CreateObject(sk.SKQuoteLib,interface=sk.ISKQuoteLib)
skR = cc.CreateObject(sk.SKReplyLib,interface=sk.ISKReplyLib)

class SKReplyLibEvent():
    def __init__(self) -> None:
        self.singals = SignalManager.get_instance()

    def OnReplyMessage(self, bstrUserID, bstrMessages):
        nConfirmCode = -1
        msg = "【Announcement】" + bstrMessages
        self.singals.log_sig.emit(msg)
        return nConfirmCode 

class SKCenterLibEvent():
    def OnShowAgreement(self, bstrData):
        msg = "【OnShowAgreement】" + bstrData

class SKQuoteLibEvent():
    def __init__(self) -> None:
        self.signals = SignalManager.get_instance()

    def OnConnection(self, nKind, nCode): 
        global status
        msg = "【OnConnection】" + skC.SKCenterLib_GetReturnCodeMessage(nKind) + "_" + skC.SKCenterLib_GetReturnCodeMessage(nCode) 
        self.signals.log_sig.emit(msg)
        print(msg)
        status = nKind-3000
        if status==2 or status==33:
            time.sleep(6)
            self.signals.dc_sig.emit()

    def OnNotifyServerTime(self, sHour, sMinute, sSecond, nTotal): 
        if sSecond:
            return
        msg = "【ServerTime】" + f"{sHour:02}" + ":" + f"{sMinute:02}" + ":" + f"{sSecond:02}"
        print(msg)
    def OnNotifyKLineData(self, bstrStockNo, bstrData):
        global Klines
        msg = "【OnNotifyKLineData】" + bstrStockNo + "_" + bstrData 
        data = bstrData.split(',')
        Klines.append(data)
    def OnNotifyTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        if nSimulate:
            return
        global df
        nTimehms=str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]
        side = 0
        if nClose>=nAsk:
            side = 1
        elif nClose<=nBid:
            side = -1
        self.signals.data_sig.emit("【Ticks】Time:"+time+" Bid:"+str(int(nBid/100))+" Ask:"+str(int(nAsk/100))+" Strike:"+str(int(nClose/100))+" Qty:"+str(nQty), side)
    def OnNotifyHistoryTicksLONG(self, sMarketNo, nIndex, nPtr, nDate, nTimehms, nTimemillismicros, nBid, nAsk, nClose, nQty, nSimulate):
        if nSimulate:
            return
        # Recreate 15 mins Klines
        global df
        nTimehms = str(f'{nTimehms:06}')
        time = nTimehms[:2]+':'+nTimehms[2:4]+':'+nTimehms[4:]

    def OnNotifyQuoteLONG(self, sMarketNo, nIndex):
        pSKStock = sk.SKSTOCKLONG()
        pSKStock, nCode= skQ.SKQuoteLib_GetStockByIndexLONG(sMarketNo, nIndex, pSKStock)

        if (pSKStock.nBid == skQ.SKQuoteLib_GetMarketPriceTS()):
            nBidValue = "市價"
        else:
            nBidValue = pSKStock.nBid / 100.0

        if (pSKStock.nBid == skQ.SKQuoteLib_GetMarketPriceTS()):
            nAskValue = "市價"
        else:
            nAskValue = pSKStock.nAsk / 100.0

class SKOrderLibEvent():
    def __init__(self) -> None:
        self.singals = SignalManager.get_instance()
    # 帳號資訊
    def OnAccount(self, bstrLogInID, bstrAccountData):
        msg = "【OnAccount】"
        values = bstrAccountData.split(',')
        # broker ID (IB)4碼 + 帳號7碼
        Account = values[0]+": "+values[1] + values[3]
        self.singals.log_sig.emit(msg+Account)
    def OnTelnetTest(self, bstrData):
        msg = "【OnTelnetTest】"+bstrData
        # print(msg)
    def OnProxyStatus(self, bstrUserId, nCode):
        msg = "【OnProxyStatus】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.singals.log_sig.emit(msg)
    # 新版期貨智慧單(包含停損單、移動停損、二擇一、觸價單)被動回報查詢。透過呼叫GetStopLossReport後，資訊由該事件回傳。
    def OnStopLossReport(self, bstrData):
        msg = "【OnStopLossReport】" + bstrData
        print(msg)
    # 非同步委託結果。
    def OnAsyncOrder(self, nThreadID, nCode, bstrMessage):
        msg = "【OnAsyncOrder】" + str(nThreadID) + str(nCode) + bstrMessage
        print(msg)
    def OnOpenInterest(self, bstrData):
        msg = "【OnOpenInterest】" + bstrData
        print(msg) 
    # 國內期貨權益數。透過呼叫 GetFutureRights 後，資訊由該事件回傳
    def OnFutureRights(self, bstrData):
        msg = "【OnFutureRights】" + bstrData
        print(msg)

SKReplyEvent = SKReplyLibEvent()
SKReplyLibEventHandler = cc.GetEvents(skR, SKReplyEvent)
SKCenterEvent = SKCenterLibEvent()
SKCenterEventHandler = cc.GetEvents(skC, SKCenterEvent)
SKQuoteEvent = SKQuoteLibEvent()
SKQuoteEventHandler = cc.GetEvents(skQ, SKQuoteEvent)
SKOrderEvent = SKOrderLibEvent()
SKOrderLibEventHandler = cc.GetEvents(skO, SKOrderEvent)

Klines = []
queue = []
status=-1 # download is 1, dc is 2, ready is 3

class TradingBot(QObject): 
    def __init__(self, *args,**kwargs):
        super().__init__()
        self.signals = SignalManager.get_instance()
        nCode = skC.SKCenterLib_SetLogPath("CapitalLog")
        if nCode:
            msg = "【SetLogPath】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
        self.debug_state = False
        self.update_debug()
        self.signals.dc_sig.connect(self.quoteConnect)

    def run(self):
        if self.id:
            self.init()
        else:
            self.signals.log_sig.emit("Failed to initialize. Login first.")
        cnt = 0
        while True:
            time.sleep(0.1)
            cnt+=1
            if cnt==30:
                quoteDC()
                break

    def login(self, acc, passwd):
        nCode = skC.SKCenterLib_Login(acc, passwd)
        msg = "【Login】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        if nCode==0:
            self.id=acc
        self.signals.log_sig.emit(msg)
        return nCode
    
    def init(self):
        # This function should recreate Klines and basic TA such as ema and vwap
        # vwap refresh at 9 AM less than 9 means yesterday greater means today
        # if current time<9 we need history K 9~14 and history T 15~now
        # else if (9~23) two cases <15 histoty K 9~14 and 
        global df, vwap
        # while skQ.SKQuoteLib_IsConnected()==0:
        print("init quoteConnect")
        self.quoteConnect()
        df = self.requestKlines("TX00")
        now = datetime.datetime.now().time()
        if now>=datetime.time(15) or now<datetime.time(8,45):
        #     # 夜盤範圍 vwap用Klines跟history Tick組成
            vwap=df.between_time(datetime.time(9), datetime.time(13,45)).tail(20)
        #     # 日盤範圍 vwap用History Tick組成
            # ta.vwap()
        self.signals.log_sig.emit("Data initialization finished!")
        self.subtick("TX00")
        self.order_init(self.id)  


    def quoteConnect(self):
        while status!=3:
            nCode = skQ.SKQuoteLib_EnterMonitorLONG()
            msg = "【Quote_Connect】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)
            self.signals.log_sig.emit("Downloading Data...")
            cnt=0
            while status==1:
                time.sleep(0.1)
                cnt+=1
                if cnt>150:
                    self.signals.log_sig.emit("Time Out! Failed to connect.")
                    self.quoteDC()
                    time.sleep(3)
                    break
            
    def subtick(self, stockNo: str):
        # 8 o'clock
        if status!=3:
            self.quoteConnect()
        psPageNo, nCode = skQ.SKQuoteLib_RequestTicks(-1,stockNo)
        msg = "【SubscribeTicks】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
    
    def requestKlines(self, stockNo: str):
        # 4k data per call
        if status!=3:
            self.quoteConnect()
        now = datetime.datetime.now()
        since = now - pd.Timedelta(days=120)
        nCode = skQ.SKQuoteLib_RequestKLineAMByDate(stockNo,0,1,0,since.strftime("%Y%m%d"),now.strftime("%Y%m%d"),15)
        msg = "【RequestKLine】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)        
        try:
            while True:
                time.sleep(0.1)
                if nCode==0:
                    break
        except KeyboardInterrupt:            
            raise
        df = pd.DataFrame(Klines,columns=["Time","Open","High","Low","Close","Volume"]).set_index("Time").astype(float).astype(int)
        df.index = pd.to_datetime(df.index)
        df['EMA_78'] = df['Close'].ewm(span=78,adjust=False).mean().round().astype(int)
        print(df)
        df.to_csv(f"{stockNo}.csv")
        return df.tail(300)
        
    def order_init(self, id):
        nCode = skO.SKOrderLib_Initialize()
        msg = "【OrderLib_Init】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
        
        nCode = skO.ReadCertByID(id)
        msg = "【ReadCertByID】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        # nCode = skO.SKOrderLib_InitialProxyByID(self.id)
        # msg = "【Proxy_init】"+skC.SKCenterLib_GetReturnCodeMessage(nCode)
        # self.signals.log_sig.emit(msg)

        nCode = skO.GetUserAccount()
        msg = "【GetUserAccount】"+skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)

        nCode = skO.SKOrderLib_GetSpeedyType(id)
        if nCode == 0:
            msg = "一般線路"
        else:
            msg = "Speedy線路"
        msg = "【OrderLib_SpeedyType】" + msg
        self.signals.log_sig.emit(msg)  

        nCode = skO.SKOrderLib_TelnetTest()
        msg = "【OrderLib_TelnetTest】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
        self.signals.log_sig.emit(msg)
      
    def update_debug(self):
        nCode = skC.SKCenterLib_Debug(self.debug_state)
        if nCode:
            msg = "【SetDebug】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
            self.signals.log_sig.emit(msg)

def quoteDC():
    nCode = skQ.SKQuoteLib_LeaveMonitor()
    msg = "【Quote_DC】" + skC.SKCenterLib_GetReturnCodeMessage(nCode)
    print(msg)
    pythoncom.PumpWaitingMessages()
    while status!=2:
        pythoncom.PumpWaitingMessages()
        time.sleep(0.3)
    pythoncom.PumpWaitingMessages()
